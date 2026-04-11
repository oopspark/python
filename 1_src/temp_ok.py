import os
from dataclasses import dataclass
from typing import Dict

import numpy as np
import pandas as pd

# statsmodels는 있으면 회귀까지, 없으면 지표 계산까지만
try:
    import statsmodels.api as sm
    HAS_STATSMODELS = True
except ImportError:
    HAS_STATSMODELS = False


# ======================================================
# 🔹 설정: 품목 이름 매핑 (FAOSTAT item → 4차원 벡터 차원)
# ======================================================

# canonical commodity names (벡터 순서)
COMMODITY_ORDER = ["wheat", "soy", "maize", "rice"]

# raw FAOSTAT item 이름을 canonical 이름으로 매핑
ITEM_TO_DIM = {
    # 밀
    "Wheat": "wheat",
    "Wheat and products": "wheat",

    # 콩
    "Soya beans": "soy",
    "Soybeans": "soy",
    "Soya beans and products": "soy",

    # 옥수수
    "Maize (corn)": "maize",
    "Maize": "maize",
    "Maize and products": "maize",

    # 쌀
    "Rice, paddy": "rice",
    "Rice": "rice",
    "Rice and products": "rice",
}


# ======================================================
# 🔹 1. Parquet 로드 & 벡터 테이블 구성
# ======================================================

def load_trade_parquet(parquet_path: str) -> pd.DataFrame:
    """
    Parquet 파일을 로드하고 dtype 정리.
    필수 컬럼:
        importer, exporter, item, element, year, value
    """
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"Parquet 파일을 찾을 수 없음: {parquet_path}")

    df = pd.read_parquet(parquet_path)
    df = df.convert_dtypes()

    required_cols = ["importer", "exporter", "item", "element", "year", "value"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"필수 컬럼 누락: {missing}")

    return df


def build_commodity_vectors(df: pd.DataFrame) -> Dict[int, pd.DataFrame]:
    """
    (importer, exporter, year)별로 [wheat, soy, maize, rice] 벡터 생성.

    반환:
        year_to_vectors: dict[year -> DataFrame]
        각 DataFrame:
            index: (importer, exporter)
            columns: ['wheat', 'soy', 'maize', 'rice']
    """
    df = df.copy()

    # 1) 수입량만 사용
    df = df[df["element"] == "Import quantity"].copy()

    # 2) 관심 품목만 필터링
    df["dim"] = df["item"].map(ITEM_TO_DIM)
    df = df[~df["dim"].isna()].copy()

    # 3) 타입 정리
    df["year"] = df["year"].astype(int)
    df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0.0)

    # 4) 그룹핑 & 피벗
    g = (
        df.groupby(["year", "importer", "exporter", "dim"], observed=True)["value"]
          .sum()
          .reset_index()
    )

    # 5) 연도별로 나누어서 (importer, exporter) × commodity pivot
    year_to_vectors: Dict[int, pd.DataFrame] = {}
    for y, df_y in g.groupby("year", observed=True):
        pivot = df_y.pivot_table(
            index=["importer", "exporter"],
            columns="dim",
            values="value",
            aggfunc="sum",
            fill_value=0.0,
        )

        # COMMODITY_ORDER 순서로 맞춰두기
        for c in COMMODITY_ORDER:
            if c not in pivot.columns:
                pivot[c] = 0.0
        pivot = pivot[COMMODITY_ORDER].sort_index()

        year_to_vectors[int(y)] = pivot

    return dict(sorted(year_to_vectors.items()))


# ======================================================
# 🔹 2. 연도별 comp / scale 지표 계산
# ======================================================

@dataclass
class YearlyChangeResult:
    # distances:
    #   index: year
    #   columns: prev_year, D_comp, D_comp_sq, D_scale, D_scale_sq, n_links
    distances: pd.DataFrame
    # link_changes:
    #   year -> DataFrame with per-link metrics (comp + scale)
    link_changes: Dict[int, pd.DataFrame]


def _compute_link_changes_for_pair(
    prev_vectors: pd.DataFrame,
    cur_vectors: pd.DataFrame,
    year_prev: int,
    year_cur: int,
) -> pd.DataFrame:
    """
    전년도(prev)와 당해(cur)의 (i,j) 벡터를 align시킨 뒤,
    각 링크별:
        - 구성 변화: m_comp = scale * theta
        - 규모 변화: m_scale = 0.5 * | ||x_cur|| - ||x_prev|| |

    여기서
        scale = (||x_prev|| + ||x_cur||) / 2.
    """
    # Outer join으로 (i,j) 전체 집합 align
    prev = prev_vectors.copy()
    cur = cur_vectors.copy()

    prev_reset = prev.reset_index()
    cur_reset = cur.reset_index()

    merged = prev_reset.merge(
        cur_reset,
        on=["importer", "exporter"],
        how="outer",
        suffixes=("_prev", "_cur"),
    ).fillna(0.0)

    # 벡터 추출
    prev_cols = [f"{c}_prev" for c in COMMODITY_ORDER]
    cur_cols = [f"{c}_cur" for c in COMMODITY_ORDER]

    X_prev = merged[prev_cols].to_numpy(dtype=float)
    X_cur = merged[cur_cols].to_numpy(dtype=float)

    # 노름 계산
    norm_prev = np.linalg.norm(X_prev, axis=1)
    norm_cur = np.linalg.norm(X_cur, axis=1)

    # dot / angle 계산
    dot = np.einsum("ij,ij->i", X_prev, X_cur)
    with np.errstate(divide="ignore", invalid="ignore"):
        denom = norm_prev * norm_cur
        # denom <= 0 이면 cosθ = 1 → θ = 0 (구성변화 없음으로 처리)
        cos_theta = np.where(denom > 0, dot / denom, 1.0)

    # 수치 오차 보정
    cos_theta = np.clip(cos_theta, -1.0, 1.0)
    theta = np.arccos(cos_theta)  # [0, π], norm이 0인 경우는 0으로 처리됨

    # 규모(scale): 전년도와 당해년도 노름의 산술평균
    scale = 0.5 * (norm_prev + norm_cur)

    # 🔹 구성 변화량: m_comp = scale * theta
    m_comp = scale * theta

    # 🔹 규모 변화량: m_scale = 0.5 * |norm_cur - norm_prev|
    #     → 구성 지표와 규모 지표의 크기 스케일을 어느 정도 맞추기 위한 rescaling
    m_scale = 0.5 * np.abs(norm_cur - norm_prev)

    # 제곱 및 기여도 (composition / scale 각각)
    comp_sq = m_comp ** 2
    scale_sq = m_scale ** 2

    total_comp_sq = comp_sq.sum()
    total_scale_sq = scale_sq.sum()

    share_comp = comp_sq / total_comp_sq if total_comp_sq > 0 else np.zeros_like(comp_sq)
    share_scale = scale_sq / total_scale_sq if total_scale_sq > 0 else np.zeros_like(scale_sq)

    # 결과 DataFrame
    out = merged[["importer", "exporter"] + prev_cols + cur_cols].copy()
    out["prev_year"] = year_prev
    out["year"] = year_cur
    out["norm_prev"] = norm_prev
    out["norm_cur"] = norm_cur
    out["theta"] = theta
    out["scale"] = scale
    out["m_comp"] = m_comp
    out["m_scale"] = m_scale
    out["comp_sq"] = comp_sq
    out["scale_sq"] = scale_sq
    out["share_comp"] = share_comp
    out["share_scale"] = share_scale

    return out


def compute_yearly_changes(
    year_to_vectors: Dict[int, pd.DataFrame]
) -> YearlyChangeResult:
    """
    연도별 글로벌 구조변화 인덱스를 계산하고,
    각 연도마다 링크별 변화량 및 기여도를 저장.

    distances:
        year, prev_year,
        D_comp, D_comp_sq,
        D_scale, D_scale_sq,
        n_links
    """
    years = sorted(year_to_vectors.keys())
    rows = []
    link_changes: Dict[int, pd.DataFrame] = {}

    for prev, cur in zip(years[:-1], years[1:]):
        prev_vec = year_to_vectors[prev]
        cur_vec = year_to_vectors[cur]

        changes_df = _compute_link_changes_for_pair(prev_vec, cur_vec, prev, cur)
        link_changes[cur] = changes_df

        D_comp_sq = changes_df["comp_sq"].sum()
        D_comp = np.sqrt(D_comp_sq)

        D_scale_sq = changes_df["scale_sq"].sum()
        D_scale = np.sqrt(D_scale_sq)

        n_links = len(changes_df)

        rows.append({
            "year": cur,
            "prev_year": prev,
            "D_comp": D_comp,
            "D_comp_sq": D_comp_sq,
            "D_scale": D_scale,
            "D_scale_sq": D_scale_sq,
            "n_links": n_links,
        })

    distances = pd.DataFrame(rows).set_index("year")
    return YearlyChangeResult(distances=distances, link_changes=link_changes)


# ======================================================
# 🔹 3. Top N 링크 기여도 뽑기 (comp / scale)
# ======================================================

def get_top_contributing_links(
    yearly_result: YearlyChangeResult,
    year: int,
    top_n: int = 10,
    metric: str = "comp",
) -> pd.DataFrame:
    """
    특정 year (prev_year → year) 구조변화에 가장 크게 기여한 링크 Top N 추출.

    metric:
        - "comp": 구성 기반 기여도 (share_comp)
        - "scale": 규모 기반 기여도 (share_scale)
    """
    if year not in yearly_result.link_changes:
        raise ValueError(f"{year}에 대한 링크 변화 데이터가 없습니다.")

    df = yearly_result.link_changes[year].copy()

    if metric == "comp":
        sort_col = "share_comp"
    elif metric == "scale":
        sort_col = "share_scale"
    else:
        raise ValueError("metric은 'comp' 또는 'scale'이어야 합니다.")

    df = df.sort_values(sort_col, ascending=False).head(top_n)
    return df


# ======================================================
# 🔹 4. (선택) 이벤트 더미 회귀: D_t 시계열 통계 처리
# ======================================================

def run_event_dummy_regression(
    distances: pd.DataFrame,
    trade_war_start: int = 2018,
    ru_ua_start: int = 2022,
    metric: str = "comp",
):
    """
    log(D_t)를 종속변수로, 이벤트 더미를 설명변수로 하는 회귀.
    metric:
        - "comp": D_comp 사용
        - "scale": D_scale 사용
    statsmodels가 설치되지 않았으면 None 반환.
    """
    if not HAS_STATSMODELS:
        print("⚠ statsmodels가 설치되어 있지 않아 회귀는 생략합니다.")
        return None

    df = distances.reset_index().copy()  # year 컬럼 살리기

    if metric == "comp":
        D_col = "D_comp"
        label = "composition-based"
    elif metric == "scale":
        D_col = "D_scale"
        label = "scale-based"
    else:
        raise ValueError("metric은 'comp' 또는 'scale'이어야 합니다.")

    df = df[df[D_col] > 0].copy()
    df["logD"] = np.log(df[D_col])

    # 간단한 time trend
    df["trend"] = np.arange(len(df))

    years = df["year"]
    df["TradeWar"] = (years >= trade_war_start).astype(int)
    df["RUUA"] = (years >= ru_ua_start).astype(int)

    X = df[["trend", "TradeWar", "RUUA"]]
    X = sm.add_constant(X)
    y = df["logD"]

    print(f"\n=== OLS 회귀 (metric={label}) ===")
    model = sm.OLS(y, X).fit(cov_type="HAC", cov_kwds={"maxlags": 1})
    print(model.summary())
    return model


# ======================================================
# 🔹 5. 결과를 CSV로 저장하는 함수
# ======================================================

def export_results_to_csv(
    yearly_result: YearlyChangeResult,
    out_dir: str,
    top_n: int = 10,
    prefix: str = "grain_network",
):
    """
    연도별 Frobenius 놈(D_comp, D_scale)과
    연도별 Top N 링크 기여도를 CSV로 저장.

    생성되는 파일:
      - {out_dir}/{prefix}_distances.csv
         (D_comp, D_scale 모두 포함)
      - {out_dir}/{prefix}_top{top_n}_comp.csv
         (구성 기반 top N 링크)
      - {out_dir}/{prefix}_top{top_n}_scale.csv
         (규모 기반 top N 링크)
    """
    os.makedirs(out_dir, exist_ok=True)

    # 1) 연도별 거리 요약
    distances_df = yearly_result.distances.reset_index()  # year 컬럼 살리기
    dist_path = os.path.join(out_dir, f"{prefix}_distances.csv")
    distances_df.to_csv(dist_path, index=False, encoding="utf-8-sig")
    print(f"💾 Saved yearly distances → {dist_path}")

    # 2) 연도별 Top N 링크 기여도 (composition / scale 각각 별도 파일)
    for metric in ["comp", "scale"]:
        top_rows = []
        sort_col = "share_comp" if metric == "comp" else "share_scale"

        for year, df_links in sorted(yearly_result.link_changes.items()):
            df_sorted = df_links.sort_values(sort_col, ascending=False).head(top_n).copy()
            df_sorted["rank"] = np.arange(1, len(df_sorted) + 1)
            df_sorted["metric"] = metric  # 어떤 지표 기준인지 표시
            top_rows.append(df_sorted)

        if top_rows:
            top_df = pd.concat(top_rows, ignore_index=True)
            top_path = os.path.join(out_dir, f"{prefix}_top{top_n}_{metric}.csv")
            top_df.to_csv(top_path, index=False, encoding="utf-8-sig")
            print(f"💾 Saved top-{top_n} contributing links ({metric}) → {top_path}")
        else:
            print(f"⚠ link_changes가 비어 있어 top-N 링크 CSV (metric={metric})는 생성되지 않았습니다.")


# ======================================================
# 🔹 6. 전체 파이프라인 예시
# ======================================================

def run_full_analysis(parquet_file: str, top_n: int = 10, out_dir: str | None = None):
    """
    1) Parquet 로드
    2) (importer, exporter, year) → 4차원 곡물 벡터 구성
    3) 연도별 구조변화 지표 D_comp, D_scale 계산
    4) 마지막 연도 기준 Top N 링크 기여도 출력
    5) (옵션) 이벤트 더미 회귀 실행
    6) (옵션) 결과를 CSV로 저장
    """
    print(f"📂 Parquet 로드: {parquet_file}")
    df = load_trade_parquet(parquet_file)

    print("🔧 곡물 벡터 구성 중...")
    year_to_vectors = build_commodity_vectors(df)
    print(f"   · 연도 수: {len(year_to_vectors)}")
    print(f"   · 예시 연도: {list(year_to_vectors.keys())[:5]}")

    print("📏 연도별 구조변화 지표 계산 중...")
    yearly_result = compute_yearly_changes(year_to_vectors)
    print("\n=== 연도별 글로벌 구조변화 인덱스 (D_comp, D_scale) ===")
    print(yearly_result.distances.head())

    # 마지막 연도 기준 Top N 링크
    if yearly_result.distances.shape[0] > 0:
        last_year = yearly_result.distances.index.max()
        prev_year = yearly_result.distances.loc[last_year, "prev_year"]
        print(f"\n🔥 {last_year}년 (prev_year={prev_year})")

        print(f"   구성 변화( composition ) 기준 Top {top_n} 링크:")
        top_links_comp = get_top_contributing_links(
            yearly_result, year=last_year, top_n=top_n, metric="comp"
        )
        print(top_links_comp[[
            "importer", "exporter", "norm_prev", "norm_cur",
            "theta", "scale", "m_comp", "share_comp"
        ]])

        print(f"\n   규모 변화( scale ) 기준 Top {top_n} 링크:")
        top_links_scale = get_top_contributing_links(
            yearly_result, year=last_year, top_n=top_n, metric="scale"
        )
        print(top_links_scale[[
            "importer", "exporter", "norm_prev", "norm_cur",
            "m_scale", "share_scale"
        ]])

    # (옵션) 이벤트 더미 회귀
    if yearly_result.distances.shape[0] >= 5 and HAS_STATSMODELS:
        print("\n📈 이벤트 더미 회귀 (log D_comp ~ trend + TradeWar + RUUA)")
        run_event_dummy_regression(yearly_result.distances, metric="comp")

        print("\n📈 이벤트 더미 회귀 (log D_scale ~ trend + TradeWar + RUUA)")
        run_event_dummy_regression(yearly_result.distances, metric="scale")
    elif yearly_result.distances.shape[0] < 5:
        print("\n⚠ 연도 수가 적어서 회귀는 생략합니다.")
    else:
        print("\n⚠ statsmodels가 없어 회귀는 생략합니다.")

    # ✅ 결과를 CSV로 저장
    if out_dir is not None:
        export_results_to_csv(
            yearly_result,
            out_dir=out_dir,
            top_n=top_n,
            prefix="grain_network",
        )

    return yearly_result


if __name__ == "__main__":
    parquet_path = r"G:\내 드라이브\data\parquet\260121_trade_faostat_grains_import_vector.parquet"
    download_dir = r"C:\Users\parkj\Documents\workspace\python\temp"

    run_full_analysis(
        parquet_file=parquet_path,
        top_n=10,
        out_dir=download_dir,
    )
