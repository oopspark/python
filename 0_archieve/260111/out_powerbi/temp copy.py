import pandas as pd
from pathlib import Path

def save_missing_rows(in_path: str, out_path: str | None = None):
    in_path = Path(in_path)
    if out_path is None:
        out_path = in_path.with_name(in_path.stem + "_missing_rows.csv")

    df = pd.read_csv(
        in_path,
        sep=None,
        engine="python",
        encoding="utf-8-sig",
        na_values=["", " ", "NA", "N/A", "null", "None"]
    )

    # 결측치가 있는 행만 필터
    mask = df.isna().any(axis=1)
    missing_df = df.loc[mask].copy()

    missing_df.to_csv(out_path, index=False, encoding="utf-8-sig")

    # 간단 리포트
    print(f"\n=== {in_path.name} ===")
    print(f"rows={len(df):,}  missing_rows={len(missing_df):,}  saved -> {out_path}")
    if len(missing_df) > 0:
        cols = df.columns[df.isna().any()].tolist()
        print("columns_with_missing:", cols)

# ✅ 여기에 파일들 넣기
files = [
    "/home/user/문서/workspace/python/out_powerbi/UN_tourism_arrival_monthly_with_m49.csv"
]

for f in files:
    try:
        save_missing_rows(f)
    except FileNotFoundError:
        print(f"\n❌ 파일 없음: {f} (경로/파일명 확인)")
