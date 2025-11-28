import streamlit as st
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl

# ─────────────────────────────────────────
# 🔤 한글 폰트 깨짐 방지
mpl.rcParams["font.family"] = "NanumGothic"
mpl.rcParams["axes.unicode_minus"] = False

# 🔥 전체 레이아웃을 widescreen으로 확장
st.set_page_config(layout="wide")


# ─────────────────────────────────────────
# 📌 parquet 파일 선택
def file_select():
    DATA_DIR = "/home/user/GoogleDrive/data/"

    files = [f[:-8] for f in os.listdir(DATA_DIR) if f.endswith(".parquet")]
    file_select = st.selectbox("📌 데이터 선택", files)

    df = pd.read_parquet(os.path.join(DATA_DIR, file_select + ".parquet"))
    st.success(f"✔ 데이터 로드 완료: {file_select}")
    return df


# ─────────────────────────────────────────
# 📌 X/Y축 선택
def select_axis_columns(df):
    st.subheader("📌 그래프 축 설정")
    cols = df.columns.tolist()

    left, right = st.columns(2)
    with left:
        data_col = st.selectbox("Y축(데이터) 컬럼", cols)

    with right:
        x_candidates = [c for c in cols if c != data_col]
        x_col = st.selectbox("X축(가로축) 컬럼", x_candidates)

    return data_col, x_col


# ─────────────────────────────────────────
# 📌 조합 기반 필터 구조 생성
def build_combos(df, filter_cols):
    combos = []
    for _, row in df[filter_cols].iterrows():
        combos.append({col: row[col] for col in filter_cols})
    return tuple(combos)


# 후보값 계산
def update_candidates(combos, filter_cols, selected):
    filtered = []
    for combo in combos:
        ok = True
        for col, val in selected.items():
            if val is not None and combo[col] != val:
                ok = False
                break
        if ok:
            filtered.append(combo)

    candidates = {col: set() for col in filter_cols}
    for combo in filtered:
        for col in filter_cols:
            candidates[col].add(combo[col])

    return {col: sorted(list(v)) for col, v in candidates.items()}


# 필터 UI
def ui_filter_area(df, data_col, x_col):
    filter_cols = [c for c in df.columns if c not in [data_col, x_col]]

    # 세션 상태 초기화
    if "selected" not in st.session_state or \
       set(st.session_state.selected.keys()) != set(filter_cols):
        st.session_state.selected = {col: None for col in filter_cols}

    selected = st.session_state.selected

    # 조합 기반 후보값 생성
    combos = build_combos(df, filter_cols)
    candidates = update_candidates(combos, filter_cols, selected)

    st.subheader("🔽 필터 조건 선택 (선택 안 해도 됨)")

    # ---- 🔥 한 줄에 가능한 많이 배치 ----
    cols_per_row = 6   # 한 줄에 최대 6개까지 표시(원하는 만큼 조절 가능)
    rows = (len(filter_cols) + cols_per_row - 1) // cols_per_row

    idx = 0
    for _ in range(rows):
        row_cols = st.columns(cols_per_row)
        for col_box in row_cols:
            if idx >= len(filter_cols):
                break
            col = filter_cols[idx]
            options = [None] + candidates[col]
            default = options.index(selected[col]) if selected[col] in options else 0
            with col_box:
                selected[col] = st.selectbox(col, options, index=default)
            idx += 1

    return selected


# 필터 적용
def apply_filter(df, selected):
    mask = pd.Series(True, index=df.index)
    for col, val in selected.items():
        if val is not None:
            mask &= (df[col] == val)
    return df[mask]


# 그래프
def draw_graph(df, x_col, data_col, container):
    if df.empty:
        container.warning("⚠ 조건에 맞는 데이터가 없습니다.")
        return

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(df[x_col], df[data_col], linewidth=2)  # 선그래프 / 마커 없음
    ax.set_xlabel(x_col)
    ax.set_ylabel(data_col)
    ax.set_title(f"{data_col} / {x_col}")
    container.pyplot(fig)


# ─────────────────────────────────────────
# ⭐ 전체 실행
def main():
    st.title("📊 데이터 탐색 시각화 (드롭다운 필터 기반) — Wide Layout")

    df = file_select()
    data_col, x_col = select_axis_columns(df)
    selected = ui_filter_area(df, data_col, x_col)
    filtered_df = apply_filter(df, selected)

    # ←→ 좌우 배치
    left, right = st.columns([1.2, 1])  # 왼쪽이 표, 오른쪽이 그래프

    with left:
        st.subheader("📋 필터링된 데이터")
        st.dataframe(filtered_df, use_container_width=True)

    with right:
        st.subheader("📈 시각화")
        draw_graph(filtered_df, x_col, data_col, right)


# ─────────────────────────────────────────
if __name__ == "__main__":
    main()
