import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from parts.relation_tensor import build_filter_tensor, valid_candidates


def select_axis_columns(df: pd.DataFrame):
    cols = df.columns.tolist()
    st.subheader("📌 그래프 축 설정")
    left, right = st.columns(2)

    with left:
        data_col = st.selectbox("Y(데이터) 컬럼", cols)

    with right:
        x_candidates = [c for c in cols if c != data_col]
        x_col = st.selectbox("X(가로축) 컬럼", x_candidates)

    return data_col, x_col


def init_filter_states(filter_cols):
    if "selected" not in st.session_state or \
       set(st.session_state.selected.keys()) != set(filter_cols):
        st.session_state.selected = {col: None for col in filter_cols}
    return st.session_state.selected


def ui_filter_dropdowns(filter_cols, candidates, selected):
    st.subheader("🔽 필터 조건 선택 (선택 안 해도 됨)")
    for col in filter_cols:
        options = [None] + candidates[col]
        index = options.index(selected[col]) if selected[col] in options else 0
        selected[col] = st.selectbox(col, options, index=index)
    return selected


def apply_filter(df, selected):
    mask = pd.Series(True, index=df.index)
    for col, val in selected.items():
        if val is not None:
            mask &= (df[col] == val)
    return df[mask]


def draw_graph(df, x_col, data_col):
    st.subheader("📈 그래프")
    if df.empty:
        st.warning("조건에 맞는 데이터가 없습니다.")
        return
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(df[x_col], df[data_col], marker="o")
    ax.set_xlabel(x_col)
    ax.set_ylabel(data_col)
    st.pyplot(fig)


def graph_display(df: pd.DataFrame):
    data_col, x_col = select_axis_columns(df)

    # 필터 컬럼 = 전체 컬럼에서 Y/X 축 제외한 나머지
    tensor, filter_cols, uniques, index_maps = build_filter_tensor(df, data_col, x_col)

    selected = init_filter_states(filter_cols)
    candidates = valid_candidates(tensor, filter_cols, uniques, index_maps, selected)
    selected = ui_filter_dropdowns(filter_cols, candidates, selected)

    filtered_df = apply_filter(df, selected)
    draw_graph(filtered_df, x_col, data_col)


