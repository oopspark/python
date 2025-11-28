import numpy as np
import pandas as pd

def build_filter_tensor(df: pd.DataFrame, data_col: str, x_col: str):
    """
    data_col과 x_col을 제외한 모든 컬럼을 필터 컬럼으로 사용하여 텐서를 생성.
    반환:
      tensor        : n차원 필터 가능성 텐서
      filter_cols   : 필터에 사용되는 컬럼 목록
      uniques       : 각 필터 컬럼의 고유값 dict
      index_maps    : 고유값 → 인덱스 변환 dict
    """
    # 필터 컬럼 = 전체 컬럼 - (데이터 컬럼 + X축 컬럼)
    filter_cols = [c for c in df.columns if c not in [data_col, x_col]]

    uniques = {c: df[c].astype(str).unique().tolist() for c in filter_cols}
    index_maps = {c: {v: i for i, v in enumerate(uniques[c])} for c in filter_cols}

    shape = tuple(len(uniques[c]) for c in filter_cols)
    tensor = np.zeros(shape, dtype=np.int8)

    for _, row in df.iterrows():
        pos = tuple(index_maps[c][str(row[c])] for c in filter_cols)
        tensor[pos] = 1

    return tensor, filter_cols, uniques, index_maps

def valid_candidates(tensor, filter_cols, uniques, index_maps, selected: dict):
    mask = tensor
    for col, val in selected.items():
        if val is None:
            continue
        axis = filter_cols.index(col)
        idx = index_maps[col][val]
        mask = np.take(mask, idx, axis=axis)

    candidates = {}
    for col in filter_cols:
        axis = filter_cols.index(col)
        sums = np.sum(mask, axis=tuple(i for i in range(mask.ndim) if i != axis))
        candidates[col] = [u for u, s in zip(uniques[col], sums) if s > 0]
    return candidates



