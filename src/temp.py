import pandas as pd
from data_load_save import *

df = pd.read_csv("/home/user/문서/workspace/python/graph/data/콩_수급균형.csv")

df = df.sort_values("Year").reset_index(drop=True)

# 새 변수 생성
# df["Prod_minus_Supply"] = df["Production"] - df["Domestic supply quantity"]
df["Export_minus_Import"] = df["Export quantity"] - df["Import quantity"]

# 원하는 4개 컬럼 선택
df_mod = df[["Year", "Production", "Domestic supply quantity", "Export_minus_Import", "Stock Variation"]].copy()

# # 차분 + 첫행 제거
# df_diff = df_new.copy()
# df_diff[["Production", "Domestic supply quantity", "Export_minus_Import", "Stock Variation"]] = \
#     df_new[["Production", "Domestic supply quantity", "Export_minus_Import", "Stock Variation"]].diff()

# # 첫 행 제거
# df_diff = df_diff.iloc[1:].reset_index(drop=True)

# 🔥 정수 변환
df_mod = df_mod.astype({
    "Year": int,
    "Production": int,
    "Domestic supply quantity": int,
    "Export_minus_Import": int,
    "Stock Variation": int
})

print(df_mod)
save_df_to_csv(df_mod, "/home/user/문서/workspace/python/graph/data/콩_수급균형_modified.csv")
