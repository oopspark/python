from data_load_save import *
import pandas as pd

csv_file = f"/home/user/문서/workspace/python/graph/data/주요_작목_소득비교_20251207074843.csv"
df = pd.read_csv(csv_file)

base_year = 2010  # 기준연도

# 기준연도 행 선택
base_row = df.loc[df["시점"] == base_year]

# "시점"을 제외한 모든 수치형 컬럼 선택
value_cols = [col for col in df.columns if col != "시점"]

# 각 작목을 기준연도 = 100으로 지수화
df_index = df.copy()
for col in value_cols:
    base_value = base_row[col].values[0]
    df_index[col] = (df[col] / base_value * 100).round(2)

print(df_index)

save_df_to_csv(df_index, "/home/user/문서/workspace/python/graph/data/주요작물소득_비율.csv")