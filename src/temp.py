import pandas as pd
from data_load_save import *

df1 = pd.read_csv("/home/user/문서/workspace/python/graph/data/콩_10아르당소득_비율.csv")
df2 = pd.read_csv("/home/user/문서/workspace/python/graph/data/총수입_비율.csv")

df_merged = pd.merge(df1, df2, on="시점", how="outer")


print(df_merged)

df_merged = df_merged.rename(columns={
    "지수(=100×비율)_x": "소득 지수",
    "지수(=100×비율)_y": "총수입 지수"
})



save_df_to_csv(df_merged, "/home/user/문서/workspace/python/graph/data/소득_총수입_비율.csv")
