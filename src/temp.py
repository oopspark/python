import pandas as pd
import csv   # ← 이거 반드시 필요

df = pd.read_csv(
    "/home/user/문서/workspace/python/src/out_hhi_compare_major_importers.csv"
)

df["top_partners"] = (
    df["top_partners"]
    .astype(str)
    .str.replace("%", "", regex=False)
    .str.strip()
)

df.to_csv(
    "output_no_percent.csv",
    index=False,
    quoting=csv.QUOTE_ALL
)
