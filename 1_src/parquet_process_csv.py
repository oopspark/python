from data_load_save import *
import pandas as pd

# ====== 파일 경로 설정 ======
parquet_path = "/home/user/GoogleDrive/data/parquet"
csv_path = "/home/user/문서/workspace/python/graph/data"

focus = "251204_crop_self_merged_korea"

parquet_file = f"{parquet_path}/{focus}.parquet"

# ====== Parquet 로드 ======
df = load_parquet_as_df(parquet_file)

print(df.head())






# save_df_to_csv(df, csv_path)
