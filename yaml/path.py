doc_path = "/home/user/문서"

down_path = f"/home/user/다운로드"



parquet_path = "/home/user/GoogleDrive/data"
csv_path = f"{doc_path}/csv_data"

focus = "agrifood_elasticity_usa"  # 필요시 다른 키로 변경

raw_csv_file = f"{down_path}/raw_data/aaa.csv"

csv_file = f"{csv_path}/{focus}.csv"
parquet_file = f"{parquet_path}/{focus}.parquet"

postgres_uri = "postgresql+psycopg2://supersetuser:StrongPassword123!@localhost:5432/parquetsyncdb"