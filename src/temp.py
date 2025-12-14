import pandas as pd
from pathlib import Path

DATA_DIR = Path("/home/user/문서/workspace/python/graph/data/temp/a")   # CSV들이 있는 폴더
OUTPUT = "powerbi_all_years.csv"

# csv 파일 전부 읽기 (연도순 정렬)
csv_files = sorted(DATA_DIR.glob("*.csv"))

dfs = []
for f in csv_files:
    df = pd.read_csv(f)
    dfs.append(df)

# 세로로 이어 붙이기
df_all = pd.concat(dfs, ignore_index=True)

df_all.to_csv(OUTPUT, index=False)

print(f"✅ {len(csv_files)} files merged → {OUTPUT}")
