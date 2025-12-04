
import pandas as pd
import sys

# file = f"/home/user/GoogleDrive/data/parquet/251204_crop_self_with_feed_korea.parquet"
file = f"/home/user/gdrive/data/parquet/251204_crop_self_with_feed_korea.parquet"

# ① Parquet 읽기
df = pd.read_parquet(file)

# ② 형식 변환할 컬럼 지정 (예: value, amount, price 등)
columns_to_float = ["데이터"]   # ← 변환할 컬럼명을 리스트로 넣어
for col in columns_to_float:
    df[col] = pd.to_numeric(df[col], errors="coerce")  # 실수 변환 + 실패값 NaN


# ③ 저장 (덮어쓰기)
df.to_parquet(file, index=False)

print("🚀 완료:", file)
print(df.dtypes)

