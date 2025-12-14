import pandas as pd
import chardet
from sqlalchemy import create_engine, text
import os
from datetime import datetime


def detect_encoding(file_path: str) -> str:
    """파일 인코딩 자동 탐지"""
    with open(file_path, "rb") as f:
        return chardet.detect(f.read())["encoding"]


# ======================================================
# 🔹 CSV → Parquet 저장
# ======================================================
def csv_to_parquet(csv_file: str, parquet_folder: str, focus: str) -> str:
    """
    CSV → Parquet (temp) 변환
    - Parquet 파일명: {focus}_YYYYMMDD_temp.parquet
    - 처리 후 파일 경로 반환
    """
    parquet_file = os.path.join(parquet_folder, f"{focus}.parquet")

    encoding = detect_encoding(csv_file)
    df = pd.read_csv(csv_file, encoding=encoding, engine="python").convert_dtypes()
    df.to_parquet(parquet_file, index=False)

    print(f"🚀 CSV → Parquet 변환 완료: {parquet_file}")
    print(pd.read_parquet(parquet_file).head(1))
    return parquet_file


# ======================================================
# 🔹 Parquet → DataFrame 로딩
# ======================================================
def load_parquet_as_df(parquet_file: str) -> pd.DataFrame:
    """
    Parquet 파일을 DataFrame으로 로딩
    """
    if not os.path.exists(parquet_file):
        raise FileNotFoundError(f"❌ Parquet 파일 없음: {parquet_file}")

    df = pd.read_parquet(parquet_file)
    df = df.convert_dtypes()
    print(f"📥 Loaded DF from Parquet → {parquet_file} ({len(df)} rows)")
    return df


# ======================================================
# 🔹 DataFrame → Parquet 저장
# ======================================================
def save_df_to_parquet(df: pd.DataFrame, parquet_file: str):
    """
    DataFrame을 Parquet으로 저장
    """
    os.makedirs(os.path.dirname(parquet_file), exist_ok=True)
    df.to_parquet(parquet_file, index=False)
    print(f"💾 Saved DF → {parquet_file} ({len(df)} rows)")
    print(df.head(1))

# ======================================================
# 🔹 DataFrame → CSV 저장
# ======================================================
def save_df_to_csv(df: pd.DataFrame, csv_file: str, encoding: str = "utf-8-sig"):
    """
    DataFrame을 CSV로 저장
    """
    os.makedirs(os.path.dirname(csv_file), exist_ok=True)
    df.to_csv(csv_file, index=False, encoding=encoding)
    print(f"💾 Saved DF → {csv_file} ({len(df)} rows)")
    print(df.head(1))
    return csv_file




# ======================================================
# 🔹 Parquet ↔ PostgreSQL 동기화 (mirror sync)
# ======================================================
def sync_parquet_and_postgres(parquet_folder: str, postgres_uri: str):
    """
    parquet 폴더를 기준으로 PostgreSQL과 동기화 (mirror sync)
    """
    engine = create_engine(postgres_uri)
    parquet_files = [f for f in os.listdir(parquet_folder) if f.lower().endswith(".parquet")]
    parquet_tables = [os.path.splitext(f)[0] for f in parquet_files]

    with engine.connect() as conn:
        pg_tables = pd.read_sql(
            "SELECT tablename FROM pg_tables WHERE schemaname='public';", conn
        )["tablename"].tolist()

    to_create = [t for t in parquet_tables if t not in pg_tables]
    to_drop = [t for t in pg_tables if t not in parquet_tables]

    for table in to_create:
        parquet_file = os.path.join(parquet_folder, f"{table}.parquet")
        df = pd.read_parquet(parquet_file)
        with engine.begin() as conn:
            df.to_sql(name=table, con=conn, index=False, if_exists="fail")
        print(f"🟢 CREATED → {table} ({len(df)} rows)")

    for table in to_drop:
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE;'))
        print(f"🔴 DROPPED → {table}")

    print("\n===== Sync Summary =====")
    print(f"📂 Parquet tables ({len(parquet_tables)}): {parquet_tables}")
    print(f"🗄 PostgreSQL tables ({len(pg_tables)}): {pg_tables}")
    print(f"🟢 Created: {to_create}")
    print(f"🔴 Dropped: {to_drop}")





# ======================================================
#                    MAIN PIPELINE
# ======================================================
def main():
    focus = "faostat_item"
    csv_file_name = "/home/user/다운로드/faostat_item.csv"
    # parquet_folder = "/home/user/gdrive/data/parquet"
    parquet_folder = "/home/user/GoogleDrive/data/parquet"
    postgres_uri = "postgresql+psycopg2://supersetuser:StrongPassword123!@localhost:5432/parquetsyncdb"

    # 예: 필요 시 CSV → Parquet
    # csv_to_parquet(csv_file=csv_file_name, parquet_folder=parquet_folder, focus=focus)

    sync_parquet_and_postgres(parquet_folder=parquet_folder, postgres_uri=postgres_uri)


if __name__ == "__main__":
    main()
