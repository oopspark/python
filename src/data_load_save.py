import pandas as pd
import chardet
from sqlalchemy import create_engine, text
import os
from datetime import datetime


def detect_encoding(file_path: str) -> str:
    """파일 인코딩 자동 탐지"""
    with open(file_path, "rb") as f:
        return chardet.detect(f.read())["encoding"]


def csv_to_parquet_temp(csv_file: str, parquet_temp_folder: str, focus: str) -> str:
    """
    CSV → Parquet (temp) 변환
    - Parquet 파일명: {focus}_YYYYMMDD_temp.parquet
    - 처리 후 파일 경로 반환
    """
    from datetime import datetime

    os.makedirs(parquet_temp_folder, exist_ok=True)

    date_str = datetime.now().strftime("%Y%m%d")
    parquet_file = os.path.join(parquet_temp_folder, f"{focus}_{date_str}_temp.parquet")

    encoding = detect_encoding(csv_file)
    df = pd.read_csv(csv_file, encoding=encoding, engine="python")
    df = df.convert_dtypes()
    df.to_parquet(parquet_file, index=False)

    print(f"🚀 CSV → Parquet 변환 완료: {parquet_file}")
    print(pd.read_parquet(parquet_file).head(1))




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

def move_temp_to_final(parquet_temp_folder: str, parquet_data_folder: str, focus: str) -> str:
    """
    temp 폴더에 있는 parquet 파일 중 focus에 완전히 매칭되는 파일을
    data 폴더로 이동 (파일명에서 _temp 제거)
    - 예: focus = "crop_20250112"
      crop_20250112_temp.parquet → crop_20250112.parquet
    """
    os.makedirs(parquet_data_folder, exist_ok=True)

    target_name = f"{focus}_temp.parquet"
    source_file = os.path.join(parquet_temp_folder, target_name)

    if not os.path.exists(source_file):
        raise FileNotFoundError(f"⚠ 해당 파일을 찾을 수 없습니다: {source_file}")

    dest_file = os.path.join(parquet_data_folder, f"{focus}.parquet")

    os.rename(source_file, dest_file)
    print(f"📦 parquet 이동 완료 → {dest_file}")

    return dest_file



# ======================================================
#                    MAIN PIPELINE
# ======================================================

def main():
    focus = "crop_dry_korea_20250215"   # 날짜 포함된 focus
    csv_file_name = "/home/user/다운로드/Production_Crops_Livestock_E_All_Data_(Normalized)"
    parquet_folder = "/home/user/gdrive"
    parquet_temp_folder = f"{parquet_folder}/data/temp"
    parquet_data_folder = f"{parquet_folder}/data/parquet"

    postgres_uri = "postgresql+psycopg2://supersetuser:StrongPassword123!@localhost:5432/parquetsyncdb"

    # csv_to_parquet_temp(csv_file_name, parquet_temp_folder, focus)

    # final_parquet_file = move_temp_to_final(
    #     parquet_temp_folder, parquet_data_folder, focus
    # )

    # print("\n📌 최종 Parquet 저장 위치:", final_parquet_file)

    sync_parquet_and_postgres(parquet_folder = parquet_data_folder, postgres_uri=postgres_uri)


if __name__ == "__main__":
    main()
