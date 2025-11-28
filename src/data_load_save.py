import pandas as pd
import chardet
from sqlalchemy import create_engine, text
import os


def detect_encoding(file_path: str) -> str:
    """파일 인코딩 자동 탐지 후 반환"""
    with open(file_path, "rb") as f:
        return chardet.detect(f.read())["encoding"]


def load_csv_to_df(csv_file: str) -> pd.DataFrame:
    """CSV 파일을 DataFrame으로 반환 (dtype 변환 포함)"""
    encoding = detect_encoding(csv_file)
    df = pd.read_csv(csv_file, encoding=encoding, engine="python")
    df = df.convert_dtypes()
    return df

def save_df_to_csv(df: pd.DataFrame, csv_file: str):
    df.to_csv(csv_file, index=False, encoding="utf-8-sig")
    print(f"Saved to CSV: {csv_file}")
    print(pd.read_csv(csv_file).head(1))

def save_df_to_parquet(df: pd.DataFrame, parquet_file: str):
    """DataFrame을 Parquet으로 저장 + 저장 후 확인 출력"""
    df.to_parquet(parquet_file, index=False)
    print(f"Saved to: {parquet_file}")
    # 저장된 parquet 파일을 다시 읽어 1개 row를 출력
    print(pd.read_parquet(parquet_file).head(1))


def load_parquet_to_df(parquet_file: str) -> pd.DataFrame:
    """Parquet 파일을 읽어 DataFrame으로 반환"""
    df = pd.read_parquet(parquet_file)
    return df


def save_parquet_to_postgres(parquet_file: str, table_name: str, postgres_uri: str):
    """
    Parquet → PostgreSQL(supersetdb) 업로드
    """
    df = pd.read_parquet(parquet_file)
    engine = create_engine(postgres_uri)

    with engine.begin() as conn:
        df.to_sql(
            name=table_name,
            con=conn,
            index=False,
            if_exists="replace"   # 같은 테이블명 있으면 덮어쓰기
        )
    print(f"💾 Uploaded to PostgreSQL table: {table_name}")
    print(df.head(1))


def batch_parquet_to_csv(parquet_folder: str, csv_folder: str, overwrite: bool = True):
    """
    폴더 내 모든 파켓(.parquet) 파일을 CSV로 변환하여 저장

    Args:
        parquet_folder (str): 파켓 파일들이 있는 폴더 경로
        csv_folder (str): CSV 저장할 폴더 경로
        overwrite (bool): True = 기존 CSV 덮어쓰기, False = 존재 시 건너뛰기
    """
    os.makedirs(csv_folder, exist_ok=True)

    parquet_files = [f for f in os.listdir(parquet_folder) if f.lower().endswith(".parquet")]

    if not parquet_files:
        print("⚠ No parquet files found.")
        return

    for file in parquet_files:
        parquet_file = os.path.join(parquet_folder, file)
        csv_file = os.path.join(csv_folder, file.replace(".parquet", ".csv"))

        if not overwrite and os.path.exists(csv_file):
            print(f"⏩ Skip (exists): {csv_file}")
            continue

        df = pd.read_parquet(parquet_file)
        df.to_csv(csv_file, index=False, encoding="utf-8-sig")

        print(f"✅ Converted → {csv_file}")

from sqlalchemy import create_engine, text
import pandas as pd
import os


def sync_parquet_and_postgres(parquet_folder: str, postgres_uri: str):
    """
    parquet 폴더를 기준으로 PostgreSQL과 동기화 (mirror sync)
    - parquet에 있는 테이블은 없으면 생성
    - parquet에 없는 PostgreSQL 테이블은 삭제
    - Superset 시스템 테이블은 삭제 제외
    """

    engine = create_engine(postgres_uri)

    # parquet 폴더 파일명 → 테이블명
    parquet_files = [f for f in os.listdir(parquet_folder) if f.lower().endswith(".parquet")]
    parquet_tables = [os.path.splitext(f)[0] for f in parquet_files]

    # PostgreSQL public schema 테이블 목록
    with engine.connect() as conn:
        pg_tables = pd.read_sql(
            "SELECT tablename FROM pg_tables WHERE schemaname='public';",
            conn
        )["tablename"].tolist()

    # Superset이 사용하는 시스템 테이블 보호
    SYSTEM_TABLE_PREFIXES = [
        "ab_",   # Superset 권한/유저 관리
        "sl_",   # SQL Lab 로그 테이블
    ]
    SYSTEM_TABLES_EXACT = [
        "alembic_version",
        "logs",
    ]

    def is_system_table(table: str) -> bool:
        if table in SYSTEM_TABLES_EXACT:
            return True
        if any(table.startswith(prefix) for prefix in SYSTEM_TABLE_PREFIXES):
            return True
        return False

    # CREATE 필요 테이블
    to_create = [t for t in parquet_tables if t not in pg_tables]

    # DROP 필요 테이블
    to_drop = [t for t in pg_tables if t not in parquet_tables]

    # 🔹 CREATE missing tables
    for table in to_create:
        parquet_file = os.path.join(parquet_folder, f"{table}.parquet")
        df = pd.read_parquet(parquet_file)

        with engine.begin() as conn:
            df.to_sql(name=table, con=conn, index=False, if_exists="fail")

        print(f"🟢 CREATED → {table} ({len(df)} rows)")

    # 🔹 DROP tables not in parquet (system table 제외)
    for table in to_drop:
        if is_system_table(table):
            print(f"⛔ SKIP SYSTEM TABLE → {table}")
            continue

        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE;'))

        print(f"🔴 DROPPED → {table}")

    # 🔻 결과 요약
    print("\n===== Sync Summary =====")
    print(f"📂 Parquet tables ({len(parquet_tables)}): {parquet_tables}")
    print(f"🗄 PostgreSQL tables ({len(pg_tables)}): {pg_tables}")
    print(f"🟢 Created: {to_create}")
    print(f"🔴 Dropped (excluding system): {[t for t in to_drop if not is_system_table(t)]}")



def basic_path():
    print(
        """
doc_path = "/home/user/문서"

down_path = f"/home/user/다운로드"



parquet_path = "/home/user/GoogleDrive/data"
csv_path = f"{doc_path}/csv_data"

focus = "agrifood_elasticity_usa"  # 필요시 다른 키로 변경

raw_csv_file = f"{down_path}/raw_data/aaa.csv"

csv_file = f"{csv_path}/{focus}.csv"
parquet_file = f"{parquet_path}/{focus}.parquet"

postgres_uri = "postgresql+psycopg2://supersetuser:StrongPassword123!@localhost:5432/parquetsyncdb"
        """
    )


#############################################################################

###########################################################################

def main():


    focus = "crop_dry_korea"  # 필요시 다른 키로 변경

    raw_csv_file = f"/home/user/다운로드/작물별_재배면적(밭)_20251128225511.csv"

    csv_file = f"/home/user/문서/csv_data/{focus}.csv"
    parquet_file = f"/home/user/GoogleDrive/data/{focus}.parquet"

    # df = load_csv_to_df(raw_csv_file)
    # save_df_to_csv(df, csv_file)
    # save_df_to_parquet(df, parquet_file)


    parquet_folder = "/home/user/GoogleDrive/data"

    postgres_uri = "postgresql+psycopg2://supersetuser:StrongPassword123!@localhost:5432/parquetsyncdb"
    sync_parquet_and_postgres(parquet_folder, postgres_uri)


if __name__ == "__main__":
    main()