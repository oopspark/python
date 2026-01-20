import os
from datetime import datetime

import pandas as pd
import chardet
import polars as pl
from sqlalchemy import create_engine, text


# ======================================================
# 🔹 Polars 기본 설정 (멀티코어 & 출력)
# ======================================================
pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_rows(20)
pl.Config.set_fmt_str_lengths(10_000)
pl.Config.set_tbl_width_chars(10_000)
# 필요시 스레드 수 수동 설정도 가능:
# pl.Config.set_tbl_threads(os.cpu_count())


def detect_encoding(file_path: str, sample_size: int = 1_000_000) -> str:
    """
    파일 인코딩 자동 탐지 (처음 sample_size 바이트만 사용해서 메모리 절약)
    """
    with open(file_path, "rb") as f:
        raw = f.read(sample_size)
    return chardet.detect(raw)["encoding"] or "utf-8"


# ======================================================
# 🔹 CSV → Parquet (pandas, 소/중형용)
# ======================================================
def csv_to_parquet(csv_file: str, parquet_folder: str, focus: str) -> str:
    """
    CSV → Parquet 변환 (pandas 버전)
    - 전체 파일을 한 번에 메모리에 올림 (소/중형용)
    - Parquet 파일명: {focus}.parquet
    """
    os.makedirs(parquet_folder, exist_ok=True)
    parquet_file = os.path.join(parquet_folder, f"{focus}.parquet")

    encoding = detect_encoding(csv_file)
    df = pd.read_csv(csv_file, encoding=encoding, engine="python").convert_dtypes()
    df.to_parquet(parquet_file, index=False)

    print(f"🚀 [pandas] CSV → Parquet 변환 완료: {parquet_file}")
    print(pd.read_parquet(parquet_file).head(1))
    return parquet_file


# ======================================================
# 🔹 CSV → Parquet (Polars, 대용량/스트리밍용)  ✅ 추가
# ======================================================
def csv_to_parquet_polars(
    csv_file: str,
    parquet_folder: str,
    focus: str,
    encoding: str | None = None,
) -> str:
    """
    CSV → Parquet 변환 (Polars + Lazy + sink_parquet)
    - 대용량 CSV에 적합 (스트리밍/멀티코어)
    - 파일 전체를 메모리에 올리지 않음
    - Parquet 파일명: {focus}.parquet
    """
    os.makedirs(parquet_folder, exist_ok=True)
    parquet_file = os.path.join(parquet_folder, f"{focus}.parquet")

    # 🔴 Polars는 encoding으로 'utf8' 또는 'utf8-lossy'만 받음
    #    → 그냥 utf8-lossy로 고정하는 게 가장 안전/간단
    polars_encoding = "utf8-lossy"

    print(f"🚀 [polars] CSV → Parquet 변환 시작: {csv_file}")
    print(f"   · encoding = {polars_encoding}")
    print(f"   · output   = {parquet_file}")

    lf = pl.scan_csv(
        csv_file,
        encoding=polars_encoding,
        infer_schema_length=10_000,
        null_values=["", "NA", "NULL"],
        ignore_errors=True,
        truncate_ragged_lines=True,
    )

    lf.sink_parquet(
        parquet_file,
        compression="zstd",
        statistics=True,
    )

    df_head = pl.read_parquet(parquet_file).head(1)
    print(f"💾 [polars] CSV → Parquet 변환 완료: {parquet_file}")
    print(df_head)

    return parquet_file



# ======================================================
# 🔹 Parquet → DataFrame 로딩
# ======================================================
def load_parquet_as_df(parquet_file: str) -> pd.DataFrame:
    """
    Parquet 파일을 DataFrame으로 로딩 (pandas)
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
    DataFrame을 Parquet으로 저장 (pandas)
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
    - Parquet 파일명 = 테이블명
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
    focus = "260121_trade_matrix_faostat_vector"
    csv_file_name = "/home/user/다운로드/260121_trade_matrix_faostat.csv"
    parquet_folder = "/home/user/GoogleDrive/data/parquet"
    parquet_heavy_folder = "/home/user/GoogleDrive/data/parquet_heavy"

    postgres_uri = "postgresql+psycopg2://supersetuser:StrongPassword123!@localhost:5432/parquetsyncdb"

    # ----------------------------------
    # ✅ 대용량 CSV → Parquet (Polars 버전 권장)
    #    - heavy 폴더에 원본 전체 저장
    # ----------------------------------
    # csv_to_parquet_polars(
    #     csv_file=csv_file_name,
    #     parquet_folder=parquet_heavy_folder,
    #     focus=focus,
    #     # encoding=None 이면 detect_encoding 사용
    #     encoding=None,
    # )

    # 필요하면 pandas 버전도 유지:
    # csv_to_parquet(csv_file=csv_file_name, parquet_folder=parquet_folder, focus=focus)

    # ----------------------------------
    # 이후: Parquet ↔ PostgreSQL sync
    # ----------------------------------
    sync_parquet_and_postgres(parquet_folder=parquet_folder, postgres_uri=postgres_uri)


if __name__ == "__main__":
    main()
