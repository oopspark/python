import pandas as pd
import yaml
import chardet
from sqlalchemy import create_engine

def detect_encoding(file_path: str) -> str:
    """파일 인코딩 자동 탐지 후 반환"""
    with open(file_path, "rb") as f:
        return chardet.detect(f.read())["encoding"]


def load_yaml(yaml_path: str, yaml_name: str) -> dict:
    """YAML 파일 읽어 dict 반환"""
    with open(f"{yaml_path}/{yaml_name}", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_raw_to_df(raw_file: str) -> pd.DataFrame:
    """CSV 파일을 DataFrame으로 반환 (dtype 변환 포함)"""
    encoding = detect_encoding(raw_file)
    df = pd.read_csv(raw_file, encoding=encoding, engine="python")
    df = df.convert_dtypes()
    return df


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



def save_df_to_postgres(df: pd.DataFrame, table_name: str, postgres_uri: str):
    """DataFrame을 PostgreSQL 테이블에 저장"""
    engine = create_engine(postgres_uri)

    with engine.begin() as conn:
        df.to_sql(
            table_name,
            conn,
            index=False,
            if_exists="replace",   # 필요에 따라 append 가능
            method="multi"
        )

    print(f"Uploaded to PostgreSQL table: {table_name}")

#############################################################################

###########################################################################

def main_path():
    yaml_path = "/home/user/문서/workspace/python/yaml"
    yaml_name = "crop_trim.yaml"

    doc_path = "/home/user/문서"

    data_gdrive_path = "/home/user/GoogleDrive/data"
    raw_path = f"{doc_path}/raw_data"

    focus = "agrifood_elasticity_usa"  # 필요시 다른 키로 변경

    yaml_data = load_yaml(yaml_path, yaml_name)

    raw_file = f"{raw_path}/{yaml_data[focus]['raw_name']}"
    parquet_file = f"{data_gdrive_path}/{yaml_data[focus]['parquet_name']}"

    postgres_uri = "postgresql+psycopg2://supersetuser:StrongPassword123!@localhost:5432/supersetdb"

    return focus, raw_file, parquet_file, postgres_uri, yaml_data

def main_raw_parquet(focus, raw_file, parquet_file, postgres_uri, yaml_data):

    # CSV → DF 로드
    df = load_raw_to_df(raw_file)

    # (원하는 전처리 여기서 직접)
    # e.g., df = df.rename(columns=lambda x: x.lower())
    # e.g., df["date"] = pd.to_datetime(df["date"])

    # DF → Parquet 저장 (출력 포함)
    save_df_to_parquet(df, parquet_file)




def main_parquet_postgres(focus, raw_file, parquet_file, postgres_uri, yaml_data):

    # ─ Parquet → PostgreSQL ─

    table_name = focus  # YAML에 테이블 이름 넣어두면 깔끔

    df = load_parquet_to_df(parquet_file)

    ###전처리


    save_df_to_postgres(df, table_name, postgres_uri)




if __name__ == "__main__":
    path = main_path()
    # main_raw_parquet(*path)
    main_parquet_postgres(*path)