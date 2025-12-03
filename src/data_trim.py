import polars as pl

def expand_field_auto(parquet_path: str, target_col: str, sep: str = ",") -> pl.LazyFrame:
    # ① 전체를 로드하지 않고 지정 컬럼만 Arrow로 스캔
    lf_col = pl.scan_parquet(parquet_path, columns=[target_col])

    # ② 토큰 자동 수집
    tokens = (
        lf_col
        .select(pl.col(target_col).drop_nulls().str.split(sep))
        .explode(target_col)
        .select(pl.col(target_col).str.strip())
        .unique()
        .collect()
        .to_series()
        .to_list()
    )

    # ③ 전체 DF lazy load
    lf = pl.scan_parquet(parquet_path)

    # ④ 불린 확장 컬럼 생성
    for tok in tokens:
        lf = lf.with_columns(
            pl.col(target_col)
              .cast(str)
              .str.contains(tok)
              .fill_null(False)
              .alias(f"is_{target_col}_{tok}")
        )

    return lf



focus = "crop_dry_korea_20250215"   # 날짜 포함된 focus
csv_file_name = "/home/user/다운로드/Production_Crops_Livestock_E_All_Data_(Normalized)"
parquet_folder = "/home/user/GoogleDrive"
parquet_temp_folder = f"{parquet_folder}/data/temp"
parquet_data_folder = f"{parquet_folder}/data"

lf = expand_field_auto(
    parquet_path=f"{parquet_temp_folder}/{focus}.parquet",
    target_col="country"
)
df = lf.collect()


focus_expanded = f"{focus}_expanded"
df.write_parquet(f"{parquet_temp_folder}/{focus_expanded}.parquet")



