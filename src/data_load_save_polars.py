import os
import polars as pl


# ======================================================
# 🔹 Polars 기본 설정 (멀티코어)
# ======================================================
pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_rows(20)
pl.Config.set_fmt_str_lengths(10_000)
pl.Config.set_tbl_width_chars(10_000)

# 필요 시 명시 (보통 자동)
# pl.Config.set_tbl_threads(os.cpu_count())


# ======================================================
# 🔹 CSV → Heavy Parquet (Lazy, 멀티코어 OK)
# ======================================================
def csv_to_parquet_polars(
    csv_file: str,
    parquet_folder: str,
    focus: str,
) -> str:
    """
    초대형 CSV → Heavy Parquet
    - Lazy scan
    - 멀티코어
    """
    os.makedirs(parquet_folder, exist_ok=True)
    parquet_file = os.path.join(parquet_folder, f"{focus}.parquet")

    print("🚀 CSV → Heavy Parquet 시작")

    lf = pl.scan_csv(
        csv_file,
        encoding="utf8-lossy",
        infer_schema_length=10_000,
        null_values=["", "NA", "NULL"],
        ignore_errors=True,
        truncate_ragged_lines=True,
    )

    lf.sink_parquet(
        parquet_file,
        compression="zstd",   # lz4로 바꾸면 더 빠름
        statistics=True,
    )

    print(f"💾 Heavy Parquet 저장 완료 → {parquet_file}")
    return parquet_file


# ======================================================
# 🔹 Heavy Parquet → Filtered Light Parquet (🔥 핵심)
# ======================================================
def filter_and_save_parquet_polars(
    parquet_folder: str,
    output_parquet_folder: str,
    focus_parquet: str,
    output_name: str,
    filter_data: dict[str, object] | None = None,
    select_columns: dict[str, str] | None = None,
) -> str:
    """
    Heavy Parquet →
    - scan_parquet (Lazy)
    - filter (predicate pushdown)
    - select (projection pushdown)
    - sink_parquet (streaming, 멀티코어)
    """

    os.makedirs(output_parquet_folder, exist_ok=True)

    # ----------------------------------
    # Heavy Parquet 선택
    # ----------------------------------
    parquet_files = [
        f for f in os.listdir(parquet_folder)
        if f.endswith(".parquet") and focus_parquet in f
    ]

    if not parquet_files:
        raise FileNotFoundError(
            f"No parquet file matching '{focus_parquet}' in {parquet_folder}"
        )

    parquet_path = os.path.join(parquet_folder, parquet_files[0])
    output_path = os.path.join(output_parquet_folder, f"{output_name}.parquet")

    print(f"📥 scan_parquet → {parquet_path}")

    # ----------------------------------
    # 🔥 Lazy Scan (여기서부터 멀티코어)
    # ----------------------------------
    lf = pl.scan_parquet(parquet_path)

    # ----------------------------------
    # 🔥 컬럼 최소화 (가장 중요)
    # ----------------------------------
    if select_columns:
        lf = lf.select(list(select_columns.keys()))

    # ----------------------------------
    # 🔥 필터 (predicate pushdown)
    # ----------------------------------
    if filter_data:
        for col, value in filter_data.items():
            lf = lf.filter(pl.col(col) == value)

    # ----------------------------------
    # 🔥 컬럼 리네이밍
    # ----------------------------------
    if select_columns:
        lf = lf.rename(select_columns)

    # ----------------------------------
    # 🔥 병렬 + streaming write
    # ----------------------------------
    print("🚀 Filter + Select → Light Parquet 저장 중")

    lf.sink_parquet(
        output_path,
        compression="zstd",   # 속도 중시 시 lz4
        statistics=True,
    )

    print(f"💾 Light Parquet 저장 완료 → {output_path}")
    return output_path


# ======================================================
# 🔹 Preview (확인용, 소량만)
# ======================================================
def preview_parquet_head(parquet_path: str, n: int = 5):
    print(f"\n👀 Preview head ({n} rows)")
    df = pl.read_parquet(parquet_path)
    print(df.head(n))


# ======================================================
# 🔹 MAIN PIPELINE
# ======================================================
def main():

    parquet_heavy_folder = "/home/user/gdrive/data/parquet_heavy"
    parquet_light_folder = "/home/user/gdrive/data/parquet"

    focus_parquet = "251214_trade_matrix_faostat"

    # ----------------------------------
    # 필터 조건
    # ----------------------------------
    filter_data = {
        "Reporter Countries": "Republic of Korea",
        "Element": "Import quantity",
        "Item": "Soya beans",
    }

    # ----------------------------------
    # 분석용 컬럼 스키마
    # ----------------------------------
    select_columns = {
        "Reporter Countries": "importer",
        "Partner Countries": "exporter",
        "Item": "item",
        "Element": "element",
        "Year": "year",
        "Unit": "unit",
        "Value": "value",
    }

    output_name = "251214_trade_faostat_soybeans_import_korea"

    # ----------------------------------
    # 실행
    # ----------------------------------
    output_path = filter_and_save_parquet_polars(
        parquet_folder=parquet_heavy_folder,
        output_parquet_folder=parquet_light_folder,
        focus_parquet=focus_parquet,
        output_name=output_name,
        filter_data=filter_data,
        select_columns=select_columns,
    )

    # ----------------------------------
    # Preview (선택)
    # ----------------------------------
    preview_parquet_head(output_path, n=5)


if __name__ == "__main__":
    main()
