import os
import polars as pl


# ======================================================
# 🔹 Polars 출력 옵션 (전체 컬럼 표시)
# ======================================================
pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_rows(20)
pl.Config.set_fmt_str_lengths(10_000)
pl.Config.set_tbl_width_chars(10_000)


# ======================================================
# 🔹 CSV → Parquet (Polars Lazy, full columns)
# ======================================================
def csv_to_parquet_polars(
    csv_file: str,
    parquet_folder: str,
    focus: str,
) -> str:
    """
    초대형 CSV → Heavy Parquet 변환
    - 모든 컬럼 유지
    - Polars Lazy + utf8-lossy
    """
    os.makedirs(parquet_folder, exist_ok=True)
    parquet_file = os.path.join(parquet_folder, f"{focus}.parquet")

    print("🚀 CSV → Parquet (Polars Lazy) 시작")

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
        compression="zstd",
        statistics=True,
    )

    print(f"💾 Heavy Parquet 저장 완료 → {parquet_file}")
    return parquet_file


# ======================================================
# 🔹 Parquet → Polars DataFrame
# ======================================================
def load_parquet_polars(parquet_file: str) -> pl.DataFrame:
    if not os.path.exists(parquet_file):
        raise FileNotFoundError(parquet_file)

    df = pl.read_parquet(parquet_file)
    print(f"📥 Loaded Parquet → {parquet_file} ({df.height:,} rows)")
    return df


# ======================================================
# 🔹 Focus Parquet 파일 head 확인
# ======================================================
def preview_focus_parquet_head(
    parquet_folder: str,
    focus: str,
    n: int = 5,
) -> pl.DataFrame:
    """
    parquet_folder 내에서
    - 파일명에 focus가 포함된 Parquet 파일을 찾고
    - 상위 n행(head)을 출력
    """
    parquet_files = [
        f for f in os.listdir(parquet_folder)
        if f.endswith(".parquet") and focus in f
    ]

    if not parquet_files:
        raise FileNotFoundError(
            f"No parquet file matching focus='{focus}' in {parquet_folder}"
        )

    parquet_path = os.path.join(parquet_folder, parquet_files[0])

    print(f"👀 Preview Parquet head → {parquet_path}")

    df = pl.read_parquet(parquet_path)
    head_df = df.head(n)

    print(f"📊 Columns ({len(df.columns)}): {df.columns}")
    print(f"📥 Head ({n} rows):")
    print(head_df)

    return head_df


# ======================================================
# 🔹 Filter + Select → Lightweight Parquet 저장
# ======================================================
def filter_and_save_parquet_polars(
    parquet_folder: str,
    output_parquet_folder: str,
    focus_parquet: str,
    output_name: str,
    filter_data: dict[str, object] | None = None,
    select_columns: dict[str, str] | None = None,
    preview_head: bool = True,
    head_n: int = 5,
) -> str:
    """
    Heavy Parquet →
    1) 필터
    2) 컬럼 선택 + 리네이밍
    3) Lightweight Parquet 저장
    4) (옵션) 저장 직후 head 출력
    """
    # --------------------------------------------------
    # 출력 폴더 생성
    # --------------------------------------------------
    os.makedirs(output_parquet_folder, exist_ok=True)

    # --------------------------------------------------
    # Heavy Parquet 파일 선택
    # --------------------------------------------------
    parquet_files = [
        f for f in os.listdir(parquet_folder)
        if f.endswith(".parquet") and focus_parquet in f
    ]

    if not parquet_files:
        raise FileNotFoundError(
            f"No parquet file matching '{focus_parquet}' in {parquet_folder}"
        )

    parquet_path = os.path.join(parquet_folder, parquet_files[0])
    print(f"📥 Loaded Heavy Parquet → {parquet_path}")

    # --------------------------------------------------
    # Parquet 로드
    # --------------------------------------------------
    df = pl.read_parquet(parquet_path)

    # --------------------------------------------------
    # 🔥 범용 필터 적용
    # --------------------------------------------------
    if filter_data:
        for col, value in filter_data.items():
            if col not in df.columns:
                raise KeyError(f"Filter column not found: {col}")
            df = df.filter(pl.col(col) == value)

    # --------------------------------------------------
    # 🔥 컬럼 선택 + 리네이밍
    # --------------------------------------------------
    if select_columns:
        missing_cols = [
            col for col in select_columns.keys()
            if col not in df.columns
        ]
        if missing_cols:
            raise KeyError(f"Select columns not found: {missing_cols}")

        df = df.select(list(select_columns.keys()))
        df = df.rename(select_columns)

    # --------------------------------------------------
    # Lightweight Parquet 저장
    # --------------------------------------------------
    output_path = os.path.join(output_parquet_folder, f"{output_name}.parquet")

    df.write_parquet(
        output_path,
        compression="zstd",
        statistics=True,
    )

    print(
        f"💾 Lightweight Parquet 저장 완료 → {output_path} "
        f"({df.height:,} rows × {len(df.columns)} cols)"
    )

    # --------------------------------------------------
    # 🔍 Head 미리보기 (옵션)
    # --------------------------------------------------
    if preview_head:
        print(f"\n👀 Preview Lightweight Parquet head ({head_n} rows)")
        print(df.head(head_n))

    return output_path


# ======================================================
# 🔹 MAIN PIPELINE
# ======================================================
def main():
    # ----------------------------------
    # 경로 설정
    # ----------------------------------
    parquet_heavy_folder = "/home/user/GoogleDrive/data/parquet_heavy"
    parquet_light_folder = "/home/user/GoogleDrive/data/parquet"

    focus_parquet = "251214_trade_matrix_faostat"

    # ----------------------------------
    # 필터 조건
    # ----------------------------------
    filter_data = {
        # "Country Group": "World",
        "Year": 2023,
        "Element": "Import quantity",
        "Item": "Soya beans",
    }

    # ----------------------------------
    # SQL / 분석용 컬럼 스키마
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

    output_name = "251214_trade_faostat_soybeans_import_2023"

    # ----------------------------------
    # 실행
    # ----------------------------------
    filter_and_save_parquet_polars(
        parquet_folder=parquet_heavy_folder,
        output_parquet_folder=parquet_light_folder,
        focus_parquet=focus_parquet,
        output_name=output_name,
        filter_data=filter_data,
        select_columns=select_columns,
    )


if __name__ == "__main__":
    main()
