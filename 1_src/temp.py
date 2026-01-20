import os
import polars as pl

pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_rows(20)
pl.Config.set_tbl_width_chars(10_000)
# 필요하면 스레드 수 수동 지정 가능
# pl.Config.set_tbl_threads(os.cpu_count())


def csv_to_parquet_polars(
    csv_file: str,
    parquet_file: str,
) -> None:
    """
    대용량 CSV -> Parquet (메모리 효율 + 멀티코어)
    - 전체 파일을 한 번에 메모리에 올리지 않음 (Lazy + streaming)
    """

    os.makedirs(os.path.dirname(parquet_file), exist_ok=True)

    print("🚀 [polars] CSV → Parquet 변환 시작")
    print(f"   · input  = {csv_file}")
    print(f"   · output = {parquet_file}")

    lf = pl.scan_csv(
        csv_file,
        encoding="utf8-lossy",      # Polars는 utf8 / utf8-lossy만 허용
        infer_schema_length=1_000,  # 타입 추론을 앞부분 일부에서만
        low_memory=True,            # ✅ 메모리 절약 모드
        null_values=["", "NA", "NULL"],
        ignore_errors=True,
        truncate_ragged_lines=True,
    )

    # sink_parquet: 스트리밍 기반 → 메모리 피크 줄어듦
    lf.sink_parquet(
        parquet_file,
        compression="zstd",         # 속도 위주면 "lz4"
        statistics=False,           # ✅ 통계 계산 끄면 메모리 피크 ↓
    )

    print(f"💾 [polars] 변환 완료 → {parquet_file}")


if __name__ == "__main__":
    csv_path = "/home/user/다운로드/260121_trade_matrix_faostat.csv"
    parquet_path = "/home/user/GoogleDrive/data/parquet_heavy/260121_trade_matrix_faostat.parquet"
    csv_to_parquet_polars(csv_path, parquet_path)
