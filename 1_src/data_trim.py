import pandas as pd
import os


def expand_field_auto(parquet_path: str, target_col: str, sep: str = ",") -> pd.DataFrame:
    """
    Parquet 파일에서 target_col을 자동으로 토큰 분해해서
    is_{col}_{token} 형식의 one-hot boolean 컬럼을 자동 생성
    """
    if not os.path.exists(parquet_path):
        raise FileNotFoundError(f"❌ Parquet 파일 없음: {parquet_path}")

    # ① 전체 DF 로딩
    df = pd.read_parquet(parquet_path)

    if target_col not in df.columns:
        raise KeyError(f"❌ 타겟 컬럼 '{target_col}'이 존재하지 않음")

    # ② 토큰 자동 수집
    tokens = (
        df[target_col]
        .dropna()
        .astype(str)
        .str.split(sep)
        .explode()
        .str.strip()
        .unique()
        .tolist()
    )

    # ③ 확장 boolean 컬럼 생성
    for tok in tokens:
        col_name = f"is_{target_col}_{tok}"
        df[col_name] = (
            df[target_col]
            .astype(str)
            .str.contains(rf"(?:^|{sep})\s*{tok}\s*(?:{sep}|$)")  # 정확한 단어 포함
            .fillna(False)
        )

    return df


def main():
    focus = "crop_dry_korea_20250215"
    parquet_folder = "/home/user/GoogleDrive/parquet"

    df_expanded = expand_field_auto(
        parquet_path=f"{parquet_folder}/{focus}.parquet",
        target_col="country",
        sep=","
    )

    focus_expanded = f"{focus}_expanded"
    df_expanded.to_parquet(f"{parquet_folder}/{focus_expanded}.parquet", index=False)
    print("🚀 Saved:", f"{parquet_folder}/{focus_expanded}.parquet")
    print(df_expanded.head())

if __name__ == "__main__":
    main()





#     import pandas as pd

# file_dry   = "/home/user/GoogleDrive//data/parquet/20251203_crop_dry_korea.parquet"
# file_paddy = "/home/user/GoogleDrive/data/parquet/20251203_crop_paddy_korea.parquet"

# # ① 불러오기
# df_dry = pd.read_parquet(file_dry)
# df_paddy = pd.read_parquet(file_paddy)

# # ② 구분 컬럼 추가
# df_dry["구분"] = "밭"
# df_paddy["구분"] = "논"

# # ③ 행 병합
# df_merged = pd.concat([df_dry, df_paddy], ignore_index=True)

# # ④ 저장
# out = "/home/user/GoogleDrive/data/parquet/20251203_crop_land_merged_korea.parquet"
# df_merged.to_parquet(out, index=False)

# print("🚀 저장 완료:", out)
# print(df_merged.head())






# import pandas as pd
# import sys

# file = f"/home/user/GoogleDrive/data/parquet/20251203_soybean_procurement_korea.parquet"

# try:
#     # ① Parquet 읽기
#     df = pd.read_parquet(file)
# except Exception as e:
#     print(f"❌ Parquet 파일 로딩 실패: {e}")
#     sys.exit(1)


# # ② 형식 변환할 컬럼
# columns_to_float = ["데이터"]

# try:
#     for col in columns_to_float:

#         # 컬럼 존재 여부 확인
#         if col not in df.columns:
#             raise KeyError(f"'{col}' 컬럼이 존재하지 않음 (컬럼 목록: {list(df.columns)})")

#         # 실제 변환
#         before_count = len(df)
#         df[col] = pd.to_numeric(df[col], errors="raise")  # ← 실패 시 바로 예외 발생

# except Exception as e:
#     print(f"❌ 컬럼 '{col}' 실수 변환 실패")
#     print("🔍 원인:", e)

#     # 어떤 값들이 문제인지 상위 몇 개 보여주기
#     invalid = df[col][~df[col].astype(str).str.replace('.', '', 1).str.isnumeric()]
#     print("\n⚠️ 문제된 값 예시:")
#     print(invalid.head(10))
#     sys.exit(1)   # 실행 취소 (저장하지 않음)


# # ③ 저장
# try:
#     df.to_parquet(file, index=False)
# except Exception as e:
#     print(f"❌ Parquet 저장 실패: {e}")
#     sys.exit(1)


# print("🚀 완료:", file)
# print(df.dtypes)




# import pandas as pd

# file = "/home/user/GoogleDrive/data/parquet/20251203_soybean_procurement_korea.parquet"

# # ① Parquet 읽기
# df = pd.read_parquet(file)

# # ② 형식 변환할 컬럼 지정 (예: value, amount, price 등)
# columns_to_float = ["데이터"]   # ← 변환할 컬럼명을 리스트로 넣어
# for col in columns_to_float:
#     df[col] = pd.to_numeric(df[col], errors="coerce")  # 실수 변환 + 실패값 NaN


# file_modified = "/home/user/GoogleDrive/data/parquet/20251204_soybean_procurement_korea.parquet"
# # ③ 저장 (덮어쓰기)
# df.to_parquet(file_modified, index=False)

# print("🚀 완료:", file)
# print(df.dtypes)
