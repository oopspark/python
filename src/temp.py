import os
from datetime import datetime


def add_today_suffix_to_parquet_files(parquet_folder: str):
    """
    parquet_folder 경로 안의 모든 .parquet 파일명 뒤에 _오늘날짜 suffix 추가
    - 예: crop.parquet → crop_20251203.parquet
    """
    today = datetime.now().strftime("%Y%m%d")
    files = [f for f in os.listdir(parquet_folder) if f.lower().endswith(".parquet")]

    if not files:
        print("⚠ parquet 폴더에 파일이 없습니다.")
        return

    for file in files:
        src = os.path.join(parquet_folder, file)

        # .parquet 제거 후 날짜 suffix 추가
        base = file[:-8]  # ".parquet" 제거 (길이 8)
        new_name = f"{base}_{today}.parquet"
        dst = os.path.join(parquet_folder, new_name)

        os.rename(src, dst)
        print(f"📌 {file}  →  {new_name}")

    print("\n✅ 모든 parquet 파일 이름에 오늘 날짜 추가 완료!")

def main():
    parquet_folder = "/home/user/gdrive/data"

    add_today_suffix_to_parquet_files(parquet_folder)

main()