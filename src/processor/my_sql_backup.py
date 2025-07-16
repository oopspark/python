import subprocess
import datetime
import getpass
import gzip
import shutil
import os

def backup_and_compress(host, user, schemas, output_dir='.'):
    password = getpass.getpass("MySQL 비밀번호: ")
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    schema_part = "_".join(schemas)
    sql_file = os.path.join(output_dir, f"backup_{schema_part}_{timestamp}.sql")
    gz_file = sql_file + ".gz"

    cmd = [
        "mysqldump",
        f"--host={host}",
        f"--user={user}",
        f"--password={password}",  # 비밀번호를 여기 직접 넣음
        "--databases", *schemas,
        "--routines",
        "--single-transaction",
        "--no-tablespaces",
    ]

    try:
        print(f"🔄 백업 중: {', '.join(schemas)}")
        with open(sql_file, 'w', encoding='utf-8') as f:
            subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=True, check=True)

        print("📦 gzip 압축 중...")
        with open(sql_file, 'rb') as f_in, gzip.open(gz_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

        os.remove(sql_file)  # 원본 SQL 파일 삭제
        print(f"✅ 완료: {gz_file}")

    except subprocess.CalledProcessError as e:
        print("❌ 백업 실패!")
        if e.stderr:
            print("오류 메시지:", e.stderr)

def main():
    host = "localhost"
    user = "root"
    schemas = ["bank", "wpp"]  # 원하는 스키마 리스트
    output_dir = r'C:\Users\parkj\Documents\workspace\my_projects\data\sql_data'  # 저장 위치

    backup_and_compress(host, user, schemas, output_dir)

if __name__ == "__main__":
    main()
