import os
import re
import csv
import shutil
import zipfile
import pymysql
from charset_normalizer import from_bytes

# ───────────────────────────────────────────────
# 1. 유틸 함수들
# ───────────────────────────────────────────────


def unzip_and_delete_all_in_dir(target_dir=None):
    if target_dir is None:
        target_dir = os.getcwd()
    zip_files = [f for f in os.listdir(target_dir) if f.endswith(".zip")]
    for filename in zip_files:
        zip_path = os.path.join(target_dir, filename)
        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(target_dir)
                print(f"✅ Extracted: {filename}")
            os.remove(zip_path)
            print(f"🗑️ Deleted: {filename}")
        except zipfile.BadZipFile:
            print(f"❌ Bad zip file: {filename}")
        except Exception as e:
            print(f"⚠️ Error with {filename}: {e}")


def keep_files_with_keyword(directory, keyword):
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path) and keyword not in filename:
            try:
                os.remove(file_path)
                print(f"🗑️ Deleted: {filename}")
            except Exception as e:
                print(f"⚠️ Error deleting {filename}: {e}")
        else:
            print(f"✅ Kept: {filename}")


def detect_encoding(file_path, sample_size=100_000):
    try:
        with open(file_path, "rb") as f:
            sample = f.read(sample_size)
        result = from_bytes(sample).best()
        return result.encoding if result else None
    except Exception as e:
        print(f"⚠️ Error detecting encoding: {e}")
        return None


def convert_csvs_to_utf8_and_move(folder, done_folder):
    os.makedirs(done_folder, exist_ok=True)
    for fname in os.listdir(folder):
        if not fname.lower().endswith(".csv"):
            continue
        path = os.path.join(folder, fname)
        dest_path = os.path.join(done_folder, fname)
        enc = detect_encoding(path)
        if enc is None:
            print(f"❌ Encoding unknown: {fname}")
            continue
        if enc.lower() == "utf-8":
            shutil.move(path, dest_path)
            print(f"📁 Moved UTF-8: {fname}")
            continue
        try:
            with open(path, "r", encoding=enc, errors="replace") as fin, open(
                dest_path, "w", encoding="utf-8"
            ) as fout:
                for line in fin:
                    fout.write(line)
            os.remove(path)
            print(f"✅ Converted: {fname} ({enc})")
        except Exception as e:
            print(f"❌ Failed to convert {fname}: {e}")
            if os.path.exists(dest_path):
                os.remove(dest_path)


def is_number(value):
    return re.fullmatch(r"-?\d+(\.\d+)?", value.strip()) is not None


def analyze_csv_folder_column_types(folder, sample_limit=1000):
    result = {}
    for fname in os.listdir(folder):
        if not fname.lower().endswith(".csv"):
            continue
        fpath = os.path.join(folder, fname)
        with open(fpath, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                print(f"⚠️ No headers: {fname}")
                continue
            col_values = {col: [] for col in reader.fieldnames}
            for i, row in enumerate(reader):
                if i >= sample_limit:
                    break
                for col in col_values:
                    val = (row.get(col) or "").strip()
                    if val:
                        col_values[col].append(val)
            col_types = {}
            for col, values in col_values.items():
                if not values:
                    col_types[col] = "string"
                elif all(is_number(v) for v in values):
                    col_types[col] = "number"
                else:
                    col_types[col] = "string"
            result[fname] = col_types
    return result


# ───────────────────────────────────────────────
# 2. MySQL 관련 함수
# ───────────────────────────────────────────────


def to_mysql_table_name(fname, keyword=None, position=None):
    base = os.path.splitext(fname)[0]
    if keyword and position:
        if position == "prefix" and base.startswith(keyword):
            base = base[len(keyword) :]
        elif position == "suffix" and base.endswith(keyword):
            base = base[: -len(keyword)]
    return re.sub(r"[^\w]+", "_", base).lower()


def to_mysql_column_name(colname):
    return re.sub(r"[^\w]+", "_", colname.strip()).lower()


def connect_mysql(user="root", password="1120", host="localhost", port=3306):
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        charset="utf8mb4",
        autocommit=False,
    )


def create_schema(cursor, schema_name):

    cursor.execute(f"DROP SCHEMA IF EXISTS `{schema_name}`;")
    cursor.execute(
        f"CREATE SCHEMA `{schema_name}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
    )


def create_table(cursor, schema, table_name, col_types):
    cols = []
    for col, typ in col_types.items():
        sql_col = to_mysql_column_name(col)
        sql_type = "FLOAT" if typ == "number" else "VARCHAR(255)"
        cols.append(f"`{sql_col}` {sql_type}")
    stmt = f"CREATE TABLE `{schema}`.`{table_name}` (\n  " + ",\n  ".join(cols) + "\n);"
    cursor.execute(stmt)


def insert_csv_to_table(
    cursor, schema, table_name, filepath, col_types, batch_size=500
):
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            print(f"⚠️ Skipped: {filepath}")
            return

        columns = [to_mysql_column_name(col) for col in reader.fieldnames]
        placeholders = ", ".join(["%s"] * len(columns))
        insert_stmt = f"INSERT INTO `{schema}`.`{table_name}` ({', '.join('`' + c + '`' for c in columns)}) VALUES ({placeholders})"

        batch = []
        for row in reader:
            values = []
            for col in reader.fieldnames:
                val = (row.get(col) or "").strip()
                if val == "":
                    values.append(None)
                elif col_types.get(col) == "number":
                    try:
                        values.append(float(val))
                    except:
                        values.append(None)
                else:
                    values.append(val if len(val) <= 255 else None)
            batch.append(values)
            if len(batch) >= batch_size:
                cursor.executemany(insert_stmt, batch)
                batch.clear()

        if batch:
            cursor.executemany(insert_stmt, batch)


# ───────────────────────────────────────────────
# 3. 전체 처리 함수
# ───────────────────────────────────────────────


def process_folder_with_schema(root_dir, schema_dict, keyword=None, position=None):
    conn = connect_mysql()
    cursor = conn.cursor()

    base_schema = "faostat"
    for fname, col_types in schema_dict.items():

        table_name = to_mysql_table_name(fname, keyword, position)
        fpath = os.path.join(root_dir, fname)

        print(f"🧱 Creating table: {base_schema}.{table_name}")
        create_table(cursor, base_schema, table_name, col_types)

        print(f"📥 Inserting: {fname}")
        insert_csv_to_table(cursor, base_schema, table_name, fpath, col_types)

        try:
            os.remove(fpath)
            print(f"🗑️ Deleted: {fname}")
        except Exception as e:
            print(f"⚠️ Failed to delete {fname}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ All done!")


# ───────────────────────────────────────────────
# 4. 실행 메인
# ───────────────────────────────────────────────


def main():
    target_dir = "/home/park/workspace/my_projects/temp/faostat"
    utf8_done_folder = target_dir + "_utf8_done"

    unzip_and_delete_all_in_dir(target_dir)
    keep_files_with_keyword(target_dir, "_E_All_Data_(Normalized)")

    convert_csvs_to_utf8_and_move(target_dir, utf8_done_folder)

    types = analyze_csv_folder_column_types(utf8_done_folder, sample_limit=1000)

    base_schema = "faostat"
    conn = connect_mysql()
    cursor = conn.cursor()
    create_schema(cursor, base_schema)

    process_folder_with_schema(
        utf8_done_folder, types, keyword="_E_All_Data_(Normalized)", position="suffix"
    )

    return


if __name__ == "__main__":
    main()
