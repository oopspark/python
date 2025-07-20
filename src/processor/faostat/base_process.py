import os
import re
import csv
import shutil
import zipfile
import pandas as pd
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


def to_parquet_filename(fname, keyword=None, position=None):
    base = os.path.splitext(fname)[0]
    if keyword and position:
        if position == "prefix" and base.startswith(keyword):
            base = base[len(keyword):]
        elif position == "suffix" and base.endswith(keyword):
            base = base[:-len(keyword)]
    return re.sub(r"[^\w]+", "_", base).lower() + ".parquet"

# ───────────────────────────────────────────────
# 2. Parquet 저장 함수 (chunk 단위 메모리 절약 버전)
# ───────────────────────────────────────────────

def process_folder_to_parquet(csv_folder, schema_dict, out_folder, keyword=None, position=None, chunksize=100_000):
    os.makedirs(out_folder, exist_ok=True)

    for fname, col_types in schema_dict.items():
        csv_path = os.path.join(csv_folder, fname)
        if not os.path.exists(csv_path):
            continue

        try:
            parquet_fname = to_parquet_filename(fname, keyword, position)
            parquet_path = os.path.join(out_folder, parquet_fname)
            if os.path.exists(parquet_path):
                os.remove(parquet_path)

            first_chunk = True
            for chunk in pd.read_csv(csv_path, encoding="utf-8", dtype=str, chunksize=chunksize):
                for col, typ in col_types.items():
                    if col in chunk.columns:
                        if typ == "number":
                            chunk[col] = pd.to_numeric(chunk[col], errors="coerce")
                        elif typ == "string":
                            chunk[col] = chunk[col].astype(str)

                chunk.to_parquet(parquet_path, index=False, engine="fastparquet", append=not first_chunk)
                first_chunk = False

            print(f"✅ Saved: {parquet_fname}")

            os.remove(csv_path)
            print(f"🗑️ Deleted CSV: {fname}")
        except Exception as e:
            print(f"❌ Failed to process {fname}: {e}")

# ───────────────────────────────────────────────
# 3. 실행 메인
# ───────────────────────────────────────────────

def main():
    target_dir = "/home/park/workspace/python/data/temp"
    utf8_done_folder = target_dir + "_utf8_done"
    parquet_out_folder = target_dir + "_parquet"

    unzip_and_delete_all_in_dir(target_dir)
    keep_files_with_keyword(target_dir, "_E_All_Data_(Normalized)")
    convert_csvs_to_utf8_and_move(target_dir, utf8_done_folder)
    types = analyze_csv_folder_column_types(utf8_done_folder, sample_limit=1000)

    process_folder_to_parquet(
        utf8_done_folder,
        types,
        parquet_out_folder,
        keyword="_E_All_Data_(Normalized)",
        position="suffix",
    )


if __name__ == "__main__":
    main()
