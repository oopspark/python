import csv
import os
import re
import platform
import pymysql
from collections import OrderedDict
from charset_normalizer import from_path
from pymysql.connections import Connection



class EncodingPreprocessor:
    def __init__(self):
        self.input_folder : str
        self.output_folder : str
        self.converted_files : list
        self.files_to_process : list

    def _detect_encoding(self, file_path):
        try:
            result = from_path(file_path).best()
            return result.encoding if result else "utf-8"
        except Exception:
            return "utf-8"

    def preprocess(self):
        os.makedirs(self.output_folder, exist_ok=True)
        for fname in os.listdir(self.input_folder):
            if not fname.lower().endswith(".csv"):
                continue

            in_path = os.path.join(self.input_folder, fname)
            out_path = os.path.join(self.output_folder, fname)

            enc = self._detect_encoding(in_path)
            try:
                with open(in_path, 'r', encoding=enc, errors='replace') as fin, \
                     open(out_path, 'w', encoding='utf-8') as fout:
                    for line in fin:
                        fout.write(line)
                os.remove(in_path)
                self.converted_files.append(fname)
                print(f"✅ Converted & removed original: {fname} ({enc})")
            except Exception as e:
                print(f"❌ Failed to convert {fname}: {e}")

        self.files_to_process = [fname for fname in os.listdir(self.output_folder) if fname.lower().endswith(".csv")]

        return self



class SchemaAnalyzer:
    def __init__(self):
        self.file_column : list
        self.files_to_process : list
        self.keyword_to_remove : str
        self.keyword_position : str
        self.output_folder : str


    def _infer_type(self, values):
        is_int = True
        is_float = True
        max_len = 0
        has_null = False

        for val in values:
            val = val.strip()
            if val == '':
                has_null = True
                continue
            max_len = max(max_len, len(val))
            if not re.fullmatch(r"-?\d+", val):
                is_int = False
            if not re.fullmatch(r"-?\d+(\.\d+)?", val):
                is_float = False

        if is_int:
            return "INT", has_null, max_len
        elif is_float:
            return "FLOAT", has_null, max_len
        else:
            return ("TEXT", has_null, max_len) if max_len > 255 else (f"VARCHAR({max_len})", has_null, max_len)


    def _to_sql_name_table(self, fname):
        base = os.path.splitext(fname)[0]
        if self.keyword_to_remove and self.keyword_position:
            if self.keyword_position == "prefix":
                base = base[len(self.keyword_to_remove):]
            elif self.keyword_position == "suffix":
                base = base[:-len(self.keyword_to_remove)]
        return re.sub(r'[ \-\.]+', '_', base).lower()


    def _clean_column_name(self, name):
        return name.strip().strip('"').strip("'").lstrip('\ufeff') if name else ""

    def _to_sql_name_column(self, name):
        name = self._clean_column_name(name)
        name = re.sub(r'[ \-\.]+', '_', name)
        return name.lower()




    def _analyze(self, file_path, sample_limit=5000):
        columns = []
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            col_values = OrderedDict((col, []) for col in reader.fieldnames)

            for i, row in enumerate(reader):
                if i >= sample_limit:
                    break
                for col in col_values:
                    col_values[col].append(row.get(col, "") or "")

            for col, vals in col_values.items():
                mysql_type, has_null, max_len = self._infer_type(vals)
                columns.append({
                    "full" : col,
                    "mysql_column": self._to_sql_name_column(col),
                    "mysql_type": mysql_type,
                    "nullable": has_null,
                    "max_length": max_len
                })
        return columns



    def analyze_files(self):
        for fname in self.files_to_process:
            file_property = {}
            fpath = os.path.join(self.output_folder, fname)
            print(f"🔍 Analyzing: {fname}")
            file_property["file_name"] = {
                "full" : fname,
                "mysql_table" : self._to_sql_name_table(fname)
            }          
            file_property["columns"] = self._analyze(fpath)
            self.file_column.append(file_property)
        return self



class MySQLSchemaCreate:
    def __init__(self):
        self.conn : Connection
        self.schema_name : str
        self.file_column : list



    def _cursor_commit(self, lines):
        try:
            with self.conn.cursor() as cursor:
                for stmt in "".join(lines).split(";"):
                    if stmt.strip():
                        cursor.execute(stmt)
            self.conn.commit()
        except Exception as e:
            print(f"❌ SQL 실행 실패: {e}")
            raise


  
    def _create_schema_title(self):
        lines = []
        lines.append(f"DROP SCHEMA IF EXISTS `{self.schema_name}`;")
        lines.append(f"CREATE SCHEMA `{self.schema_name}` DEFAULT CHARACTER SET utf8mb4 COLLATE=utf8mb4_unicode_ci;")
        self._cursor_commit(lines)        

    def _create_table(self, table_name, column_dict):
        lines = []
        lines.append(f"USE `{self.schema_name}`;")
        lines.append(f"DROP TABLE IF EXISTS `{table_name}`;")
        lines.append(f"CREATE TABLE `{table_name}` (")

        defs = []
        for column in column_dict:
            col_sql = column["mysql_column"]
            null_str = "NULL" if column.get("nullable", True) else "NOT NULL"
            defs.append(f"  `{col_sql}` {column['mysql_type']} {null_str}")

        lines.append(",\n".join(defs))
        lines.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;")
        
        self._cursor_commit(lines)

        return self
    
    def create_schema(self):

        self._create_schema_title()

        for file in self.file_column:
            
            table_name = file["file_name"]["mysql_table"]
            column_dict = file["columns"]
            self._create_table(table_name, column_dict)
   
    

        return self




class MySQLLoadData:
    def __init__(self):
        self.conn : Connection
        self.files_to_process : list
        self.file_column : list
        self.output_folder : str


    def _load_data_file(self, table_name, file_path, columns):
        line_term = '\r\n' if platform.system() == 'Windows' else '\n'
        sql_cols = ", ".join(f"`{col}`" for col in columns)

        sql = f"""
        LOAD DATA LOCAL INFILE %s
        INTO TABLE `{table_name}`
        CHARACTER SET utf8mb4
        FIELDS TERMINATED BY ',' 
        ENCLOSED BY '"'
        LINES TERMINATED BY '{line_term}'
        IGNORE 1 LINES
        ({sql_cols});
        """

        with self.conn.cursor() as cursor:
            cursor.execute(sql, (file_path,))
        self.conn.commit()
        return self


    def load_data(self):
        for file in self.file_column:
            fname = file["file_name"]["full"]
            fpath = os.path.join(self.output_folder, fname)
            table_name = file["file_name"]["mysql_table"]
            print(f"📥 Loading: {fname}")
            columns = [col["mysql_column"] for col in file["columns"]]
            self._load_data_file(table_name, fpath, columns)
            print(f"✅ Loaded {fname}")
  
        return self



class Finalize:
    def __init__(self):
        self.conn : Connection
    def finalize(self):
        if self.conn: 
            self.conn.close()
            print("🔒 MySQL connection closed.")





class CSVtoMySQLController(EncodingPreprocessor, SchemaAnalyzer, MySQLSchemaCreate, MySQLLoadData, Finalize):
    def __init__(self, input_folder, output_folder, schema_name,
                 mysql_host="localhost", mysql_user="root", mysql_password="",
                 keyword_to_remove=None, keyword_position=None):

        self.input_folder : str = input_folder
        self.output_folder : str = output_folder
        self.schema_name : str = schema_name
        self.mysql_host : str = mysql_host
        self.mysql_user : str = mysql_user
        self.mysql_password : str = mysql_password
        self.keyword_to_remove : str = keyword_to_remove
        self.keyword_position : str = keyword_position



        self.conn : Connection = pymysql.connect(
            host=self.mysql_host,
            user=self.mysql_user,
            password=self.mysql_password,
            charset="utf8mb4",
            local_infile=True
        )

        self.schema_created : bool= False
        self.converted_files : list = []
        self.files_to_process : list = []
        self.file_column : list = []
        self.table_schema : OrderedDict = OrderedDict()






def main():
    controller = CSVtoMySQLController(
        input_folder="./data/input",
        output_folder="./data/utf8",
        schema_name="fao_all",
        mysql_host="localhost",
        mysql_user="root",
        mysql_password="1120",
        keyword_to_remove="_E_All_Data_(Normalized)",
        keyword_position="suffix"
    )

    controller.preprocess()
    controller.analyze_files()
    controller.create_schema()
    controller.load_data()
    controller.finalize()


if __name__ == "__main__":
    main()