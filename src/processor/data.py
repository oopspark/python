import pandas as pd
from sqlalchemy import create_engine, text

tempsql = r"C:\Users\parkj\Documents\workspace\my_projects\code\temp\temp.sql"


class PostgreSQLDB:
    def __init__(
        self, host="localhost", database="", user="root", password="1120", port="3306"
    ):
        self.db_url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        self.engine = create_engine(self.db_url)
        print("Database engine created.")

    def exedf(self, query=None, sql_path=None):
        if sql_path is None:
            sql_path = tempsql
        if query is None:
            with open(sql_path, "r", encoding="utf-8") as file:
                query = text(file.read())
        else:
            query = text(query)
        with self.engine.connect() as conn:
            result = conn.execute(query)
            columns = result.keys()
            data = result.fetchall()
            df = pd.DataFrame(data, columns=columns)
        print("Query executed successfully.")
        return df

    def runsql(self, query=None, sql_path=None):
        if sql_path is None:
            sql_path = tempsql
        try:
            if query is None:
                with open(sql_path, "r", encoding="utf-8") as file:
                    query = text(file.read())
            else:
                query = text(query)
            print("🔍 Executing SQL from file:")
            print("────────────────────────────────────")
            print(query)
            print("────────────────────────────────────")
            with self.engine.connect() as conn:
                conn.execute(query)

            print(f"✅ SQL from '{sql_path}' executed successfully.")

        except Exception as e:
            print(f"❌ Error executing SQL file '{sql_path}': {e}")


def df_column_snake_case(df):
    """
    Convert DataFrame column names to snake_case.
    """
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]
    return df


if __name__ == "__main__":
    db = PostgreSQLDB()
    df = db.exedf()  # SQL 파일 경로를 지정하려면 exedf(sql_path="./myquery.sql")
    print(df)
