import sqlite3
import pandas as pd
from .data_handlers import DataHandler
import json

class SQLiteHandler(DataHandler):
    def __init__(self, db_path: str):
        self.db_path = db_path

    def load_data(self, query: str) -> pd.DataFrame:
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(query, conn)

    def save_data(self, data: pd.DataFrame, table_name: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            data.to_sql(table_name, conn, if_exists='replace', index=False)

    def store_da_results(self, data: dict, table_name: str) -> None:
        if not data:
            raise ValueError("The data dictionary should contain at least one key-value pair.")
        
        description, result = next(iter(data.items()))       
        json_result = json.dumps(result)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE description = ?", (description,))
            exists = cursor.fetchone()[0]
            if exists:
                cursor.execute(f"UPDATE {table_name} SET result = ? WHERE description = ?", (json_result, description))
            else:
                cursor.execute(f"INSERT INTO {table_name} (description, result) VALUES (?, ?)", (description, json_result))
            conn.commit()