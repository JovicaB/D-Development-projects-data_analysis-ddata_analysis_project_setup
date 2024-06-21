import pandas as pd
from .data_handlers import DataHandler


class ExcelHandler(DataHandler):
    def __init__(self, file_path: str):
        self.file_path = file_path

    def load_data(self, sheet_name: str = None, dtype: dict = None) -> pd.DataFrame:
        return pd.read_excel(self.file_path, sheet_name=sheet_name, dtype=dtype)

    def save_data(self, data: pd.DataFrame, sheet_name: str = 'Sheet1') -> None:
        with pd.ExcelWriter(self.file_path, mode='w') as writer:
            data.to_excel(writer, sheet_name=sheet_name, index=False)


# Using ExcelHandler
# excel_handler = ExcelHandler('data/raw_data/financial_statements_data.xlsx')
# dtype = {"AOP": str}
# df = excel_handler.load_data('FI', dtype)
# print(df)
# excel_handler.save_data(df, 'Sheet1')