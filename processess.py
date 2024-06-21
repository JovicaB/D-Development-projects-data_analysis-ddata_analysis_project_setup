import datetime
import sqlite3
from typing import List, Union

import pandas as pd

from data_access import ExcelHandler, SQLiteHandler
from utils.file_utilities import FileUtilities


class ValidateRawDataFiles:
    def __init__(self, journal_file_path: List, journal_sheet_name: str, financial_reports_file_path: str, financial_reports_sheet_name: str) -> None:
        self.journal_file_path = journal_file_path
        self.journal_sheet_name = journal_sheet_name
        self.financial_reports_file_path = financial_reports_file_path
        self.financial_reports_sheet_name = financial_reports_sheet_name 

    def check_financial_reports_raw_data(self) -> bool:
        """
        Checks if the financial reports raw data file exists and contains the specified sheet.

        Returns:
            bool: True if the file exists and the sheet is valid, False otherwise.
        """
        return FileUtilities.file_exists(self.financial_reports_file_path) and FileUtilities.excel_sheet_exists(self.financial_reports_file_path, self.financial_reports_sheet_name)
    
    def check_journal_raw_data(self) -> bool:
        """
        Checks if the journal raw data file exists and contains the specified sheet.

        Returns:
            bool: True if the file exists and the sheet is valid, False otherwise.
        """
        for path in self.journal_file_path:
            if not FileUtilities.file_exists(path) or not FileUtilities.excel_sheet_exists(path, self.journal_sheet_name):
                return False
        return True

    def start_process(self) -> bool:
        """
        Verifies that both the journal and financial reports raw data files are valid.

        Returns:
            bool: True if both journal and financial reports raw data files are valid, False otherwise.
        """
        return self.check_journal_raw_data() and self.check_financial_reports_raw_data()


## USAGE ##
# journal_file_path = ["data/raw_data/journal_data_2022.xlsx", "data/raw_data/journal_data_2023.xlsx"]
# journal_sheet_name = "DK"
# financial_reports_file_path = "data/raw_data/financial_statements_data.xlsx"
# financial_reports_sheet_name = "FI"

# class_instance = ValidateRawDataFiles(journal_file_path, journal_sheet_name, financial_reports_file_path, financial_reports_sheet_name)
# print(class_instance.start_process())


class SetDataEnviroment:
    def __init__(self, database_path: str, fr_datatable: str, fr_table_info: str, journal_datatable: str, journal_table_info: str) -> None:
        self.file_utils_class_instance = FileUtilities
        self.data_path = "data"

        self.database_path = database_path
        self.fr_datatable = fr_datatable
        self.fr_table_info = fr_table_info
        self.journal_datatable = journal_datatable
        self.journal_table_info = journal_table_info

    def clean_data_folder(self):
        self.file_utils_class_instance.clean_directory_root(self.data_path)


    def create_database(self):
            self.file_utils_class_instance.create_database(self.database_path)

            # journal datatable
            self.file_utils_class_instance.create_datatable(self.database_path, self.fr_datatable, self.fr_table_info)

            # financial reports datatable
            self.file_utils_class_instance.create_datatable(self.database_path, self.journal_datatable, self.journal_table_info)

    def start_process(self):
        self.clean_data_folder()
        self.create_database()

        return "Data envoroment has been created"


## USAGE ##
# class_instance = SetOutputDataEnviroment()
# class_instance.start_process()


class RawDataLoader:
    def __init__(self, file_path: Union[str, List[str]], sheet_name: str) -> pd.DataFrame:
        self.file_path = file_path
        self.sheet_name = sheet_name
        self.excel_loader_class_instance = ExcelHandler(self.file_path)

    def load_fs_data(self):
        dtype = {"AOP": str}
        return self.excel_loader_class_instance.load_data(self.sheet_name, dtype)
    
    def load_journal_data(self):
        data = pd.DataFrame()   

        if isinstance(self.file_path, list):
            for file in self.file_path:
                data_temp = ExcelHandler(file).load_data(self.sheet_name)
                if isinstance(data_temp, pd.DataFrame): 
                    data = pd.concat([data, data_temp], ignore_index=True) 
                else:
                    print("Data from file {} is not a DataFrame".format(file))
        else:
            data = self.excel_loader_class_instance.load_data(self.sheet_name)

        return data


# journal_data = RawDataLoader(["data/raw_data/journal_data_2022.xlsx", "data/raw_data/journal_data_2023.xlsx"], "DK").load_journal_data()


class CleanData:
    def __init__(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        self.raw_data = raw_data

    @staticmethod
    def is_value(value: float) -> bool:
        if pd.isna(value):
            return False
        try:
            float(value)
            return True
        except ValueError:
            return False
        
    @staticmethod
    def is_date(value) -> bool:
        return isinstance(value, (datetime.date, datetime.datetime))

    def remove_rows_with_wrong_values(self):
        data_frame_values = self.raw_data.iloc[:, -2:]
        valid_rows = data_frame_values.map(CleanData.is_value).all(axis=1)
        self.raw_data = self.raw_data[valid_rows]
        return self.raw_data

    def remove_rows_with_wrong_dates(self):
        data_frame_dates = self.raw_data.iloc[:, 0]
        valid_rows = data_frame_dates.apply(CleanData.is_date)
        self.raw_data = self.raw_data[valid_rows]
        return self.raw_data
       
    def start_process(self):
        self.remove_rows_with_wrong_values()
        self.remove_rows_with_wrong_dates()
        return self.raw_data
    
# print(CleanData(journal_data).data_cleaning_process())


class StoreCleanedData:
    def __init__(self, sql_file_path: str, sql_datatable: str, data: pd.DataFrame) -> None:
        self.sql_datatable = sql_datatable
        self.data = data
        self.data_storage_class_instance = SQLiteHandler(sql_file_path)

    def store_cleaned_data_process(self):
            try:
                self.data_storage_class_instance.save_data(self.data, self.sql_datatable)
                return "Data stored successfully"
            except sqlite3.DatabaseError as db_err:
                return f"Database error occurred: {db_err}"
            except pd.errors.EmptyDataError as ed_err:
                return f"Data error occurred: {ed_err}"
            except Exception as e:
                return f"An unexpected error occurred: {e}"