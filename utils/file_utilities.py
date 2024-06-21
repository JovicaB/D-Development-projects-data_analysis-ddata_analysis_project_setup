import json
import os 
import sqlite3
from typing import Dict, List, Literal

import openpyxl 


class FileUtilities:
    
    @staticmethod
    def read_config_value(file_path: str, config_keys: List) -> str:
        """
        Reads a JSON file and returns the value associated with the provided list of keys.

        Parameters:
        file_path (str): The path to the JSON file.
        config_keys (list): A list of keys to retrieve the value for.

        Returns:
        The value corresponding to the nested keys in the JSON, or None if any key is not found.
        """
        with open(file_path, 'r') as file:
            data = json.load(file)
        
        value = data
        try:
            for key in config_keys:
                value = value[key]
            return value
        except KeyError:
            return None
        
    @staticmethod
    def file_exists(file_path: str) -> bool:
        """ Checks if a file exists at the specified path. 

        Args: 
            file_path (str): Relative path to the file. 
            
        Returns: bool: True if the file exists, False otherwise. """
        
        return os.path.exists(file_path)

    @staticmethod
    def excel_sheet_exists(file_path: str, sheet_name: str) -> bool:
        """ Checks if a specific sheet exists in an Excel file. 
        Args: 
            file_path (str): Relative path to the Excel file. 
            sheet_name (str): The name of the sheet to check for. 
            
        Returns: bool: True if the sheet exists, False otherwise. """ 
        try: 
            workbook = openpyxl.load_workbook(file_path) 
            return sheet_name in workbook.sheetnames 
        except FileNotFoundError: 
            print(f"File '{file_path}' not found.") 
            return False 
        except Exception as e: 
            print(f"An error occurred: {e}") 
            return False
        
    @staticmethod
    def clean_directory_root(directory_path: str) -> Literal["Directory has been cleaned"]:
        """
        Delete all files from a directory, leaving subdirectories intact.

        Args:
            directory_path (str): The path to the directory to clean.
        """
        try:
            # Iterate over all items in the directory
            for filename in os.listdir(directory_path):
                file_path = os.path.join(directory_path, filename)
                # Check if the path is a file
                if os.path.isfile(file_path):
                    os.remove(file_path)  
                    print("Directory has been cleaned")

        except Exception as e:
            print(f"An error occurred: {e}")        
                
    @staticmethod
    def create_database(database_file_path: str) -> Literal["Database has been created"]:
        conn = sqlite3.connect(database_file_path)
        conn.close()
        return "Database has been created"

    @staticmethod
    def create_datatable(database_file_path: str, datatable_name: str, datatable_info: Dict[str, str]) -> Literal["Datatable structure has been created"]:
        # Connect to the SQLite database
        conn = sqlite3.connect(database_file_path)
        cursor = conn.cursor()
        
        # Construct the CREATE TABLE SQL statement
        columns_definitions = ", ".join([f"{column_name} {column_type}" for column_name, column_type in datatable_info.items()])
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {datatable_name} ({columns_definitions});"
        
        # Execute the SQL statement
        cursor.execute(create_table_sql)
        
        # Commit the transaction and close the connection
        conn.commit()
        conn.close()
        
        return "Datatable structure has been created"




## USAGE ##
# class_instance = FileUtilities
# config_file_path = "data/misc/config..json"    
# excel_file_path = class_instance.read_config_value(config_file_path, ["data_setup", "raw_data_files_info", 'financial_statements', 'file_path'])

# # print(class_instance.file_exists(excel_file_path))

# sheet_name = class_instance.read_config_value(config_file_path, ["data_setup", "raw_data_files_info", 'financial_statements', 'sheet_name'])
# # print(class_instance.excel_sheet_exists(excel_file_path, sheet_name))

# clean_folder_path = class_instance.clean_directory_root("data")



# print(class_instance.read_config_value(config_file_path, ["data_setup", "raw_data_files_info", 'financial_statements', 'file_path']))