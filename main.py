from processess import ValidateRawDataFiles, SetDataEnviroment, RawDataLoader, CleanData, StoreCleanedData
from utils.file_utilities import FileUtilities

RAW_DATA_INFO = None
INPUT_CONTAINER_INFO = None


def main():
    
    ## Step 0. Set global variables
    config_file_path = "data/config/setup.json"

    raw_data_info = FileUtilities.read_config_value(config_file_path, ['data_setup', 'raw_data_files_info'])
    input_data_info = FileUtilities.read_config_value(config_file_path, ['data_setup', 'input_container_info'])

    # variables:
    journal_file_path = raw_data_info['journal']['file_path']
    journal_sheet_name = raw_data_info['journal']['sheet_name']
    financial_reports_file_path = raw_data_info['financial_statements']['file_path']
    financial_reports_sheet_name = raw_data_info['financial_statements']['sheet_name']

    database_path = input_data_info['file_path']
    fr_datatable = input_data_info['datatables']['fs_table_name']
    fr_table_info = input_data_info['datatables']['fs_datatable_columns']
    journal_datatable = input_data_info['datatables']['journal_table_name']
    journal_table_info = input_data_info['datatables']['je_datatable_columns']
    print("Data access variables are set")

    try:
        # Step 1. Validate Raw Data Files
        if not ValidateRawDataFiles(journal_file_path, journal_sheet_name, financial_reports_file_path, financial_reports_sheet_name).start_process():
            print("Data validation failed")
            return
        print("Data has been validated")

        # Step 2. Set data environment
        SetDataEnviroment(database_path, fr_datatable, fr_table_info, journal_datatable, journal_table_info).start_process()
        print("Data environment has been set")

        # Step 3. Load raw data
        fs_data = RawDataLoader(financial_reports_file_path, financial_reports_sheet_name).load_fs_data()
        journal_data = RawDataLoader(journal_file_path, journal_sheet_name).load_journal_data()
        print("Raw data has been loaded")

        # Step 4. Clean Data
        journal_cleaned_data = CleanData(journal_data).start_process()
        print("Journal data has been cleaned")

        # Step 5. Store Clean Data
        StoreCleanedData(database_path, fr_datatable, fs_data).store_cleaned_data_process()
        print("Financial statements data has been stored and it is ready for data analysis")

        StoreCleanedData(database_path, journal_datatable, journal_cleaned_data).store_cleaned_data_process()
        print("Clean journal data has been stored and it is ready for data analysis")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()