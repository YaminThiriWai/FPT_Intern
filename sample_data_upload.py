import pandas as pd
import sqlalchemy
from sqlalchemy import text
import os
from dotenv import load_dotenv
import logging
import sys
from pathlib import Path

class DataUploadProcessor:
    def __init__(self, table_name='AllData'):
        self.table_name = table_name
        self.engine = None
        self.df = None

        self._setup_logging()
        self._load_db_config()
        self._create_engine()

    def _setup_logging(self):
        logging.basicConfig(
            filename='excel_upload.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filemode='w'
        )
        self.logger = logging.getLogger(__name__)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(console_handler)

    def _load_db_config(self):
        load_dotenv()
        self.db_config = {
            'server': os.getenv('DB_SERVER', 'FIT-DB-PBI'),
            'database': os.getenv('DB_NAME', 'FPTCOM_Sua'),
            'username': os.getenv('DB_USER', 'sa'),
            'password': os.getenv('DB_PASSWORD', '123qwe!!@@##4%%')
        }

    def _create_engine(self):
        connection_string = (
            f"mssql+pyodbc:///?odbc_connect="
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.db_config['server']};"
            f"DATABASE={self.db_config['database']};"
            f"UID={self.db_config['username']};"
            f"PWD={self.db_config['password']};"
        )
        self.engine = sqlalchemy.create_engine(connection_string, fast_executemany=True)
        self.logger.info("Database engine created.")

    def upload_excel_file(self, filepath):
        """Reads an Excel file and uploads it to the SQL table."""
        if not Path(filepath).is_file():
            raise FileNotFoundError(f"The file {filepath} does not exist.")

        self.logger.info(f"Reading Excel file: {filepath}")
        self.df = pd.read_excel(filepath)

        if self.df.empty:
            self.logger.warning("Excel file is empty. Nothing to upload.")
            return

        self.logger.info(f"Read {len(self.df)} rows and {len(self.df.columns)} columns from the Excel file.")

        # Upload to SQL
        self.df.to_sql(
            name=self.table_name,
            con=self.engine,
            schema='dbo',
            if_exists='append',
            index=False,
            #method='multi',
            chunksize=1000
        )

        self.logger.info(f"Successfully uploaded {len(self.df)} rows to table '{self.table_name}'.")

    def cleanup(self):
        if self.engine:
            self.engine.dispose()
            self.logger.info("Database engine disposed.")


if __name__ == "__main__":
    excel_file_path = r"C:\Users\adminvm\Downloads\FPTCOM_Sua.xlsx"

    processor = DataUploadProcessor(table_name="AllData")

    try:
        processor.upload_excel_file(excel_file_path)
    except Exception as e:
        processor.logger.error(f"Excel upload failed: {e}")
    finally:
        processor.cleanup()
