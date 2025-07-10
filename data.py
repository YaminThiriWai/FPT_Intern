import pandas as pd
import sqlalchemy
from sqlalchemy import text
import os
from dotenv import load_dotenv
import logging
import sys
import numpy as np
from pathlib import Path

class DataUploadProcessor:
    def __init__(self, table_name='FPTCOM_Sua'):
        self.table_name = table_name
        self.engine = None
        self.df = None

        self.expected_columns = [
            'Name', 'Server', 'Storage', 'Job Type', 'Job Status',
            'Percent Complete', 'Start Time', 'End Time', 'Elapsed Time',
            'Byte Count', 'Job Rate', 'Error Code', 'Deduplication Ratio',
            'Selections', 'Backup Method', 'Calculate Elapsed Time', 'System',
            'ElapsedTimeSeconds', 'ByteCountBytes', 'JobRateBytesPerSec',
            'CalculatedElapsedTimeSeconds'
        ]

        self._setup_logging()
        self._load_db_config()
        self._create_engine()

    def _setup_logging(self):
        logging.basicConfig(
            filename='manual_upload_test.log',
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

    def insert_manual_data(self):
        """Manually define test data"""
        data = [
            {
                'Name': 'HO_DB_Quet Xe-Duplicate _ INCREMENT', 'Server': '10.86.108.244', 'Storage': 'FISHCM_Site 2_186.34_DB Quet Xe',
                'Job Type': 'Duplicate', 'Job Status': 'Completed', 'Percent Complete': 1.0, 'Start Time': pd.Timestamp('2025-07-01 10:00:00'),
                'End Time': pd.Timestamp('2025-07-01 10:30:00'), 'Elapsed Time': '0:00:44',
                'Byte Count': 1234567890, 'Job Rate': '10 MB/min', 'Error Code': 'E00123',
                'Deduplication Ratio': '', 'Selections': '\\10.86.30.201\E: - (Partially selected)', 'Backup Method': 'Increment',
                'Calculate Elapsed Time': '0:0:30:00', 'System': 'NEW',
                'ElapsedTimeSeconds': 1800, 'ByteCountBytes': 1234567890,
                'JobRateBytesPerSec': 174762.6666, 'CalculatedElapsedTimeSeconds': 1800
            },
            {
                'Name': 'TestJob2', 'Server': 'Server2', 'Storage': 'StorageB', 'Job Type': 'Incremental',
                'Job Status': 'Running', 'Percent Complete': 50.0, 'Start Time': pd.Timestamp('2025-07-02 12:00:00'),
                'End Time': pd.Timestamp('2025-07-02 12:20:00'), 'Elapsed Time': '00:20:00',
                'Byte Count': 987654321, 'Job Rate': '5 MB/min', 'Error Code': None,
                'Deduplication Ratio': '1.5:1', 'Selections': 'D:\\Backup', 'Backup Method': 'Incremental',
                'Calculate Elapsed Time': '0:0:20:00', 'System': 'NEW',
                'ElapsedTimeSeconds': 1200, 'ByteCountBytes': 987654321,
                'JobRateBytesPerSec': 87381.3333, 'CalculatedElapsedTimeSeconds': 1200
            }
        ]

        self.df = pd.DataFrame(data)
        self.logger.info("Manual test data created.")

    def upload_data(self):
        if self.df is None or self.df.empty:
            raise ValueError("No data to upload")

        dtype_mapping = {
            'Name': sqlalchemy.types.NVARCHAR(255),
            'Server': sqlalchemy.types.NVARCHAR(100),
            'Storage': sqlalchemy.types.NVARCHAR(100),
            'Job Type': sqlalchemy.types.NVARCHAR(100),
            'Job Status': sqlalchemy.types.NVARCHAR(100),
            'Percent Complete': sqlalchemy.types.Float,
            'Start Time': sqlalchemy.types.DateTime,
            'End Time': sqlalchemy.types.DateTime,
            'Elapsed Time': sqlalchemy.types.NVARCHAR(50),
            'Byte Count': sqlalchemy.types.BigInteger,
            'Job Rate': sqlalchemy.types.NVARCHAR(50),
            'Error Code': sqlalchemy.types.NVARCHAR(50),
            'Deduplication Ratio': sqlalchemy.types.NVARCHAR(50),
            'Selections': sqlalchemy.types.NVARCHAR(255),
            'Backup Method': sqlalchemy.types.NVARCHAR(20),
            'Calculate Elapsed Time': sqlalchemy.types.NVARCHAR(50),
            'System': sqlalchemy.types.NVARCHAR(10),
            'ElapsedTimeSeconds': sqlalchemy.types.BigInteger,
            'ByteCountBytes': sqlalchemy.types.BigInteger,
            'JobRateBytesPerSec': sqlalchemy.types.Float,
            'CalculatedElapsedTimeSeconds': sqlalchemy.types.BigInteger
        }

        self.df.to_sql(
            name=self.table_name,
            con=self.engine,
            schema='dbo',
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000,
            dtype=dtype_mapping
        )

        self.logger.info(f"Uploaded {len(self.df)} rows successfully.")

    def cleanup(self):
        if self.engine:
            self.engine.dispose()
            self.logger.info("Database engine disposed.")


if __name__ == "__main__":
    processor = DataUploadProcessor("FPTCOM_Sua")

    try:
        processor.insert_manual_data()
        processor.upload_data()
    except Exception as e:
        processor.logger.error(f"Manual upload failed: {e}")
    finally:
        processor.cleanup()
