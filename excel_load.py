import pandas as pd
import sqlalchemy
from sqlalchemy import text
import os
from dotenv import load_dotenv
import logging
import sys
from pathlib import Path
import numpy as np
import warnings
import re
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

class OptimizedDataUploadProcessor:
    """
    Optimized data upload processor with robust null handling and reliable SQL insertion.
    Combines best practices from both original scripts for maximum reliability.
    """
    
    def __init__(self, excel_path: str, table_name: str = 'FPTCOM_Sua'):
        self.excel_path = Path(excel_path)
        self.table_name = table_name
        self.engine = None
        self.df = None
        self.sql_columns = None
        
        # Define expected columns in correct order
        self.expected_columns = [
            'Name', 'Server', 'Storage', 'Job Type', 'Job Status', 
            'Percent Complete', 'Start Time', 'End Time', 'Elapsed Time', 
            'Byte Count', 'Job Rate', 'Error Code', 'Deduplication Ratio', 
            'Selections', 'Backup Method', 'Calculate Elapsed Time', 'System',
            'ElapsedTimeSeconds', 'ByteCountBytes', 'JobRateBytesPerSec', 
            'CalculatedElapsedTimeSeconds'
        ]
        
        # Comprehensive null representations (from first script)
        self.nan_representations = {
            'nan', 'null', 'none', 'n/a', 'na', '#n/a', '#null!', '',
            'missing', 'unknown', '-', 'n.a.', 'n/a.', 'na.', 'nil'
        }

        self._setup_logging()
        self._load_db_config()
        self._create_engine()

    def _setup_logging(self):
        """Setup comprehensive logging"""
        log_filename = f'data_upload_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        logging.basicConfig(
            filename=log_filename,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filemode='w'
        )
        self.logger = logging.getLogger(__name__)
        
        # Add console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def _load_db_config(self):
        """Load database configuration"""
        load_dotenv()
        self.db_config = {
            'server': os.getenv('DB_SERVER', 'FIT-DB-PBI'),
            'database': os.getenv('DB_NAME', 'FPTCOM_Sua'),
            'username': os.getenv('DB_USER', 'sa'),
            'password': os.getenv('DB_PASSWORD', '123qwe!!@@##4%%')
        }

        missing = [k for k in self.db_config if not self.db_config[k]]
        if missing:
            raise ValueError(f"Missing DB config: {missing}")

    def _create_engine(self):
        """Create SQLAlchemy engine with optimal settings"""
        try:
            connection_string = (
                f"mssql+pyodbc:///?odbc_connect="
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.db_config['server']};"
                f"DATABASE={self.db_config['database']};"
                f"UID={self.db_config['username']};"
                f"PWD={self.db_config['password']};"
            )
            self.engine = sqlalchemy.create_engine(
                connection_string, 
                fast_executemany=True,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            self.logger.info("DB engine created successfully")
        except Exception as e:
            self.logger.error(f"Engine creation failed: {e}")
            raise

    def clean_nan_values(self, value):
        """
        Comprehensive NaN cleaning (from first script - proven to work)
        Convert various representations of NaN/null to None for database consistency
        """
        if pd.isna(value) or pd.isnull(value):
            return None
        
        str_val = str(value).strip().lower()
        
        # Check for empty or whitespace-only strings
        if re.match(r'^\s*$', str_val):
            return None
        
        # Check against known null representations
        if str_val in self.nan_representations:
            return None
        
        return value

    def load_excel_data(self):
        """Load Excel data with error handling"""
        try:
            if not self.excel_path.exists():
                raise FileNotFoundError(f"File not found: {self.excel_path}")
            
            self.df = pd.read_excel(self.excel_path, engine='openpyxl')
            self.logger.info(f"Loaded Excel: {len(self.df)} rows, {len(self.df.columns)} columns")
            
            # Log column names
            self.logger.info(f"Excel columns: {self.df.columns.tolist()}")
            
            return self
        except Exception as e:
            self.logger.error(f"Excel load failed: {e}")
            raise

    def get_sql_table_schema(self):
        """Get SQL table schema for proper data type mapping"""
        try:
            self.sql_columns = pd.read_sql(
                f"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE "
                f"FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{self.table_name}'",
                self.engine
            ).set_index('COLUMN_NAME').to_dict('index')
            
            self.logger.info(f"SQL table columns: {list(self.sql_columns.keys())}")
            return self
        except Exception as e:
            self.logger.error(f"Failed to get SQL schema: {e}")
            raise

    def clean_and_transform_data(self):
        """Comprehensive data cleaning and transformation"""
        if self.df is None:
            raise ValueError("Load data first")

        self.logger.info("Starting comprehensive data cleaning and transformation")
        
        # Clean all NaN values across entire DataFrame (from first script)
        self._clean_all_nan_values()

        # detect and fix misplaced "Backup Method" values in the Selections column
        self._correct_misplaced_backup_method()

        # moving the cell which belongs to Selections column
        self._fix_partially_selected_in_deduplication_ratio()
        
        # Filter System = 'new' if System column exists
        self._filter_system_new()
        
        # Get SQL schema for proper type mapping
        self.get_sql_table_schema()
        
        # Validate columns against SQL table
        self._validate_columns()
        
        # Transform data types based on SQL schema
        self._transform_data_types()
        
        # Calculate derived columns
        self._calculate_derived_columns()
        
        # Ensure column order
        self._ensure_column_order()
        
        # Final cleanup
        self._final_cleanup()

        self.logger.info("Data cleaning and transformation completed")
        return self

    def _clean_all_nan_values(self):
        """Clean all NaN values across the entire DataFrame"""
        self.logger.info("Cleaning NaN values across all columns...")
        
        total_cleaned = 0
        for col in self.df.columns:
            # Count NaN representations before cleaning
            nan_count_before = self.df[col].apply(
                lambda x: (pd.isna(x) or 
                          re.match(r'^\s*$', str(x).strip()) or 
                          str(x).strip().lower() in self.nan_representations)
            ).sum()
            
            # Apply cleaning
            self.df[col] = self.df[col].apply(self.clean_nan_values)
            
            # Count actual NaN values after cleaning
            nan_count_after = self.df[col].isna().sum()
            
            if nan_count_before > 0:
                cleaned_count = nan_count_before - nan_count_after
                if cleaned_count > 0:
                    self.logger.info(f"{col}: Converted {cleaned_count} NaN representations to NULL")
                    total_cleaned += cleaned_count
        
        self.logger.info(f"Total NaN representations cleaned: {total_cleaned}")

    def _filter_system_new(self):
        """Filter rows to keep only System = 'new' (case-insensitive)"""
        if 'System' in self.df.columns:
            initial_count = len(self.df)
            
            # Log System column value counts
            system_counts = self.df['System'].value_counts(dropna=False).to_dict()
            self.logger.info(f"System column value counts before filtering: {system_counts}")
            
            # Normalize and filter
            self.df['System'] = self.df['System'].apply(
                lambda x: str(x).strip().lower() if pd.notnull(x) else None
            )
            self.df = self.df[self.df['System'] == 'new']
            
            filtered_count = len(self.df)
            dropped_count = initial_count - filtered_count
            
            self.logger.info(f"System filtering: Kept {filtered_count} rows, dropped {dropped_count} rows")
            
            if self.df.empty:
                raise ValueError("No rows remain after filtering System='new'")
        else:
            self.logger.warning("System column not found - skipping system filtering")

    def _validate_columns(self):
        """Validate Excel columns against SQL table"""
        if self.sql_columns is None:
            raise ValueError("SQL schema not loaded")
        
        excel_cols = self.df.columns.tolist()
        sql_cols = list(self.sql_columns.keys())
        
        # Find missing and extra columns
        missing_in_sql = [col for col in excel_cols if col not in sql_cols]
        valid_cols = [col for col in excel_cols if col in sql_cols]
        
        if missing_in_sql:
            self.logger.warning(f"Excel columns not in SQL table: {missing_in_sql}")
        
        if not valid_cols:
            raise ValueError("No valid columns match between Excel and SQL table")
        
        # Keep only valid columns
        self.df = self.df[valid_cols]
        self.logger.info(f"Validated columns: {len(valid_cols)} columns retained")

    def _fix_partially_selected_in_deduplication_ratio(self):
        """
        Move values like 'Partially Selected' from Deduplication Ratio to Selections
        """
        if 'Deduplication Ratio' in self.df.columns and 'Selections' in self.df.columns:
            mask = self.df['Deduplication Ratio'].astype(str).str.contains('Partially Selected', case=False, na=False)
            affected_rows = mask.sum()

            if affected_rows > 0:
                self.logger.info(f"Fixing {affected_rows} rows with 'Partially Selected' in Deduplication Ratio")

                # Move value to Selections if Selections is empty
                self.df.loc[mask & self.df['Selections'].isna(), 'Selections'] = self.df.loc[mask, 'Deduplication Ratio']

                # Clear Deduplication Ratio
                self.df.loc[mask, 'Deduplication Ratio'] = None

    def _transform_data_types(self):
        """Transform data types based on SQL schema (from first script logic)"""
        self.logger.info("Transforming data types based on SQL schema...")
        
        dtype_mapping = {}
        
        for col in self.df.columns:
            if col not in self.sql_columns:
                continue
                
            sql_info = self.sql_columns[col]
            sql_type = sql_info['DATA_TYPE'].lower()
            max_len = sql_info['CHARACTER_MAXIMUM_LENGTH']
            
            self.logger.info(f"Processing column {col} (SQL type: {sql_type})")
            
            # Handle different SQL data types
            if sql_type in ['float', 'real', 'numeric', 'decimal']:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
                dtype_mapping[col] = sqlalchemy.types.FLOAT
                
            elif sql_type in ['int', 'bigint', 'smallint', 'tinyint']:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce').astype('Int64')
                dtype_mapping[col] = sqlalchemy.types.BIGINT if sql_type == 'bigint' else sqlalchemy.types.INTEGER
                
            elif sql_type in ['datetime', 'datetime2', 'smalldatetime']:
                self._transform_datetime_column(col)
                dtype_mapping[col] = sqlalchemy.types.DATETIME
                
            elif sql_type in ['nvarchar', 'varchar', 'char', 'nchar']:
                self._transform_string_column(col, max_len)
                if max_len and max_len > 0:
                    dtype_mapping[col] = sqlalchemy.types.NVARCHAR(length=int(max_len))
                else:
                    dtype_mapping[col] = sqlalchemy.types.TEXT
                    
            elif sql_type == 'bit':
                self._transform_bit_column(col)
                dtype_mapping[col] = sqlalchemy.types.BOOLEAN
                
            else:
                # Default handling
                self._transform_string_column(col, 255)
                dtype_mapping[col] = sqlalchemy.types.NVARCHAR(length=255)
                self.logger.warning(f"Unknown SQL type {sql_type} for {col}, treating as NVARCHAR(255)")
        
        self.dtype_mapping = dtype_mapping
        self.logger.info("Data type transformation completed")

    def _transform_datetime_column(self, col):
        """Transform datetime column with multiple format attempts"""
        datetime_formats = [
            '%m/%d/%Y %I:%M:%S %p',
            '%Y-%m-%d %H:%M:%S',
            '%d-%m-%Y %H:%M:%S',
            '%Y-%m-%d',
            '%m/%d/%Y',
            '%d/%m/%Y'
        ]
        
        original_col = self.df[col].copy()
        self.df[col] = pd.NaT
        
        # Try each format
        for fmt in datetime_formats:
            mask = self.df[col].isna()
            if mask.sum() == 0:  # All values converted
                break
            try:
                self.df.loc[mask, col] = pd.to_datetime(
                    original_col[mask], 
                    format=fmt, 
                    errors='coerce'
                )
            except:
                continue
        
        # Final attempt with dateutil parser
        mask = self.df[col].isna()
        if mask.sum() > 0:
            with warnings.catch_warnings():
                warnings.filterwarnings('ignore', category=UserWarning)
                self.df.loc[mask, col] = pd.to_datetime(
                    original_col[mask], 
                    errors='coerce'
                )
        
        invalid_count = self.df[col].isna().sum()
        if invalid_count > 0:
            self.logger.warning(f"{col}: {invalid_count} invalid datetime values")

    def _transform_string_column(self, col, max_len):
        """Transform string column with length limits"""
        if max_len and max_len > 0:
            max_len = int(max_len)
            self.df[col] = self.df[col].apply(
                lambda x: str(x)[:max_len] if pd.notnull(x) else None
            )
        else:
            self.df[col] = self.df[col].apply(
                lambda x: str(x) if pd.notnull(x) else None
            )

    def _transform_bit_column(self, col):
        """Transform bit/boolean column"""
        def convert_to_bit(x):
            if pd.isna(x):
                return None
            str_val = str(x).strip().lower()
            if str_val in ['true', '1', 'yes', 'y', 'on']:
                return 1
            elif str_val in ['false', '0', 'no', 'n', 'off']:
                return 0
            else:
                return None
        
        self.df[col] = self.df[col].apply(convert_to_bit)

    def _calculate_derived_columns(self):
        """Calculate derived columns with robust error handling"""
        self.logger.info("Calculating derived columns...")
        
        # ElapsedTimeSeconds
        self.df['ElapsedTimeSeconds'] = self.df.get('Elapsed Time', pd.Series()).apply(
            self._parse_elapsed_time_to_seconds
        )
        
        # ByteCountBytes
        self.df['ByteCountBytes'] = self.df.get('Byte Count', pd.Series()).apply(
            self._parse_byte_count_to_bytes
        )
        
        # JobRateBytesPerSec
        self.df['JobRateBytesPerSec'] = self.df.get('Job Rate', pd.Series()).apply(
            self._parse_job_rate_to_bytes_per_sec
        )
        
        # CalculatedElapsedTimeSeconds
        self.df['CalculatedElapsedTimeSeconds'] = self.df.get('Calculate Elapsed Time', pd.Series()).apply(
            self._parse_calculated_elapsed_time_to_seconds
        )

    def _parse_elapsed_time_to_seconds(self, time_str):
        """Parse elapsed time string to seconds"""
        try:
            if pd.isna(time_str) or time_str is None:
                return None
            
            time_str = str(time_str).strip()
            if not time_str:
                return None
            
            # Handle HH:MM:SS format
            if re.match(r'^\d{1,2}:\d{2}:\d{2}$', time_str):
                parts = time_str.split(':')
                hours = int(parts[0])
                minutes = int(parts[1])
                seconds = int(parts[2])
                return hours * 3600 + minutes * 60 + seconds
            
            return None
        except Exception as e:
            self.logger.debug(f"Error parsing elapsed time '{time_str}': {e}")
            return None

    def _parse_byte_count_to_bytes(self, byte_str):
        """Parse byte count string to bytes"""
        try:
            if pd.isna(byte_str) or byte_str is None:
                return None
            
            byte_str = str(byte_str).strip().upper()
            if not byte_str:
                return None
            
            # Try parsing as plain number first
            try:
                return int(float(byte_str))
            except ValueError:
                pass
            
            # Parse with units
            parts = byte_str.split()
            if len(parts) == 2:
                num_str, unit = parts
                num = float(num_str)
                
                multipliers = {
                    'BYTES': 1, 'BYTE': 1, 'B': 1,
                    'KB': 1024, 'MB': 1024**2, 'GB': 1024**3, 'TB': 1024**4
                }
                
                multiplier = multipliers.get(unit, None)
                if multiplier:
                    return int(num * multiplier)
            
            return None
        except Exception as e:
            self.logger.debug(f"Error parsing byte count '{byte_str}': {e}")
            return None

    def _parse_job_rate_to_bytes_per_sec(self, rate_str):
        """Parse job rate string to bytes per second"""
        try:
            if pd.isna(rate_str) or rate_str is None:
                return None
            
            rate_str = str(rate_str).strip().upper()
            if not rate_str:
                return None
            
            parts = rate_str.split()
            if len(parts) == 2:
                num_str, unit = parts
                num = float(num_str)
                
                # Handle different rate units
                if '/MIN' in unit:
                    base_unit = unit.replace('/MIN', '')
                    multipliers = {'BYTES': 1, 'KB': 1024, 'MB': 1024**2, 'GB': 1024**3}
                    multiplier = multipliers.get(base_unit, 1)
                    return (num * multiplier) / 60  # Convert per minute to per second
                elif '/SEC' in unit:
                    base_unit = unit.replace('/SEC', '')
                    multipliers = {'BYTES': 1, 'KB': 1024, 'MB': 1024**2, 'GB': 1024**3}
                    multiplier = multipliers.get(base_unit, 1)
                    return num * multiplier
            
            return None
        except Exception as e:
            self.logger.debug(f"Error parsing job rate '{rate_str}': {e}")
            return None

    def _parse_calculated_elapsed_time_to_seconds(self, time_str):
        """Parse calculated elapsed time to seconds"""
        try:
            if pd.isna(time_str) or time_str is None:
                return None
            
            time_str = str(time_str).strip()
            if not time_str:
                return None
            
            parts = time_str.split(':')
            if len(parts) == 4:  # D:HH:MM:SS
                days, hours, minutes, seconds = map(int, parts)
                return days * 86400 + hours * 3600 + minutes * 60 + seconds
            elif len(parts) == 3:  # HH:MM:SS
                hours, minutes, seconds = map(int, parts)
                return hours * 3600 + minutes * 60 + seconds
            
            return None
        except Exception as e:
            self.logger.debug(f"Error parsing calculated elapsed time '{time_str}': {e}")
            return None

    def _ensure_column_order(self):
        """Ensure DataFrame has all expected columns in correct order"""
        # Add missing columns
        for col in self.expected_columns:
            if col not in self.df.columns:
                self.df[col] = None
                self.logger.warning(f"Added missing column: {col}")
        
        # Remove extra columns
        extra_cols = [col for col in self.df.columns if col not in self.expected_columns]
        if extra_cols:
            self.df = self.df.drop(columns=extra_cols)
            self.logger.warning(f"Removed extra columns: {extra_cols}")
        
        # Reorder columns
        self.df = self.df[self.expected_columns]
        self.logger.info(f"DataFrame reordered to {len(self.df.columns)} expected columns")

    def _correct_misplaced_backup_method(self):
        """
        Move misplaced backup method values from 'Selections' to 'Backup Method'.
        Typical misplaced values: 'full', 'incremental', 'differential'
        """
        if 'Selections' not in self.df.columns or 'Backup Method' not in self.df.columns:
            self.logger.warning("Selections or Backup Method column not found — skipping misplaced correction")
            return

        backup_methods = ['full', 'incremental', 'differential']

        # Normalize Selections column
        self.df['Selections'] = self.df['Selections'].apply(
            lambda x: str(x).strip().lower() if pd.notnull(x) else None
        )

        self.df['Backup Method'] = self.df['Backup Method'].apply(
            lambda x: str(x).strip().lower() if pd.notnull(x) else None
        )

        moved_count = 0
        for method in backup_methods:
            mask = self.df['Selections'] == method
            if mask.any():
                # Move to Backup Method only if it's empty
                self.df.loc[mask & self.df['Backup Method'].isnull(), 'Backup Method'] = method
                self.df.loc[mask, 'Selections'] = None
                moved_count += mask.sum()

        if moved_count > 0:
            self.logger.info(f"Moved {moved_count} misplaced backup method values from 'Selections' to 'Backup Method'")


    def _final_cleanup(self):
        """Final data cleanup before upload"""
        # Replace any remaining pandas NaT, NaN with None
        self.df = self.df.where(pd.notnull(self.df), None)
        
        # Log final statistics
        for col in self.df.columns:
            null_count = self.df[col].isnull().sum()
            null_percent = (null_count / len(self.df)) * 100 if len(self.df) > 0 else 0
            self.logger.info(f"Final {col}: {null_count} NULL values ({null_percent:.1f}%)")

    def truncate_table(self):
        """Truncate the target table"""
        try:
            with self.engine.begin() as conn:
                # Get initial row count
                initial_count = pd.read_sql(
                    f"SELECT COUNT(*) FROM dbo.{self.table_name}", 
                    conn
                ).iloc[0, 0]
                
                # Truncate table
                conn.execute(text(f"TRUNCATE TABLE dbo.{self.table_name}"))
                
                self.logger.info(f"Truncated table dbo.{self.table_name} (had {initial_count} rows)")
                
            return self
        except Exception as e:
            self.logger.error(f"Truncate failed: {e}")
            raise

    def upload_data(self):
        """Upload data to SQL Server with comprehensive error handling"""
        if self.df is None or self.df.empty:
            raise ValueError("No data to upload")
        
        try:
            self.logger.info(f"Starting upload of {len(self.df)} rows to {self.table_name}")
            
            # Log sample data for debugging
            self.logger.info("Sample data being uploaded:")
            for col in self.df.columns[:5]:  # Log first 5 columns
                sample_vals = self.df[col].dropna().head(3).tolist()
                self.logger.info(f"  {col}: {sample_vals}")
            
            # Upload with error handling
            self.df.to_sql( #self.df.head(10).to_sql - to test 10 rows
                name=self.table_name,
                con=self.engine,
                schema='dbo',
                if_exists='append',
                index=False,
                chunksize=1000, # for loading the whole data from excel file, uncomment if want to test a few rows
                dtype=getattr(self, 'dtype_mapping', None)
            )


            self.logger.info(f"Successfully uploaded {len(self.df)} rows to {self.table_name}")
            
            # Verify upload
            self._verify_upload()
            
        except Exception as e:
            self.logger.error(f"Upload failed: {e}")
            self.logger.error("DataFrame info for debugging:")
            self.logger.error(f"Shape: {self.df.shape}")
            self.logger.error(f"Columns: {self.df.columns.tolist()}")
            self.logger.error(f"Data types: {self.df.dtypes.to_dict()}")
            raise

    def _verify_upload(self):
        """Verify the upload was successful"""
        try:
            with self.engine.connect() as conn:
                # Check row count
                final_count = pd.read_sql(
                    f"SELECT COUNT(*) FROM dbo.{self.table_name}", 
                    conn
                ).iloc[0, 0]
                
                if final_count == len(self.df):
                    self.logger.info(f"Upload verified: {final_count} rows in table")
                else:
                    self.logger.error(f"Row count mismatch: Expected {len(self.df)}, found {final_count}")
                
                # Sample the uploaded data
                sample_query = f"SELECT TOP 3 * FROM dbo.{self.table_name} ORDER BY [Start Time] DESC"
                sample_data = pd.read_sql(sample_query, conn)
                self.logger.info(f"Sample of uploaded data:\n{sample_data}")
                
        except Exception as e:
            self.logger.error(f"Upload verification failed: {e}")

    def display_summary(self):
        """Display comprehensive data summary"""
        if self.df is not None:
            print(f"\n{'='*50}")
            print(f"DATA PROCESSING SUMMARY")
            print(f"{'='*50}")
            print(f"Total rows: {len(self.df):,}")
            print(f"Total columns: {len(self.df.columns)}")
            
            print(f"\nColumn Information:")
            for i, col in enumerate(self.df.columns, 1):
                null_count = self.df[col].isnull().sum()
                null_percent = (null_count / len(self.df)) * 100 if len(self.df) > 0 else 0
                print(f"  {i:2d}. {col:<30} | Nulls: {null_count:>6} ({null_percent:>5.1f}%)")
            
            print(f"\nSample Data (first 2 rows):")
            print(self.df.head(2).to_string(max_colwidth=25))
            print(f"{'='*50}")
            
        return self

    def cleanup(self):
        """Clean up resources"""
        if self.engine:
            self.engine.dispose()
            self.logger.info("Database engine disposed")

    def process_and_upload(self, confirm_upload: bool = True):
        """
        Main workflow method that combines all steps
        
        Args:
            confirm_upload: Whether to ask for confirmation before uploading
        """
        try:
            self.logger.info("=== OPTIMIZED DATA UPLOAD WORKFLOW STARTED ===")
            
            # Load and process data
            (self
             .load_excel_data()
             .clean_and_transform_data()
             .display_summary())
            
            # Confirm upload if requested
            if confirm_upload:
                response = input(f"\nProceed with truncating {self.table_name} and uploading {len(self.df)} rows? (y/n): ")
                if response.lower() != 'y':
                    self.logger.info("Upload cancelled by user")
                    return self
            
            # Upload data
            (self
             .truncate_table()
             .upload_data())
            
            self.logger.info("=== WORKFLOW COMPLETED SUCCESSFULLY ===")
            
        except Exception as e:
            self.logger.error(f"=== WORKFLOW FAILED ===: {e}")
            raise
        finally:
            self.cleanup()
        
        return self


# Example usage
if __name__ == "__main__":
    # Initialize processor
    processor = OptimizedDataUploadProcessor(
        excel_path="Combined_File.xlsx",
        table_name="FPTCOM_Sua"
    )
    
    try:
        # Run the complete workflow
        processor.process_and_upload(confirm_upload=True)
        
    except Exception as e:
        print(f"Processing failed: {e}")
        processor.logger.error(f"Main execution failed: {e}")
    
    finally:
        processor.cleanup()