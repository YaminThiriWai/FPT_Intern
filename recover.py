import pandas as pd
import sqlalchemy
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

db_config = {
    'server': os.getenv('DB_SERVER', 'FIT-DB-PBI'),
    'database': os.getenv('DB_NAME', 'FPTCOM_Sua'),
    'username': os.getenv('DB_USER', 'sa'),
    'password': os.getenv('DB_PASSWORD', '123qwe!!@@##4%%')
}

# Create the SQLAlchemy engine
connection_string = (
    f"mssql+pyodbc:///?odbc_connect="
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={db_config['server']};"
    f"DATABASE={db_config['database']};"
    f"UID={db_config['username']};"
    f"PWD={db_config['password']};"
)
engine = sqlalchemy.create_engine(connection_string)

# Read data back from the table
df = pd.read_sql("SELECT * FROM dbo.FPTCOM_Sua_Excel", engine)

# Save to Excel
df.to_excel("Recovered_Main_File.xlsx", index=False)
print("Recovered data has been saved to 'Recovered_Main_File.xlsx'")

print("Saved to:", os.path.abspath("Recovered_Main_File.xlsx"))

