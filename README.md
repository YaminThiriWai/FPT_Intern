## Upload Configuration: Testing vs. Full Upload

The `upload_data()` function in the script is designed to support both **test uploads** and **full production uploads**.

### Test Mode (Subset of Rows)

If you want to test the SQL upload with a small number of rows, simply **uncomment** one of the following lines:

# upload_df = self.df.head(10)    # Upload only the first 10 rows
# upload_df = self.df.head(100)   # Upload only the first 100 rows
# upload_df = self.df.head(500)   # Upload only the first 500 rows

To switch back to full data upload, make sure the following line is active and uncommented:

# upload_df = self.df

This will upload all available rows from the processed DataFrame to SQL Server.