import pandas as pd
import sys

def combine_excel_files(main_file_path, other_file_path, output_file_path):
    try:
        # Load the main Excel file
        main_df = pd.read_excel(main_file_path)
        print(f"Loaded main file '{main_file_path}' with {len(main_df)} rows and {len(main_df.columns)} columns.")
        
        # Load the other Excel file
        other_df = pd.read_excel(other_file_path)
        print(f"Loaded other file '{other_file_path}' with {len(other_df)} rows and {len(other_df.columns)} columns.")
        
        # Print columns in both files
        print("\nMain file columns:")
        print(main_df.columns.tolist())
        print("\nOther file columns:")
        print(other_df.columns.tolist())

        # Reorder columns in other_df to match main_df columns
        # This will add missing columns as NaN and drop extra columns not in main_df
        other_df_aligned = other_df.reindex(columns=main_df.columns)
        
        # Concatenate dataframes
        combined_df = pd.concat([main_df, other_df_aligned], ignore_index=True)
        print(f"\nCombined dataframe has {len(combined_df)} rows and {len(combined_df.columns)} columns.")
        
        # Save the combined dataframe to Excel
        combined_df.to_excel(output_file_path, index=False)
        print(f"\nCombined data saved to '{output_file_path}'.")
    
    except Exception as e:
        print(f"Error during combining files: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Replace these paths with your actual file paths
    main_file = "Recovered_Main_File.xlsx"
    other_file = r"C:\Users\adminvm\Desktop\data June_2025.xlsx"
    output_file = "Combined_File.xlsx"
    
    combine_excel_files(main_file, other_file, output_file)
