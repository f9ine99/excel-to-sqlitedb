import pandas as pd
import sqlite3
import os
from datetime import datetime

# Paths to your files
file_path = 'mm.xlsx'
db_path = 'orders.db'

# The expected columns in both Excel and the database, including the new columns
expected_columns = ['order_id', 'customer_name', 'carta_id', 'status', 'width_of_carta', 'shape_of_carta']

# Function to load data from Excel and clean it
def load_and_clean_excel(file_path):
    try:
        # Load the Excel file
        df = pd.read_excel(file_path)

        # Filter out unnamed columns
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

        # Ensure all expected columns are present
        if not all(col in df.columns for col in expected_columns):
            missing_cols = [col for col in expected_columns if col not in df.columns]
            raise KeyError(f"Missing required columns: {', '.join(missing_cols)}")

        # Select only the relevant columns
        df_filtered = df[expected_columns]

        # Fill empty values with "None"
        df_filtered.fillna("None", inplace=True)

        # Add updated_at column with the current timestamp as a string
        df_filtered['updated_at'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        return df_filtered
    except KeyError as e:
        print(f"Error: {e}")
        raise
    except Exception as e:
        print(f"Error loading Excel file: {e}")
        raise

# Function to insert DataFrame data into the existing SQLite table
def insert_into_existing_table(df, db_path):
    try:
        # Default values for created_by and updated_by
        default_created_by = 'system'  # Change as needed
        default_updated_by = 'system'   # Change as needed

        # Connect to SQLite database using context manager
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            for _, row in df.iterrows():
                cursor.execute('''
                    INSERT INTO `Order` (order_id, customer_name, carta_id, status, width_of_carta, shape_of_carta, created_by, updated_by, updated_at) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (row['order_id'], row['customer_name'], row['carta_id'], row['status'], row['width_of_carta'], row['shape_of_carta'], default_created_by, default_updated_by, row['updated_at']))

        print("Data has been successfully inserted into the existing table.")
    except sqlite3.DatabaseError as db_err:
        print(f"Database error: {db_err}")
        raise
    except Exception as e:
        print(f"Error inserting into the SQLite database: {e}")
        raise

# Main function
def main():
    # Print current working directory to confirm the path
    print("Current Working Directory:", os.getcwd())

    # Check if the Excel file exists
    if not os.path.isfile(file_path):
        print(f"File not found: {file_path}")
        return

    # Load and clean data, filtering for the relevant columns
    df = load_and_clean_excel(file_path)

    # Print detected column headers (for logging)
    column_headers = df.columns.tolist()
    print("Detected and selected column headers:")
    for header in column_headers:
        print(header)

    # Insert data into the existing `order` table
    insert_into_existing_table(df, db_path)

if __name__ == '__main__':
    main()
