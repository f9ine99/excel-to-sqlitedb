import pandas as pd
import sqlite3
import os
from datetime import datetime
import logging
import re

# Paths to your files
file_path = '08mm.xlsx'
db_path = 'orders.db'

# Logging configuration
logging.basicConfig(
    filename='data_transfer.log',
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# The expected columns in both Excel and the database
expected_columns = ['order_id', 'customer_name', 'carta_id', 'status', 'width_of_carta', 'shape_of_carta']

# Function to validate order_id format
def validate_order_id(order_id):
    if not isinstance(order_id, str):
        return False
    return order_id.strip().upper().startswith('OR')

# Function to clean value (convert empty strings to None)
def clean_value(value):
    if pd.isna(value) or value == '' or value == ' ':
        return None
    return value

# Function to load data from Excel and clean it
def load_and_clean_excel(file_path):
    try:
        logging.info(f"Loading Excel file: {file_path}")

        # Load the Excel file
        df = pd.read_excel(file_path)

        # Filter out unnamed columns
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

        # Ensure all expected columns are present
        if not all(col in df.columns for col in expected_columns):
            missing_cols = [col for col in expected_columns if col not in df.columns]
            raise KeyError(f"Missing required columns: {', '.join(missing_cols)}")
        else:
            logging.info(f"All expected columns are present in the Excel file.")

        # Select only the relevant columns
        df_filtered = df[expected_columns]

        # Create a mask for valid order_ids
        valid_order_ids = df_filtered['order_id'].apply(validate_order_id)
        
        # Log invalid order_ids
        invalid_rows = df_filtered[~valid_order_ids]
        if not invalid_rows.empty:
            for index, row in invalid_rows.iterrows():
                logging.error(f"Invalid order_id format at row {index + 2}: {row['order_id']}. Order ID must start with 'OR'")
            logging.warning(f"Found {len(invalid_rows)} invalid order IDs. These records will be skipped.")

        # Filter to keep only valid order_ids
        df_filtered = df_filtered[valid_order_ids]

        # If all orders are invalid, log warning but continue
        if df_filtered.empty:
            logging.warning("No valid order IDs found in the Excel file.")
            return df_filtered

        # Replace empty strings and NaN values with None for all columns
        for column in df_filtered.columns:
            df_filtered[column] = df_filtered[column].apply(clean_value)

        # Add updated_at column with the current timestamp as a string
        df_filtered['updated_at'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        logging.info(f"Excel file loaded and cleaned successfully. {len(df_filtered)} valid records found.")
        return df_filtered
    except KeyError as e:
        logging.error(f"Error: {e}")
        raise
    except Exception as e:
        logging.error(f"Error loading Excel file: {e}")
        raise

# Function to insert DataFrame data into the existing SQLite table
def insert_into_existing_table(df, db_path):
    try:
        # If DataFrame is empty, log and return
        if df.empty:
            logging.info("No valid records to insert into database.")
            return

        logging.info(f"Connecting to SQLite database at: {db_path}")

        # Default values for created_by and updated_by
        default_created_by = 'system'
        default_updated_by = 'system'

        successful_transfers = []  # List to track successfully transferred order IDs
        failed_transfers = {}      # Dictionary to track failed order IDs and error messages

        with sqlite3.connect(db_path) as conn:
            # Enable foreign keys
            conn.execute('PRAGMA foreign_keys = ON')
            
            # First, modify the table to allow NULL values if needed
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    ALTER TABLE `Order` MODIFY COLUMN customer_name TEXT NULL;
                ''')
            except sqlite3.OperationalError:
                # If the column already allows NULL, this will fail, which is fine
                pass

            for index, row in df.iterrows():
                try:
                    # Insert or replace record to handle primary key conflicts
                    cursor.execute('''
                        INSERT OR REPLACE INTO `Order` 
                        (order_id, customer_name, carta_id, status, width_of_carta, shape_of_carta, 
                         created_by, updated_by, updated_at) 
                        VALUES (?, ?, ?, ?, ?, ?, 
                                COALESCE((SELECT created_by FROM `Order` WHERE order_id = ?), ?),
                                ?, ?)
                    ''', (
                        row['order_id'], 
                        clean_value(row['customer_name']),
                        clean_value(row['carta_id']),
                        clean_value(row['status']),
                        clean_value(row['width_of_carta']),
                        clean_value(row['shape_of_carta']),
                        row['order_id'], 
                        default_created_by,
                        default_updated_by,
                        row['updated_at']
                    ))
                    
                    # If successful, log the success and add to the list
                    successful_transfers.append(row['order_id'])
                    logging.debug(f"Successfully transferred order_id: {row['order_id']}")

                except sqlite3.DatabaseError as db_err:
                    # Log the error and add the order_id to the failed_transfers dictionary
                    error_msg = f"Error at row {index + 1}: {db_err}"
                    failed_transfers[row['order_id']] = error_msg
                    logging.error(f"Failed to transfer order_id: {row['order_id']} | {error_msg}")
                    continue

        # Final summary of transfers
        logging.info(f"Successfully transferred {len(successful_transfers)} order(s): {successful_transfers}")
        if failed_transfers:
            logging.warning(f"Failed to transfer {len(failed_transfers)} order(s): {failed_transfers}")

        logging.info("Data transfer process completed.")
    except Exception as e:
        logging.critical(f"Critical error while inserting data into the SQLite database: {e}")
        raise

# Main function
def main():
    try:
        logging.info("Starting data transfer process.")

        # Print current working directory to confirm the path
        logging.debug(f"Current Working Directory: {os.getcwd()}")

        # Check if the Excel file exists
        if not os.path.isfile(file_path):
            error_msg = f"File not found: {file_path}"
            logging.error(error_msg)
            return

        # Load and clean data, filtering for the relevant columns
        df = load_and_clean_excel(file_path)

        # Print detected column headers (for logging)
        column_headers = df.columns.tolist()
        logging.debug(f"Detected and selected column headers: {column_headers}")

        # Insert data into the existing `Order` table
        insert_into_existing_table(df, db_path)

    except Exception as e:
        logging.critical(f"Critical error in main function: {e}")
        raise

if __name__ == '__main__':
    main()