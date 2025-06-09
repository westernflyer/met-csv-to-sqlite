#!/usr/bin/env python3
"""
CSV to SQLite Importer

Imports a CSV file from the metdata track into a SQLite database. The CSV file is expected to have
columns 'Time', 'gps_hdt', 'PosLat', and 'PosLon'. The 'Time' column is converted from the format
'DD-MMM-YYYY HH:MM:SS' to a Unix epoch time integer and used as primary key.
"""

import sqlite3
import csv
from datetime import datetime
import sys
import os


def convert_time_to_epoch(time_str: str):
    """
    Converts a given time string into its corresponding Unix epoch time.

    This function takes a datetime string, parses it using a specific format,
    and converts it to an integer Unix epoch time. If the input string cannot
    be parsed due to invalid formatting, it returns None and prints an error
    message.

    :param time_str: The datetime string in the format '%d-%b-%Y %H:%M:%S'
         (e.g., '25-Dec-1995 15:30:45').
    :return: The corresponding Unix epoch time as an integer, or None if
         parsing fails.
    """
    try:
        # Parse the datetime string
        dt = datetime.strptime(time_str, '%d-%b-%Y %H:%M:%S')
        # Convert to Unix epoch time (integer)
        return int(dt.timestamp())
    except ValueError as e:
        print(f"Error parsing time '{time_str}': {e}")
        return None


def create_database_table(db_path: str):
    """Create SQLite database and table with proper schema"""
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        # Create table with timestamp as primary key
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS location_data
                       (
                           timestamp INTEGER PRIMARY KEY NOT NULL,
                           latitude REAL,
                           longitude REAL,
                           heading REAL
                       )
                       ''')

        conn.commit()
    print(f"Database and table created at: {db_path}")


def read_csv_file(csv_file_path: str) -> list[dict[str, str]] | None:
    """Read CSV file and return data as list of dictionaries"""
    try:
        with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:

            reader = csv.DictReader(csvfile, delimiter=',')
            data = list(reader)

            print(f"CSV file loaded successfully. {len(data)} rows found.")
            if data:
                print(f"Columns: {list(data[0].keys())}")
                print(f"First row: {data[0]}")

            return data

    except FileNotFoundError:
        print(f"Error: CSV file '{csv_file_path}' not found.")
        return None
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None


def validate_csv_data(data: list[dict]) -> bool:
    """Validate that CSV data has required columns"""
    if not data:
        print("Error: No data found in CSV file.")
        return False

    required_columns = ['Time', 'gps_hdt', 'PosLat', 'PosLon']
    available_columns = list(data[0].keys())
    missing_columns = [col for col in required_columns if col not in available_columns]

    if missing_columns:
        print(f"Error: Missing required columns: {missing_columns}")
        print(f"Available columns: {available_columns}")
        return False

    return True


def process_csv_data(data):
    """Process CSV data and convert time to epoch"""
    processed_data = []
    conversion_errors = 0
    duplicate_timestamps = set()
    duplicates_found = 0

    print("Converting time_utc to Unix epoch time...")

    for i, row in enumerate(data):
        # Convert time to epoch
        epoch_time = convert_time_to_epoch(row['Time'])

        if epoch_time is None:
            conversion_errors += 1
            continue

        # Check for duplicates
        if epoch_time in duplicate_timestamps:
            duplicates_found += 1
            print(f"Warning: Duplicate timestamp at row {i + 1}: {row['time_utc']}")
            continue

        duplicate_timestamps.add(epoch_time)

        # Convert other fields to appropriate types
        try:
            heading = float(row['gps_hdt']) if row['gps_hdt'].strip() else None
            latitude = float(row['PosLat']) if row['PosLat'].strip() else None
            longitude = float(row['PosLon']) if row['PosLon'].strip() else None

            processed_data.append({
                'timestamp': epoch_time,
                'latitude': latitude,
                'longitude': longitude,
                'heading': heading,
            })

        except ValueError as e:
            print(f"Warning: Error converting numeric values in row {i + 1}: {e}")
            conversion_errors += 1

    if conversion_errors > 0:
        print(f"Warning: {conversion_errors} rows had conversion errors and were skipped.")

    if duplicates_found > 0:
        print(f"Warning: {duplicates_found} duplicate timestamps found and were skipped.")

    print(f"Successfully processed {len(processed_data)} rows.")
    return processed_data


def insert_data_to_database(processed_data: list[dict], db_path: str) -> bool:
    """

    Inserts pre-processed location data into a database table. This function connects to the
    specified SQLite database file and clears the existing `location_data` table before
    inserting the new rows from the provided data. Each row is expected to include the
    timestamp, latitude, longitude, and heading. If the operation succeeds, the function commits
    the changes and outputs the number of rows imported.

    :param processed_data: A list of dictionaries, with each dictionary
        representing a row of data to insert. Keys in the dictionary must
        include 'timestamp', 'latitude', 'longitude', and 'heading'.
    :param db_path: The file path to the SQLite database. The database file must
        have a `location_data` table present with the required schema.
    :return: A boolean value indicating success or failure of the operation. Returns True
        on success or False if any exception is encountered during execution.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Clear existing data
            cursor.execute("DELETE FROM location_data")

            # Insert new data
            for row in processed_data:
                cursor.execute('''
                               INSERT INTO location_data (timestamp, latitude, longitude, heading)
                               VALUES (?, ?, ?, ?)
                               ''', (row['timestamp'], row['latitude'], row['longitude'],
                                     row['heading']))

            conn.commit()

        print(f"Successfully imported {len(processed_data)} rows into the database.")
        return True

    except sqlite3.IntegrityError as e:
        print(f"Database integrity error: {e}")
        return False
    except Exception as e:
        print(f"Error inserting data into database: {e}")
        return False


def import_csv_to_sqlite(csv_file_path: str, db_path: str) -> bool:
    """
    Imports data from a CSV file into an SQLite database by performing several steps: reading the CSV file,
    validating its contents, processing the data, and inserting valid data into the database. Each step returns
    an intermediate result to ensure proper handling of errors and data inconsistencies.

    :param csv_file_path: The file path of the CSV file to be imported.
    :type csv_file_path: str
    :param db_path: The location of the SQLite database to which the data will be imported.
    :type db_path: str
    :return: A boolean indicating whether the CSV data was successfully imported into the database.
    :rtype: bool
    """
    # Read CSV file
    data = read_csv_file(csv_file_path)
    if data is None:
        return False

    # Validate data
    if not validate_csv_data(data):
        return False

    # Process data
    processed_data = process_csv_data(data)
    if not processed_data:
        print("Error: No valid data to import.")
        return False

    # Insert into database
    return insert_data_to_database(processed_data, db_path)


def verify_database_data(db_path: str, limit: int = 5):
    """
    Verify the structure and contents of the "location_data" table within the specified
    SQLite database. This function ensures that the table exists, prints its schema,
    counts the number of records, and optionally displays a sample of the data.

    :param db_path: Path to the SQLite database file.
    :type db_path: str
    :param limit: The maximum number of records to display as a sample; default is 5.
    :type limit: int
    :return: None
    :rtype: None
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Check if table exists
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='location_data'")
        if not cursor.fetchone():
            print("Error: Table 'location_data' does not exist.")
            conn.close()
            return

        # Get table info
        cursor.execute("PRAGMA table_info(location_data)")
        table_info = cursor.fetchall()

        print("Table schema:")
        for column in table_info:
            pk_info = " PRIMARY KEY" if column[5] else ""
            null_info = " NOT NULL" if column[3] else ""
            print(f"  {column[1]} {column[2]}{pk_info}{null_info}")

        # Get record count
        cursor.execute("SELECT COUNT(*) FROM location_data")
        count = cursor.fetchone()[0]
        print(f"\nTotal records: {count}")

        if count > 0:
            # Show sample data
            cursor.execute(f"SELECT * FROM location_data ORDER BY timestamp LIMIT {limit}")
            rows = cursor.fetchall()

            print(f"\nFirst {min(limit, count)} records:")
            print("timestamp (epoch) | latitude | longitude | heading |")
            print("-" * 60)
            for row in rows:
                # Convert epoch back to readable time for display
                readable_time = datetime.fromtimestamp(row[0]).strftime('%d-%b-%Y %H:%M:%S')
                print(f"{row[0]} ({readable_time}) | {row[1]} | {row[2]} | {row[3]}")

        conn.close()

    except Exception as e:
        print(f"Error verifying database: {e}")


def main():
    """Main function"""
    # Configuration
    csv_file_path = "/Users/tkeffer/WesternFlyerData/metdata/track/WFF_Pos_1min.csv"
    db_path = "/Users/tkeffer/WesternFlyerData/metdata/track/WFF_Pos_1min.sdb"

    # Check if CSV file path was provided as command line argument
    if len(sys.argv) > 1:
        csv_file_path = sys.argv[1]

    if len(sys.argv) > 2:
        db_path = sys.argv[2]

    print(f"CSV file: {csv_file_path}")
    print(f"Database: {db_path}")
    print("-" * 50)

    # Create database and table
    create_database_table(db_path)

    # Check if CSV file exists
    if not os.path.exists(csv_file_path):
        print(f"CSV file '{csv_file_path}' not found.")
        print("Usage: python csv_to_sqlite.py <csv_file> [database_file]")
        return

    # Import CSV data
    success = import_csv_to_sqlite(csv_file_path, db_path)

    if success:
        print("\n" + "=" * 50)
        print("IMPORT COMPLETED SUCCESSFULLY")
        print("=" * 50)

        # Verify the imported data
        verify_database_data(db_path)
    else:
        print("\n" + "=" * 50)
        print("IMPORT FAILED")
        print("=" * 50)


if __name__ == "__main__":
    # Test the time conversion function
    test_time = "15-Mar-2023 14:30:25"
    epoch_time = convert_time_to_epoch(test_time)
    print(f"Time conversion test:")
    print(f"Original time: {test_time}")
    print(f"Unix epoch time: {epoch_time}")
    if epoch_time:
        converted_back = datetime.fromtimestamp(epoch_time)
        print(f"Converted back: {converted_back}")
    print("-" * 50)

    main()
