import pymysql
import csv
from datetime import date, timedelta

# Database connection details
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'testdb'
}

# Calculate yesterday's date
today = (date.today())

# SQL query to fetch yesterday's records
query = f"SELECT * FROM all_data_csv_report WHERE DATE(date) = '{today}'"

# Filepath for the output CSV
output_file = 'yesterdays_records.csv'

try:
    # Connect to the database
    connection = pymysql.connect(**db_config)
    cursor = connection.cursor()

    # Execute the query
    cursor.execute(query)

    # Fetch all records
    records = cursor.fetchall()

    # Get column names
    column_names = [desc[0] for desc in cursor.description]

    # Write records to a CSV file
    with open(output_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(column_names)  # Write header
        writer.writerows(records)  # Write data

    print(f"Data successfully exported to {output_file}")

except Exception as e:
    print("Error:", e)

finally:
    # Close database connection
    if 'connection' in locals():
        connection.close()
