# import subprocess
# import os

# def backup_and_zip_database(username, password, database_name):
#     try:
#         # Backup the database
#         backup_command = f"mysqldump -u {username} -p{password} {database_name} > backup.sql"
#         subprocess.run(backup_command, shell=True, check=True)

#         # Compress the backup file into a zip file
#         zip_command = "zip -r backup.zip backup.sql"
#         subprocess.run(zip_command, shell=True, check=True)

#         # Clean up the .sql backup file
#         os.remove("backup.sql")

#         print("Backup and compression completed successfully.")
#     except Exception as e:
#         print(f"An error occurred: {e}")

# # Replace with your MySQL credentials
# username = 'root'
# password = 'root'
# database_name = 'testdb'

# backup_and_zip_database(username, password, database_name)
