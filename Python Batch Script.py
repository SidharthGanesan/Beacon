import pyodbc
from datetime import datetime

# Connection
server = 'your_server.database.windows.net'
database = 'your_database'
username = 'your_username'
password = 'your_password'
driver = '{ODBC Driver 17 for SQL Server}'

# Establish connection
conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# previous batch by updating the BatchEndTimestamp
cursor.execute("""
    UPDATE batch_table
    SET BatchEndTimestamp = GETDATE(), BatchStatus='close'
    WHERE BatchID = (SELECT MAX(BatchID) FROM batch_table)
""")
conn.commit()

# Opening a new batch
cursor.execute("""
    INSERT INTO batch_table (BatchID, BatchStartTimestamp, BatchStatus)
    VALUES ((SELECT ISNULL(MAX(BatchID), 0) + 1 FROM batch_table), GETDATE(), 'Open')
""")
conn.commit()

# Clean up
cursor.close()
conn.close()

print("Previous batch closed and new batch opened successfully.")