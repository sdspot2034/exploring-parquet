import mysql.connector
import pandas as pd
import datetime
import json

with open ("secrets.json", 'rb') as file:
    SQL_CREDENTIALS = json.load(file)

USERNAME = SQL_CREDENTIALS['username']
PASSWORD = SQL_CREDENTIALS['password']
HOSTNAME = SQL_CREDENTIALS['ip']

chunk_size = 150000  # Adjust chunk size based on your system's memory capacity
cnx = mysql.connector.connect(host=HOSTNAME, database='airportdb', user=USERNAME, password=PASSWORD)
query = "SELECT * FROM OBT"

with open('test_csv.csv', 'w+') as file:
    file.truncate(0)

print("Started process", datetime.datetime.now())
i=1
for chunk in pd.read_sql(query, cnx, chunksize=chunk_size):
    print("Chunk ",i)
    chunk.to_csv('test_csv.csv', mode='a', sep='|', line_terminator='\r\n')
    i+=1

print("Process complete", datetime.datetime.now())

