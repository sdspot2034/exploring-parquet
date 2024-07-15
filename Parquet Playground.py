import mysql.connector
import pandas as pd
import datetime
import json
from pyspark.sql import SparkSession

with open ("secrets.json", 'rb') as file:
    SQL_CREDENTIALS = json.load(file)

USERNAME = SQL_CREDENTIALS['username']
PASSWORD = SQL_CREDENTIALS['password']
HOSTNAME = SQL_CREDENTIALS['ip']
SECONDS_IN_DAY = 24 * 60 * 60



from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Parquetfy") \
    .config("spark.jars", "/Users/shreyan/Applications/Utilities/mysql-connector-j-9.0.0/mysql-connector-j-9.0.0.jar") \
    .config("spark.executor.instances", "2") \
    .config("spark.executor.memory", "3g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.extraJavaOptions", "-XX:ReservedCodeCacheSize=256m") \
    .config("spark.driver.extraJavaOptions", "-XX:ReservedCodeCacheSize=256m") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.ui.port", "4050") \
    .getOrCreate()

print(f"Spark Web UI is available at: http://localhost:4050")

# JDBC connection properties
jdbc_url = f"jdbc:mysql://{HOSTNAME}/airportdb"
properties = {
    "user": USERNAME,
    "password": PASSWORD,
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Read data from MySQL in chunks
chunk_size = 500000 
offset = 0

start_time=datetime.datetime.now()
i=1

while True:
    query = f"(SELECT * FROM booking LIMIT {chunk_size} OFFSET {offset}) AS subquery"
    df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)
    
    if df.rdd.isEmpty():
        break

    if offset == 0:
        df.write.csv("exports/csv/booking/csv_file.csv", mode='overwrite', header=True)
        # df.write.parquet("<path_to_save_parquet>", mode='overwrite')
    else:
        df.write.csv("exports/csv/booking/csv_file.csv", mode='append', header=False)
        # df.write.parquet("<path_to_save_parquet>", mode='append')
    
    offset += chunk_size
    print(f"Finished writing Chunk {i}.")
    i += 1

# Stop the Spark session
end_time = datetime.datetime.now()
spark.stop()


print("Started process", start_time)
print("Process complete", end_time)
time_diff = end - start
runtime=divmod(time_diff.days * SECONDS_IN_DAY + time_diff.seconds, 60)
mins=runtime[0]
secs=runtime[1]

# Print runtime
print(f"Execution time: {mins} min {secs} sec")