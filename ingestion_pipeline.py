from datetime import datetime
import json
from pyspark.sql import SparkSession

# Decorator for timing command
def timer(func):
    def inner(*args, **kwargs):
        start = datetime.now()
        returned_value = func(*args, **kwargs)
        end = datetime.now()
        
        # Calculate runtime
        time_diff = end - start
        runtime=divmod(time_diff.days * SECONDS_IN_DAY + time_diff.seconds, 60)
        mins=runtime[0]
        secs=runtime[1]
        
        # Print runtime
        print("Started process", start)
        print("Process complete", end)
        print(f"Execution time: {mins} min {secs} sec")
        
        return returned_value

    return inner


@timer
def export_dataframe(
    hostname
    , username
    , password
    , table_name
    , spark_port = "4050"
    , spark_shuffle_partitions = "200"
    , spark_executor_instances = "2"
    , spark_executor_memory = "3g"
    , spark_driver_memory = "2g"
    , spark_driver_cache = "256m"
    , spark_executor_cache = "256m"
    , chunk_size = 500000
    , write_mode = "csv"
    , destination = "exports/"
):
    spark = SparkSession.builder \
        .appName("Parquetfy") \
        .config("spark.jars", "/Users/shreyan/Applications/Utilities/mysql-connector-j-9.0.0/mysql-connector-j-9.0.0.jar") \
        .config("spark.executor.instances", spark_executor_instances) \
        .config("spark.executor.memory", spark_executor_memory) \
        .config("spark.driver.memory", spark_driver_memory) \
        .config("spark.executor.extraJavaOptions", f"-XX:ReservedCodeCacheSize={spark_driver_cache}") \
        .config("spark.driver.extraJavaOptions", f"-XX:ReservedCodeCacheSize={spark_executor_cache}") \
        .config("spark.sql.shuffle.partitions", spark_shuffle_partitions) \
        .config("spark.ui.port", spark_port) \
        .getOrCreate()

    print(f"Spark Web UI is available at: http://localhost:{spark_port}")

    # JDBC connection properties
    jdbc_url = f"jdbc:mysql://{hostname}/airportdb"
    properties = {
        "user": username,
        "password": password,
        "driver": "com.mysql.cj.jdbc.Driver"
    }


    # Initialising variables
    offset = 0
    i=1

    while True:
        query = f"(SELECT * FROM {table_name} LIMIT {chunk_size} OFFSET {offset}) AS subquery"
        df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)
        
        if df.rdd.isEmpty():
            break

        if offset == 0:
            if write_mode == "parquet":
                df.write.parquet(destination, mode='overwrite')
            else:
                df.write.csv(destination, mode='overwrite', header=True)
        else:
            if write_mode == "parquet":
                df.write.parquet(destination, mode='append')
            else:
                df.write.csv(destination, mode='append', header=False)
        
        offset += chunk_size
        print(f"Finished writing Chunk {i}.")
        i += 1



    # Stop the Spark session
    input("Spark process complete, press any key to end session.")
    spark.stop()


if __name__ == '__main__':
    with open ("secrets.json", 'rb') as file:
        SQL_CREDENTIALS = json.load(file)

    USERNAME = SQL_CREDENTIALS['username']
    PASSWORD = SQL_CREDENTIALS['password']
    HOSTNAME = SQL_CREDENTIALS['ip']
    SECONDS_IN_DAY = 24 * 60 * 60

    export_dataframe(HOSTNAME, USERNAME, PASSWORD, "booking"
                     , write_mode="parquet", destination="exports/bookings/parquet_file/")