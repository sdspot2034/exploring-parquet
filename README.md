# exploring-parquet

## Introduction
In this mini-project, I sought to compare the result of storing a table as a CSV file against parquet. It is well known that Parquet is a read-efficient file format that supercharges query performance for analytical use cases due to its columnar nature. By default, it uses `snappy` compression that reduces file size by more than half. Due to this compression, a slight overhead is associated with the write time of the file. Read my [blog article](https://shreyandas.hashnode.dev/parquet-vs-csv) to know more.

## Procedure
1. Set up a MySQL Server on your local machine.
2. Download all Python packages and dependencies mentioned below.
3. Download the `airportdb` Database from [MySQL's Official Pages](https://dev.mysql.com/doc/index-other.html).
4. Extract the files in your preferred directory.
5. Copy all files with the extension `.zst` and place it inside a folder called `zst_folder`.
6. Run the Python script `zst_extractor.py` making appropriate changes in the source and destination folder paths.
7. Run the `tsv_to_csv_converter.py` Python Script to convert to a format readable by MySQL Server. (*This step is optional.*)
8. Run the `DDL Queries.sql` file on the MySQL Server to create the schemas for all the required tables on the `airportdb` database.
9. Set `allow_local_infile` parameter on the MySQL Server configurations to `true`.
10. Run the `Data Loader.ipynb` notebook to load all the files to the database.
11. Query the tables on the database to ensure proper data load.
12. Run the `ingestion_pipeline.py` twice, once with `write_mode='csv'` and then with `write_mode='parquet'` and changing the sink folder location for each run.

You can monitor the Spark Jobs on the Web UI by opening the link http://localhost:4050 on your browser.

## Dependencies

1. **MySQL Database**
   Ensure you have a MySQL database set up either locally or hosted somewhere. Ensure you have the hostname (IP Address), username and password (preferrably, non-root) with read and create table and create database access on the server.
   
2. **Zstandard**
   A Python compression library that is used to convert database into TSV files.
   Installed version: `zstandard=0.22.0`
   Command to install: `pip install zstandard`

3. **MySQL Connector Python**
   A Python library to connect and query on the MySQL Database.
   Installed version: `mysql-connector-python=9.0.0`
   Command to install: `pip install mysql-connector-python`

4. **PySpark**
   Python API for Apache Spark
   [Official Documentation](https://spark.apache.org/docs/latest/api/python/index.html)
   Installed Version: `pyspark=3.5.1`
   Command to install: `pip install pyspark`

5. **MySQL JDBC Driver**
   [Official MySQL JDBC Connector link](https://www.mysql.com/products/connector/)
   Installed Version: 9.0.0


## References

1. [Adrian Jay Timbal's Article](https://www.linkedin.com/pulse/uploading-airportdb-free-sample-database-from-mysql-local-timbal/)
2. [`airportdb` Documentation](https://dev.mysql.com/doc/airportdb/en/)
3. ChatGPT


For any questions, comments or suggestions, please feel free to reach out to me via email! 😄📬