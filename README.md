# Cloud Data Warehouse: S3 and Redshift

##### [Israel Mendes](israelmendes.com.br)

Creating a Database and using an ETL in Python. Moving data from AWS S3 to Redshift.

---

## Process bellow:

1. The first file to produce is `dwh.cfg` , where is the data from the account of Redshift, as DB_NAME, Endpoint, DB_PASSWORD, and others. For obvious reasons, **it isn't included in this repo** :)

2. Then, the `sql_queries.py` is used. It makes all the specifications, as the data types, columns in each table and others;

3. After that, the `creating_tables.py` to produce the tables with a connection in AWS Redshift. The file uses `sql_queries.py` to make the specifications, as the data types, columns in each table and others;

4. In the end, the `etl.py` script will run to pull data from a S3 bucket, transform and load into the Redshift tables that were previously created.

## Commands

The main steps would be to run the Data Warehouse in the AWS Cloud:

> python creating_tables.py
>
> python etl.py

After that, the data will be in the Redshift cluster.
