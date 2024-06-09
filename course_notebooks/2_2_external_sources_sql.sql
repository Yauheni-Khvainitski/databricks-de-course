-- Databricks notebook source
-- MAGIC %md
-- MAGIC **Prepare the environment**

-- COMMAND ----------

-- MAGIC %run "/Workspace/Users/euheniy.khvoinitski@gmail.com/Databricks DE Course/DE 2 - ETL with Spark/Includes/Classroom-Setup-02.2"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Check the sample sales data**

-- COMMAND ----------

SELECT
  *
FROM
  csv.`${DA.paths.sales_csv}`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Specify option to correctly read the files**

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sales_csv (
  order_id LONG,
  email STRING,
  transactions_timestamp LONG,
  total_item_quantity INTEGER,
  purchase_revenue_in_usd DOUBLE,
  unique_items INTEGER,
  items STRING
) USING CSV OPTIONS (header = "true", delimiter = "|") LOCATION "${DA.paths.sales_csv}"

-- COMMAND ----------

select
  *
from
  sales_csv

-- COMMAND ----------

SELECT
  COUNT(*)
FROM
  sales_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""
-- MAGIC CREATE TABLE IF NOT EXISTS sales_csv_pyspark
-- MAGIC   (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
-- MAGIC USING CSV
-- MAGIC OPTIONS (
-- MAGIC   header = "true",
-- MAGIC   delimiter = "|"
-- MAGIC )
-- MAGIC LOCATION "{DA.paths.sales_csv}"
-- MAGIC """)

-- COMMAND ----------

select
  *
from
  sales_csv_pyspark

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **CSV data**
-- MAGIC
-- MAGIC It's important to ensure that column order does not change if additional data files will be added to the source directory. Because the data format does not have strong schema enforcement, Spark will load columns and apply column names and data types in the order specified during table declaration.

-- COMMAND ----------

DESCRIBE EXTENDED sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Limits of Tables with External Data Sources**
-- MAGIC
-- MAGIC While Delta Lake tables will guarantee that you always query the most recent version of your source data, tables registered against other data sources may represent older cached versions.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read
-- MAGIC       .option("header", "true")
-- MAGIC       .option("delimiter", "|")
-- MAGIC       .csv(DA.paths.sales_csv)
-- MAGIC       .write.mode("append")
-- MAGIC       .format("csv")
-- MAGIC       .save(DA.paths.sales_csv, header="true"))

-- COMMAND ----------

SELECT
  COUNT(*)
FROM
  sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC At the time we previously queried this data source, Spark automatically cached the underlying data in local storage. This ensures that on subsequent queries, Spark will provide the optimal performance by just querying this local cache.

-- COMMAND ----------

REFRESH TABLE sales_csv

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Extracting data from SQL databases**

-- COMMAND ----------

--local SQLite, the code will work only on single node cluster
DROP TABLE IF EXISTS users_jdbc;

CREATE TABLE users_jdbc
USING JDBC
OPTIONS (
  url = "jdbc:sqlite:${DA.paths.ecommerce_db}",
  dbtable = "users"
)

-- COMMAND ----------

select
  *
from
  users_jdbc

-- COMMAND ----------

DESCRIBE EXTENDED users_jdbc

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pyspark.sql.functions as F
-- MAGIC
-- MAGIC location = spark.sql("DESCRIBE EXTENDED users_jdbc").filter(F.col("col_name") == "Location").first()["data_type"]
-- MAGIC print(location)
-- MAGIC
-- MAGIC files = dbutils.fs.ls(location)
-- MAGIC print(f"Found {len(files)} files")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
