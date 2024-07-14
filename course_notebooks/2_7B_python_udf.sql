-- Databricks notebook source
-- MAGIC %run "/Workspace/Users/euheniy.khvoinitski@gmail.com/Databricks DE Course/DE 2 - ETL with Spark/Includes/Classroom-Setup-02.7B"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sales_df = spark.table("sales")
-- MAGIC display(sales_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def first_letter_function(email):
-- MAGIC     return email[0]
-- MAGIC
-- MAGIC first_letter_function("annagray@kaufman.com")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # serialization of the function for Spark
-- MAGIC first_letter_udf = udf(first_letter_function)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC display(sales_df.select(first_letter_udf(col("email"))))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Register Python UDF for use in the SQL namespace

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sales_df.createOrReplaceTempView("sales")
-- MAGIC
-- MAGIC first_letter_udf = spark.udf.register("first_letter_sql_udf", first_letter_function)

-- COMMAND ----------

-- This function still has Python overhead comparing to native Spark SQL
select
  first_letter_sql_udf(email) as email_first_letter
from
  sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Using Python UDF decorator

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Our input/output is a string
-- MAGIC @udf("string")
-- MAGIC def first_letter_udf(email: str) -> str:
-- MAGIC     return email[0]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC sales_df = spark.table("sales")
-- MAGIC display(sales_df.select(first_letter_udf(col("email"))))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pandas UDFs
-- MAGIC
-- MAGIC ### Articles
-- MAGIC 1. [Introducing Vectorized UDFs for PySpark](https://www.databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html)
-- MAGIC 1. [New Pandas UDFs and Python Type Hints in the Upcoming Release of Apache Spark 3.0](https://www.databricks.com/blog/2020/05/20/new-pandas-udfs-and-python-type-hints-in-the-upcoming-release-of-apache-spark-3-0.html)
-- MAGIC 1. [Test Notebook](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/1281142885375883/2174302049319883/7729323681064935/latest.html)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC from pyspark.sql.functions import pandas_udf
-- MAGIC
-- MAGIC # We have a string input/output
-- MAGIC @pandas_udf("string")
-- MAGIC def vectorized_udf(email: pd.Series) -> pd.Series:
-- MAGIC     return email.str[0]
-- MAGIC
-- MAGIC # Alternatively
-- MAGIC # def vectorized_udf(email: pd.Series) -> pd.Series:
-- MAGIC #     return email.str[0]
-- MAGIC # vectorized_udf = pandas_udf(vectorized_udf, "string")https://www.databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(sales_df.select(vectorized_udf(col("email"))))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.udf.register("sql_vectorized_udf", vectorized_udf)

-- COMMAND ----------

-- Use the Pandas UDF from SQL
SELECT sql_vectorized_udf(email) AS firstLetter FROM sales

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
