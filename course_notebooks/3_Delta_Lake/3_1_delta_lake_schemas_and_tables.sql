-- Databricks notebook source
-- MAGIC %run "/Workspace/Users/euheniy.khvoinitski@gmail.com/Databricks DE Course/DE 3 - Delta Lake/Includes/Classroom-Setup-03.1"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Schema/Database

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(DA.schema_name)

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS ${da.schema_name}_default_location;

DESCRIBE SCHEMA EXTENDED ${da.schema_name}_default_location;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Managed tables

-- COMMAND ----------

USE ${da.schema_name}_default_location;

CREATE
OR REPLACE TABLE managed_table (width INT, length INT, height INT);

INSERT INTO
  managed_table
VALUES
  (3, 2, 1);

SELECT
  *
FROM
  managed_table;

-- COMMAND ----------

DESCRIBE EXTENDED managed_table;

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC tbl_location = spark.sql(f"DESCRIBE DETAIL managed_table").first().location
-- MAGIC print(tbl_location)
-- MAGIC
-- MAGIC files = dbutils.fs.ls(tbl_location)
-- MAGIC display(files)

-- COMMAND ----------

DROP TABLE managed_table;

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC schema_default_location = spark.sql(f"DESCRIBE SCHEMA {DA.schema_name}_default_location").collect()[3].database_description_value
-- MAGIC print(schema_default_location)
-- MAGIC dbutils.fs.ls(schema_default_location)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### External tables

-- COMMAND ----------

USE ${da.schema_name}_default_location;

CREATE
OR REPLACE TEMPORARY VIEW temp_delays USING CSV OPTIONS (
  path = '${da.paths.datasets}/flights/departuredelays.csv',
  header = "true",
  mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed lines are encountered
);

CREATE
OR REPLACE TABLE external_table LOCATION '${da.paths.working_dir}/external_table' AS
SELECT
  *
FROM
  temp_delays;

SELECT
  *
FROM
  external_table;

-- COMMAND ----------

DESCRIBE EXTENDED external_table;

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC tbl_path = f"{DA.paths.working_dir}/external_table"
-- MAGIC files = dbutils.fs.ls(tbl_path)
-- MAGIC display(files)

-- COMMAND ----------

DROP TABLE external_table;

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC tbl_path = f"{DA.paths.working_dir}/external_table"
-- MAGIC files = dbutils.fs.ls(tbl_path)
-- MAGIC display(files)

-- COMMAND ----------

DROP SCHEMA ${da.schema_name}_default_location CASCADE;

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC DA.cleanup()
