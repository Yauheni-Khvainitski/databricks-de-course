-- Databricks notebook source
-- MAGIC %run "/Workspace/Users/euheniy.khvoinitski@gmail.com/Databricks DE Course/DE 3 - Delta Lake/Includes/Classroom-Setup-03.4"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### CTAS

-- COMMAND ----------

CREATE OR REPLACE TABLE sales AS
SELECT * FROM parquet.`${DA.paths.datasets}/ecommerce/raw/sales-historical`;

DESCRIBE EXTENDED sales;

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_unparsed AS
SELECT * FROM csv.`${da.paths.datasets}/ecommerce/raw/sales-csv`;

SELECT * FROM sales_unparsed;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW sales_tmp_vw
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  path = "${da.paths.datasets}/ecommerce/raw/sales-csv",
  header = "true",
  delimiter = "|"
);

CREATE TABLE sales_delta AS
  SELECT * FROM sales_tmp_vw;
  
SELECT * FROM sales_delta

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Filtering and Renaming Columns from Existing Tables

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases AS
SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
FROM euheniy_khvoinitski_5nea_da_dewd.sales;

SELECT * FROM purchases

-- COMMAND ----------

CREATE OR REPLACE VIEW purchases_vw AS
SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
FROM euheniy_khvoinitski_5nea_da_dewd.sales;

SELECT * FROM purchases_vw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Declare Schema with Generated Columns

-- COMMAND ----------

CREATE OR REPLACE TABLE purchase_dates (
  id STRING, 
  transaction_timestamp STRING, 
  price STRING,
  date DATE GENERATED ALWAYS AS (
    cast(cast(transaction_timestamp/1e6 AS TIMESTAMP) AS DATE))
    COMMENT "generated based on `transactions_timestamp` column")

-- COMMAND ----------

SET spark.databricks.delta.schema.autoMerge.enabled=true; 

MERGE INTO purchase_dates a
USING purchases b
ON a.id = b.id
WHEN NOT MATCHED THEN
  INSERT *

-- COMMAND ----------

SELECT * FROM purchase_dates

-- COMMAND ----------

-- if a field that would otherwise be generated is included in an insert to a table, this insert will fail if the value provided does not exactly match the value that would be derived by the logic used to define the generated column

-- INSERT INTO purchase_dates VALUES
-- (1, 600000000, 42.0, "2020-06-18")

-- InvariantViolationException: CHECK constraint Generated Column (date <=> CAST(CAST((transaction_timestamp / 1000000.0D) AS TIMESTAMP) AS DATE)) violated by row with values:
--  - date : 2020-06-18
--  - transaction_timestamp : 600000000

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Add a Table Constraint

-- COMMAND ----------

ALTER TABLE purchase_dates ADD CONSTRAINT valid_date CHECK (date > '2020-01-01');

-- COMMAND ----------

DESCRIBE EXTENDED purchase_dates;

-- COMMAND ----------

DESCRIBE DETAIL purchase_dates;

-- COMMAND ----------

SHOW TBLPROPERTIES purchase_dates;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Enrich Tables with Additional Options and Metadata

-- COMMAND ----------

CREATE OR REPLACE TABLE users_pii
COMMENT "Contains PII"
LOCATION "${da.paths.working_dir}/tmp/users_pii"
PARTITIONED BY (first_touch_date)
AS
  SELECT *, 
    cast(cast(user_first_touch_timestamp/1e6 AS TIMESTAMP) AS DATE) first_touch_date, 
    current_timestamp() updated,
    input_file_name() source_file
  FROM parquet.`${da.paths.datasets}/ecommerce/raw/users-historical/`;
  
SELECT * FROM users_pii;

-- COMMAND ----------

DESCRIBE EXTENDED users_pii;

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC files = dbutils.fs.ls(f"{DA.paths.working_dir}/tmp/users_pii")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Cloning Delta Lake Tables

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases_clone
DEEP CLONE purchases;

-- COMMAND ----------

CREATE OR REPLACE TABLE purchases_shallow_clone
SHALLOW CLONE purchases

-- COMMAND ----------

DESCRIBE EXTENDED purchases_clone;

-- COMMAND ----------

DESCRIBE EXTENDED purchases_shallow_clone;

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC DA.cleanup()
