-- Databricks notebook source
-- MAGIC %run "/Workspace/Users/euheniy.khvoinitski@gmail.com/Databricks DE Course/DE 3 - Delta Lake/Includes/Classroom-Setup-03.5"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Complete Overwrites

-- COMMAND ----------

DESCRIBE HISTORY events;

-- COMMAND ----------

CREATE
OR REPLACE TABLE events AS
SELECT
  *
FROM
  parquet.`${da.paths.datasets}/ecommerce/raw/events-historical`

-- COMMAND ----------

INSERT OVERWRITE events
SELECT
  *
FROM
  parquet.`${da.paths.datasets}/ecommerce/raw/events-historical`

-- COMMAND ----------

DESCRIBE HISTORY events;

-- COMMAND ----------

DESCRIBE HISTORY sales;

-- COMMAND ----------

INSERT OVERWRITE sales
SELECT * FROM parquet.`${da.paths.datasets}/ecommerce/raw/sales-historical/`

-- COMMAND ----------

DESCRIBE HISTORY sales;

-- COMMAND ----------

-- will fail if we try to change our schema (unless we provide optional settings)
INSERT OVERWRITE sales
SELECT *, current_timestamp() FROM parquet.`${da.paths.datasets}/ecommerce/raw/sales-historical`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Append Rows

-- COMMAND ----------

INSERT INTO sales
SELECT * FROM parquet.`${da.paths.datasets}/ecommerce/raw/sales-30m`

-- COMMAND ----------

DESCRIBE HISTORY sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Merge Updates
-- MAGIC
-- MAGIC The main benefits of **`MERGE`**:
-- MAGIC * updates, inserts, and deletes are completed as a single transaction
-- MAGIC * multiple conditionals can be added in addition to matching fields
-- MAGIC * provides extensive options for implementing custom logic

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW users_update AS 
SELECT *, current_timestamp() AS updated 
FROM parquet.`${da.paths.datasets}/ecommerce/raw/users-30m`

-- COMMAND ----------

SELECT
  *
FROM
  users_update
LIMIT
  10;

-- COMMAND ----------

MERGE INTO users a
USING users_update b
ON a.user_id = b.user_id
WHEN MATCHED AND a.email IS NULL AND b.email IS NOT NULL THEN
  UPDATE SET email = b.email, updated = b.updated
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

DESCRIBE HISTORY users;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Insert-Only Merge for Deduplication

-- COMMAND ----------

MERGE INTO events a
USING events_update b
ON a.user_id = b.user_id AND a.event_timestamp = b.event_timestamp
WHEN NOT MATCHED AND b.traffic_source = 'email' THEN 
  INSERT *

-- COMMAND ----------

DESCRIBE HISTORY events;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Load Incrementally
-- MAGIC
-- MAGIC Note that this operation does have some expectations:
-- MAGIC - Data schema should be consistent
-- MAGIC - Duplicate records should try to be excluded or handled downstream
-- MAGIC
-- MAGIC The real value is in multiple executions over time picking up new files in the source automatically.

-- COMMAND ----------

COPY INTO sales
FROM "${da.paths.datasets}/ecommerce/raw/sales-30m"
FILEFORMAT = PARQUET

-- COMMAND ----------

COPY INTO sales
FROM "${da.paths.datasets}/ecommerce/raw/sales-30m"
FILEFORMAT = PARQUET

-- COMMAND ----------

DESCRIBE HISTORY sales;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
