-- Databricks notebook source
-- MAGIC %md
-- MAGIC **Prepare the environment**

-- COMMAND ----------

-- MAGIC %run "/Workspace/Users/euheniy.khvoinitski@gmail.com/Databricks DE Course/DE 2 - ETL with Spark/Includes/Classroom-Setup-02.1"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **There are some sample Kafka streaming files as a raw data**

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(DA.paths.kafka_events)
-- MAGIC
-- MAGIC files = dbutils.fs.ls(DA.paths.kafka_events)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Check the sample data**

-- COMMAND ----------

SELECT
  *
FROM
  json.`${DA.paths.kafka_events}/001.json`;

-- COMMAND ----------

select
  *
from
  json.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Adding abstractions**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **View**

-- COMMAND ----------

CREATE
OR REPLACE VIEW event_view AS
SELECT
  *
FROM
  json.`${DA.paths.kafka_events}`

-- COMMAND ----------

select
  *
from
  event_view;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Temporary view**

-- COMMAND ----------

CREATE
OR REPLACE TEMP VIEW events_temp_view AS
SELECT
  *
FROM
  json.`${DA.paths.kafka_events}`

-- COMMAND ----------

SELECT
  *
FROM
  events_temp_view

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **CTE**

-- COMMAND ----------

WITH cte_json
AS (SELECT * FROM json.`${DA.paths.kafka_events}`)
SELECT * FROM cte_json

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Reading as a text. file format**

-- COMMAND ----------

SELECT * FROM text.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Extract the Raw Bytes and Metadata of a File**
-- MAGIC
-- MAGIC Using `binaryFile` to query a directory will provide file metadata alongside the binary representation of the file contents (`path`, `modificationTime`, `length`, and `content`).

-- COMMAND ----------

SELECT * FROM binaryFile.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC DA.cleanup()
