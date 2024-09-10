-- Databricks notebook source
-- MAGIC %run "/Workspace/Users/euheniy.khvoinitski@gmail.com/Databricks DE Course/DE 3 - Delta Lake/Includes/Classroom-Setup-03.2"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Creating a Delta Table with History

-- COMMAND ----------

CREATE TABLE students
  (id INT, name STRING, value DOUBLE);
  
INSERT INTO students VALUES (1, "Yve", 1.0);
INSERT INTO students VALUES (2, "Omar", 2.5);
INSERT INTO students VALUES (3, "Elia", 3.3);

INSERT INTO students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3);
  
UPDATE students 
SET value = value + 1
WHERE name LIKE "T%";

DELETE FROM students 
WHERE value > 6;

CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
  (2, "Omar", 15.2, "update"),
  (3, "", null, "delete"),
  (7, "Blue", 7.7, "insert"),
  (11, "Diya", 8.8, "update");
  
MERGE INTO students b
USING updates u
ON b.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED -- AND u.type = "insert"
  THEN INSERT *;

-- COMMAND ----------

SELECT
  *
FROM
  hive_metastore.euheniy_khvoinitski_5nea_da_dewd.students;

-- COMMAND ----------

DESCRIBE EXTENDED hive_metastore.euheniy_khvoinitski_5nea_da_dewd.students;

-- COMMAND ----------

DESCRIBE DETAIL hive_metastore.euheniy_khvoinitski_5nea_da_dewd.students;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Explore Delta Lake Files

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000007.json`"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Compacting Small Files and Indexing
-- MAGIC
-- MAGIC Files will be combined toward an optimal size (scaled based on the size of the table) by using the **`OPTIMIZE`** command.
-- MAGIC
-- MAGIC **`OPTIMIZE`** will replace existing data files by combining records and rewriting the results.
-- MAGIC
-- MAGIC When executing **`OPTIMIZE`**, users can optionally specify one or several fields for **`ZORDER`** indexing. While the specific math of Z-order is unimportant, it speeds up data retrieval when filtering on provided fields by colocating data with similar values within data files.

-- COMMAND ----------

OPTIMIZE students
ZORDER BY id;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000008.json`"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Reviewing Delta Lake Transactions

-- COMMAND ----------

-- transactions history
DESCRIBE HISTORY students;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Those extra data files that had been marked as removed in our transaction log provide us with the ability to query previous versions of our table.
-- MAGIC
-- MAGIC These time travel queries can be performed by specifying either the integer version or a timestamp.

-- COMMAND ----------

SELECT * 
FROM students VERSION AS OF 3;

-- COMMAND ----------

SELECT * 
FROM students TIMESTAMP AS OF '2024-07-28 18:30:58';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Rollback Versions

-- COMMAND ----------

DELETE FROM students;

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

RESTORE TABLE students TO VERSION AS OF 8;

-- COMMAND ----------

SELECT * FROM students;

-- COMMAND ----------

DESCRIBE HISTORY students;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Cleaning Up Stale Files

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

VACUUM students RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

VACUUM students RETAIN 0 HOURS;

-- COMMAND ----------

DESCRIBE HISTORY students;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
