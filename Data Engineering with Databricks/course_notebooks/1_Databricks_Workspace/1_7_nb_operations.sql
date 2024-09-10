-- Databricks notebook source
-- MAGIC %run "/Workspace/Users/euheniy.khvoinitski@gmail.com/Databricks DE Course/DE 1 - Databricks Workspace/Includes/Classroom-Setup-01.2"

-- COMMAND ----------

SELECT * FROM demo_tmp_vw

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"DA:                   {DA}")
-- MAGIC print(f"DA.username:          {DA.username}")
-- MAGIC print(f"DA.paths.working_dir: {DA.paths.working_dir}")
-- MAGIC print(f"DA.schema_name:       {DA.schema_name}")

-- COMMAND ----------

SELECT '${da.username}' AS current_username,
       '${da.paths.working_dir}' AS working_directory,
       '${da.schema_name}' as schema_name

-- COMMAND ----------

-- MAGIC %python
-- MAGIC path = f"{DA.paths.datasets}"
-- MAGIC dbutils.fs.ls(path)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC path = f"{DA.paths.datasets}"
-- MAGIC files = dbutils.fs.ls(path)
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
