# Databricks notebook source
# MAGIC %run "/Workspace/Users/euheniy.khvoinitski@gmail.com/Databricks DE Course/DE 4 - Delta Live Tables/Includes/Classroom-Setup-04.3"

# COMMAND ----------

# MAGIC %sql
# MAGIC USE ${DA.schema_name};
# MAGIC
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_silver

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The **`customers_silver`** table correctly represents the current active state of our Type 1 table with changes applied. However, our **customers_silver** table is actually implemented as a view against a hidden table named **__apply_changes_storage_customers_silver**, which includes additional fields: **__Timestamp**, **__DeleteVersion**, and **__UpsertVersion**.
# MAGIC
# MAGIC We can see this if we run **`DESCRIBE EXTENDED`**.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED customers_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM __apply_changes_storage_customers_silver

# COMMAND ----------

files = dbutils.fs.ls(DA.paths.storage_location)
display(files)

# COMMAND ----------

display(spark.sql(f"SELECT * FROM delta.`{DA.paths.storage_location}/system/events`"))

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/tables")
display(files)

# COMMAND ----------

DA.cleanup()
