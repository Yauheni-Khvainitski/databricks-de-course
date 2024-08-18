# Databricks notebook source
# MAGIC %run "/Workspace/Users/euheniy.khvoinitski@gmail.com/Databricks DE Course/DE 5 - Workflow Jobs/Includes/Classroom-Setup-05.1.1"

# COMMAND ----------

DA.print_job_config_v1()

# COMMAND ----------

DA.create_pipeline()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   csv.`dbfs:/mnt/dbacademy-datasets/data-engineer-learning-path/v04/retail-org/customers/`

# COMMAND ----------

DA.cleanup()
