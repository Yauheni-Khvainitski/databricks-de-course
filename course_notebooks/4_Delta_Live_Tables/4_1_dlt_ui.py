# Databricks notebook source
# MAGIC %run "/Workspace/Users/euheniy.khvoinitski@gmail.com/Databricks DE Course/DE 4 - Delta Live Tables/Includes/Classroom-Setup-04.1"

# COMMAND ----------

pipeline_language = "SQL"
# pipeline_language = "Python"

DA.print_pipeline_config(pipeline_language)

# COMMAND ----------

DA.validate_pipeline_config(pipeline_language)

# COMMAND ----------

# MAGIC %md
# MAGIC # More DLT SQL Syntax

# COMMAND ----------

DA.cleanup()
