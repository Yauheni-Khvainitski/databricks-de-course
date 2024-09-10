# Databricks notebook source
# MAGIC %md
# MAGIC ## Magic commands notebook

# COMMAND ----------

# MAGIC %sh
# MAGIC ls

# COMMAND ----------

# MAGIC %fs
# MAGIC mounts

# COMMAND ----------

# MAGIC %sql
# MAGIC select 1;

# COMMAND ----------

# MAGIC %pip install openai

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import openai

openai.__version__

# COMMAND ----------

dbutils.fs.ls(path='.')
