# Databricks notebook source
# MAGIC %run "/Workspace/Users/euheniy.khvoinitski@gmail.com/Databricks DE Course/DE 4 - Delta Live Tables/Includes/Classroom-Setup-04.4"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Event Log

# COMMAND ----------


event_log_path = f"{DA.paths.storage_location}/system/events"

event_log = spark.read.format('delta').load(event_log_path)
event_log.createOrReplaceTempView("event_log_raw")

display(event_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Latest Update ID
# MAGIC
# MAGIC In many cases, you may wish to gain updates about the latest update (or the last N updates) to your pipeline.

# COMMAND ----------


latest_update_id = spark.sql("""
    SELECT origin.update_id
    FROM event_log_raw
    WHERE event_type = 'create_update'
    ORDER BY timestamp DESC LIMIT 1""").first().update_id

print(f"Latest Update ID: {latest_update_id}")

# Push back into the spark config so that we can use it in a later query.
spark.conf.set('latest_update.id', latest_update_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Perform Audit Logging

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT timestamp, details:user_action:action, details:user_action:user_name
# MAGIC FROM event_log_raw 
# MAGIC WHERE event_type = 'user_action'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Examine Lineage

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT details:flow_definition.output_dataset, details:flow_definition.input_datasets 
# MAGIC FROM event_log_raw 
# MAGIC WHERE event_type = 'flow_definition' AND 
# MAGIC       origin.update_id = '${latest_update.id}'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Examine Data Quality Metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT row_expectations.dataset as dataset,
# MAGIC        row_expectations.name as expectation,
# MAGIC        SUM(row_expectations.passed_records) as passing_records,
# MAGIC        SUM(row_expectations.failed_records) as failing_records
# MAGIC FROM
# MAGIC   (SELECT explode(
# MAGIC             from_json(details :flow_progress :data_quality :expectations,
# MAGIC                       "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>")
# MAGIC           ) row_expectations
# MAGIC    FROM event_log_raw
# MAGIC    WHERE event_type = 'flow_progress' AND 
# MAGIC          origin.update_id = '${latest_update.id}'
# MAGIC   )
# MAGIC GROUP BY row_expectations.dataset, row_expectations.name

# COMMAND ----------

DA.cleanup()
