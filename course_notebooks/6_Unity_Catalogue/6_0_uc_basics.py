# Databricks notebook source
# MAGIC %run "/Workspace/Users/euheniy.khvoinitski@gmail.com/data-engineering-with-databricks/DE 6 - Managing Permissions/Includes/Classroom-Setup-06.1"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_metastore();

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW STORAGE CREDENTIALS;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS edu_uc_ext_location
# MAGIC   URL 's3://edu-databricks-unity-storage-location/'
# MAGIC   WITH (STORAGE CREDENTIAL edu_storage_credentials)
# MAGIC   COMMENT 'Storage for educational unity catalogs';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW EXTERNAL LOCATIONS;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${DA.my_new_catalog}
# MAGIC MANAGED LOCATION 's3://edu-databricks-unity-storage-location/';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ${DA.my_new_catalog}

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog();

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS example;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating the silver table with patient heart rate data and PII
# MAGIC
# MAGIC CREATE OR REPLACE TABLE example.heartrate_device (device_id INT, mrn STRING, name STRING, time TIMESTAMP, heartrate DOUBLE);
# MAGIC
# MAGIC INSERT INTO example.heartrate_device VALUES
# MAGIC   (23, "40580129", "Nicholas Spears", "2020-02-01T00:01:58.000+0000", 54.0122153343),
# MAGIC   (17, "52804177", "Lynn Russell", "2020-02-01T00:02:55.000+0000", 92.5136468131),
# MAGIC   (37, "65300842", "Samuel Hughes", "2020-02-01T00:08:58.000+0000", 52.1354807863),
# MAGIC   (23, "40580129", "Nicholas Spears", "2020-02-01T00:16:51.000+0000", 54.6477014191),
# MAGIC   (17, "52804177", "Lynn Russell", "2020-02-01T00:18:08.000+0000", 95.033344842);
# MAGIC   
# MAGIC SELECT * FROM example.heartrate_device

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creating a gold view to share with other users
# MAGIC
# MAGIC CREATE OR REPLACE VIEW example.agg_heartrate AS (
# MAGIC   SELECT mrn, name, MEAN(heartrate) avg_heartrate, DATE_TRUNC("DD", time) date
# MAGIC   FROM example.heartrate_device
# MAGIC   GROUP BY mrn, name, DATE_TRUNC("DD", time)
# MAGIC );
# MAGIC SELECT * FROM example.agg_heartrate

# COMMAND ----------

# MAGIC %md
# MAGIC ####Grant access to data

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USAGE ON CATALOG ${DA.my_new_catalog} TO `account users`;
# MAGIC GRANT USAGE ON SCHEMA example TO `account users`;
# MAGIC GRANT SELECT ON VIEW example.agg_heartrate to `account users`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "SELECT * FROM ${DA.my_new_catalog}.example.agg_heartrate" AS Query

# COMMAND ----------

# MAGIC %md
# MAGIC #### Running the query from SQL Warehouse

# COMMAND ----------

# MAGIC %md
# MAGIC Another user could select from gold view, but couldn't from silver table with insufficient privilege error. As expected

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION example.my_mask(x STRING)
# MAGIC   RETURNS STRING
# MAGIC   RETURN CONCAT(REPEAT("*", LENGTH(x) - 2), RIGHT(x, 2)
# MAGIC ); 
# MAGIC SELECT example.my_mask('sensitive data') AS data

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT EXECUTE ON FUNCTION example.my_mask to `account users`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "SELECT ${DA.my_new_catalog}.example.my_mask('sensitive data') AS data" AS Query

# COMMAND ----------



# COMMAND ----------


