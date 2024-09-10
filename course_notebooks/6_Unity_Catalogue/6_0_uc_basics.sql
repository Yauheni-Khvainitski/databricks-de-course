-- Databricks notebook source
-- MAGIC %run "/Workspace/Users/euheniy.khvoinitski@gmail.com/data-engineering-with-databricks/DE 6 - Managing Permissions/Includes/Classroom-Setup-06.1"

-- COMMAND ----------

SELECT current_metastore();

-- COMMAND ----------

SHOW STORAGE CREDENTIALS;

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS edu_uc_ext_location
  URL 's3://edu-databricks-unity-storage-location/'
  WITH (STORAGE CREDENTIAL edu_storage_credentials)
  COMMENT 'Storage for educational unity catalogs';

-- COMMAND ----------

SHOW EXTERNAL LOCATIONS;

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS ${DA.my_new_catalog}
MANAGED LOCATION 's3://edu-databricks-unity-storage-location/';

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

SELECT current_catalog();

-- COMMAND ----------

USE CATALOG ${DA.my_new_catalog}

-- COMMAND ----------

SELECT current_catalog();

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS example;

-- COMMAND ----------

-- Creating the silver table with patient heart rate data and PII

CREATE OR REPLACE TABLE example.heartrate_device (device_id INT, mrn STRING, name STRING, time TIMESTAMP, heartrate DOUBLE);

INSERT INTO example.heartrate_device VALUES
  (23, "40580129", "Nicholas Spears", "2020-02-01T00:01:58.000+0000", 54.0122153343),
  (17, "52804177", "Lynn Russell", "2020-02-01T00:02:55.000+0000", 92.5136468131),
  (37, "65300842", "Samuel Hughes", "2020-02-01T00:08:58.000+0000", 52.1354807863),
  (23, "40580129", "Nicholas Spears", "2020-02-01T00:16:51.000+0000", 54.6477014191),
  (17, "52804177", "Lynn Russell", "2020-02-01T00:18:08.000+0000", 95.033344842);
  
SELECT * FROM example.heartrate_device

-- COMMAND ----------

-- Creating a gold view to share with other users

CREATE OR REPLACE VIEW example.agg_heartrate AS (
  SELECT mrn, name, MEAN(heartrate) avg_heartrate, DATE_TRUNC("DD", time) date
  FROM example.heartrate_device
  GROUP BY mrn, name, DATE_TRUNC("DD", time)
);
SELECT * FROM example.agg_heartrate

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Grant access to data

-- COMMAND ----------

GRANT USAGE ON CATALOG ${DA.my_new_catalog} TO `account users`;
GRANT USAGE ON SCHEMA example TO `account users`;
GRANT SELECT ON VIEW example.agg_heartrate to `account users`

-- COMMAND ----------

SELECT "SELECT * FROM ${DA.my_new_catalog}.example.agg_heartrate" AS Query

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Running the query from SQL Warehouse

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Another user could select from gold view, but couldn't from silver table with insufficient privilege error. As expected

-- COMMAND ----------

CREATE OR REPLACE FUNCTION example.my_mask(x STRING)
  RETURNS STRING
  RETURN CONCAT(REPEAT("*", LENGTH(x) - 2), RIGHT(x, 2)
); 
SELECT example.my_mask('sensitive data') AS data

-- COMMAND ----------

GRANT EXECUTE ON FUNCTION example.my_mask to `account users`;

-- COMMAND ----------

SELECT "SELECT ${DA.my_new_catalog}.example.my_mask('sensitive data') AS data" AS Query

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Dynamic views (protect rows and columns)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Dynamic views provide the ability to do fine-grained access control of columns and rows within a table, conditional on the principal running the query. Dynamic views are an extension to standard views that allow us to do things like:
-- MAGIC * partially obscure column values or redact them entirely
-- MAGIC * omit rows based on specific criteria
-- MAGIC
-- MAGIC Access control with dynamic views is achieved through the use of functions within the definition of the view. These functions include:
-- MAGIC * **`current_user()`**: returns the email address of the user querying the view
-- MAGIC * **`is_account_group_member()`**: returns TRUE if the user querying the view is a member of the specified group
-- MAGIC
-- MAGIC Note: please refrain from using the legacy function **`is_member()`**, which references workspace-level groups. This is not good practice in the context of Unity Catalog.

-- COMMAND ----------

CREATE OR REPLACE VIEW example.agg_heartrate AS
SELECT
  CASE WHEN
    is_account_group_member('account users') THEN 'REDACTED'
    ELSE mrn
  END AS mrn,
  CASE WHEN
    is_account_group_member('account users') THEN 'REDACTED'
    ELSE name
  END AS name,
  MEAN(heartrate) avg_heartrate,
  DATE_TRUNC("DD", time) date
  FROM example.heartrate_device
  GROUP BY mrn, name, DATE_TRUNC("DD", time)

-- COMMAND ----------

GRANT SELECT ON VIEW example.agg_heartrate to `account users`;

-- COMMAND ----------

SELECT "SELECT * FROM ${DA.my_new_catalog}.example.agg_heartrate" AS Query

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Restrict rows

-- COMMAND ----------

CREATE OR REPLACE VIEW example.agg_heartrate AS
SELECT
  mrn,
  time,
  device_id,
  heartrate
FROM example.heartrate_device
WHERE
  CASE WHEN
    is_account_group_member('account users') THEN device_id < 30
    ELSE TRUE
  END

-- COMMAND ----------

GRANT SELECT ON VIEW example.agg_heartrate to `account users`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Data masking

-- COMMAND ----------

DROP VIEW IF EXISTS example.agg_heartrate;

CREATE VIEW example.agg_heartrate AS
SELECT
  CASE WHEN
    is_account_group_member('account users') THEN example.my_mask(mrn)
    ELSE mrn
  END AS mrn,
  time,
  device_id,
  heartrate
FROM example.heartrate_device
WHERE
  CASE WHEN
    is_account_group_member('account users') THEN device_id < 30
    ELSE TRUE
  END

-- COMMAND ----------

GRANT SELECT ON VIEW example.agg_heartrate to `account users`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Explore objects

-- COMMAND ----------

SHOW TABLES IN example;

-- COMMAND ----------

SHOW VIEWS IN example;

-- COMMAND ----------

SHOW SCHEMAS IN ${DA.my_new_catalog};

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

SHOW GRANTS ON VIEW example.agg_heartrate;

-- COMMAND ----------

SHOW GRANTS ON TABLE example.heartrate_device;

-- COMMAND ----------

SHOW GRANTS ON SCHEMA example

-- COMMAND ----------

SHOW GRANTS ON CATALOG ${DA.my_new_catalog}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Revoke access

-- COMMAND ----------

SHOW GRANTS ON FUNCTION example.my_mask

-- COMMAND ----------

REVOKE EXECUTE ON FUNCTION example.my_mask FROM `account users`

-- COMMAND ----------

SHOW GRANTS ON FUNCTION example.my_mask

-- COMMAND ----------

REVOKE USAGE ON CATALOG ${DA.my_new_catalog} FROM `account users`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Cleanup

-- COMMAND ----------

USE CATALOG hive_metastore;
DROP CATALOG IF EXISTS ${DA.my_new_catalog} CASCADE;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
