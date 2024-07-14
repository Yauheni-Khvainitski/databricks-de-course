-- Databricks notebook source
-- MAGIC %run "/Workspace/Users/euheniy.khvoinitski@gmail.com/Databricks DE Course/DE 2 - ETL with Spark/Includes/Classroom-Setup-02.7A"

-- COMMAND ----------

CREATE
OR REPLACE FUNCTION sale_announcement(item_name STRING, item_price INT) RETURNS STRING RETURN concat(
  "The ",
  item_name,
  " is on sale for $",
  round(item_price * 0.8, 0)
);

-- COMMAND ----------

SELECT
  *,
  sale_announcement(name, price) AS message
FROM
  item_lookup

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED sale_announcement;

-- COMMAND ----------

CREATE
OR REPLACE FUNCTION item_preference(name STRING, price INT) RETURNS STRING RETURN CASE
  WHEN name = "Standard Queen Mattress" THEN "This is my default mattress"
  WHEN name = "Premium Queen Mattress" THEN "This is my favorite mattress"
  WHEN price > 100 THEN concat(
    "I'd wait until the ",
    name,
    " is on sale for $",
    round(price * 0.8, 0)
  )
  ELSE concat("I don't need a ", name)
END;

-- COMMAND ----------

SELECT
  *,
  item_preference(name, price)
FROM
  item_lookup

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
