-- Databricks notebook source
-- MAGIC %run "/Workspace/Users/euheniy.khvoinitski@gmail.com/Databricks DE Course/DE 2 - ETL with Spark/Includes/Classroom-Setup-02.99"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### FILTER

-- COMMAND ----------

SELECT
  *
FROM
  (
    SELECT
      order_id,
      FILTER (items, i -> i.item_id LIKE "%K") AS king_items,
      items
    FROM
      sales
  )
WHERE
  size(king_items) > 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### TRANSFORM

-- COMMAND ----------

SELECT
  *,
  TRANSFORM (
    items,
    i -> CAST(i.item_revenue_in_usd * 100 AS INT)
  ) AS item_revenues
FROM
  sales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### EXISTS

-- COMMAND ----------

CREATE OR REPLACE TABLE sales_product_flags AS
SELECT
  items,
  EXISTS(items, i -> i.item_name LIKE "%Mattress") AS mattress,
  EXISTS(items, i -> i.item_name LIKE "%Pillow") AS pillow
FROM
  sales

-- COMMAND ----------

SELECT
  *
FROM
  sales_product_flags

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def check_table_results(table_name, num_rows, column_names):
-- MAGIC     assert spark.table(table_name), f"Table named **`{table_name}`** does not exist"
-- MAGIC     assert set(spark.table(table_name).columns) == set(column_names), "Please name the columns as shown in the schema above"
-- MAGIC     assert spark.table(table_name).count() == num_rows, f"The table should have {num_rows} records"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC check_table_results("sales_product_flags", 10510, ['items', 'mattress', 'pillow'])
-- MAGIC product_counts = spark.sql("SELECT sum(CAST(mattress AS INT)) num_mattress, sum(CAST(pillow AS INT)) num_pillow FROM sales_product_flags").first().asDict()
-- MAGIC assert product_counts == {'num_mattress': 9986, 'num_pillow': 1384}, "There should be 9986 rows where mattress is true, and 1384 where pillow is true"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
