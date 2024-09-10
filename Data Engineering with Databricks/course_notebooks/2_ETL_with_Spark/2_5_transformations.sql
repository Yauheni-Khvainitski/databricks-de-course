-- Databricks notebook source
-- MAGIC %run "/Workspace/Users/euheniy.khvoinitski@gmail.com/Databricks DE Course/DE 2 - ETL with Spark/Includes/Classroom-Setup-02.5"

-- COMMAND ----------

DESCRIBE EXTENDED events_raw;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW events_strings AS 
SELECT string(key), string(value) FROM events_raw;

SELECT * FROM events_strings

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC
-- MAGIC events_stringsDF = (spark
-- MAGIC     .table("events_raw")
-- MAGIC     .select(col("key").cast("string"), 
-- MAGIC             col("value").cast("string"))
-- MAGIC     )
-- MAGIC display(events_stringsDF)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **NOTE:** Spark SQL has built-in functionality to directly interact with nested data stored as JSON strings or struct types.
-- MAGIC - Use **`:`** syntax in queries to access subfields in JSON strings
-- MAGIC - Use **`.`** syntax in queries to access subfields in struct types

-- COMMAND ----------

SELECT
  *
FROM
  events_strings
WHERE
  value:event_name = "finalize"
ORDER BY
  key
LIMIT
  10

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(events_stringsDF
-- MAGIC     .where("value:event_name = 'finalize'")
-- MAGIC     .orderBy("key")
-- MAGIC     .limit(10)
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's use the JSON string example above to derive the schema, then parse the entire JSON column into struct types.
-- MAGIC - **`schema_of_json()`** returns the schema derived from an example JSON string.
-- MAGIC - **`from_json()`** parses a column containing a JSON string into a struct type using the specified schema.
-- MAGIC
-- MAGIC After we unpack the JSON string to a struct type, let's unpack and flatten all struct fields into columns.
-- MAGIC - **`*`** unpacking can be used to flattens structs; **`col_name.*`** pulls out the subfields of **`col_name`** into their own columns.

-- COMMAND ----------

SELECT schema_of_json('{"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1047.6,"total_item_quantity":2,"unique_items":2},"event_name":"finalize","event_previous_timestamp":1593879787820475,"event_timestamp":1593879948830076,"geo":{"city":"Huntington Park","state":"CA"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_Q","item_name":"Standard Queen Mattress","item_revenue_in_usd":940.5,"price_in_usd":1045.0,"quantity":1},{"coupon":"NEWBED10","item_id":"P_DOWN_S","item_name":"Standard Down Pillow","item_revenue_in_usd":107.10000000000001,"price_in_usd":119.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593583891412316,"user_id":"UA000000106459577"}') AS schema

-- COMMAND ----------

CREATE
OR REPLACE TEMP VIEW parsed_events AS
SELECT
  json.*
FROM
  (
    SELECT
      from_json(
        value,
        'STRUCT<device: STRING, ecommerce: STRUCT<purchase_revenue_in_usd: DOUBLE, total_item_quantity: BIGINT, unique_items: BIGINT>, event_name: STRING, event_previous_timestamp: BIGINT, event_timestamp: BIGINT, geo: STRUCT<city: STRING, state: STRING>, items: ARRAY<STRUCT<coupon: STRING, item_id: STRING, item_name: STRING, item_revenue_in_usd: DOUBLE, price_in_usd: DOUBLE, quantity: BIGINT>>, traffic_source: STRING, user_first_touch_timestamp: BIGINT, user_id: STRING>'
      ) AS json
    FROM
      events_strings
  );

SELECT
  *
FROM
  parsed_events

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import from_json, schema_of_json
-- MAGIC
-- MAGIC json_string = """
-- MAGIC {"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1047.6,"total_item_quantity":2,"unique_items":2},"event_name":"finalize","event_previous_timestamp":1593879787820475,"event_timestamp":1593879948830076,"geo":{"city":"Huntington Park","state":"CA"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_Q","item_name":"Standard Queen Mattress","item_revenue_in_usd":940.5,"price_in_usd":1045.0,"quantity":1},{"coupon":"NEWBED10","item_id":"P_DOWN_S","item_name":"Standard Down Pillow","item_revenue_in_usd":107.10000000000001,"price_in_usd":119.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593583891412316,"user_id":"UA000000106459577"}
-- MAGIC """
-- MAGIC parsed_eventsDF = (events_stringsDF
-- MAGIC     .select(from_json("value", schema_of_json(json_string)).alias("json"))
-- MAGIC     .select("json.*")
-- MAGIC )
-- MAGIC
-- MAGIC display(parsed_eventsDF)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW exploded_events AS
SELECT *, explode(items) AS item
FROM parsed_events;

SELECT * FROM exploded_events WHERE size(items) > 2

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import explode, size
-- MAGIC
-- MAGIC exploded_eventsDF = (parsed_eventsDF
-- MAGIC     .withColumn("item", explode("items"))
-- MAGIC )
-- MAGIC
-- MAGIC display(exploded_eventsDF.where(size("items") > 2))

-- COMMAND ----------

DESCRIBE exploded_events;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The code below combines array transformations to create a table that shows the unique collection of actions and the items in a user's cart.
-- MAGIC - **`collect_set()`** collects unique values for a field, including fields within arrays.
-- MAGIC - **`flatten()`** combines multiple arrays into a single array.
-- MAGIC - **`array_distinct()`** removes duplicate elements from an array.

-- COMMAND ----------

SELECT user_id,
  collect_set(event_name) AS event_history,
  array_distinct(flatten(collect_set(items.item_id))) AS cart_history
FROM exploded_events
GROUP BY user_id

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import array_distinct, collect_set, flatten
-- MAGIC
-- MAGIC display(exploded_eventsDF
-- MAGIC     .groupby("user_id")
-- MAGIC     .agg(collect_set("event_name").alias("event_history"),
-- MAGIC             array_distinct(flatten(collect_set("items.item_id"))).alias("cart_history"))
-- MAGIC )

-- COMMAND ----------

CREATE
OR REPLACE TEMP VIEW item_purchases AS
SELECT
  *
FROM
  (
    SELECT
      *,
      explode(items) AS item
    FROM
      sales
  ) a
  INNER JOIN item_lookup b ON a.item.item_id = b.item_id;
  
SELECT
  *
FROM
  item_purchases

-- COMMAND ----------

-- MAGIC %python
-- MAGIC exploded_salesDF = (spark
-- MAGIC     .table("sales")
-- MAGIC     .withColumn("item", explode("items"))
-- MAGIC )
-- MAGIC
-- MAGIC itemsDF = spark.table("item_lookup")
-- MAGIC
-- MAGIC item_purchasesDF = (exploded_salesDF
-- MAGIC     .join(itemsDF, exploded_salesDF.item.item_id == itemsDF.item_id, how="inner")
-- MAGIC )
-- MAGIC
-- MAGIC display(item_purchasesDF)

-- COMMAND ----------

SELECT *
FROM item_purchases
PIVOT (
  sum(item.quantity) FOR item_id IN (
    'P_FOAM_K',
    'M_STAN_Q',
    'P_FOAM_S',
    'M_PREM_Q',
    'M_STAN_F',
    'M_STAN_T',
    'M_PREM_K',
    'M_PREM_F',
    'M_STAN_K',
    'M_PREM_T',
    'P_DOWN_S',
    'P_DOWN_K')
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC transactionsDF = (item_purchasesDF
-- MAGIC     .groupBy("order_id", 
-- MAGIC         "email",
-- MAGIC         "transaction_timestamp", 
-- MAGIC         "total_item_quantity", 
-- MAGIC         "purchase_revenue_in_usd", 
-- MAGIC         "unique_items",
-- MAGIC         "items",
-- MAGIC         "item",
-- MAGIC         "name",
-- MAGIC         "price")
-- MAGIC     .pivot("item_id")
-- MAGIC     .sum("item.quantity")
-- MAGIC )
-- MAGIC display(transactionsDF)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()
