# Databricks notebook source
# MAGIC %run "/Workspace/Users/euheniy.khvoinitski@gmail.com/Databricks DE Course/DE 4 - Delta Live Tables/Includes/Classroom-Setup-04.1"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   json.`dbfs:/mnt/dbacademy-users/euheniy.khvoinitski@gmail.com/data-engineering-with-databricks/pipeline_demo/stream-source/customers`;
# MAGIC -- no duplicates by customer id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   DISTINCT operation
# MAGIC FROM
# MAGIC   json.`dbfs:/mnt/dbacademy-users/euheniy.khvoinitski@gmail.com/data-engineering-with-databricks/pipeline_demo/stream-source/customers`;
# MAGIC -- NEW
# MAGIC -- UPDATE

# COMMAND ----------

# MAGIC %md
# MAGIC Note that the code below omits the Auto Loader option to infer schema. When data is ingested from JSON without the schema provided or inferred, fields will have the correct names but will all be stored as **`STRING`** type.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REFRESH STREAMING TABLE customers_bronze
# MAGIC -- COMMENT "Raw data from customers CDC feed"
# MAGIC -- AS SELECT current_timestamp() processing_time, input_file_name() source_file, *
# MAGIC -- FROM cloud_files("${source}/customers", "json")
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   hive_metastore.euheniy_khvoinitski_5nea_da_dewd_pipeline_demo.customers_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE STREAMING TABLE customers_bronze_clean
# MAGIC -- (CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
# MAGIC -- CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW,
# MAGIC -- CONSTRAINT valid_name EXPECT (name IS NOT NULL or operation = "DELETE"),
# MAGIC -- CONSTRAINT valid_address EXPECT (
# MAGIC --   (address IS NOT NULL and
# MAGIC --   city IS NOT NULL and
# MAGIC --   state IS NOT NULL and
# MAGIC --   zip_code IS NOT NULL) or
# MAGIC --   operation = "DELETE"),
# MAGIC -- CONSTRAINT valid_email EXPECT (
# MAGIC --   rlike(email, '^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$') or
# MAGIC --   operation = "DELETE") ON VIOLATION DROP ROW)
# MAGIC -- AS SELECT *
# MAGIC --   FROM STREAM(LIVE.customers_bronze)
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   hive_metastore.euheniy_khvoinitski_5nea_da_dewd_pipeline_demo.customers_bronze_clean;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Enforcement Continued
# MAGIC
# MAGIC The query demonstrates:
# MAGIC * The 3 options for behavior when constraints are violated
# MAGIC * A query with multiple constraints
# MAGIC * Multiple conditions provided to one constraint
# MAGIC * Using a built-in SQL function in a constraint
# MAGIC
# MAGIC About the data source:
# MAGIC * Data is a CDC feed that contains **`INSERT`**, **`UPDATE`**, and **`DELETE`** operations. 
# MAGIC * Update and insert operations should contain valid entries for all fields.
# MAGIC * Delete operations should contain **`NULL`** values for all fields other than the timestamp, **`customer_id`**, and operation fields.
# MAGIC
# MAGIC In order to ensure only good data makes it into our silver table, we'll write a series of quality enforcement rules that ignore the expected null values in delete operations.
# MAGIC
# MAGIC We'll break down each of these constraints below:
# MAGIC
# MAGIC ##### **`valid_id`**
# MAGIC This constraint will cause our transaction to fail if a record contains a null value in the **`customer_id`** field.
# MAGIC
# MAGIC ##### **`valid_operation`**
# MAGIC This constraint will drop any records that contain a null value in the **`operation`** field.
# MAGIC
# MAGIC ##### **`valid_address`**
# MAGIC This constraint checks if the **`operation`** field is **`DELETE`**; if not, it checks for null values in any of the 4 fields comprising an address. Because there is no additional instruction for what to do with invalid records, violating rows will be recorded in metrics but not dropped.
# MAGIC
# MAGIC ##### **`valid_email`**
# MAGIC This constraint uses regex pattern matching to check that the value in the **`email`** field is a valid email address. It contains logic to not apply this to records if the **`operation`** field is **`DELETE`** (because these will have a null value for the **`email`** field). Violating records are dropped.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processing CDC Data with **`APPLY CHANGES INTO`**
# MAGIC
# MAGIC DLT introduces a new syntactic structure for simplifying CDC feed processing.
# MAGIC
# MAGIC **`APPLY CHANGES INTO`** has the following guarantees and requirements:
# MAGIC * Performs incremental/streaming ingestion of CDC data
# MAGIC * Provides simple syntax to specify one or many fields as the primary key for a table
# MAGIC * Default assumption is that rows will contain inserts and updates
# MAGIC * Can optionally apply deletes
# MAGIC * Automatically orders late-arriving records using user-provided sequencing key
# MAGIC * Uses a simple syntax for specifying columns to ignore with the **`EXCEPT`** keyword
# MAGIC * Will default to applying changes as Type 1 SCD
# MAGIC
# MAGIC The code below:
# MAGIC * Creates the **`customers_silver`** table; **`APPLY CHANGES INTO`** requires the target table to be declared in a separate statement
# MAGIC * Identifies the **`customers_silver`** table as the target into which the changes will be applied
# MAGIC * Specifies the table **`customers_bronze_clean`** as the streaming source
# MAGIC * Identifies the **`customer_id`** as the primary key
# MAGIC * Specifies that records where the **`operation`** field is **`DELETE`** should be applied as deletes
# MAGIC * Specifies the **`timestamp`** field for ordering how operations should be applied
# MAGIC * Indicates that all fields should be added to the target table except **`operation`**, **`source_file`**, and **`_rescued_data`**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REFRESH STREAMING TABLE customers_silver;
# MAGIC -- APPLY CHANGES INTO LIVE.customers_silver
# MAGIC --   FROM STREAM(LIVE.customers_bronze_clean)
# MAGIC --   KEYS (customer_id)
# MAGIC --   APPLY AS DELETE WHEN operation = "DELETE"
# MAGIC --   SEQUENCE BY timestamp
# MAGIC --   COLUMNS * EXCEPT (operation, source_file, _rescued_data)
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   hive_metastore.euheniy_khvoinitski_5nea_da_dewd_pipeline_demo.customers_silver;
# MAGIC
# MAGIC -- https://dbc-5dfcd422-972e.cloud.databricks.com#notebook/3138341963253922/command/3138341963253946

# COMMAND ----------

# MAGIC %md
# MAGIC While the target of our operation in the previous cell was defined as a streaming table, data is being updated and deleted in this table (and so breaks the append-only requirements for streaming table sources). As such, downstream operations cannot perform streaming queries against this table. 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE LIVE TABLE customer_counts_state
# MAGIC --   COMMENT "Total active customers per state"
# MAGIC -- AS SELECT state, count(*) as customer_count, current_timestamp() updated_at
# MAGIC --   FROM LIVE.customers_silver
# MAGIC --   GROUP BY state
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   hive_metastore.euheniy_khvoinitski_5nea_da_dewd_pipeline_demo.customer_counts_state

# COMMAND ----------

# MAGIC %md
# MAGIC ## DLT Views
# MAGIC
# MAGIC Views in DLT differ from persisted tables, and can optionally be defined as **`STREAMING`** (shows incremental data).
# MAGIC
# MAGIC Views have the same update guarantees as live tables, but the results of queries are not stored to disk.
# MAGIC
# MAGIC Unlike views used elsewhere in Databricks, DLT views are not persisted to the metastore, meaning that they can only be referenced from within the DLT pipeline they are a part of. (This is similar scoping to temporary views in most SQL systems.)
# MAGIC
# MAGIC Views can still be used to enforce data quality, and metrics for views will be collected and reported as they would be for tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE LIVE VIEW subscribed_order_emails_v
# MAGIC --   AS SELECT a.customer_id, a.order_id, b.email
# MAGIC --     FROM LIVE.orders_silver a
# MAGIC --     INNER JOIN LIVE.customers_silver b
# MAGIC --     ON a.customer_id = b.customer_id
# MAGIC --     WHERE notifications = 'Y'
# MAGIC
# MAGIC -- select
# MAGIC --   *
# MAGIC -- from
# MAGIC --   hive_metastore.euheniy_khvoinitski_5nea_da_dewd_pipeline_demo.subscribed_order_emails_v
# MAGIC
# MAGIC -- The table or view `hive_metastore`.`euheniy_khvoinitski_5nea_da_dewd_pipeline_demo`.`subscribed_order_emails_v` cannot be found. Verify the spelling and correctness of the schema and catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joins and Referencing Tables Across Notebook Libraries
# MAGIC
# MAGIC The code we've reviewed thus far has shown 2 source datasets propagating through a series of steps in separate notebooks.
# MAGIC
# MAGIC DLT supports scheduling multiple notebooks as part of a single DLT Pipeline configuration. You can edit existing DLT pipelines to add additional notebooks.
# MAGIC
# MAGIC Within a DLT Pipeline, code in any notebook library can reference tables and views created in any other notebook library.
# MAGIC
# MAGIC Essentially, we can think of the scope of the schema reference by the **`LIVE`** keyword to be at the DLT Pipeline level, rather than the individual notebook.
# MAGIC
# MAGIC We create a new view by joining the silver tables from our **`orders`** and **`customers`** datasets. Note that this view is not defined as streaming; as such, we will always capture the current valid **`email`** for each customer, and will automatically drop records for customers after they've been deleted from the **`customers_silver`** table.

# COMMAND ----------

# MAGIC %md
# MAGIC # Python DLT Syntax

# COMMAND ----------

# MAGIC %pip install databricks-dlt

# COMMAND ----------

# MAGIC %md
# MAGIC * **`dlt.apply_changes()`** is used to automatically process CDC data into the silver layer as a Type 1 <a href="https://en.wikipedia.org/wiki/Slowly_changing_dimension" target="_blank">slowly changing dimension (SCD) table]</a>

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

# source = spark.conf.get("source")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Specifying Table Names
# MAGIC
# MAGIC The code below demonstrates the use of the **`name`** option for DLT table declaration. The option allows developers to specify the name for the resultant table separate from the function definition that creates the DataFrame the table is defined from.
# MAGIC
# MAGIC In the example below, we use this option to fulfill a table naming convention of **`<dataset-name>_<data-quality>`** and a function naming convention that describes what the function is doing. (If we hadn't specified this option, the table name would have been inferred from the function as **`ingest_customers_cdc`**.)

# COMMAND ----------

dlt.enable_local_execution()

# COMMAND ----------

@dlt.table(
    name = "customers_bronze_python",
    comment = "Raw data from customers CDC feed"
)
def ingest_customers_cdc(): # Separate from the function name. Allows to use parameters for the naming
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"{source}/customers")
        .select(
            F.current_timestamp().alias("processing_time"),
            F.input_file_name().alias("source_file"),
            "*"
        )
    )

# COMMAND ----------

@dlt.table
@dlt.expect_or_fail("valid_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_operation", "operation IS NOT NULL")
@dlt.expect("valid_name", "name IS NOT NULL or operation = 'DELETE'")
@dlt.expect("valid_adress", """
    (address IS NOT NULL and 
    city IS NOT NULL and 
    state IS NOT NULL and 
    zip_code IS NOT NULL) or
    operation = "DELETE"
    """)
@dlt.expect_or_drop("valid_email", """
    rlike(email, '^([a-zA-Z0-9_\\\\-\\\\.]+)@([a-zA-Z0-9_\\\\-\\\\.]+)\\\\.([a-zA-Z]{2,5})$') or 
    operation = "DELETE"
    """)
def customers_bronze_clean_python():
    return (
        dlt.read_stream("customers_bronze_python")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processing CDC Data with **`dlt.apply_changes()`**
# MAGIC
# MAGIC DLT introduces a new syntactic structure for simplifying CDC feed processing.
# MAGIC
# MAGIC **`dlt.apply_changes()`** has the following guarantees and requirements:
# MAGIC * Uses a simple syntax for specifying columns to ignore with the **`except_column_list`**
# MAGIC
# MAGIC The code below:
# MAGIC * Creates the **`customers_silver`** table; **`dlt.apply_changes()`** requires the target table to be declared in a separate statement

# COMMAND ----------

dlt.create_target_table(    # Function create_target_table has been deprecated. Please use create_streaming_table instead.
    name = "customers_silver_python")

dlt.apply_changes(
    target = "customers_silver_python",
    source = "customers_bronze_clean_python",
    keys = ["customer_id"],
    sequence_by = F.col("timestamp"),
    apply_as_deletes = F.expr("operation = 'DELETE'"),
    except_column_list = ["operation", "source_file", "_rescued_data"])

# COMMAND ----------

@dlt.table(
    comment="Total active customers per state")
def customer_counts_state_python():
    return (
        dlt.read("customers_silver_python")
            .groupBy("state")
            .agg( 
                F.count("*").alias("customer_count"), 
                F.first(F.current_timestamp()).alias("updated_at")
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## DLT Views
# MAGIC
# MAGIC The query below defines a DLT view by using the **`@dlt.view`** decorator.

# COMMAND ----------

@dlt.view
def subscribed_order_emails_v_python():
    return (
        dlt.read("orders_silver_python").filter("notifications = 'Y'").alias("a")
            .join(
                dlt.read("customers_silver_python").alias("b"), 
                on="customer_id"
            ).select(
                "a.customer_id", 
                "a.order_id", 
                "b.email"
            )
    )

# COMMAND ----------

DA.cleanup()
