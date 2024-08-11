# Databricks notebook source
# MAGIC %run "/Workspace/Users/euheniy.khvoinitski@gmail.com/Databricks DE Course/DE 4 - Delta Live Tables/Includes/Classroom-Setup-04.1"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run [DLT Pipeline](https://dbc-5dfcd422-972e.cloud.databricks.com/pipelines/930ea604-8c5e-45f6-a97b-4b9c60f850a3/updates/9643506c-fc1d-4e94-ae78-6ab5a186da24?o=8666654081477859) at this point

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   current_timestamp() AS processing_time,
# MAGIC   input_file_name() AS source_file,
# MAGIC   *
# MAGIC FROM
# MAGIC   json.`dbfs:/mnt/dbacademy-users/euheniy.khvoinitski@gmail.com/data-engineering-with-databricks/pipeline_demo/stream-source/orders`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REFRESH STREAMING TABLE orders_bronze
# MAGIC -- AS SELECT current_timestamp() processing_time, input_file_name() source_file, *
# MAGIC -- FROM cloud_files("${source}/orders", "json", map("cloudFiles.inferColumnTypes", "true"))
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   hive_metastore.euheniy_khvoinitski_5nea_da_dewd_pipeline_demo.orders_bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REFRESH STREAMING TABLE orders_silver
# MAGIC -- (CONSTRAINT valid_date EXPECT (order_timestamp > "2021-01-01") ON VIOLATION FAIL UPDATE)
# MAGIC -- COMMENT "Append only orders with valid timestamps"
# MAGIC -- TBLPROPERTIES ("quality" = "silver")
# MAGIC -- AS SELECT timestamp(order_timestamp) AS order_timestamp, * EXCEPT (order_timestamp, source_file, _rescued_data)
# MAGIC -- FROM STREAM(LIVE.orders_bronze)
# MAGIC
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   hive_metastore.euheniy_khvoinitski_5nea_da_dewd_pipeline_demo.orders_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Select Statement
# MAGIC
# MAGIC The select statement contains the core logic of your query. In this example, we:
# MAGIC * Cast the field **`order_timestamp`** to the timestamp type
# MAGIC * Select all of the remaining fields (except a list of 3 we're not interested in, including the original **`order_timestamp`**)
# MAGIC
# MAGIC Note that the **`FROM`** clause has two constructs that you may not be familiar with:
# MAGIC * The **`LIVE`** keyword is used in place of the schema name to refer to the target schema configured for the current DLT pipeline
# MAGIC * The **`STREAM`** method allows users to declare a streaming data source for SQL queries
# MAGIC
# MAGIC Note that if no target schema is declared during pipeline configuration, your tables won't be published (that is, they won't be registered to the metastore and made available for queries elsewhere). The target schema can be easily changed when moving between different execution environments, meaning the same code can easily be deployed against regional workloads or promoted from a dev to prod environment without needing to hard-code schema names.
# MAGIC
# MAGIC ### Data Quality Constraints
# MAGIC
# MAGIC DLT uses simple boolean statements to allow <a href="https://docs.databricks.com/delta-live-tables/expectations.html#delta-live-tables-data-quality-constraints&language-sql" target="_blank">quality enforcement checks</a> on data. In the statement below, we:
# MAGIC * Declare a constraint named **`valid_date`**
# MAGIC * Define the conditional check that the field **`order_timestamp`** must contain a value greater than January 1, 2021
# MAGIC * Instruct DLT to fail the current transaction if any records violate the constraint
# MAGIC
# MAGIC Each constraint can have multiple conditions, and multiple constraints can be set for a single table. In addition to failing the update, constraint violation can also automatically drop records or just record the number of violations while still processing these invalid records.
# MAGIC
# MAGIC ### Table Properties
# MAGIC
# MAGIC The **`TBLPROPERTIES`** field can be used to pass any number of key/value pairs for custom tagging of data. Here, we set the value **`silver`** for the key **`quality`**.
# MAGIC
# MAGIC Note that while this field allows for custom tags to be arbitrarily set, it is also used for configuring number of settings that control how a table will perform. While reviewing table details, you may also encounter a number of settings that are turned on by default any time a table is created.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED hive_metastore.euheniy_khvoinitski_5nea_da_dewd_pipeline_demo.orders_silver;
# MAGIC
# MAGIC -- delta.enableChangeDataFeed = true,
# MAGIC -- delta.minReaderVersion = 1,
# MAGIC -- delta.minWriterVersion = 4,
# MAGIC -- pipelines.metastore.tableName = `euheniy_khvoinitski_5nea_da_dewd_pipeline_demo`.`orders_silver`,
# MAGIC -- pipelines.pipelineId = 930ea604-8c5e-45f6-a97b-4b9c60f850a3,
# MAGIC -- quality = silver

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REFRESH LIVE TABLE orders_by_date
# MAGIC -- AS SELECT date(order_timestamp) AS order_date, count(*) AS total_daily_orders
# MAGIC -- FROM LIVE.orders_silver
# MAGIC -- GROUP BY date(order_timestamp)
# MAGIC
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   hive_metastore.euheniy_khvoinitski_5nea_da_dewd_pipeline_demo.orders_by_date;

# COMMAND ----------

DA.dlt_data_factory.load()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Run [DLT Pipeline](https://dbc-5dfcd422-972e.cloud.databricks.com/pipelines/930ea604-8c5e-45f6-a97b-4b9c60f850a3/updates/9643506c-fc1d-4e94-ae78-6ab5a186da24?o=8666654081477859) at this point

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   current_timestamp() AS processing_time,
# MAGIC   input_file_name() AS source_file,
# MAGIC   *
# MAGIC FROM
# MAGIC   json.`dbfs:/mnt/dbacademy-users/euheniy.khvoinitski@gmail.com/data-engineering-with-databricks/pipeline_demo/stream-source/orders`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE OR REFRESH STREAMING TABLE orders_bronze
# MAGIC -- AS SELECT current_timestamp() processing_time, input_file_name() source_file, *
# MAGIC -- FROM cloud_files("${source}/orders", "json", map("cloudFiles.inferColumnTypes", "true"))
# MAGIC
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   hive_metastore.euheniy_khvoinitski_5nea_da_dewd_pipeline_demo.orders_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full refresh
# MAGIC
# MAGIC `Full refresh will truncate and recompute ALL tables in this pipeline from scratch. This can lead to data loss for non-idempotent sources. Are you sure you want to full refresh?`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fundamentals of DLT SQL Syntax

# COMMAND ----------

# MAGIC %md
# MAGIC There are two distinct types of persistent tables that can be created with DLT:
# MAGIC * **Materialized View** are materialized views for the lakehouse; they will return the current results of any query with each refresh
# MAGIC * **Streaming Tables** are designed for incremental, near-real time data processing
# MAGIC
# MAGIC The basic syntax for a SQL DLT query is:
# MAGIC
# MAGIC **`CREATE OR REFRESH [STREAMING] TABLE table_name`**<br/>
# MAGIC **`AS select_statement`**<br/>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Streaming Ingestion with Auto Loader
# MAGIC
# MAGIC Databricks has developed the [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html) functionality to provide optimized execution for incrementally loading data from cloud object storage into Delta Lake. Using Auto Loader with DLT is simple: just configure a source data directory, provide a few configuration settings, and write a query against your source data. Auto Loader will automatically detect new data files as they land in the source cloud object storage location, incrementally processing new records without the need to perform expensive scans and recomputing results for infinitely growing datasets.
# MAGIC
# MAGIC The **`cloud_files()`** method enables Auto Loader to be used natively with SQL. This method takes the following positional parameters:
# MAGIC * The source location, which should be cloud-based object storage
# MAGIC * The source data format, which is JSON in this case
# MAGIC * An arbitrarily sized comma-separated list of optional reader options. In this case, we set **`cloudFiles.inferColumnTypes`** to **`true`**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Materialized View vs. Streaming Tables
# MAGIC
# MAGIC The two queries we've reviewed so far have both created streaming tables. Below, we see a simple query that returns a live table (or materialized view) of some aggregated data.
# MAGIC
# MAGIC Spark has historically differentiated between batch queries and streaming queries. Live tables and streaming tables have similar differences.
# MAGIC
# MAGIC Note the only syntactic differences between streaming tables and live tables are the lack of the **`STREAMING`** keyword in the create clause and not wrapping the source table in the **`STREAM()`** method.
# MAGIC
# MAGIC Below are some of the differences between these types of tables.
# MAGIC
# MAGIC ### Live Tables
# MAGIC * Always "correct", meaning their contents will match their definition after any update.
# MAGIC * Return same results as if table had just been defined for first time on all data.
# MAGIC * Should not be modified by operations external to the DLT Pipeline (you'll either get undefined answers or your change will just be undone).
# MAGIC
# MAGIC ### Streaming Tables
# MAGIC * Only supports reading from "append-only" streaming sources.
# MAGIC * Only reads each input batch once, no matter what (even if joined dimensions change, or if the query definition changes, etc).
# MAGIC * Can perform operations on the table outside the managed DLT Pipeline (append data, perform GDPR, etc).

# COMMAND ----------

# MAGIC %md
# MAGIC # Python DLT Syntax

# COMMAND ----------

# MAGIC %md
# MAGIC ## About DLT Library Notebooks
# MAGIC
# MAGIC DLT syntax is not intended for interactive execution in a notebook. This notebook will need to be scheduled as part of a DLT pipeline for proper execution. 
# MAGIC
# MAGIC ## Parameterization
# MAGIC
# MAGIC During the configuration of the DLT pipeline, a number of options were specified. One of these was a key-value pair added to the **Configurations** field.
# MAGIC
# MAGIC Configurations in DLT pipelines are similar to parameters in Databricks Jobs, but are actually set as Spark configurations.
# MAGIC
# MAGIC In Python, we can access these values using **`spark.conf.get()`**.
# MAGIC
# MAGIC ## A Note on Imports
# MAGIC
# MAGIC The **`dlt`** module should be explicitly imported into your Python notebook libraries.
# MAGIC
# MAGIC Here, we should importing **`pyspark.sql.functions`** as **`F`**.
# MAGIC
# MAGIC Some developers import **`*`**, while others will only import the functions they need in the present notebook.
# MAGIC
# MAGIC These lessons will use **`F`** throughout so that students clearly know which methods are imported from this library.

# COMMAND ----------

# MAGIC %pip install databricks-dlt

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

# works in the context of Delta Live pipeline
# source = spark.conf.get("source")
source = "dbfs:/mnt/dbacademy-users/euheniy.khvoinitski@gmail.com/data-engineering-with-databricks/pipeline_demo/stream-source"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tables as DataFrames
# MAGIC
# MAGIC Delta Live Tables introduces a number of new Python functions that extend familiar PySpark APIs.
# MAGIC
# MAGIC At the heart of this design, the decorator **`@dlt.table`** is added to any Python function that returns a Spark DataFrame. (**NOTE**: This includes Koalas DataFrames, but these won't be covered in this course.)
# MAGIC
# MAGIC If you're used to working with Spark and/or Structured Streaming, you'll recognize the majority of the syntax used in DLT. The big difference is that you'll never see any methods or options for DataFrame writers, as this logic is handled by DLT.
# MAGIC
# MAGIC As such, the basic form of a DLT table definition will look like:
# MAGIC
# MAGIC **`@dlt.table`**<br/>
# MAGIC **`def <function-name>():`**<br/>
# MAGIC **`    return (<query>)`**</br>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Streaming Ingestion with Auto Loader
# MAGIC
# MAGIC Auto Loader can be combined with Structured Streaming APIs to perform incremental data ingestion throughout Databricks by configuring the **`format("cloudFiles")`** setting. In DLT, you'll only configure settings associated with reading data, noting that the locations for schema inference and evolution will also be configured automatically if those settings are enabled.
# MAGIC
# MAGIC The query below returns a streaming DataFrame from a source configured with Auto Loader.
# MAGIC
# MAGIC In addition to passing **`cloudFiles`** as the format, here we specify:
# MAGIC * The option **`cloudFiles.format`** as **`json`** (this indicates the format of the files in the cloud object storage location)
# MAGIC * The option **`cloudFiles.inferColumnTypes`** as **`True`** (to auto-detect the types of each column)
# MAGIC * The path of the cloud object storage to the **`load`** method
# MAGIC * A select statement that includes a couple of **`pyspark.sql.functions`** to enrich the data alongside all the source fields
# MAGIC
# MAGIC By default, **`@dlt.table`** will use the name of the function as the name for the target table.

# COMMAND ----------

dlt.enable_local_execution()

# COMMAND ----------

@dlt.table
def orders_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", True)
            .load(f"{source}/orders")
            .select(
                F.current_timestamp().alias("processing_time"), 
                F.input_file_name().alias("source_file"), 
                "*"
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validating, Enriching, and Transforming Data
# MAGIC
# MAGIC ### Options for **`@dlt.table()`**
# MAGIC
# MAGIC There are <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-python-ref.html#create-table" target="_blank">a number of options</a> that can be specified during table creation. Here, we use two of these to annotate our dataset.
# MAGIC
# MAGIC ##### **`comment`**
# MAGIC
# MAGIC ##### **`table_properties`**
# MAGIC
# MAGIC This field can be used to pass any number of key/value pairs for custom tagging of data.
# MAGIC
# MAGIC While reviewing table details, you may also encounter a number of settings that are turned on by default any time a table is created.
# MAGIC
# MAGIC ### Data Quality Constraints
# MAGIC
# MAGIC The Python version of DLT uses decorator functions to set <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-expectations.html#delta-live-tables-data-quality-constraints" target="_blank">data quality constraints</a>.
# MAGIC
# MAGIC DLT uses simple boolean statements to allow quality enforcement checks on data. In the statement below, we:
# MAGIC * Declare a constraint named **`valid_date`**
# MAGIC * Define the conditional check that the field **`order_timestamp`** must contain a value greater than January 1, 2021
# MAGIC * Instruct DLT to fail the current transaction if any records violate the constraint by using the decorator **`@dlt.expect_or_fail()`**
# MAGIC
# MAGIC Each constraint can have multiple conditions, and multiple constraints can be set for a single table. In addition to failing the update, constraint violation can also automatically drop records or just record the number of violations while still processing these invalid records.
# MAGIC
# MAGIC ### DLT Read Methods
# MAGIC
# MAGIC The Python **`dlt`** module provides the **`read()`** and **`read_stream()`** methods to easily configure references to other tables and views in your DLT Pipeline. This syntax allows you to reference these datasets by name without any database reference. You can also use **`spark.table("LIVE.<table_name.")`**, where **`LIVE`** is a keyword substituted for the database being referenced in the DLT Pipeline.

# COMMAND ----------

@dlt.table(
    comment = "Append only orders with valid timestamps",
    table_properties = {"quality": "silver"})
@dlt.expect_or_fail("valid_date", F.col("order_timestamp") > "2021-01-01")
def orders_silver_python():
    return (
        dlt.read_stream("orders_bronze_python")
            .select(
                "processing_time",
                "customer_id",
                "notifications",
                "order_id",
                F.col("order_timestamp").cast("timestamp").alias("order_timestamp")
            )
    )

# COMMAND ----------

@dlt.table
def orders_by_date_python():
    return (
        dlt.read("orders_silver_python")  # read and read_stream makes a difference between streaming table and mat view
            .groupBy(F.col("order_timestamp").cast("date").alias("order_date"))
            .agg(F.count("*").alias("total_daily_orders"))
    )

# COMMAND ----------

DA.cleanup()
