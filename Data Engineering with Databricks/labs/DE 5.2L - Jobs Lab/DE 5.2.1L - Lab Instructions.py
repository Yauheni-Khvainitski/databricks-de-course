# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-7b85ffd8-b52c-4ea8-84db-b3668e13b402
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC # Lab: Orchestrating Jobs with Databricks
# MAGIC
# MAGIC In this lab, you'll be configuring a multi-task job comprising of:
# MAGIC * A notebook that lands a new batch of data in a storage directory
# MAGIC * A Delta Live Table pipeline that processes this data through a series of tables
# MAGIC * A notebook that queries the gold table produced by this pipeline as well as various metrics output by DLT
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you should be able to:
# MAGIC * Schedule a notebook as a task in a Databricks Job
# MAGIC * Schedule a DLT pipeline as a task in a Databricks Job
# MAGIC * Configure linear dependencies between tasks using the Databricks Workflows UI

# COMMAND ----------

# MAGIC %run "/Workspace/Users/euheniy.khvoinitski@gmail.com/Databricks DE Course/DE 5 - Workflow Jobs/Includes/Classroom-Setup-05.2.1L"

# COMMAND ----------

# DBTITLE 0,--i18n-003b65fd-018a-43e5-8d1d-1ce2bee52fe3
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Land Initial Data
# MAGIC Seed the landing zone with some data before proceeding. 
# MAGIC
# MAGIC You will re-run this command to land additional data later.

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   input_file_name() source_file,
# MAGIC   *
# MAGIC FROM
# MAGIC   json.`dbfs:/mnt/dbacademy-users/euheniy.khvoinitski@gmail.com/data-engineering-with-databricks/jobs_lab/stream/`

# COMMAND ----------

# DBTITLE 0,--i18n-cc5b4584-59c4-49c9-a6d2-efcdadb98dbe
# MAGIC %md
# MAGIC
# MAGIC ## Generate Job Configuration
# MAGIC
# MAGIC Configuring this job will require parameters unique to a given user.
# MAGIC
# MAGIC Run the cell below to print out values you'll use to configure your pipeline in subsequent steps.

# COMMAND ----------

DA.print_job_config()

# COMMAND ----------

# DBTITLE 0,--i18n-542fd1c6-0322-4c8f-8719-fe980f2a8013
# MAGIC %md
# MAGIC
# MAGIC ## Configure Job with a Single Notebook Task
# MAGIC
# MAGIC Let's start by scheduling the first notebook.
# MAGIC
# MAGIC Steps:
# MAGIC 1. Click the **Workflows** button on the sidebar, click the **Jobs** tab, and click the **Create Job** button.
# MAGIC 2. Configure the job and task as specified below. You'll need the values provided in the cell output above for this step.
# MAGIC
# MAGIC | Setting | Instructions |
# MAGIC |--|--|
# MAGIC | Task name | Enter **Batch-Job** |
# MAGIC | Type | Choose **Notebook** |
# MAGIC | Source | Choose **Workspace** |
# MAGIC | Path | Use the navigator to specify the **Batch Notebook Path** provided above |
# MAGIC | Cluster | Select your cluster from the dropdown, under **Existing All Purpose Clusters** |
# MAGIC | Job name | In the top-left of the screen, enter the **Job Name** provided above to add a name for the job (not the task) |
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 3. Click the **Create** button.
# MAGIC 4. Click the blue **Run now** button in the top right to start the job.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **Note**: When selecting your all-purpose cluster, you will get a warning about how this will be billed as all-purpose compute. Production jobs should always be scheduled against new job clusters appropriately sized for the workload, as this is billed at a much lower rate.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   DISTINCT input_file_name() AS source_file
# MAGIC FROM
# MAGIC   json.`dbfs:/mnt/dbacademy-users/euheniy.khvoinitski@gmail.com/data-engineering-with-databricks/jobs_lab/stream/`

# COMMAND ----------

# DBTITLE 0,--i18n-4678fc9d-ab2f-4f8c-b4be-a67774b2afd4
# MAGIC %md
# MAGIC ## Generate Pipeline
# MAGIC
# MAGIC In this step, we'll add a DLT pipeline to execute after the success of the task we configured above.
# MAGIC
# MAGIC To focus on jobs and not pipelines, we are going to use the following utility command to create a simple pipeline for us.

# COMMAND ----------

DA.create_pipeline()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   hive_metastore.euheniy_khvoinitski_5nea_da_dewd_jobs_lab.daily_patient_avg;

# COMMAND ----------

# DBTITLE 0,--i18n-8ab4bc2a-e08e-47ca-8507-9565342acfb6
# MAGIC %md
# MAGIC
# MAGIC ## Add a Pipeline Task
# MAGIC
# MAGIC Steps:
# MAGIC 1. On the Job details page, click the **Tasks** tab.
# MAGIC 1. Click the blue **+ Add task** button at the center bottom of the screen, and select **Delta Live Tables pipeline** in the dropdown menu.
# MAGIC 1. Configure the task:
# MAGIC
# MAGIC | Setting | Instructions |
# MAGIC |--|--|
# MAGIC | Task name | Enter **DLT** |
# MAGIC | Type | Leave **Delta Live Tables pipeline** |
# MAGIC | Pipeline | Choose the DLT pipeline configured above |
# MAGIC | Depends on | Choose **Batch-Job**, which is the previous task we defined |
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 4. Click the blue **Create task** button
# MAGIC     - You should now see a screen with 2 boxes and a downward arrow between them. 
# MAGIC     - Your **`Batch-Job`** task will be at the top, leading into your **`DLT`** task.

# COMMAND ----------

# DBTITLE 0,--i18n-1cdf590a-6da0-409d-b4e1-bd5a829f3a66
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Add Another Notebook Task
# MAGIC
# MAGIC An additional notebook has been provided which queries some of the DLT metrics and the gold table defined in the DLT pipeline. We'll add this as a final task in our job.
# MAGIC
# MAGIC Steps:
# MAGIC 1. On the Job details page, click the **Tasks** tab.
# MAGIC 1. Click the blue **+ Add task** button at the center bottom of the screen and select **Notebook** in the dropdown menu.
# MAGIC 1. Configure the task:
# MAGIC
# MAGIC | Setting | Instructions |
# MAGIC |--|--|
# MAGIC | Task name | Enter **Query-Results** |
# MAGIC | Type | Choose **Notebook** |
# MAGIC | Source | Choose **Workspace** |
# MAGIC | Path | Use the navigator to specify the **Query Notebook Path** provided above |
# MAGIC | Cluster | Select your cluster from the dropdown, under **Existing All Purpose Clusters** |
# MAGIC | Depends on | Choose **DLT**, which is the previous task we defined |
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 4. Click the blue **Create task** button
# MAGIC 5. Click the blue **Run now** button in the top right to run this job.
# MAGIC     - From the **Runs** tab, you will be able to click on the start time for this run under the **Active runs** section and visually track task progress.
# MAGIC     - Once all your tasks have succeeded, review the contents of each task to confirm expected behavior.

# COMMAND ----------

# This function is provided for students who do not 
# want to work through the exercise of creating the job.
# DA.create_job()

# COMMAND ----------

# DA.validate_job_config()

# COMMAND ----------

# This function is provided to start the job and  
# block until it has completed, canceled or failed
# DA.start_job()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
