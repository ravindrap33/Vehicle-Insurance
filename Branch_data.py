# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------


branch_data_df = spark.read.option("header", "true").format("csv").load("/FileStore/tables/branch_data.csv")
agent_data_df = spark.read.option("header", "true").format("csv").load("/FileStore/tables/agent_data.csv")
claims_data_df = spark.read.option("header", "true").format("csv").load("/FileStore/tables/claims_data.csv")
policy_data_df = spark.read.option("header", "true").format("csv").load("/FileStore/tables/policy_data.csv")
accident_data_df = spark.read.option("header", "true").format("csv").load("/FileStore/tables/accident_data.csv")
customer_data_df = spark.read.option("header", "true").format("csv").load("/FileStore/tables/customer_data.csv")
external_data_df = spark.read.option("header", "true").format("csv").load("/FileStore/tables/external_data.csv")
telematic_data_df = spark.read.option("header", "true").format("csv").load("/FileStore/tables/telematic_data.csv")
vehicle_data_df = spark.read.option("header", "true").format("csv").load("/FileStore/tables/vehicle_data.csv")

# COMMAND ----------

# Create a view or table

temp_table_name = "branch_data"

branch_data_df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "branch_data"

# df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM branch_data LIMIT 10

# COMMAND ----------

display(branch_data_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### EDA For Branch Data

# COMMAND ----------

# DBTITLE 1,Schema
branch_data_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Null Count
from pyspark.sql.functions import col, sum, when

# Count nulls for each column
null_counts = branch_data_df.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in branch_data_df.columns
])
null_counts.show()


# COMMAND ----------

# DBTITLE 1,Duplicate Count
branch_data_df.groupBy("Branch_ID", "Location", "Manager","Employees").count().filter("count > 1").show()


# COMMAND ----------

# DBTITLE 1,SQL Duplicate Count
# MAGIC %sql
# MAGIC SELECT *, COUNT(*) AS cnt 
# MAGIC FROM branch_data 
# MAGIC -- WHERE Branch_ID IS NOT NULL AND Location IS NOT NULL AND Manager IS NOT NULL AND Employees IS NOT NULL
# MAGIC GROUP BY ALL 
# MAGIC HAVING cnt > 1 

# COMMAND ----------

# DBTITLE 1,Location Count
branch_data_df.filter(branch_data_df["Location"].isNotNull()).groupBy("Location").count().orderBy("count", ascending=False).show(10)

# COMMAND ----------

branch_data_df.withColumn("Employees",col("Employees").cast("int"))

# COMMAND ----------

# DBTITLE 1,Count of Employees By Location
from pyspark.sql import functions as F

branch_data_df.filter(branch_data_df["Location"].isNotNull()).groupBy("Location").agg(
    F.sum(F.col("Employees").cast("int")).alias("sum(Employees)")
).orderBy("sum(Employees)", ascending=False).show(10)

# COMMAND ----------

branch_data_df.filter(branch_data_df["Employees"].isNotNull()) \
    .orderBy(branch_data_df["Employees"],ascending=False) \
    .show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC #  **Transformations**
# MAGIC ****

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT Branch_ID) distinct_count FROM branch_data WHERE Branch_ID IS NULL OR Location IS NULL OR Manager IS NULL OR Employees IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM branch_data
# MAGIC WHERE Branch_ID IN (
# MAGIC     SELECT Branch_ID
# MAGIC     FROM branch_data
# MAGIC     GROUP BY Branch_ID
# MAGIC     HAVING COUNT(*) > 1
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE branch_data
# MAGIC SET Employees = (SELECT avg(Employees) FROM branch_data)
# MAGIC WHERE Employees IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC SUM(CASE WHEN Employees IS NULL THEN 1 ELSE 0 END) AS Employees_Status FROM branch_data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(Branch_ID) FROM branch_data WHERE Employees IS NULL
