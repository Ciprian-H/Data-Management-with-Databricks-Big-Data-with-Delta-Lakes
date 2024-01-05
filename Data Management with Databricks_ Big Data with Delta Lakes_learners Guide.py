# Databricks notebook source
# MAGIC %md
# MAGIC # Guided Project : Data Management with Databricks: Big Data with <img src="https://docs.delta.io/latest/_static/delta-lake-logo.png" width=300/>

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://upload.wikimedia.org/wikipedia/commons/9/97/Coursera-Logo_600x600.svg" width=50 height=50/>
# MAGIC
# MAGIC **Project Scneario**: You are a Data Engineer working for an online clothing brand that sells a wide range of fashion Brands. The company's Supply Chain team has been tasked with building a dashboard to Analyze Orders history.
# MAGIC
# MAGIC The supply chain team has been tasked with building a dashboard to **Analyze Orders history**. Your dashboard will be used to inform purchasing behaviour and ensure that the company has enough inventory to meet demand for the upcoming holiday season.
# MAGIC
# MAGIC Throughout this real-world business scenario, you will learn how to create and ingest data into a delta table. Then use Databricks notebooks (using Python and SQL) to process/transform the data and produce the Supply chain dashboard. At the end you'll leverage Delta Lake's built-in functionalities such as merge operations and time travel to create a scalable data pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC # TASK 2 - Upload project JSON files to Databricks file system

# COMMAND ----------

# First Check that the parameter "DBFS File Browser" is Enable. Navigate to "Settings > Admin > Workspace settings"  to check

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Upload ORDERS Json files in Databricks File System

# COMMAND ----------

## Load Data Using the UI to this path dbfs:/FileStore/SupplyChain/ORDERS_RAW/

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Check loaded files

# COMMAND ----------

# Use Databricks Utilities (dbutils). Documentation : https://docs.databricks.com/dev-tools/databricks-utils.html#ls-command-dbutilsfsls 
dbutils.fs.ls("dbfs:/FileStore/SupplyChain/ORDERS_RAW/")


# COMMAND ----------

# MAGIC %md
# MAGIC # TASK 3 - Create Delta Table : ORDERS_RAW

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Read multiline json files using spark dataframe:

# COMMAND ----------

# Read multiple line json files using spark dataframeAPI


orders_raw_df = spark.read.option("multiline", "true").json("dbfs:/FileStore/SupplyChain/ORDERS_RAW/ORDERS_RAW_PART_*.json")

## Show the datafarme
orders_raw_df.show(n=5, truncate=False) 

## click on orders_raw_df to Check the schema

# COMMAND ----------

#Validate loaded files Count Number of Rows in the DataFrame, the total Should be "1510"
orders_raw_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![b.](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) b. Create Delta Table ORDERS_RAW

# COMMAND ----------

# MAGIC %md
# MAGIC Delta Lake is 100% compatible with Apache Spark&trade;, which makes it easy to get started with if you already use Spark for your big data workflows.
# MAGIC Delta Lake features APIs for **SQL**, **Python**, and **Scala**, so that you can use it in whatever language you feel most comfortable in.
# MAGIC
# MAGIC
# MAGIC    <img src="https://databricks.com/wp-content/uploads/2020/12/simplysaydelta.png" width=400/>

# COMMAND ----------

# First, Create Database SupplyChainDB if it doesn't exist
db = "SupplyChainDB"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
spark.sql(f"USE {db}")

# COMMAND ----------

## Create DelaTable ORDERS_RAW in the metastore using DataFrame's schema and write data to it
## Documentation : https://docs.delta.io/latest/quick-start.html#create-a-table

orders_raw_df.write.mode("overwrite").format("delta").option("overwriteSchema", "True").saveAsTable("ORDERS_RAW")

# COMMAND ----------

# MAGIC %md
# MAGIC ### c. Show Created Delta Table:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Switch to SQL Cell using %SQL
# MAGIC SHOW tables
# MAGIC  
# MAGIC  -- Alternativerly you can use Python: display(spark.sql(f"SHOW TABLES"))

# COMMAND ----------

# Alternativerly you can use Python: display(spark.sql(f"SHOW TABLES"))
display(spark.sql(f"SHOW TABLES"))

# COMMAND ----------

# MAGIC %md
# MAGIC **d. Validate data loaded successfully to Delta Table ORDERS_RAW**:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM ORDERS_RAW
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ORDERS_RAW

# COMMAND ----------

# MAGIC %md
# MAGIC **e. Decsribe Detail of the Delta Table**:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe DETAIL ORDERS_RAW
# MAGIC
# MAGIC -- Returns the basic metadata information of a delta table.

# COMMAND ----------

# MAGIC %md
# MAGIC #Practice Activity 1 : Create INVENTORY Delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Upload INVENTORY.Json file in DBFS

# COMMAND ----------

## Load the file using the UI to this path dbfs:/FileStore/SupplyChain/INVENTORY/

# COMMAND ----------

# MAGIC %md
# MAGIC ###b. Read the File using spark dataframe

# COMMAND ----------

inventory_df = spark.read.option("multiline","true").json("dbfs:/FileStore/SupplyChain/INVENTORY/INVENTORY.json")

## Show the datafarme
inventory_df.show(n=5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ![c.](https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png) c. Create Delta Table INVENTORY

# COMMAND ----------

# First, Create Database SupplyChainDB
db = "SupplyChainDB"
spark.sql(f"USE {db}")

# COMMAND ----------

## Create INVENTORY Delta Table 
inventory_df.write.mode("overwrite").format("delta").option("overwriteSchema", "True").saveAsTable("INVENTORY")

# COMMAND ----------

# MAGIC %md
# MAGIC ### d. Show Created Delta Tables:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Switch to SQL Cell using %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# Alternativerly you can use Python: display(spark.sql(f"SHOW TABLES"))
display(spark.sql(f"SHOW TABLES"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from INVENTORY

# COMMAND ----------

# MAGIC %md
# MAGIC # TASK 4 - Transform data in delta table

# COMMAND ----------

# MAGIC %md
# MAGIC <a href="https://www.databricks.com/glossary/medallion-architecture" target="_blank">Medallion Architecture</a>   
# MAGIC </br>
# MAGIC <img src="https://databricks.com/wp-content/uploads/2020/09/delta-lake-medallion-model-scaled.jpg" width=900/>

# COMMAND ----------

# MAGIC %md
# MAGIC During this Task you will : 
# MAGIC * 1- Read delta Table using Spark Dataframe
# MAGIC * 2- Convert Data Type String --> Date
# MAGIC * 3- Drop Rows with Null Values
# MAGIC * 4- Add a Computed Column "TOTAL_ORDER"
# MAGIC * 5- Create new deltatable Orders_Gold

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Read ORDERS_RAW delta table using spark Dataframe

# COMMAND ----------

#read Delta Table using spark dataframe

ORDERS_Gold_df=spark.read.table("supplychaindb.ORDERS_raw")

ORDERS_Gold_df.show(n=5,truncate=False)
# Click on ORDERS_DF to See the Schema of the Table. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Update ORDER_DATE Column's Data Type

# COMMAND ----------

#Use withColumn method & to_date()
# withColumn Documentation : https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html
# TO_DATE() Documentation : https://docs.databricks.com/sql/language-manual/functions/to_date.html

from pyspark.sql.functions import *

ORDERS_Gold_df =  ORDERS_Gold_df.withColumn("ORDER_DATE", to_date(col("ORDER_DATE"), "yyyy-MM-dd"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### c. Drop Rows with Null Values

# COMMAND ----------

# Count Nulls for each column
from pyspark.sql.functions import *

display(ORDERS_Gold_df.select([count(when(col(c).isNull(),c)).alias(c) for c in ORDERS_Gold_df.columns]))

# COMMAND ----------

#  Remove Nulls using dropna() method which removes all rows with Null Values 

ORDERS_Gold_df = ORDERS_Gold_df.dropna()

ORDERS_Gold_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### d. Add new Column TOTAL_ORDER

# COMMAND ----------

#Use withColumn function
#Documentation : https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.DataFrame.withColumn.html


ORDERS_Gold_df = ORDERS_Gold_df.withColumn("TOTAL_ORDER", col("QUANTITY")*col("UNIT_PRICE"))

# Display ORDERS_Gold_df to validate the creation of the New Column TOTAL_ORDER
display(ORDERS_Gold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### e. Create Delta Table ORDERS_GOLD

# COMMAND ----------

# Make sure you are using SupplyChainDB
spark.sql(f"USE SupplyChainDB")

## Create DeltaTable Orders_GOLD: 

ORDERS_Gold_df.write.mode("overwrite").format("delta").saveAsTable("ORDERS_Gold")

## Validate that the table was created successfully
display(spark.sql(f"SHOW TABLES"))

# COMMAND ----------

display(spark.sql(f"SHOW TABLES"))

# COMMAND ----------

# MAGIC %md
# MAGIC -- Read more about different write options and parameters here https://docs.delta.io/latest/delta-batch.html#write-to-a-table 
# MAGIC
# MAGIC * **Append** to automatically add new data to an existing Delta table, 
# MAGIC * **Overwrite** To automatically replace all the data in a table:

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ORDERS_Gold

# COMMAND ----------

# MAGIC %md
# MAGIC # TASK 5 - Query Orders Delta table using SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Familiar with Orders_Gold dataset

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get top 30 rows Get Familiar with the Data
# MAGIC select * from supplychaindb.ORDERS_GOLD limit 30

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI-1: Quantity Sold by Country

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dont forget to Filter out Cancelled Orders
# MAGIC select ORDER_COUNTRY, sum(QUANTITY) as TOTAL_DEMAND 
# MAGIC from supplychaindb.ORDERS_GOLD 
# MAGIC where ORDER_STATUS != "Cancelled" 
# MAGIC group by ORDER_COUNTRY 
# MAGIC sort by TOTAL_DEMAND desc

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI-2: Sales by Division ($)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Division = CATEGORY 
# MAGIC -- Dont forget to Filter out Cancelled Orders
# MAGIC select CATEGORY, sum(TOTAL_ORDER) as Revenue
# MAGIC from supplychaindb.ORDERS_GOLD 
# MAGIC where ORDER_STATUS != "Cancelled" 
# MAGIC group by CATEGORY 
# MAGIC order by Revenue desc

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI-3: Top-5 Popular Brands

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Limit Result to 5 and Order Results and order by Sold Quanity
# MAGIC select BRAND, sum(QUANTITY) as TOTAL_SOLD_ITEMS
# MAGIC from supplychaindb.ORDERS_GOLD 
# MAGIC -- where ORDER_STATUS != "Cancelled" 
# MAGIC group by BRAND 
# MAGIC order by TOTAL_SOLD_ITEMS desc
# MAGIC limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(ORDER_ID), ORDER_STATUS
# MAGIC from supplychaindb.ORDERS_GOLD 
# MAGIC where ORDER_STATUS = "Cancelled" 
# MAGIC group by ORDER_STATUS
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # TASK 6 - Create Dashboard

# COMMAND ----------

# Use Databricks UI
# 1- Turn results of Previous Queries into visualisations
# 2- Create Dashboard and add Visualisations

# COMMAND ----------

# MAGIC %md
# MAGIC # Practice Activity 2 : Add Monthly Sales Trend to your Dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC ### KPI-4: Monthly Sales Trend (In QTY)

# COMMAND ----------

# MAGIC %md
# MAGIC ** Instructions :**  
# MAGIC   # 1- Query Delta Table: Orders_Gold to extract Monthly Sales (in Quantity, across all brands and all regions) 
# MAGIC   # 2- Turn the result (Table) into a visualisation (line chart) to Show the Trend for the last 18 months.
# MAGIC   # 3- Add your visualization to the Supply Chain Dashboard.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use DATE_TRUNC()  
# MAGIC select DATE_TRUNC('month',ORDER_DATE) as Month, sum(QUANTITY) as TOTAL_DEMAND 
# MAGIC from supplychaindb.ORDERS_GOLD 
# MAGIC where ORDER_STATUS != "Cancelled" 
# MAGIC group by 1
# MAGIC order by 1 asc

# COMMAND ----------

# MAGIC %md
# MAGIC # TASK 7 - Update Data in Orders table using Merge

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://databricks.com/wp-content/uploads/2020/09/delta-lake-medallion-model-scaled.jpg" width=1012/>

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Upload Json files into DBFS

# COMMAND ----------

# MAGIC %md
# MAGIC Use UI to upload the file "UPDATE_ORDERS_RAW.json" into DBFS, use the same folder dbfs:/FileStore/SupplyChain/ORDERS_RAW/

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Read file using Spark dataframe

# COMMAND ----------

# Read multiple line json file UPDATE_ORDERS_RAW.json
Update_orders_df = spark.read.option("multiline", "true").json("dbfs:/FileStore/SupplyChain/ORDERS_RAW/UPDATE_ORDERS_RAW.json")

## Show the datafarme
display(Update_orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC -->Check the original data **BEFORE MERGE**

# COMMAND ----------

# MAGIC %sql 
# MAGIC select ORDER_ID,ORDER_STATUS,Quantity from Supplychaindb.ORDERS_RAW WHERE ORDER_ID in ("ORD-1281","ORD-829","ORD-193","ORD-826","ORD-842")

# COMMAND ----------

# MAGIC %md
# MAGIC ### c. Update Orders_RAW deltatable using Merge

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL supplychaindb.ORDERS_RAW

# COMMAND ----------

from delta.tables import *

# programmatically interacting with Delta tables using the class delta.tables.DeltaTable(spark: pyspark.sql.session.SparkSession, jdt: JavaObject)
delta_orders_raw =  DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/supplychaindb.db/orders_raw")

# COMMAND ----------

display(delta_orders_raw)

# COMMAND ----------

## merge data into delta Table ORDER_RAW
# DOCUMENTATION https://docs.delta.io/latest/delta-update.html#language-python 

delta_orders_raw.alias("ORDERS_RAW").merge(Update_orders_df.alias("UpdateOrders"), 
                                            "ORDERS_RAW.ORDER_ID = UpdateOrders.ORDER_ID")\
                                              .whenMatchedUpdateAll()\
                                                .whenNotMatchedInsertAll()\
                                                  .execute()

# must be at least one WHEN clause in a MERGE statement.

# COMMAND ----------

# MAGIC %md
# MAGIC --> check the udaptes rows **AFTER MERGE**

# COMMAND ----------

# MAGIC %sql 
# MAGIC select ORDER_ID,ORDER_STATUS,Quantity from SUPPLYCHAINDB.ORDERS_RAW WHERE ORDER_ID in ("ORD-1281","ORD-829","ORD-193","ORD-826","ORD-842")

# COMMAND ----------

# MAGIC %md
# MAGIC Learn More about Merge Operations check out https://docs.delta.io/latest/delta-update.html#language-python

# COMMAND ----------

# MAGIC %md
# MAGIC # TASK 8 - Query previous versions of delta table using **Time Travel**

# COMMAND ----------

# MAGIC %md
# MAGIC **This Task shows how to time travel between different versions of a Delta table with Delta Lake. You can time travel by table version or by timestamp. You’ll learn about the benefits of time travel and why it’s an essential feature for production data workloads.**
# MAGIC
# MAGIC **Documentation : https://delta.io/blog/2023-02-01-delta-lake-time-travel/** 
# MAGIC
# MAGIC <img src="https://delta.io/static/9c42ea9f028932de03257ed75d35a8ba/cf8e5/image1.png" width=1012/>

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Describe Detla Table History:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check Table History 
# MAGIC describe history supplychaindb.orders_raw
# MAGIC -- Use the UI to see Delta Table History

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Using SQL:

# COMMAND ----------

# MAGIC %sql 
# MAGIC  select ORDER_ID,ORDER_STATUS,Quantity 
# MAGIC  from SUPPLYCHAINDB.ORDERS_RAW VERSION AS OF 5 
# MAGIC  WHERE ORDER_ID in ("ORD-1281","ORD-829","ORD-193","ORD-826","ORD-842")
# MAGIC
# MAGIC -- CHange Version Number to See different Versions of the delta table

# COMMAND ----------

# MAGIC %md
# MAGIC ### c. Using Spark dataframe:

# COMMAND ----------

#Time Travel
version_1 = spark.read.format('delta').option('TimeStamp', "2023-05-16").table("SUPPLYCHAINDB.ORDERS_RAW")
display(version_1)

# COMMAND ----------

# MAGIC %md
# MAGIC # END OF THE PROJECT

# COMMAND ----------

# MAGIC %md
# MAGIC # CUMULATIVE CHALLENGE

# COMMAND ----------

# MAGIC %md
# MAGIC **Your Task :</br> Using the “Inventory” data, your task is to enrich the Supply Chain Dashboard with the list of low-stock and out-of-stock Items.** 
# MAGIC
# MAGIC Using Databricks notebook you will : </br>
# MAGIC 1-Upload INVENTORY.JSON file to DBFS(1) </br>
# MAGIC 2-Read the file using spark dataframe (1)</br>
# MAGIC 3-Create Delta Table INVENTORY (1)</br>
# MAGIC 4-Write an SQL query to cross join ORDERS_GOLD and INVENTORY DeltaTables to find the list of Items Low-in Stock or Out-of Stock</br>
# MAGIC 5-Turn the result into a Visualisation (Table type) and Add it to your SupplyChain Dashboard</br>
# MAGIC
# MAGIC </br>(1) Skip if you have completed Practice Activity 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Upload INVENTORY.Json file in DBFS

# COMMAND ----------

## Load the file using the UI to this path dbfs:/FileStore/SupplyChain/INVENTORY/

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Read the File using spark dataframe

# COMMAND ----------

# inventory_df =

## Show the datafarme
inventory_df.show(n=5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### c. Create Delta Table INVENTORY <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-tiny-logo.png" width=35 height=35/>

# COMMAND ----------

# Use SupplyChainDB Database
db = "SupplyChainDB"
spark.sql(f"USE {db}")

# COMMAND ----------

## Create INVENTORY Delta Table 
# inventory_df. 

## Validate that the table was created successfully
display(spark.sql(f"SHOW TABLES"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### d. Cross Join ORDERS_GOLD and INVENTORY DeltaTables to find the list of Low Stock or Out-of Stock Items

# COMMAND ----------

# MAGIC %md
# MAGIC **Your Goal** is to find the list of Low-Stock or Out-of-Stock Items and Add the result to your SupplyChain Dashboard<br />
# MAGIC **Hints:**
# MAGIC * Group all Orders (from ORDERS_GOLD) based-on BRAND, COLOR, PRODUCT_NAME AND SIZE And Add QTY_SOLD (SUM QUANTITY) 
# MAGIC * Cross join the result with INVENTORY using an inner join on BRAND, PRODUCT_NAME, COLOR AND SIZE.
# MAGIC * Add calculated column "QTY_LEFT_STOCK" as (STOCK - QTY_SOLD)
# MAGIC * Filter-out Cancelled ORDERS (ORDER_STATUS)
# MAGIC * Keep only Items with QTY_LEFT_STOCK < 20
# MAGIC * Sort the result by "QTY_LEFT_STOCK" in ascending order

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * FROM (
# MAGIC select O.BRAND, O.COLOR, O.PRODUCT_NAME, O.SIZE, I.STOCK, sum(O.QUANTITY) as QTY_SOLD, (I.STOCK - QTY_SOLD) AS QTY_LEFT_STOCK
# MAGIC from supplychaindb.ORDERS_GOLD O
# MAGIC inner join supplychaindb.INVENTORY I
# MAGIC ON O.BRAND=I.BRAND AND O.COLOR=I.COLOR AND O.PRODUCT_NAME=I.PRODUCT_NAME AND O.SIZE=I.SIZE
# MAGIC where O.ORDER_STATUS != "Cancelled" 
# MAGIC group by O.BRAND, O.COLOR, O.PRODUCT_NAME, O.SIZE, I.STOCK
# MAGIC )
# MAGIC where QTY_LEFT_STOCK < 20
# MAGIC order by QTY_LEFT_STOCK

# COMMAND ----------

# MAGIC %md
# MAGIC ### e. Turn the result into a Visualisation (Table) and Add it to SupplyChain Dashboard

# COMMAND ----------

# Use Databricks UI to Turn results into a visualisation and then add it to your SupplyChain Dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC #  
# MAGIC    <img src="https://www.freeiconspng.com/uploads/congratulations-png-1.png" width=350/>
# MAGIC    .... THIS IS THE END OF THE GUIDED PROJECT
