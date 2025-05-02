# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import *

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.awdatalakechennu.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.awdatalakechennu.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.awdatalakechennu.dfs.core.windows.net", "1dd16d08-e6f2-4312-9a59-42606f1af056")
spark.conf.set("fs.azure.account.oauth2.client.secret.awdatalakechennu.dfs.core.windows.net", 'VfK8Q~dL~34RXdyWKYnC3bbCC7I.EzW622EgwcH4')
spark.conf.set("fs.azure.account.oauth2.client.endpoint.awdatalakechennu.dfs.core.windows.net", "https://login.microsoftonline.com/aaa14a06-7a47-4cee-a47c-4fec9ef3cc4f/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Loading

# COMMAND ----------

df_cal=spark.read.format("csv")\
    .option("header","true").option("inferSchema","true")\
        .load("abfss://bronze@awdatalakechennu.dfs.core.windows.net/AdventureWorks_Calendar")

# COMMAND ----------

df_cal.display()

# COMMAND ----------

df_cus = spark.read.format("csv")\
    .option("header","true").option("inferSchema","true")\
        .load("abfss://bronze@awdatalakechennu.dfs.core.windows.net/AdventureWorks_Customers")

# COMMAND ----------

df_procat = spark.read.format("csv")\
    .option("header","true").option("inferSchema","true")\
        .load("abfss://bronze@awdatalakechennu.dfs.core.windows.net/AdventureWorks_Product_Categories")

# COMMAND ----------

df_pro = spark.read.format("csv")\
    .option("header","true").option("inferSchema","true")\
        .load("abfss://bronze@awdatalakechennu.dfs.core.windows.net/AdventureWorks_Products")

# COMMAND ----------

df_ret = spark.read.format("csv")\
    .option("header","true").option("inferSchema","true")\
        .load("abfss://bronze@awdatalakechennu.dfs.core.windows.net/AdventureWorks_Returns")

# COMMAND ----------

df_sales = spark.read.format("csv")\
    .option("header","true").option("inferSchema","true")\
        .load("abfss://bronze@awdatalakechennu.dfs.core.windows.net/AdventureWorks_Sales*")

# COMMAND ----------

df_ter = spark.read.format("csv")\
    .option("header","true").option("inferSchema","true")\
        .load("abfss://bronze@awdatalakechennu.dfs.core.windows.net/AdventureWorks_Territories")

# COMMAND ----------

df_subcat = spark.read.format("csv")\
    .option("header","true").option("inferSchema","true")\
        .load("abfss://bronze@awdatalakechennu.dfs.core.windows.net/Product_Subcategories")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tranformations

# COMMAND ----------

df_cal=df_cal.withColumn('Month',month('Date')).withColumn('Year',year('Date'))

# COMMAND ----------

df_cal.display()

# COMMAND ----------

df_cal.write.format('parquet')\
    .mode('append')\
        .option('path','abfss://silver@awdatalakechennu.dfs.core.windows.net/AdventureWorks_Calendar')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### customers

# COMMAND ----------

  df_cus=df_cus.withColumn('FullName',concat(col('Prefix'),lit(' '),col('FirstName'),lit(' '),col('LastName')))
  df_cus.display()

# COMMAND ----------

df_cus=df_cus.withColumn('FullNmae',concat_ws(' ',col('Prefix'),col('FirstName'),col('LastName')))
df_cus.display()

# COMMAND ----------

df_cus.write.format('parquet').mode('append')\
    .option('path','abfss://silver@awdatalakechennu.dfs.core.windows.net/AdventureWorks_Customers')\
        .save()

# COMMAND ----------

df_subcat.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@awdatalakechennu.dfs.core.windows.net/AdventureWorks_SUbCategories")\
            .save()

# COMMAND ----------

df_pro = df_pro.withColumn('ProductSKU',split(col('ProductSKU'),'-')[0])\
                .withColumn('ProductName',split(col('ProductName'),' ')[0])

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_ret.display()

# COMMAND ----------

df_ret.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@awdatalakechennu.dfs.core.windows.net/AdventureWorks_Returns")\
            .save()

# COMMAND ----------

df_ter.display()

# COMMAND ----------

df_ter.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@awdatalakechennu.dfs.core.windows.net/AdventureWorks_Territories")\
            .save()

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales = df_sales.withColumn('StockDate',to_timestamp('StockDate'))

# COMMAND ----------

df_sales = df_sales.withColumn('OrderNumber',regexp_replace(col('OrderNumber'),'S','T'))

# COMMAND ----------

df_sales = df_sales.withColumn('multiply',col('OrderLineItem')*col('OrderQuantity'))

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@awdatalakechennu.dfs.core.windows.net/AdventureWorks_Sales")\
            .save()

# COMMAND ----------

df_sales.groupBy('OrderDate').agg(count('OrderNumber').alias('total_order')).display()

# COMMAND ----------

df_procat.display()