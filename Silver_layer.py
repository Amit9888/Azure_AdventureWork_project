# Databricks notebook source
# MAGIC %md
# MAGIC ##Silver Layer

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Loading with app credentials

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.awdatastoragelake.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.awdatastoragelake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.awdatastoragelake.dfs.core.windows.net", "d794f34c-cca4-49dc-bb5c-7e224365c939")
spark.conf.set("fs.azure.account.oauth2.client.secret.awdatastoragelake.dfs.core.windows.net", "3Kn8Q~yuqL2l1.b5TzEBfC1s1nitFogVtgPWIdAj")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.awdatastoragelake.dfs.core.windows.net", "https://login.microsoftonline.com/16c3a970-d0cf-4062-95da-a55b20535900/oauth2/token")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading Data

# COMMAND ----------

df_calender=spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronze@awdatastoragelake.dfs.core.windows.net/AdventureWorks_Calendar")

# COMMAND ----------

df_customers=spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronze@awdatastoragelake.dfs.core.windows.net/AdventureWorks_Customers")

# COMMAND ----------

df_Product_Subcategories=spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronze@awdatastoragelake.dfs.core.windows.net/Product_Subcategories")


# COMMAND ----------

df_product_categories=spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronze@awdatastoragelake.dfs.core.windows.net/AdventureWorks_Product_Categories")

# COMMAND ----------

df_products=spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronze@awdatastoragelake.dfs.core.windows.net/AdventureWorks_Products")

# COMMAND ----------

df_returns=spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronze@awdatastoragelake.dfs.core.windows.net/AdventureWorks_Returns")

# COMMAND ----------

df_adventureWorks_Sales =spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronze@awdatastoragelake.dfs.core.windows.net/AdventureWorks_Sales*")

# COMMAND ----------

df_adventureWorks_Sales_2016=spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronze@awdatastoragelake.dfs.core.windows.net/AdventureWorks_Sales_2016")

# COMMAND ----------

df_adventureWorks_Sales_2017=spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronze@awdatastoragelake.dfs.core.windows.net/AdventureWorks_Sales_2017")

# COMMAND ----------

df_adventureWorks_Territories=spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronze@awdatastoragelake.dfs.core.windows.net/AdventureWorks_Territories")

# COMMAND ----------

df_Product_Subcategories = spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronze@awdatastoragelake.dfs.core.windows.net/Product_Subcategories")

# COMMAND ----------

df_products_final = spark.read.format("csv").option("header",True).option("inferSchema",True).load("abfss://bronze@awdatastoragelake.dfs.core.windows.net/products")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Transformation
# MAGIC

# COMMAND ----------

df_calender = df_calender.withColumn("Month",month(col("Date"))).withColumn("Year",year(col("Date"))).withColumn("Day",dayofmonth(col("Date")))

# COMMAND ----------

df_calender.write.format('parquet')\
    .mode('append')\
        .option("path","abfss://silver@awdatastoragelake.dfs.core.windows.net/AdventureWorks_Calendar")\
        .save()

# COMMAND ----------

df_customers=df_customers.withColumn("Full_Name", concat_ws(" ",col('Prefix') ,col('FirstName'),col('LastName')))

# COMMAND ----------

df_customers.write.format('parquet')\
    .mode('append')\
        .option("path","abfss://silver@awdatastoragelake.dfs.core.windows.net/AdventureWorks_Customers")\
        .save()

# COMMAND ----------

df_Product_Subcategories.display()

# COMMAND ----------

df_product_categories.write.format('parquet')\
    .mode('append')\
        .option("path","abfss://silver@awdatastoragelake.dfs.core.windows.net/AdventureWorks_Product_Categories")\
        .save()

# COMMAND ----------

df_Product_Subcategories.write.format('parquet')\
    .mode('append')\
        .option("path","abfss://silver@awdatastoragelake.dfs.core.windows.net/AdventureWorks_Product_Subcategories")\
        .save()

# COMMAND ----------

df_Product_Subcategories.display()

# COMMAND ----------

df_products.display()

# COMMAND ----------

df_products.display()

# COMMAND ----------

df_finalproducts = df_products.withColumn("Product_Name_prefix", split(df_products.ProductName, " ")[0])\
.withColumn("ProductSKU_prefix", split(df_products.ProductSKU, "-")[0])


# COMMAND ----------

df_finalproducts.write.format('parquet')\
    .mode('append')\
        .option("path","abfss://silver@awdatastoragelake.dfs.core.windows.net/AdventureWorks_Products")\
        .save()

# COMMAND ----------

df_returns.display()

# COMMAND ----------

df_returns.write.format('parquet')\
    .mode('append')\
        .option("path","abfss://silver@awdatastoragelake.dfs.core.windows.net/AdventureWorks_Returns")\
        .save()

# COMMAND ----------

df_adventureWorks_Territories.write.format('parquet')\
    .mode('append')\
        .option("path","abfss://silver@awdatastoragelake.dfs.core.windows.net/AdventureWorks_Territories")\
        .save()

# COMMAND ----------

df_adventureWorks_Sales.display()


# COMMAND ----------

df_adventureWorks_Sales  = df_adventureWorks_Sales.withColumn("StockDate",to_timestamp("StockDate"))

# COMMAND ----------

df_adventureWorks_Sales.display()

# COMMAND ----------

df_adventureWorks_Sales = df_adventureWorks_Sales.withColumn("OrderNumber",regexp_replace(col("OrderNumber"),"S","T")) 

# COMMAND ----------

df_adventureWorks_Sales.display()

# COMMAND ----------

## Sales Analysis
df_adventureWorks_Sales.groupBy("OrderDate").agg(count("OrderNumber").alias("total_order")).display()


# COMMAND ----------

df_product_categories.display()

# COMMAND ----------

df_adventureWorks_Territories.display()

# COMMAND ----------

df_adventureWorks_Sales.write.format('parquet')\
    .mode('append')\
        .option("path","abfss://silver@awdatastoragelake.dfs.core.windows.net/AdventureWorks_Sales")\
        .save()