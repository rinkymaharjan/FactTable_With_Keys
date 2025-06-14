# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from datetime import date, timedelta
from pyspark.sql.functions import (col, upper, hash, to_date, trim, date_format, year, month, quarter, expr, lit, when, concat)




# COMMAND ----------

spark = SparkSession.builder.appName("DIM-SalesDate").getOrCreate()

# COMMAND ----------

Schema = StructType([
    StructField("Region", StringType(), True),
    StructField("ProductCategory", StringType(), True),
    StructField("ProductSubCategory", StringType(), True),
    StructField("SalesChannel", StringType(), True),
    StructField("CustomerSegment", StringType(), True),
    StructField("SalesRep", StringType(), True),
    StructField("StoreType", StringType(), True),
    StructField("SalesDate", StringType(), True),
    StructField("UnitsSold", IntegerType(), True),
    StructField("Revenue", IntegerType(), True)
])

df_Fact_Sales = spark.read.option("header", True).schema(Schema).csv("/FileStore/tables/fact_sales.csv")

# COMMAND ----------


df_Fact_Sales = df_Fact_Sales.withColumn("SalesDate", to_date(trim("SalesDate"), "M/d/yyyy"))

# COMMAND ----------

df_Fact_Sales.display()

# COMMAND ----------

df_FactSales = spark.read.format("delta").load("/FileStore/tables/Fact_Sales")

# COMMAND ----------

df_Fact_Sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #######Replacing all columns with Keys

# COMMAND ----------

df_fact = df_Fact_Sales.withColumn("DIM-RegionId", when(col("Region").isNull(), -1)
                .otherwise(hash(upper(col("Region")))).cast("bigint")).drop(col("Region")) \
    .withColumn("DIM-ProductCategoryID", when(col("ProductCategory").isNull(), -1)
                .otherwise(hash(upper(col("ProductCategory")))).cast("bigint")).drop(col("ProductCategory")) \
    .withColumn("DIM-ProductSubCategoryID", when(col("ProductSubCategory").isNull(), -1)
                .otherwise(hash(upper(col("ProductSubCategory")))).cast("bigint")).drop(col("ProductSubCategory")) \
    .withColumn("DIM-SalesChannelID", when(col("SalesChannel").isNull(), -1)
                .otherwise(hash(upper(col("SalesChannel")))).cast("bigint")).drop(col("SalesChannel")) \
    .withColumn("DIM-CustomerSegmentID", when(col("CustomerSegment").isNull(), -1)
                .otherwise(hash(upper(col("CustomerSegment")))).cast("bigint")).drop(col("CustomerSegment")) \
    .withColumn("DIM-SalesRepID", when(col("SalesRep").isNull(), -1)
                .otherwise(hash(upper(col("SalesRep")))).cast("bigint")).drop(col("SalesRep")) \
    .withColumn("DIM-StoreTypeID", when(col("StoreType").isNull(), -1)
                .otherwise(hash(upper(col("StoreType")))).cast("bigint")).drop(col("StoreType")) \
    .withColumn("DIM-DateID", when(col("SalesDate").isNull(), -1)
                .otherwise(hash(upper(col("SalesDate")))).cast("bigint")).drop(col("SalesDate"))



# COMMAND ----------

df_fact.display()

# COMMAND ----------

df_fact.write.format("delta").mode("overwrite").save("/FileStore/tables/Facts_Sales")

# COMMAND ----------

df_FactSales = spark.read.format("delta").load("/FileStore/tables/Facts_Sales")

# COMMAND ----------

df_FactSales.display()