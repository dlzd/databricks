# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %pip install https://github.com/databrickslabs/dbldatagen/releases/download/v.0.2.0-rc1-master/dbldatagen-0.2.0rc1-py3-none-any.whl

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;
# MAGIC create database if not exists erictome_cdf_delta_sharing;

# COMMAND ----------

import dbldatagen as dg
import pyspark.sql.functions as F
from faker.providers import geo, internet, address
from dbldatagen import fakerText

cdc_data_spec = (dg.DataGenerator(spark, rows=1000000, partitions = 10)
    .withColumn('RECID', 'int' , uniqueValues=1000000)
	.withColumn('COMPANYNAME', 'string' , values=['Company1','Company2','Company3'])
    .withColumn('QUANTITY', 'long' , minValue=1, maxValue=5, random=True)
    .withColumn("UPDATE_TIME", "timestamp", expr="current_timestamp()"))

cdc_data_df = cdc_data_spec.build()

cdc_data_df.write.mode("overwrite").saveAsTable("erictome_cdf_delta_sharing.share_data")

# COMMAND ----------

display(cdc_data_df)
