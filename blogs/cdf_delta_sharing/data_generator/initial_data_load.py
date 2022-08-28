# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %pip install https://github.com/databrickslabs/dbldatagen/releases/download/v.0.2.0-rc1-master/dbldatagen-0.2.0rc1-py3-none-any.whl

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;
# MAGIC create catalog if not exists erictome;
# MAGIC use catalog erictome;
# MAGIC create database if not exists erictome_cdf_delta_sharing;
# MAGIC drop table if exists erictome_cdf_delta_sharing.share_data;

# COMMAND ----------

import dbldatagen as dg
import pyspark.sql.functions as F
from faker.providers import geo, internet, address
from dbldatagen import fakerText

cdc_data_spec = (dg.DataGenerator(spark, rows=1000000, partitions = 10)
    .withColumn('RECID', 'int' , uniqueValues=1000000)
	.withColumn('COMPANYNAME', 'string' , values=['Company1','Company2','Company3'])
    .withColumn('QUANTITY', 'int' , minValue=1, maxValue=5, random=True)
    .withColumn("UPDATE_TIME", "timestamp", expr="current_timestamp()"))

cdc_data_df = cdc_data_spec.build()

cdc_data_df.write.mode("overwrite").saveAsTable("erictome_cdf_delta_sharing.share_data")

# COMMAND ----------

display(cdc_data_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table erictome_cdf_delta_sharing.cdf_ds_external;

# COMMAND ----------

# DBTITLE 1,Create External Sharing Table
# MAGIC %sql
# MAGIC create table erictome_cdf_delta_sharing.cdf_ds_external
# MAGIC partitioned by (COMPANYNAME)
# MAGIC   as
# MAGIC   select 
# MAGIC     RECID,
# MAGIC     COMPANYNAME,
# MAGIC     QUANTITY,
# MAGIC     UPDATE_TIME,
# MAGIC     _change_type change_type,
# MAGIC     _commit_version commit_version,
# MAGIC     _commit_timestamp commit_timestamp
# MAGIC     from table_changes('erictome_cdf_delta_sharing.share_data', 0);
# MAGIC     

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from erictome_cdf_delta_sharing.cdf_ds_external;

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions erictome_cdf_delta_sharing.cdf_ds_external;

# COMMAND ----------


