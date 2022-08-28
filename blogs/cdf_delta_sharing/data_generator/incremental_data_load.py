# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %pip install https://github.com/databrickslabs/dbldatagen/releases/download/v.0.2.0-rc1-master/dbldatagen-0.2.0rc1-py3-none-any.whl

# COMMAND ----------

import dbldatagen as dg
import pyspark.sql.functions as F
from faker.providers import geo, internet, address
from dbldatagen import fakerText
from delta.tables import *

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog erictome;

# COMMAND ----------

# DBTITLE 1,Generate Update Data
cdc_inc_data_spec = (dg.DataGenerator(spark, rows=100000, partitions = 1)
    .withColumn('RECID', 'int' , uniqueValues=1000000)
	.withColumn('COMPANYNAME', 'string' , values=['Company1','Company2','Company3'])
    .withColumn('QUANTITY', 'int' , minValue=5, maxValue=10, random=True)
    .withColumn("UPDATE_TIME", "timestamp", expr="current_timestamp()"))

cdc_inc_data_df = cdc_inc_data_spec.build()

display(cdc_inc_data_df)

# COMMAND ----------

# DBTITLE 1,CDC Merge
deltaTable = DeltaTable.forName(spark, 'erictome_cdf_delta_sharing.share_data')

(deltaTable.alias('existing') 
  .merge(
    cdc_inc_data_df.alias('updates'),
    'existing.RECID = updates.RECID'
  ) 
  .whenMatchedUpdate(set =
    {
      "COMPANYNAME": "updates.COMPANYNAME",
      "QUANTITY": "updates.QUANTITY",
      "UPDATE_TIME": "updates.UPDATE_TIME"
    }
  ) 
  .whenNotMatchedInsert(values =
    {
      "COMPANYNAME": "updates.COMPANYNAME",
      "QUANTITY": "updates.QUANTITY",
      "UPDATE_TIME": "updates.UPDATE_TIME"
    }
  ) 
  .execute())

# COMMAND ----------

# DBTITLE 1,Delete Records
deltaTable.delete("RECID BETWEEN 200000 AND 299999 ")

# COMMAND ----------

# DBTITLE 1,Checking Delete Worked
spark.table("erictome_cdf_delta_sharing.share_data").count()

# COMMAND ----------

# DBTITLE 1,Insert Updated Records to Share Table
# MAGIC %sql
# MAGIC insert into erictome_cdf_delta_sharing.cdf_ds_external
# MAGIC  select RECID,
# MAGIC     COMPANYNAME,
# MAGIC     QUANTITY,
# MAGIC     UPDATE_TIME,
# MAGIC     _change_type change_type,
# MAGIC     _commit_version commit_version,
# MAGIC     _commit_timestamp commit_timestamp
# MAGIC    from table_changes('erictome_cdf_delta_sharing.share_data',0) 
# MAGIC    where _change_type <> 'update_preimage' 
# MAGIC    and _commit_version > (select max(commit_version) max_commit from erictome_cdf_delta_sharing.cdf_ds_external limit 1)
# MAGIC    order by RECID

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from erictome_cdf_delta_sharing.cdf_ds_external order by RECID, commit_version
