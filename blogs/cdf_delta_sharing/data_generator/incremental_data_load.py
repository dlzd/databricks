# %%
import sqlite3


%pip install faker

# %%
%pip install https://github.com/databrickslabs/dbldatagen/releases/download/v.0.2.0-rc1-master/dbldatagen-0.2.0rc1-py3-none-any.whl

# %%
import dbldatagen as dg
import pyspark.sql.functions as F
from faker.providers import geo, internet, address
from dbldatagen import fakerText
cdc_inc_data_spec = (dg.DataGenerator(spark, rows=100000, partitions = 10)
    .withColumn('ID', 'int' , minValue=1, maxValue=1000000, random=True)
	.withColumn('COMPANYNAME', 'string' , values=['Company1','Company2','Company3'])
    .withColumn('QUANTITY', 'long' , minValue=1, maxValue=5, random=True)
    .withColumn("UPDATE_TIME", "timestamp", expr="current_timestamp()")
 	
           )
cdc_inc_data_df = cdc_inc_data_spec.build()


display(cdc_inc_data_df)

# %%
cdc_data_df = spark.table("erictome_cdf_delta_sharing.share_data") 

(cdc_data_df.alias('existing') 
  .merge(
    cdc_inc_data_df.alias('updates'),
    'existing.ID = updates.ID'
  ) 
  .whenMatchedUpdate(set =
    {
      "ID": "updates.ID",
      "COMPANYNAME": "updates.COMPANYNAME",
      "QUANTITY": "updates.QUANTITY",
      "UPDATE_TIME": "updates.UPDATE_TIME"
    }
  ) 
  .whenNotMatchedInsert(values =
    {
      "ID": "updates.ID",
      "COMPANYNAME": "updates.COMPANYNAME",
      "QUANTITY": "updates.QUANTITY",
      "UPDATE_TIME": "updates.UPDATE_TIME"
    }
  ) 
  .execute())

