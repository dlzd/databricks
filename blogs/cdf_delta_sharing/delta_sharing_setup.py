# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Sharing
# MAGIC 
# MAGIC ## Create Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS erictome;
# MAGIC USE CATALOG erictome;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Share

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SHARE IF NOT EXISTS ds_cdf_table_share
# MAGIC COMMENT 'Share for CDF/Delta Sharing Demo';
# MAGIC 
# MAGIC DESCRIBE SHARE ds_cdf_table_share;

# COMMAND ----------

# DBTITLE 1,Add Table to Share (CDF Share Only)
# MAGIC %sql
# MAGIC ALTER SHARE ds_cdf_table_share 
# MAGIC ADD TABLE erictome_cdf_delta_sharing.cdf_ds_external
# MAGIC PARTITION (`COMPANYNAME` = "Company2") as cdf_ds_external.Company2;

# COMMAND ----------

# DBTITLE 1,Add Table to Share (Data + CDF Share)
# MAGIC %sql
# MAGIC 
# MAGIC ALTER SHARE ds_cdf_table_share
# MAGIC ADD TABLE erictome_cdf_delta_sharing.share_data
# MAGIC -- PARTITION (`COMPANYNAME` = "Company2")  -- CURRENTLY UNSUPPORTED WITH CDF ENABLED
# MAGIC AS share_data.Company2
# MAGIC WITH CHANGE DATA FEED;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW ALL IN SHARE ds_cdf_table_share;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create Delta Sharing Recipient

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP RECIPIENT IF EXISTS erictome; 
# MAGIC CREATE RECIPIENT IF NOT EXISTS erictome;
# MAGIC 
# MAGIC DESC RECIPIENT erictome;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT ON SHARE ds_cdf_table_share TO RECIPIENT erictome;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revoke Select

# COMMAND ----------

# MAGIC %sql
# MAGIC REVOKE SELECT ON SHARE ds_cdf_table_share FROM RECIPIENT erictome;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean Up

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP RECIPIENT IF EXISTS erictome; 
# MAGIC DROP SHARE IF EXISTS ds_cdf_table_share;

# COMMAND ----------


