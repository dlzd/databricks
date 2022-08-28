# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Sharing
# MAGIC 
# MAGIC ## Create Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS erictomo;
# MAGIC USE CATALOG erictome;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE erictome_cdf_delta_sharing.cdf_ds_external
# MAGIC  (
# MAGIC     RECID INT,
# MAGIC     COMPANYNAME STRING,
# MAGIC     QUANTITY INT,
# MAGIC     UPDATE_TIME TIMESTAMP,
# MAGIC     _change_type STRING,
# MAGIC     _commit_version LONG,
# MAGIC     _commit_timestamp TIMESTAMP
# MAGIC  ) PARTITIONED BY(COMPANYNAME);
# MAGIC  
# MAGIC  INSERT INTO uc_etome.uc_table VALUES
# MAGIC   (10, 'FINANCE', 'EDINBURGH'),
# MAGIC   (20, 'SOFTWARE', 'PADDINGTON'),
# MAGIC   (30, 'SALES', 'MAIDSTONE'),
# MAGIC   (40, 'MARKETING', 'DARLINGTON'),
# MAGIC   (50, 'ADMIN', 'BIRMINGHAM');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Share

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SHARE IF NOT EXISTS uc_table_share
# MAGIC COMMENT 'Share for Delta Sharing Demo';
# MAGIC 
# MAGIC DESCRIBE SHARE uc_table_share;
# MAGIC 
# MAGIC ALTER SHARE uc_table_share 
# MAGIC ADD TABLE uc_etome.uc_table
# MAGIC PARTITION (`location` = "EDINBURGH") as delta_sharing_demo_recip.EDINBURGH_locations;
# MAGIC 
# MAGIC SHOW ALL IN SHARE uc_table_share;

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
# MAGIC GRANT SELECT ON SHARE uc_table_share TO RECIPIENT erictome;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add New Records

# COMMAND ----------

# MAGIC %sql
# MAGIC  INSERT INTO uc_etome.uc_table VALUES
# MAGIC   (60, 'SUPPORT', 'EDINBURGH'),
# MAGIC   (70, 'IT', 'EDINBURGH'),
# MAGIC   (80, 'NEW1', 'EDINBURGH'),
# MAGIC   (90, 'New2', 'EDINBURGH');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revoke Select

# COMMAND ----------

# MAGIC %sql
# MAGIC REVOKE SELECT ON SHARE uc_table_share FROM RECIPIENT erictome;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean Up

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP RECIPIENT IF EXISTS erictome; 
# MAGIC DROP SHARE IF EXISTS uc_table_share;
# MAGIC DROP TABLE IF EXISTS uc_etome.uc_table;
# MAGIC DROP SCHEMA IF EXISTS uc_etome;
# MAGIC DROP CATALOG IF EXISTS erictome_uc_demo CASCADE;

# COMMAND ----------


