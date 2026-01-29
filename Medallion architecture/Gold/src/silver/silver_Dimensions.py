# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


import os
import sys


# 1. Get the directory of the current notebook (e.g., .../src/silver)
current_dir = os.getcwd()

# 2. Go up one level to reach 'src'
src_path = os.path.dirname(current_dir)

# 3. Add 'src' to the system path so Python can see the 'utils' folder
if src_path not in sys.path:
    sys.path.append(src_path)

# 4. Import using the folder name
from utils.reader import BronzeReader
from utils.writer import SilverWriter
from utils.transformation import Methods

print(f"Path added: {src_path}")



# COMMAND ----------

# MAGIC %md
# MAGIC ### **Read , Tranform and write DimArtist**

# COMMAND ----------



# 1. PATHS
bronze_path = "abfss://bronze@spotifyetestorageaccount.dfs.core.windows.net/DimArtist"
silver_base = "abfss://silver@spotifyetestorageaccount.dfs.core.windows.net"
checkpoint_path = f"{silver_base}/DimArtist/_metadata/checkpoint"
schema_path = f"{silver_base}/DimArtist/_metadata/schema"

# Clear checkpoint once to resolve the "partitions" error from before
dbutils.fs.rm(checkpoint_path, True)

# 2. READ
reader = BronzeReader(spark)
df_artist_raw = reader.read_bronze_stream("parquet", schema_path, bronze_path)

# 3. TRANSFORM
df_obj = Methods()
df_artist_clean = df_obj.dropColums(df_artist_raw, ['_rescued_data'])

# NOTE: We don't need dropDuplicates here because our SilverWriter 
# class uses MERGE logic which handles duplicates automatically!

# 4. WRITE (This is your main stream)
writer = SilverWriter(silver_base)
query= writer.write_silver(df_artist_clean, "DimArtist", "artist_id", checkpoint_path)
# This stops the notebook from moving to the next line too fast
query.awaitTermination()

# 5. Create Table in Catalog
writer.create_catalog_table(spark, "DimArtist")

# 6. VERIFY (Read the saved data instead of streaming it)
df_check = spark.read.format("delta").load(f"{silver_base}/DimArtist/data")
display(df_check)


# COMMAND ----------

# MAGIC %md
# MAGIC ### **Read , Tranform and write DimUser**

# COMMAND ----------

# 1. PATHS
bronze_path = "abfss://bronze@spotifyetestorageaccount.dfs.core.windows.net/DimUser"
silver_base = "abfss://silver@spotifyetestorageaccount.dfs.core.windows.net"
checkpoint_path = f"{silver_base}/DimUser/_metadata/checkpoint"
schema_path = f"{silver_base}/DimUser/_metadata/schema"

# Clear checkpoint once to resolve the "partitions" error from before
dbutils.fs.rm(checkpoint_path, True)

# 2. READ
reader = BronzeReader(spark)
df_user_raw = reader.read_bronze_stream("parquet", schema_path, bronze_path)

# 3. TRANSFORM
df_obj = Methods()
df_user_clean = df_obj.dropColums(df_user_raw, ['_rescued_data'])

# NOTE: We don't need dropDuplicates here because our SilverWriter 
# class uses MERGE logic which handles duplicates automatically!

# 4. WRITE (This is your main stream)
writer = SilverWriter(silver_base)
query= writer.write_silver(df_user_clean, "DimUser", "user_id", checkpoint_path)
# This stops the notebook from moving to the next line too fast
query.awaitTermination()

# 5. Create Table in Catalog
writer.create_catalog_table(spark, "DimUser")

# 6. VERIFY (Read the saved data instead of streaming it)
df_check = spark.read.format("delta").load(f"{silver_base}/DimUser/data")
display(df_check)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Read , Tranform and write DimTrack**

# COMMAND ----------

# 1. PATHS
bronze_path = "abfss://bronze@spotifyetestorageaccount.dfs.core.windows.net/DimTrack"
silver_base = "abfss://silver@spotifyetestorageaccount.dfs.core.windows.net"
checkpoint_path = f"{silver_base}/DimTrack/_metadata/checkpoint"
schema_path = f"{silver_base}/DimTrack/_metadata/schema"

# Clear checkpoint once to resolve the "partitions" error from before
dbutils.fs.rm(checkpoint_path, True)

# 2. READ
reader = BronzeReader(spark)
df_track_raw = reader.read_bronze_stream("parquet", schema_path, bronze_path)

# 3. TRANSFORM
df_obj = Methods()
df_track = df_obj.dropColums(df_track_raw, ['_rescued_data']) # remove colum _rescued_data
df_track= df_track.withColumn("duration_flag", when(col('duration_sec') > 300, 'long').otherwise('short')) # add new colum duration_flagbased on duration_sec
df_track= df_track.withColumn("track_name",regexp_replace(col("track_name"), '-', ' ')) # change - to space in track_name 


#display(df_track, checkpointLocation = checkpoint_path)

# NOTE: We don't need dropDuplicates here because our SilverWriter 
# class uses MERGE logic which handles duplicates automatically!

# 4. WRITE (This is your main stream)
writer = SilverWriter(silver_base)
query= writer.write_silver(df_track, "DimTrack", "track_id", checkpoint_path)
# This stops the notebook from moving to the next line too fast
query.awaitTermination()

# 5. Create Table in Catalog
writer.create_catalog_table(spark, "DimTrack")

# 6. VERIFY (Read the saved data instead of streaming it)
df_check = spark.read.format("delta").load(f"{silver_base}/DimTrack/data")
display(df_check)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Read , Tranform and write DimDate**

# COMMAND ----------

# 1. PATHS
bronze_path = "abfss://bronze@spotifyetestorageaccount.dfs.core.windows.net/DimDate"
silver_base = "abfss://silver@spotifyetestorageaccount.dfs.core.windows.net"
checkpoint_path = f"{silver_base}/DimDate/_metadata/checkpoint"
schema_path = f"{silver_base}/DimDate/_metadata/schema"

# Clear checkpoint once to resolve the "partitions" error from before
dbutils.fs.rm(checkpoint_path, True)

# 2. READ
reader = BronzeReader(spark)
df_date_raw = reader.read_bronze_stream("parquet", schema_path, bronze_path)

# 3. TRANSFORM
df_obj = Methods()
df_date_clean = df_obj.dropColums(df_date_raw, ['_rescued_data'])

# NOTE: We don't need dropDuplicates here because our SilverWriter 
# class uses MERGE logic which handles duplicates automatically!

# 4. WRITE (This is your main stream)
writer = SilverWriter(silver_base)
query= writer.write_silver(df_date_clean, "DimDate", "date_key", checkpoint_path)
# This stops the notebook from moving to the next line too fast
query.awaitTermination()

# 5. Create Table in Catalog
writer.create_catalog_table(spark, "DimDate")

# 6. VERIFY (Read the saved data instead of streaming it)
df_check = spark.read.format("delta").load(f"{silver_base}/DimDate/data")
display(df_check)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Read , Tranform and write FactStream**

# COMMAND ----------

# 1. PATHS
bronze_path = "abfss://bronze@spotifyetestorageaccount.dfs.core.windows.net/FactStream"
silver_base = "abfss://silver@spotifyetestorageaccount.dfs.core.windows.net"
checkpoint_path = f"{silver_base}/FactStream/_metadata/checkpoint"
schema_path = f"{silver_base}/FactStream/_metadata/schema"

# Clear checkpoint once to resolve the "partitions" error from before
dbutils.fs.rm(checkpoint_path, True)

# 2. READ
reader = BronzeReader(spark)
df_factstream_raw = reader.read_bronze_stream("parquet", schema_path, bronze_path)

# 3. TRANSFORM
df_obj = Methods()
df_factstream = df_obj.dropColums(df_factstream_raw, ['_rescued_data'])


# NOTE: We don't need dropDuplicates here because our SilverWriter 
# class uses MERGE logic which handles duplicates automatically!

# 4. WRITE (This is your main stream)
writer = SilverWriter(silver_base)
query= writer.write_silver(df_factstream, "FactStream", "stream_id", checkpoint_path)
# This stops the notebook from moving to the next line too fast
query.awaitTermination()

# 5. Create Table in Catalog
writer.create_catalog_table(spark, "FactStream")

# 6. VERIFY (Read the saved data instead of streaming it)
df_check = spark.read.format("delta").load(f"{silver_base}/FactStream/data")
display(df_check)



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM spotify_catalog.gold.dimtrack
# MAGIC