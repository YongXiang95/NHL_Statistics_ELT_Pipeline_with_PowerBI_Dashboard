# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "dea5e3a4-575c-4c17-9470-c5edbb54a239",
# META       "default_lakehouse_name": "NHL_Lakehouse_test",
# META       "default_lakehouse_workspace_id": "3b7c08d5-f636-4bd8-8644-206fcbc0fb3f",
# META       "known_lakehouses": [
# META         {
# META           "id": "dea5e3a4-575c-4c17-9470-c5edbb54a239"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import os
%pip install missingno
import missingno as msno
import matplotlib.pyplot as plt
import pyspark.pandas as ps
import warnings 
warnings.filterwarnings("ignore", message=".*PYARROW_IGNORE_TIMEZONE.*")
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, \
IntegerType, FloatType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lakehouse_name = "NHL_Lakehouse_test"   # reading from NHL_Lakehouse_test
schema_name = "dbo"              # reading from dbo
table_name = "game_plays_players"         

full_table_name = f"{lakehouse_name}.{schema_name}.{table_name}"

try:
    # Read the table into a DataFrame
    df_game_plays_players = spark.read.table(full_table_name)
  
    # Show first 5 rows
    display(ps.DataFrame(df_game_plays_players) .info())

except Exception as e:
    print(f"Error reading table {full_table_name}: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ps.DataFrame(df_game_plays_players)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_no_duplicates = df_game_plays_players.dropDuplicates()
all_columns = df_game_plays_players.columns
partition_expr = all_columns
print(all_columns)
window_spec = Window.partitionBy(*partition_expr).orderBy(F.lit(1))
df_with_rowid = df_no_duplicates.withColumn("rowid", F.row_number().over(window_spec))
df_with_rowid_with_dupl = df_game_plays_players.withColumn("rowid", F.row_number().over(window_spec))

# Step 3: Filter rows where rowid > 1 (duplicates)
#df_duplicates = df_game_plays_players.groupBy(partition_expr).agg(F.count("*").alias("count")).orderBy(F.col("count").desc())

# Show duplicate rows
ps.DataFrame(df_with_rowid.filter("rowid==1")).info()
ps.DataFrame(df_with_rowid_with_dupl.filter("rowid>1")).info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Check for unique primary key after removing 1223800 duplicate values

# CELL ********************

unique_play_id=df_with_rowid.filter("rowid==1").select("play_id").distinct().count()
print(unique_play_id)
df_with_rowid = df_with_rowid.withColumns({
    "composite_key": F.concat_ws("_", F.col("play_id"), F.col("player_id"))})
unique_counts = df_with_rowid.filter("rowid==1").agg(*[F.countDistinct(F.col(c)).alias(c+"_unique_count") for c in df_with_rowid.filter("rowid==1").columns])
display(unique_counts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### no of unique count for composite key is 6362801 means there is 3 duplicate values

# CELL ********************

unique_play_id=df_with_rowid.filter("rowid==1").select("composite_key").distinct().count()
print(unique_play_id)
window_spec = Window.partitionBy("composite_key").orderBy(F.lit(1))
df_with_rowid = df_with_rowid.withColumn("rowid2", F.row_number().over(window_spec))
display(df_with_rowid.filter("rowid2>1"))
duplicates = display(
    df_with_rowid.groupBy("composite_key")
      .agg(F.count("*").alias("count"))
      .filter(F.col("count") > 1))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### checking on this 3 values

# CELL ********************

display(df_with_rowid.filter(
    F.col("composite_key").isin(
        "2016020256_119_8468508",
        "2016030234_280_8474056",
        "2015020917_78_8476880"
    )))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### looking at "Servedby" to see if there is any patterns

# CELL ********************

display(df_with_rowid.filter(
    F.col("playerType").isin(["ServedBy"]
    )))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### recommended to add a running number on top of the composite key so that all will be unique eg composite_key1, composite_key2
