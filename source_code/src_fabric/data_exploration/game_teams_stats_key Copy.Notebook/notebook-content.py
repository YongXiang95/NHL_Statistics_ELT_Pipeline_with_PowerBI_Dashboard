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

from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql.functions import count, col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.table("game_teams_stats")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.printSchema()
df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.table("game_teams_stats").limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Identifying nulls within the columns

# CELL ********************

null_counts = df.select([
    spark_sum(col(c).isNull().cast("int")).alias(c)
    for c in df.columns
])

display(null_counts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select("faceOffWinPercentage").distinct().show(20)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import isnan, col

df.filter(isnan(col("faceOffWinPercentage"))).count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.filter(col("faceOffWinPercentage") == 0).count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_df = spark.read.option("header", True).csv("Files/historical/landing/game_teams_stats.csv")

raw_df.select("faceOffWinPercentage").show(20)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import min, max

df.select(
    min("faceOffWinPercentage").alias("min_val"),
    max("faceOffWinPercentage").alias("max_val")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.select("faceOffWinPercentage").distinct().show(30)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

df.filter(col("faceOffWinPercentage") == "NA").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import when, col

df = df.withColumn(
    "faceOffWinPercentage",
    when(col("faceOffWinPercentage") == "NA", None)
    .otherwise(col("faceOffWinPercentage").cast("double"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import sum as spark_sum

df.select(
    spark_sum(col("faceOffWinPercentage").isNull().cast("int")).alias("null_count")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import isnan
df.filter(isnan(col("faceOffWinPercentage"))).count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Identifying duplicate rows within the table

# CELL ********************

total_rows = df.count()
distinct_rows = df.dropDuplicates().count()

print("total_rows:", total_rows)
print("distinct_rows:", distinct_rows)
print("exact_duplicate_rows:", total_rows - distinct_rows)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dropping exact duplicate rows.

# CELL ********************

df_silver = df.dropDuplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver.count(), df_silver.dropDuplicates().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver.groupBy("game_id", "team_id") \
    .count() \
    .filter(col("count") > 1) \
    .count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## We are using Natural Key - Composite
# 
# Each record is uniquely identified by (game_id, team_id) with zero duplicate shift records.
# - Grain: One row = One team's stats in one game
# - Each game has two teams = two rows per game

