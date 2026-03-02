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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.table("game_shifts")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display("game_shift")

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

df.filter(col("shift_end").isNull()).show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Check for duplicate rows in the table

# CELL ********************

from pyspark.sql.functions import col, count

dupe_keys = (
    df.groupBy("game_id", "player_id", "shift_start")
      .agg(count("*").alias("cnt"))
      .filter(col("cnt") > 1)
)

display(dupe_keys)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## This checks for exact row duplicates within the table

# CELL ********************

df.join(
    dupe_keys.select("game_id", "player_id", "shift_start"),
    on=["game_id", "player_id", "shift_start"],
    how="inner"
).orderBy("game_id", "player_id", "shift_start").show(20)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

df.count() - df.dropDuplicates(
    ["game_id", "player_id", "shift_start"]
).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## This shows that 

# CELL ********************

from pyspark.sql.functions import count, col

df.groupBy("game_id", "player_id", "shift_start") \
  .agg(count("*").alias("cnt")) \
  .filter(col("cnt") > 1) \
  .count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Next step is we need to deduplicate
# Given our findings, the correct Silver rule is:
# For each (game_id, player_id, shift_start),  we can keep the most complete shift
# - Prefer non-null shift_end
# - Then prefer max shift_end(reason being there might be an input error for that particular game_id so we can say there is multiple shift_end of the same game_id)

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

w = Window.partitionBy(
    "game_id", "player_id", "shift_start"
).orderBy(
    col("shift_end").isNull().cast("int"),  # non-null first
    col("shift_end").desc()                 # latest end time
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df = (
    df.withColumn("rn", row_number().over(w))
      .filter(col("rn") == 1)
      .drop("rn")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

assert (
    silver_df.dropDuplicates(
        ["game_id", "player_id", "shift_start"]
    ).count()
    == silver_df.count()
), "Duplicate shifts still detected"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

df.count() - silver_df.count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Checking after deduplication
# 0 is our target number meaning there is no one table that is identical to one another.

# CELL ********************

silver_df.groupBy(
    "game_id", "player_id", "shift_start"
).agg(
    count("*").alias("cnt")
).filter(
    col("cnt") > 1
).count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df.count() - silver_df.dropDuplicates(
    ["game_id", "player_id", "shift_start"]
).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Assertion Gate
# To check data quality
# - Pass/succeed means it's good to go
# - Fail means Pipeline has halted

# CELL ********************

assert (
    silver_df.dropDuplicates(
        ["game_id", "player_id", "shift_start"]
    ).count()
    == silver_df.count()
), "Duplicates still present in Silver"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Double check nulls on specified columns
# To check data quality
# - Pass/succeed means it's good to go
# - Fail means Pipeline has halted

# CELL ********************

assert silver_df.filter(
    col("game_id").isNull() |
    col("player_id").isNull() |
    col("shift_start").isNull()
).count() == 0, "Nulls found in mandatory columns"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("silver_df rows:", silver_df.count())
print("silver_df distinct rows:", silver_df.dropDuplicates(silver_df.columns).count())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

business_key = ["game_id", "player_id", "period", "shift_start", "shift_end"]

silver_df.groupBy(business_key) \
    .count() \
    .filter(col("count") > 1) \
    .count()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Checking For Anomalies

# CELL ********************

df.describe().show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

multi_ot_games = (
    silver_df.filter(col("period") > 4)
             .select("game_id")
             .distinct()
)

print("Number of games with period > 4:",
      multi_ot_games.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Check the minimum and maximum number of players within the table.

# CELL ********************

from pyspark.sql.functions import min, max

silver_df.select(
    min("player_id").alias("min_id"),
    max("player_id").alias("max_id")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Identified that there are 2341 distinct players in within the table.

# CELL ********************

silver_df.select("player_id").distinct().count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df.groupBy("player_id").count().orderBy("count").show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## We are using Natural Key - Composite
# 
# Each shift record is uniquely identified by (game_id, player_id, period, shift_start, shift_end) with zero duplicate shift records.
# Grain: One row = One player shift

# CELL ********************

from pyspark.sql.functions import col

df.filter(col("period") > 5).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Period 5 is basically second overtime half
# While it is extremely rare for an NHL game to exceed three overtime periods (6 total periods), the digital, real-time nature of modern NHL apps provides slots for up to 8 periods to ensure all game data is captured. 
