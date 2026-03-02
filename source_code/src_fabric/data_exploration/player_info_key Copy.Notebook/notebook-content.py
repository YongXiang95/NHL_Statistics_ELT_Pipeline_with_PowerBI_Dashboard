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

df = spark.table("player_info")

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

# MARKDOWN ********************

# ## Identifying nulls within columns

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

total_nulls = df.select([
    spark_sum(col(c).isNull().cast("int")).alias(c)
    for c in df.columns
]).collect()[0]

# Sum across all columns
total_null_count = sum(total_nulls)

print("Total null values in the DataFrame:", total_null_count)

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

# ## Identifying player_id uniqueness and if there is any duplicates within the player_id columns.

# CELL ********************

df.filter(col("player_id").isNull()).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.groupBy("player_id") \
  .count() \
  .filter(col("count") > 1) \
  .count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## This shows that player_id is suitable to be the Primary Key for the table player_info
