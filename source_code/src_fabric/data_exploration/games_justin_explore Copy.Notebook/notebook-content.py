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
# META         },
# META         {
# META           "id": "c12e591f-c036-4c28-8dd8-c656da3acd11"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Date exploration for game.csv

# MARKDOWN ********************

# ### Load data

# CELL ********************

%pip install missingno

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import matplotlib.pyplot as plt
import missingno as msno
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pyspark.pandas as ps
from pyspark.sql.types import StructType, StructField, StringType,\
IntegerType, FloatType,TimestampType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_structure = {
    'game':
    {
'game_id':StringType(),            
'season':StringType(), 
"'type'": StringType(), 
'date_time_GMT':TimestampType(), 
'away_team_id':IntegerType(), 
'home_team_id':IntegerType(), 
'away_goals':IntegerType(), 
'home_goals':IntegerType(), 
'outcome':StringType(), 
'home_rink_side_start':StringType(),
'venue':StringType(), 
'venue_link':StringType(), 
'venue_time_zone_id':StringType(), 
'venue_time_zone_offset':IntegerType(), 
'venue_time_zone_tz':StringType()
    },
    'gameplay': 
    {
'play_id':  StringType(),            
'game_id':  StringType(),            
'team_id_for': IntegerType(), 
'team_id_against': IntegerType(),
'event': StringType(),
'secondaryType': StringType(),
'x':      FloatType(), 
'y':                    FloatType(), 
'period':             IntegerType(),
'periodType':             StringType(),
'periodTime':          IntegerType(),
'periodTimeRemaining': FloatType(), 
'dateTime': TimestampType(), 
'goals_away':          IntegerType(), 
'goals_home':          IntegerType(),
'description':          StringType(), 
'st_x':               FloatType(),
'st_y':                FloatType(),
    },
    'game_plays_player':
    {
'play_id':	 StringType(), 
'game_id':	 StringType(), 
'player_id': IntegerType(),
'playerType':  StringType(),

    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Missing values in Game.csv

# CELL ********************

# tablename=[]
# for table, schema in table_structure.items():
game = (
    spark.read
         .option("header", "true")
         .option("inferSchema", True)           # Auto-detect data types
         .option("nullValue", "NA")             # Treat null as "NA"
         .option("nanValue", "NA")         # Treat nan as "NA"
#.option("emptyValue", None) 
         .csv("abfss://testing_fp_lake@onelake.dfs.fabric.microsoft.com/NHL_project.Lakehouse/Files/game.csv")

)

display(game)

df_game= game.toPandas()
msno.matrix(df_game)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### missing20042005 year

# CELL ********************

unique_years = game.select("season").distinct()
unique_years.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#game.isna().sum()
missing_counts = game.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in game.columns
])
# missing_counts = missing_counts.toPandas()
# pdf.info()
# missing_counts.info()
display(missing_counts)

# pdf.info()
# ps.DataFrame(missing_counts).info()
#col("value").isNull() | isnan(col("value"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Duplicated values in Games.csv

# CELL ********************

df_window_drop_duplicate=game.withColumn("rowid",F.row_number().over(Window.partitionBy("game_id").orderBy(F.col("date_time_GMT").desc())))
df_window_drop_duplicate=df_window_drop_duplicate.filter("rowid>1")
display(df_window_drop_duplicate.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ps.DataFrame(df_window_drop_duplicate).info()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Decision: Games
# ###### 1196 missing values in home_rink_side_start to ignore
# ###### 2570 duplicates to be removed

# MARKDOWN ********************

# # Date exploration for gameplay.csv

# MARKDOWN ********************

# Missing values in game_play

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# missing values in game_play_players
