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

# MARKDOWN ********************

# # Data Exploration for game_skater_stats, game_goalie_stats, team_info and game_plays tables

# MARKDOWN ********************

# ### Loading the data into tables and dataframes

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_structure = {
    'team_info':
        {
            'team_id': IntegerType(),
            'franchiseId': IntegerType(),
            'shortName': StringType(),
            'teamName': StringType(),
            'abbreviation': StringType(),
            'link': StringType()
        },
        'game_skater_stats':
        {
            'game_id': IntegerType(),
            'player_id': IntegerType(),
            'team_id': IntegerType(),
            'timeOnIce': IntegerType(),
            'assists': IntegerType(),
            'goals': IntegerType(),
            'shots': IntegerType(),
            'hits': IntegerType(),
            'powerPlayGoals': IntegerType(),
            'powerPlayAssists': IntegerType(),
            'penaltyMinutes': IntegerType(),
            'faceOffWins': IntegerType(),
            'faceoffTaken': IntegerType(),
            'takeaways': IntegerType(),
            'giveaways': IntegerType(),
            'shortHandedGoals': IntegerType(),
            'shortHandedAssists': IntegerType(),
            'blocked': IntegerType(),
            'plusMinus': IntegerType(),
            'evenTimeOnIce': IntegerType(),
            'shortHandedTimeOnIce': IntegerType(),
            'powerPlayTimeOnIce':IntegerType()
        },
        'game_goalie_stats':
        {
            'game_id': IntegerType(),
            'player_id': IntegerType(),
            'team_id': IntegerType(),
            'timeOnIce': IntegerType(),
            'assists': IntegerType(),
            'goals': IntegerType(),
            'pim': IntegerType(),
            'shots': IntegerType(),
            'saves': IntegerType(),
            'powerPlaySaves': IntegerType(),
            'shortHandedSaves': IntegerType(),
            'evenSaves': IntegerType(),
            'shortHandedShotsAgainst': IntegerType(),
            'evenShotsAgainst': IntegerType(),
            'powerPlayShotsAgainst': IntegerType(),
            'decision': StringType(),
            'savePercentage': FloatType(),
            'powerPlaySavePercentage': FloatType(),
            'evenStrengthSavePercentage': FloatType()
        },
        'game_plays': 
        { 
            'play_id': StringType(), 
            'game_id': StringType(), 
            'team_id_for': IntegerType(), 
            'team_id_against': IntegerType(), 
            'event': StringType(), 
            'secondaryType': StringType(), 
            'x': FloatType(), 
            'y': FloatType(), 
            'period': IntegerType(), 
            'periodType': StringType(), 
            'periodTime': IntegerType(), 
            'periodTimeRemaining': FloatType(), 
            'dateTime': TimestampType(), 
            'goals_away': IntegerType(), 
            'goals_home': IntegerType(), 
            'description': StringType(), 
            'st_x': FloatType(), 
            'st_y': FloatType() 
        }, 
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_schemas = {}

for table, col_type in table_structure.items():
    listing = []
    for col, data_type in col_type.items():
        listing.append(StructField(col, data_type, True))
    schema = StructType(listing)
    table_schemas[table] = schema

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sdfs = {}

for table, schema in table_schemas.items():
    sdfs[table] = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option('nullValue', 'NA')
        .option('nanValue', 'NA')
        .schema(schema)
        .load(f"Files/historical/landing/{table}.csv")
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(sdfs['game_skater_stats'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for df_name, df in sdfs.items():
    table_name = df_name
    full_path = 'Tables'
    df.write.format("delta").mode("overwrite").saveAsTable(df_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### game_skater_stats table
# 
# There are 4 columns with null values in this table. The treatment for each of these field's null values are shown below and justified in the following subsections.
# 
# - For all 4 columns ('hits', 'takeaways', 'giveaways', 'blocked'), the null values are missing for the same game_ids
# - This indicates that there was probably a failure to record values for these 4 statistics during particular games.
# - Due to the percentage of null values (> 40%) - it would be unwise to replace these null values with mean/ median values
# - There is also no method to calculate an approximation of these values.
# - Since there are sufficient non-null data points for meaningful analysis, decision was made to keep the data as-is in the silver layer
# - aggregations in the gold layer may choose to use the non-null values as approximation of the actual statistics.

# CELL ********************

skater_df = sdfs['game_skater_stats'].toPandas()

null_percent = skater_df.isna().mean() * 100

null_percent.plot(kind="bar")
plt.title("Percentage of Null Values per Column")
plt.ylabel("% Null")
plt.tight_layout()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

skater_df.dtypes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

msno.matrix(skater_df)
plt.show()
# confirms that all null values for hits, takeaways, giveaways and blocks are missing together.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get the game_ids for the null values of hits, takeaways, giveaways, blocked
hit_ids = set(skater_df.loc[skater_df.hits.isna(), 'game_id'])
take_ids = set(skater_df.loc[skater_df.takeaways.isna(), 'game_id'])
give_ids = set(skater_df.loc[skater_df.giveaways.isna(), 'game_id'])
blocked_ids = set(skater_df.loc[skater_df.blocked.isna(), 'game_id'])

all_equal = (hit_ids == take_ids == give_ids == blocked_ids)
all_equal

# shows that all game_ids for null hits, takeaways, giveaways, blocked values are the same

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# check if there are any non-null values for hits, takeaways, giveaways, blocked in the identified game_ids
cols = ['hits', 'takeaways', 'giveaways', 'blocked']

for col in cols:
    non_null_exist = skater_df.loc[skater_df.game_id.isin(hit_ids), col].notna().any()
    print(f"Any non-null {col}: {non_null_exist}")

# shows that all values of hits, takeaways, giveaways, blocked are null for the identified game_ids

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cols = ['hits', 'takeaways', 'giveaways', 'blocked']
skater_df[cols].hist(layout=(2, 2), figsize=(10, 8))
plt.show()

skater_df[cols].describe()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### game_goalie_stats table
# 
# There are 4 columns with null values in this table. The treatment for each of these field's null values are shown below and justified in the following subsections.
# 
# decision:
# - contains W, L or None.
# - ~7% None values
# - No clear pattern of how these None values arrived and we cannot confirm the origin from the source.
# - We will most likely not be using this field for analysis
# Conclusion: leave the None values as-is in Silver Layer
# 
# savePercentage:
# - contains float values and NaN
# - < 1% NaN values
# - NaN values result from no shots taken against the goalie
# - We may use this field for analysis
# Conclusion: leave the NaN values as-is in Silver Layer as these are valid NaN values and not errors.
# 
# powerPlaySavePercentage:
# - contains float values and NaN
# - ~8% NaN values
# - NaN values result from no power play shots taken against the goalie
# - We may use this field for analysis
# Conclusion: leave the NaN values as-is in Silver Layer as these are valid NaN values and not errors.
# 
# evenStrengthSavePercentage:
# - contains float values and NaN
# - < 1% NaN values
# - NaN values result from no even strength shots taken against the goalie
# - We may use this field for analysis
# Conclusion: leave the NaN values as-is in Silver Layer as these are valid NaN values and not errors.


# CELL ********************

goalie_df = sdfs['game_goalie_stats'].toPandas()

null_percent = goalie_df.isna().mean() * 100

null_percent.plot(kind="bar")
plt.title("Percentage of Null Values per Column")
plt.ylabel("% Null")
plt.tight_layout()
plt.show()

goalie_df.info()
goalie_df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### decision field

# CELL ********************

print(goalie_df.decision.unique())
goalie_df.loc[goalie_df.decision.isna()]

# we probably can leave the None values as is - given that there isnt a pattern and we will not be needing to calculate stats for it

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### save percentage field

# CELL ********************

print(goalie_df.savePercentage.unique())
goalie_df.loc[goalie_df.savePercentage.isna()]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# are all the NULL save% from 0 shots?

goalie_df[goalie_df['shots'] == 0].index.equals(
    goalie_df[goalie_df['savePercentage'].isna()].index
)

# Yes - meaning these are valid NULL values, not errors

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### powerPlaySavePercentage field

# CELL ********************

print(goalie_df.powerPlaySavePercentage.unique())
goalie_df.loc[goalie_df.powerPlaySavePercentage.isna()]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# are all the Nan power play save% from 0 powerplayshotsagainst?

goalie_df[goalie_df['powerPlayShotsAgainst'] == 0].index.equals(
    goalie_df[goalie_df['powerPlaySavePercentage'].isna()].index
)

# Yes - meaning these are valid None values, not errors

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### evenStrengthSavePercentage field

# CELL ********************

print(goalie_df.evenStrengthSavePercentage.unique())
goalie_df.loc[goalie_df.evenStrengthSavePercentage.isna()]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# are all the Nan evenstrength save% from 0 evenshotsagainst?

goalie_df[goalie_df['evenShotsAgainst'] == 0].index.equals(
    goalie_df[goalie_df['evenStrengthSavePercentage'].isna()].index
)

# Yes - meaning these are valid None values, not errors

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### team_info table
# 
# data is generally clean and can be loaded into silver layer as-is.

# CELL ********************

team_df = sdfs['team_info'].toPandas()

null_percent = team_df.isna().mean() * 100

null_percent.plot(kind="bar")
plt.title("Percentage of Null Values per Column")
plt.ylabel("% Null")
plt.tight_layout()
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

team_df.info()
display(team_df.head())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### game_plays table
# 
# There are 8 columns with null values in this table. The treatment for each of these field's null values are shown below and justified in the following subsections.

# MARKDOWN ********************

# #### **x, y, st_x and st_y:**
# - Contains x and y coordinates for where the shot was taken
# - ~20% NaN values
# - Missing values can be explained by:
#   - Events that cause missing `team_id_for` and `team_id_against` result in some missing `x, y, st_x, st_y` values:
#       - 'Game Scheduled'
#       - 'Period Ready'
#       - 'Period Start'
#       - 'Stoppage'
#       - 'Period End'
#       - 'Period Official'
#       - 'Game End'
#       - 'Shootout Complete'
#       - 'Early Intermission Start'
#       - 'Early Intermission End'
#       - 'Game Official'
#       - 'Emergency Goaltender'
#     
#     Missing coordinate values for these events make sense because **no shots are expected** during these events
#     
# 
# 
#     
#   - Timestamps (`dateTime` field) for which `periodTimeRemaining` is missing also have missing `x, y, st_x, st_y` values:
#     - From **2000-10-05 00:00:00** to **2010-04-12 01:00:00**
#     - Missing coordinate values for these timestamps may be due to **system outages** or **they may have not started to track these statistics** as:
#       - Events associated with missing `periodTimeStamps` are shooting-related and **expected to have coordinates**
#       - The same events outside these timestamps have coordinate values
#   - **6388 unique timestamps** between **2010-10-08 03:25:19** and **2020-09-22 03:34:02** have missing `x, y, st_x, st_y` values:
#     - Missing coordinates may be due to **system outages** as:
#       - No events associated with these timestamps fully explain the missing coordinates
#       - Timestamps outside these periods (after removing nulls caused by missing team IDs and missing periodTimeRemaining) do **not** have missing coordinates
# 
# **Conclusion:**
# - Missing coordinate data explained by events without `team_id_for` and `team_id_against` are **valid nulls** because these events do not require shot coordinates
# - Missing coordinate data during the **2000-2010 period** and the **6388 timestamps between 2010-2020** are **not random**
# 
# **Decision:**
# - Track these values in the **silver-layer as-is**
#   - Shot coordinates are difficult to infer or replicate, so we **should not estimate them**; doing so may skew analyses
#   - Aggregations should only be performed on **available data**
# - Optionally, include **labels explaining why data is missing** for traceability


# MARKDOWN ********************

# #### **team_id_for and team_id_against:**
# - Contains floats for the `team_id`s involved in the game play
#   - `team_id_for` → decision ruled in favor
#   - `team_id_against` → decision ruled against
# - < 20% NaN values
# - Missing values can be explained by:
#   - Events that cause missing `team_id_for` and `team_id_against`:
#     - 'Game Scheduled'
#     - 'Period Ready'
#     - 'Period Start'
#     - 'Stoppage'
#     - 'Period End'
#     - 'Period Official'
#     - 'Game End'
#     - 'Shootout Complete'
#     - 'Early Intermission Start'
#     - 'Early Intermission End'
#     - 'Game Official'
#     - 'Emergency Goaltender'
# 
#     Missing `team_id`s for these events make sense because **these events do not necessarily rule in favor/against a particular team**
#   - 'Official Challenge' event type has seemingly **random nulls** for the team IDs:
#     - These nulls cannot be explained without **domain knowledge**
# 
# **Conclusion:**
# - Missing `team_id_for` and `team_id_against` can be fully explained by their associated events (**except 'Official Challenge'**) and are **valid nulls**
# - For 'Official Challenge', we cannot be certain that the nulls are valid without **domain knowledge**
# 
# **Decision:**
# - Track these values in the **silver-layer as-is**
#   - In the absence of domain knowledge, take the **conservative approach** of treating the nulls as valid
#   - Optionally, include **labels explaining why data is missing** for traceability


# MARKDOWN ********************

# #### **periodTimeRemaining:**
# - Contains string values and NaN
# - <5% NaN values
# - Missing values can be explained by:
#   - Timestamps (`dateTime` field) for which `periodTimeRemaining` is missing:
#     - From **2000-10-05 00:00:00** to **2010-04-12 01:00:00**
#     - Missing values may be due to a **system outage** or **they have not started tracking these statistics**:
#       - No events can fully explain the missing `periodTimeRemaining`
#       - Timestamps with valid `periodTimeRemaining` do **not** overlap with the identified outage timestamps
#       - The same events outside the identified timestamps have valid periodTimeRemaining values
# 
# **Conclusion:**
# - Missing `periodTimeRemaining` values during **2000-2010** are **not random**
# 
# **Decision:**
# - Track these values in the **silver-layer as-is**
#   - `periodTimeRemaining` is an **in-game generated fact** that cannot be easily replicated
# - Optionally, include **labels explaining why data is missing** for traceability


# MARKDOWN ********************

# #### **secondaryType:**
# - Contains string values and `None`
# - ~75% None values
# - Missing values exist for multiple event types **except Goal, Shot, and Penalty events**
# - Only **Penalty events** definitely have values for `secondaryType`
# - **Goal and Shot events** have no clear pattern for why `secondaryType` values exist for some records and not others
# - All other event types do **not** have `secondaryType` values:
#   - Likely an optional column that the system inputs for events
#   - Not possible to determine without **domain knowledge**
# 
# **Conclusion:**
# - Missing `secondaryType` values for all events **other than Goal, Shot, and Penalty** are **valid nulls**
# - Missing `secondaryType` values for **Goal and Shot events** are **random**
# - `secondaryType` values for **Penalty events** are **compulsory**
# 
# **Decision:**
# - Track these values in the **silver-layer as-is**
# - Optionally, include a **check** for `secondaryType` when the event is **Penalty**
# - Optionally, include **labels explaining why data is missing** for traceability


# CELL ********************

game_plays_df = sdfs['game_plays'].toPandas()

null_percent = game_plays_df.isna().mean() * 100

null_percent.plot(kind="bar")
plt.title("Percentage of Null Values per Column")
plt.ylabel("% Null")
plt.tight_layout()
plt.show()

game_plays_df.info()
game_plays_df.columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

game_plays_df.sort_values(by='team_id_for', inplace=True)
msno.matrix(game_plays_df)
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# matrix shows that missing team_id_for and team_id_against have missing x, y, st_x and st_y values as well

# CELL ********************

game_plays_df[game_plays_df.team_id_for.isna()]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### checking if any event can explain missing team_id_for and team_id_against fields

# CELL ********************

game_plays_df[game_plays_df.team_id_for.isna()]['event'].unique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

events_with_null = ['Game Scheduled', 'Period Ready', 'Period Start', 'Stoppage',
       'Period End', 'Period Official', 'Game End', 'Shootout Complete',
       'Official Challenge', 'Early Intermission Start',
       'Early Intermission End', 'Game Official', 'Emergency Goaltender']

for event in events_with_null:
    print(event, game_plays_df.loc[game_plays_df['event'] == event, 'team_id_for'].unique())

# shows that all team_id_for and team_id_against nulls can be explained by these event types as all values for team_for and team_against in these events are null:
# Game Scheduled
# Period Ready
# Period Start
# Stoppage
# Period End
# Period Official
# Game End
# Shootout Complete
# Early Intermission Start
# Early Intermission End
# Game Official
# Emergency Goaltender

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

game_plays_df[game_plays_df['event'] == 'Official Challenge']

# needs domain knowledge to answer why some ids are null

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

event_list = [
    'Game Scheduled',
    'Period Ready',
    'Period Start',
    'Stoppage',
    'Period End',
    'Period Official',
    'Game End',
    'Shootout Complete',
    'Early Intermission Start',
    'Early Intermission End',
    'Game Official',
    'Emergency Goaltender'
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### exploring missing x and y values within identified events (without official challenge event)

# CELL ********************

identified_events = game_plays_df[game_plays_df['event'].isin(event_list)]
identified_events

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

identified_events.loc[identified_events['x'].isna(), 'event'].unique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

identified_events.loc[~identified_events['x'].isna(), 'event'].unique()
# all events that have missing team_id_for and team_id_against have missing x, y, st_x and st_y fields as well

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

event_list = [
    'Game Scheduled',
    'Period Ready',
    'Period Start',
    'Stoppage',
    'Period End',
    'Period Official',
    'Game End',
    'Shootout Complete',
    'Early Intermission Start',
    'Early Intermission End',
    'Game Official',
    'Emergency Goaltender',
    'Official Challenge'
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

removed_events = game_plays_df[~game_plays_df['event'].isin(event_list)]

removed_events.team_id_against.isna().unique()
# all team_id_against and team_id_for explained by events

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### removing events that have identified to cause missing team_id_for and team_id_against fields

# CELL ********************

removed_events = removed_events.copy()

removed_events['dateTime'] = pd.to_datetime(removed_events['dateTime'])
removed_events = removed_events.sort_values('dateTime')

msno.matrix(removed_events)
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# matrix shows that missing periodTimeRemaining have missing x, y, st_x and st_y values as well

# MARKDOWN ********************

# #### exploring missing periodTimeRemaining field

# CELL ********************

date_range = removed_events[removed_events['periodTimeRemaining'].isna()].dateTime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

anti_date_range = removed_events[~removed_events['periodTimeRemaining'].isna()].dateTime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(min(date_range), max(date_range))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(min(anti_date_range), max(anti_date_range))

# we can see that the dates dont overlap
# - suggests that the periodTimeRemaining was not tracked for games that happened between 2000-10-05 00:00:00 and 2010-04-12 01:00:00

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### check if any event can explain missing periodTimeRemaining values

# CELL ********************

game_plays_df.loc[game_plays_df['periodTimeRemaining'].isna(), 'event'].unique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

events_with_null = ['Missed Shot', 'Goal', 'Penalty', 'Shot']

for event in events_with_null:
    print(event, game_plays_df.loc[game_plays_df['event'] == event, 'periodTimeRemaining'].unique())

# shows that events cannot entirely explain the null values in periodTimeRemaining

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### remove games between 2000-10-05 00:00:00 and 2010-04-12 01:00:00 identified to have missing periodTimeRemaining field

# CELL ********************

further_removed = removed_events[removed_events['dateTime'].isin(anti_date_range)]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

msno.matrix(further_removed)
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### exploring missing x and y values

# CELL ********************

further_removed[further_removed['x'].isna()]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### checking if any events can fully explain missing x and y values

# CELL ********************

further_removed.loc[further_removed['x'].isna(), 'event'].unique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

further_removed.loc[~further_removed['x'].isna(), 'event'].unique()

# no events can fully explain missing x and y values

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### checking if any timestamps can explain missing x and y values

# CELL ********************

date_range = further_removed.loc[further_removed['x'].isna(), 'dateTime']
print(min(date_range), max(date_range))
print(len(list(date_range.unique())))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

anti_date_range = further_removed.loc[~further_removed['x'].isna(), 'dateTime']
print(min(anti_date_range), max(anti_date_range))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

set(date_range) in set(anti_date_range)
# shows that for certain timestamps, there was no data for x and y -> perhaps due to an outage in x and y tracking system

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### remove games with timestamps that cause missing x and y values

# CELL ********************

removed_missing_dates = further_removed[further_removed['dateTime'].isin(anti_date_range)]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

msno.matrix(removed_missing_dates)
plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### exploring missing secondaryType values

# CELL ********************

game_plays_df.loc[game_plays_df['secondaryType'].isna(), 'event'].unique()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

game_plays_df.loc[~game_plays_df['secondaryType'].isna(), 'event'].unique()
# some Goal and Shot events have secondaryTypes values

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

game_plays_df.loc[game_plays_df['event'] == 'Penalty', 'secondaryType'].isna().unique()
# all penalty event types have secondaryType values

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

goal = game_plays_df.loc[game_plays_df['event'] == 'Goal']
display(goal[goal['secondaryType'].isna()])

display(goal[~goal['secondaryType'].isna()])

# no clear pattern for why secondaryType is available for some goal events and not available for other goal events.
# Likely an optional column - not possible to determine without domain knowledge

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

shot = game_plays_df.loc[game_plays_df['event'] == 'Shot']
display(shot[shot['secondaryType'].isna()])

display(shot[~shot['secondaryType'].isna()])

# no clear pattern for why secondaryType is available for some shot events and not available for other shot events.
# Likely an optional column - not possible to determine without domain knowledge

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
