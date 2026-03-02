# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e3be1fcb-25b7-4d42-8259-43dd03e82e25",
# META       "default_lakehouse_name": "nhl_silver_lh",
# META       "default_lakehouse_workspace_id": "0db915bd-6920-49f3-952b-dfc36c46655e",
# META       "known_lakehouses": [
# META         {
# META           "id": "e3be1fcb-25b7-4d42-8259-43dd03e82e25"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "96d6a4d6-9e84-b950-479f-a1624c370ee3",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     },
# META     "warehouse": {
# META       "default_warehouse": "7d7d0497-de05-a7f3-455c-5c450de6a8a5",
# META       "known_warehouses": [
# META         {
# META           "id": "7d7d0497-de05-a7f3-455c-5c450de6a8a5",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from com.microsoft.spark.fabric.Constants import Constants
from datetime import datetime, timezone
import great_expectations as gx
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# steps:
# 1. read new silver tables
# 2. perform aggregations
# 3. perform joins
# 4. perform data quality checks
#     - if pass - load into staging - pass parameter as pass into pipeline
#     - if fail - depending on check:
#         - fail no. of column check - fail pipeline (something wrong with the join)
#         - fail range check for calculated fields - fail pipeline (something wrong with the calculation)
#         - fail no. of null checks
#             - if below threshold - log a warning that there are nulls in the columns - pass parameter as pass into pipeline
#             - if above threshold - fail pipeline (too many nulls, data may not be usable)
# 5. load table into gold
# 6. check if table loaded into gold with data

# MARKDOWN ********************

# to calculate normalized stats:
# - aggregate (groupby) player_id, primaryPosition and sum stat and timeonice fields
# - divide total stat by total timeonice for each player
# - multiply the divided stats by 60min

# MARKDOWN ********************

# filters required:
# - away vs home games
# - season, type of game

# MARKDOWN ********************

# ## logging utils

# PARAMETERS CELL ********************

exec_start_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
pipeline_id = 'manual_run'
run_id = 'manual_run'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

STEP_NAME = 'gold_aggregate_check_stage'
LOG_FOLDER = 'Files/gold_logs'
date_executed = exec_start_time.split("T")[0]
manual_run_folder = f'manual_{STEP_NAME}'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# configuring log save location
if pipeline_id == 'manual':
    path = f"{LOG_FOLDER}/{date_executed}/{manual_run_folder}/{STEP_NAME}/log_{exec_start_time}.txt"

else:
    path = f"{LOG_FOLDER}/{date_executed}/{pipeline_id}/{run_id}/{STEP_NAME}/log_{exec_start_time}.txt"

path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def init_sub_log_for_goal_aggregate_check_stage(exec_start_time):
    # initialize logging parameters for bronze validation and loading function
    log = {
        'exec_start_time': exec_start_time,
        'exec_end_time': None,
        'proceed_to_next_pipeline_step': False,
        'steps': {
            'get_silver_data': None,
            'write_to_staging': None ,
            'calculate_skater': None,
            'skater_checks': None,
            'calcuate_goalie': None,
            'goalie_checks': None,
            'add_audit_cols': None,
            'load_to_silver': None,
        }
    }
    return log

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def save_log(log, path):
    mssparkutils.fs.put(
    path,
    json.dumps(log, indent=2),
    overwrite=True
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def update_log(log, field, entry, step=None):
    try:
        if step == None:
            log[field] = entry
            print(f'log updated')
            print(f'{field}: {log[field]}')
        else:
            log[field][step] = entry
            print(f'log updated')
            print(f'{field}: {step}: {log[field][step]}')
    except Exception as e:
        print(f"failed to update log: {str(e)}")
    return log

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## utils for this notebook

# CELL ********************

REQ_COLS = {
    'game_skater_stats': [
        'game_id',
        'player_id',
        'team_id',
        'timeOnIce',
        'assists',
        'goals',
        'shots',
        'hits',
        'powerPlayGoals',
        'powerPlayAssists',
        'penaltyMinutes',
        'faceOffWins',
        'faceoffTaken',
        'takeaways',
        'giveaways',
        'shortHandedGoals',
        'shortHandedAssists',
        'blocked'
    ],
    'game_goalie_stats': [
        'game_id',
        'player_id',
        'team_id',
        'timeOnIce',
        'assists',
        'goals',
        'shots',
        'saves',
        'powerPlaySaves',
        'shortHandedSaves',
        'evenSaves',
        'shortHandedShotsAgainst',
        'evenShotsAgainst',
        'powerPlayShotsAgainst',
        'savePercentage',
        'powerPlaySavePercentage',
        'evenStrengthSavePercentage'
    ],
    'game': [
        'game_id',
        'season',
        'type',
        'date_time_GMT',
        'away_team_id',
        'home_team_id',
        'away_goals',
        'home_goals',
        'outcome'
    ],
    'team_info': [
        'team_id',
        'franchiseId',
        'shortName',
        'teamName',
        'abbreviation'
    ],
    'player_info': [
        'player_id',
        'firstName',
        'lastName',
        'nationality',
        'birthCity',
        'primaryPosition',
        'birthDate',
        'birthStateProvince',
        'height',
        'height_cm',
        'weight'
    ]
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_silver_data(REQ_COLS):
    dfs = {}
    try:
        for table, col_list in REQ_COLS.items():
            req_cols_str = ", ".join(col_list)
            df = spark.sql(f'SELECT {req_cols_str} FROM nhl_silver_lh.silver.{table}')
            dfs[table] = df
        entry = 'success'
    except Exception as e:
        entry = f"Error getting silver data: {type(e).__name__}"
    return dfs, entry

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_to_staging(df, table_name):
    exec_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    target_warehouse_table = f"nhl_gold_wh.staging.{table_name}_{exec_timestamp}"
    df.write \
    .mode("overwrite") \
    .synapsesql(target_warehouse_table)
    print(f"Success: {table_name} written to {target_warehouse_table}")
    return target_warehouse_table

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## utils for skater table

# CELL ********************

def calculate_agg_skater_stats(dfs):
    df = dfs['game_skater_stats']
    # 1. Convert seconds to minutes for the 'Per 60' calculation
    df_with_min = df.withColumn("toi_min", F.col("timeOnIce") / 60)

    # 2. Identify the stats to aggregate 
    # We want everything in REQ_COLS['game_skater_stats'] that is a numeric stat
    metrics_to_sum = [
        'goals', 'assists', 'shots', 'hits', 'powerPlayGoals', 
        'powerPlayAssists', 'penaltyMinutes', 'faceOffWins', 
        'faceoffTaken', 'takeaways', 'giveaways', 'shortHandedGoals', 
        'shortHandedAssists', 'blocked'
    ]

    # 3. GroupBy Player and Game (incorporating your filter grains)
    group_cols = [
        "player_id", "game_id", "team_id"
    ]
    
    # 4. Perform Aggregation
    df_agg = df_with_min.groupBy(group_cols).agg(
        F.sum("toi_min").alias("raw_toi_min"),  # Keep this precise for math
        *[F.sum(c).alias(f"total_{c}") for c in metrics_to_sum]
    )

    # 5. Calculate Stats Per 60 using the RAW (unrounded) time
    for c in metrics_to_sum:
        df_agg = df_agg.withColumn(
            f"{c}_per_60",
            F.round((F.col(f"total_{c}") / F.col("raw_toi_min")) * 60, 2)
        )

    # 6. NOW round the display column for the Warehouse
    df_final = df_agg.withColumn("total_toi_min", F.round(F.col("raw_toi_min"), 2)) \
                    .drop("raw_toi_min") # Remove the messy high-precision column
    
    return df_final


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_skater_stats_table(dfs, skater):
    game = dfs['game']
    team = dfs['team_info']
    player = dfs['player_info']

    game_skater_joined = skater.join(game, on='game_id', how='inner')
    
    df_with_away = game_skater_joined.join(
            team.select(
                F.col("team_id").alias("away_team_id"), # Match the join key
                F.col("shortName").alias("away_team_shortName"),
                F.col("teamName").alias("away_team_name"),
                F.col("abbreviation").alias("away_team_abbr")
            ),
            on="away_team_id",
            how="left"
        )

    df_with_home_away = df_with_away.join(
        team.select(
            F.col("team_id").alias("home_team_id"), # Match the join key
            F.col("shortName").alias("home_team_shortName"),
            F.col("teamName").alias("home_team_name"),
            F.col("abbreviation").alias("home_team_abbr")
        ),
        on="home_team_id",
        how="left"
        )

    df_with_player = df_with_home_away.join(
        player.select(
            F.col("player_id"),
            F.col('firstName'),
            F.col('lastName'),
            F.col('primaryPosition')
        ),
        on='player_id',
        how='left'
    )

    df_final = df_with_player.withColumn(
        "venue", 
        F.when(F.col("team_id") == F.col("home_team_id"), F.lit("Home"))
        .otherwise(F.lit("Away"))
    )

    return df_final

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

skater_col_dtypes = {
    'total_hits': 'int',
    'total_takeaways': 'int',
    'total_giveaways': 'int',
    'total_blocked': 'int',
    'hits_per_60': 'float',
    'takeaways_per_60': 'float',
    'giveaways_per_60': 'float',
    'blocked_per_60': 'float'
}

def cast_gold_columns(df, dtypes_dict):
    """
    Casts specific columns to types defined in a dict while preserving all other columns.
    Nulls are preserved automatically by Spark's .cast() method.
    """
    # Create a list of column expressions
    # If the column is in our dict, cast it; otherwise, keep the original column
    select_exprs = [
        F.col(c).cast(dtypes_dict[c]).alias(c) if c in dtypes_dict else F.col(c)
        for c in df.columns
    ]
    
    return df.select(*select_exprs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## goalie utils

# CELL ********************

def calculate_agg_goalie_stats(dfs):
    df = dfs['game_goalie_stats']
    # 1. Convert seconds to minutes for the 'Per 60' calculation
    df_with_min = df.withColumn("toi_min", F.col("timeOnIce") / 60)

    # 2. Identify the stats to aggregate 
    # We want everything in REQ_COLS['game_skater_stats'] that is a numeric stat
    metrics_to_sum = [
        'assists',
        'goals',
        'shots',
        'saves',
        'powerPlaySaves',
        'shortHandedSaves',
        'evenSaves',
        'shortHandedShotsAgainst',
        'evenShotsAgainst',
        'powerPlayShotsAgainst',
    ]

    # 3. GroupBy Player and Game (incorporating your filter grains)
    group_cols = [
        "player_id", "game_id", "team_id"
    ]
    
    # 4. Perform Aggregation
    df_agg = df_with_min.groupBy(group_cols).agg(
        F.sum("toi_min").alias("raw_toi_min"),  # Keep this precise for math
        *[F.sum(c).alias(f"total_{c}") for c in metrics_to_sum]
    )

    # 5. Calculate Stats Per 60 using the RAW (unrounded) time
    for c in metrics_to_sum:
        df_agg = df_agg.withColumn(
            f"{c}_per_60",
            F.round((F.col(f"total_{c}") / F.col("raw_toi_min")) * 60, 2)
        )

    # 6. NOW round the display column for the Warehouse
    df_final = df_agg.withColumn("total_toi_min", F.round(F.col("raw_toi_min"), 2)) \
                    .drop("raw_toi_min") # Remove the messy high-precision column
    
    return df_final

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_goalie_stats_table(dfs, goalie):
    game = dfs['game']
    team = dfs['team_info']
    player = dfs['player_info']

    game_goalie_joined = goalie.join(game, on='game_id', how='inner')
    
    df_with_away = game_goalie_joined.join(
            team.select(
                F.col("team_id").alias("away_team_id"), # Match the join key
                F.col("shortName").alias("away_team_shortName"),
                F.col("teamName").alias("away_team_name"),
                F.col("abbreviation").alias("away_team_abbr")
            ),
            on="away_team_id",
            how="left"
        )

    df_with_home_away = df_with_away.join(
        team.select(
            F.col("team_id").alias("home_team_id"), # Match the join key
            F.col("shortName").alias("home_team_shortName"),
            F.col("teamName").alias("home_team_name"),
            F.col("abbreviation").alias("home_team_abbr")
        ),
        on="home_team_id",
        how="left"
        )

    df_with_player = df_with_home_away.join(
        player.select(
            F.col("player_id"),
            F.col('firstName'),
            F.col('lastName'),
            F.col('primaryPosition')
        ),
        on='player_id',
        how='left'
    )

    df_final = df_with_player.withColumn(
        "venue", 
        F.when(F.col("team_id") == F.col("home_team_id"), F.lit("Home"))
        .otherwise(F.lit("Away"))
    )

    return df_final

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## main

# CELL ********************

def init_sub_log_for_goal_aggregate_check_stage(exec_start_time):
    # initialize logging parameters for bronze validation and loading function
    log = {
        'exec_start_time': exec_start_time,
        'exec_end_time': None,
        'proceed_to_next_pipeline_step': False,
        'steps': {
            'get_silver_data': None,
            'calculate_skater': None,
            'skater_checks': None,
            'calculate_goalie': None,
            'goalie_checks': None,
            'load_skater_to_stage': None,
            'load_goalie_to_stage': None,
        }
    }
    return log

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

log = init_sub_log_for_goal_aggregate_check_stage(exec_start_time)
save_log(log, path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

agg_df = {}
dfs, entry = get_silver_data(REQ_COLS)
log = update_log(log, field='steps', step='get_silver_data', entry=entry)
save_log(log, path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    skater = calculate_agg_skater_stats(dfs)
    skater = create_skater_stats_table(dfs, skater)
    skater = cast_gold_columns(skater, skater_col_dtypes)
    display(skater)
    agg_df['skater'] = skater
    entry = 'success'
except Exception as e:
    entry = f"Error calculating skater: {type(e).__name__}"

log = update_log(log, field='steps', step='calculate_skater', entry=entry)
save_log(log, path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    goalie = calculate_agg_goalie_stats(dfs)
    goalie = create_goalie_stats_table(dfs, goalie)
    display(goalie)
    agg_df['goalie'] = goalie
except Exception as e:
    entry = f"Error calculating goalie: {type(e).__name__}"

log = update_log(log, field='steps', step='calculate_goalie', entry=entry)
save_log(log, path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## run data quality checks

# CELL ********************

# create a context for the great expectations (GE) suite
context= gx.get_context()

# create a GE source in the context 
data_source = context.data_sources.add_spark(name="nhl_spark_datasource")

# create a dataframe asset in the GE source
asset_skater = data_source.add_dataframe_asset(name="skater_table")
asset_goalie = data_source.add_dataframe_asset(name="goalie_table")

# create a batch definition for the entire dataframe in the GE asset
batch_def_skater = asset_skater.add_batch_definition_whole_dataframe(name="batch_skater_whole")
batch_def_goalie = asset_goalie.add_batch_definition_whole_dataframe(name="batch_goalie_whole")

# adding the dataframe to the GE asset
skater = agg_df['skater']
goalie = agg_df['goalie']
skater_batch = batch_def_skater.get_batch(batch_parameters={"dataframe": skater})
goalie_batch = batch_def_goalie.get_batch(batch_parameters={"dataframe": goalie})

# adding the container for all the tests for the dataframe\
skater_whole_suite = gx.ExpectationSuite(name="skater_suite_whole")
goalie_whole_suite = gx.ExpectationSuite(name="goalie_suite_whole")

# creating tests for the skater suite
skater_positive_columns = [
    'assists_per_60',
    'blocked_per_60',
    'faceoffTaken_per_60',
    'faceOffWins_per_60',
    'giveaways_per_60',
    'goals_per_60',
    'hits_per_60',
    'penaltyMinutes_per_60',
    'powerPlayAssists_per_60',
    'powerPlayGoals_per_60',
    'shortHandedAssists_per_60',
    'shortHandedGoals_per_60',
    'shots_per_60',
    'takeaways_per_60',
    'total_assists',
    'total_blocked',
    'total_faceoffTaken',
    'total_faceOffWins',
    'total_giveaways',
    'total_goals',
    'total_hits',
    'total_penaltyMinutes',
    'total_powerPlayAssists',
    'total_powerPlayGoals',
    'total_shortHandedAssists',
    'total_shortHandedGoals',
    'total_shots',
    'total_takeaways',
    'total_toi_min'
]

for col in skater_positive_columns:
    expectation = gx.expectations.ExpectColumnValuesToBeBetween(column=col, min_value=0)
    # add the tests to the suited
    skater_whole_suite.add_expectation(expectation=expectation)

col_count_expectation = gx.expectations.ExpectTableColumnCountToEqual(value=50)
skater_whole_suite.add_expectation(expectation=col_count_expectation)

# creating tests for the goalie suite
goalie_positive_columns = [
    'assists_per_60',
    'evenSaves_per_60',
    'evenShotsAgainst_per_60',
    'goals_per_60',
    'powerPlaySaves_per_60',
    'powerPlayShotsAgainst_per_60',
    'saves_per_60',
    'shortHandedSaves_per_60',
    'shortHandedShotsAgainst_per_60',
    'shots_per_60',
    'total_assists',
    'total_evenSaves',
    'total_evenShotsAgainst',
    'total_goals',
    'total_powerPlaySaves',
    'total_powerPlayShotsAgainst',
    'total_saves',
    'total_shortHandedSaves',
    'total_shortHandedShotsAgainst',
    'total_shots',
    'total_toi_min'
]

for col in goalie_positive_columns:
    expectation = gx.expectations.ExpectColumnValuesToBeBetween(column=col, min_value=0)
    # add the tests to the suite
    goalie_whole_suite.add_expectation(expectation=expectation)


col_count_expectation = gx.expectations.ExpectTableColumnCountToEqual(value=42)
goalie_whole_suite.add_expectation(expectation=col_count_expectation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# run the tests for skater table
skater_results = skater_batch.validate(expect=skater_whole_suite)
print(skater_results.success)
print(skater_results.describe())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# run the tests for goalie table
goalie_results = goalie_batch.validate(expect=goalie_whole_suite)
print(goalie_results.success)
print(goalie_results.describe())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## write validation results to log

# CELL ********************

def check_if_skater_val_success(skater_results):
    if skater_results.success:
        entry = f'all data quality tests passed for skater' 
    else:
        results_dict = skater_results.to_json_dict()
        summary = results_dict['statistics']
        failures = []
        for expectation in results_dict['results']:
            if not expectation['success']:
                failures.append(expectation)
        entry = {
            'summary': summary,
            'failures': failures
        }
    print(entry)
    return skater_results.success, entry

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def check_if_goalie_val_success(goalie_results):
    if goalie_results.success:
        entry = f'all data quality tests passed for goalie' 
    else:
        results_dict = goalie_results.to_json_dict()
        summary = results_dict['statistics']
        failures = []
        for expectation in results_dict['results']:
            if not expectation['success']:
                failures.append(expectation)
        entry = {
            'summary': summary,
            'failures': failures
        }
    print(entry)
    return goalie_results.success, entry

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

goalie_val, entry1 = check_if_goalie_val_success(goalie_results)
log = update_log(log, field='steps', step='goalie_checks', entry=entry1)
save_log(log, path)

skater_val, entry2 = check_if_skater_val_success(skater_results)
log = update_log(log, field='steps', step='skater_checks', entry=entry2)
save_log(log, path)

try:
    goalie_table_name = write_to_staging(agg_df['goalie'], 'fact_goalie_stats_per_60')
    entry = 'success'
except Exception as e:
    entry = f"Error writing goalie to staging: {type(e).__name__}"

log = update_log(log, field='steps', step='load_goalie_to_stage', entry=entry)
save_log(log, path)

try:
    skater_table_name = write_to_staging(agg_df['skater'], 'fact_skater_stats_per_60')
    entry = 'success'
except Exception as e:
    entry = f"Error writing skater to staging: {type(e).__name__}"

log = update_log(log, field='steps', step='load_skater_to_stage', entry=entry)
save_log(log, path)

output = {
    'goalie_pass_val': goalie_val,
    'skater_pass_val': skater_val,
    'proceed':goalie_val and skater_val,
    'goalie': goalie_table_name,
    'skater': skater_table_name
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(goalie_table_name)
print(skater_table_name)
print(output)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

log = update_log(log, field='proceed_to_next_pipeline_step', entry=True)
log = update_log(log, field='exec_end_time', entry=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
save_log(log, path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## pass the staging table names to pipeline

# CELL ********************

mssparkutils.notebook.exit(json.dumps(output))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
