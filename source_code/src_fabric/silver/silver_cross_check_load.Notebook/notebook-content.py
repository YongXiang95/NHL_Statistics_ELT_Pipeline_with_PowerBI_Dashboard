# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d436ff99-e26d-4715-97dc-ce6b7dd4f124",
# META       "default_lakehouse_name": "nhl_bronze_lh",
# META       "default_lakehouse_workspace_id": "0db915bd-6920-49f3-952b-dfc36c46655e",
# META       "known_lakehouses": [
# META         {
# META           "id": "d436ff99-e26d-4715-97dc-ce6b7dd4f124"
# META         },
# META         {
# META           "id": "e3be1fcb-25b7-4d42-8259-43dd03e82e25"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import json
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timezone

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# pipeline parameters cell
exec_start_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
schema_name = "[\"staging_20260202_0948\"]"
pipeline_id = 'manual_run'
run_id = 'manual_run'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# non-parameter variables
STEP_NAME = 'silver_cross_check_load'
LOG_FOLDER = 'Files/silver_logs'
date_executed = exec_start_time.split("T")[0]
manual_run_folder = f'manual_{STEP_NAME}'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# e.g., "abfss://final_project_test@onelake.dfs.fabric.microsoft.com/nhl_silver_lh.Lakehouse/Tables/schema/game"
BRONZE_LAKEHOUSE = 'nhl_bronze_lh'
SILVER_LAKEHOUSE = 'nhl_silver_lh'
WORKSPACE = 'live_nhl_ws'
SILVER_LAKEHOUSE_ABFSS = f'abfss://{WORKSPACE}@onelake.dfs.fabric.microsoft.com/{SILVER_LAKEHOUSE}.Lakehouse/Tables'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# identifies the primary key for each dim table
DIM_TABLE_CHECKS = {
    'game': ['game_id'],
    'player_info': ['player_id'],
    'team_info': ['team_id'],
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# relates the fact tables with the dim table for ref integrity checks
FACT_TABLE_CHECKS = {
    'game_plays': {
        'game': 'game_id'
    },
    'game_teams_stats': {
        'game': 'game_id',
        'team_info': 'team_id'
    },
    'game_skater_stats': {
        'game': 'game_id',
        'player_info': 'player_id'
    },
    'game_goalie_stats': {
        'game': 'game_id',
        'player_info': 'player_id'
    },
    'game_shifts': {
        'game': 'game_id',
        'player_info': 'player_id'
    },
    'game_plays_players': {
        'game_plays': 'play_id',
        'player_info': 'player_id'
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# configuring log save location
if pipeline_id == 'manual':
    path = f"{LOG_FOLDER}/{date_executed}/{manual_run_folder}/silver_cross_check_load/log_{exec_start_time}.txt"

else:
    path = f"{LOG_FOLDER}/{date_executed}/{pipeline_id}/{run_id}/silver_cross_check_load/log_{exec_start_time}.txt"

path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run constants

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## logging utils

# CELL ********************

def init_sub_log_for_silver_cross_check_load(exec_start_time):
    # initialize logging parameters for bronze validation and loading function
    log = {
        'exec_start_time': exec_start_time,
        'exec_end_time': None,
        'proceed_to_next_pipeline_step': False,
        'steps': {
            'is_initial': None,
            'read_tables': None,
            'check_dim_pkey': None ,
            'check_fact_ri': None,
            'check_for_negative': None,
            'save_qrt': None,
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

def check_if_initial_load():
    try:
        # Try to run a simple count on a table you EXPECT to be there
        # If the lakehouse is empty, this SQL will fail immediately
        spark.sql(f"SELECT 1 FROM {SILVER_LAKEHOUSE}.game LIMIT 1")
        return False
    except:
        # If the table or the schema doesn't exist, treat as initial load
        return True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# function to check if there were multiple schemas for staging and return the schema name for where staging tables are stored.

def get_schema(schema_name):
    print("getting the schema name...")
    schema_list = json.loads(schema_name)
    if len(set(schema_list)) > 1:
        print("critical error in pipeline: loaded staging tables into more than 1 schema")
    elif len(set(schema_list)) < 1:
        print("no staging tables loaded")
    else:
        schema = next(iter(schema_list))
        print("schema name successfully parsed")
    return schema


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# gets the list of tables in the staging schema
def check_tables_in_staging(schema):
    tables = spark.sql(f"SHOW TABLES IN nhl_bronze_lh.{schema}")
    table_names = [r.tableName for r in tables.collect()]
    return table_names

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# adds metadata columns to working dataframe
def add_audit_columns(dfs, pipeline_id, run_id):
    proceed = True
    try:
        for table_name, df in dfs.items():
            df = df.withColumn("load_timestamp", F.current_timestamp()) \
                .withColumn("pipeline_id", F.lit(pipeline_id)) \
                .withColumn("run_id", F.lit(run_id))
            dfs[table_name] = df
        entry = 'success'
    except Exception as e:
        proceed = False
        entry = f"Error adding audit columns to working dfs: {type(e).__name__}"
    return dfs, proceed, entry

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# reads in the tables from staging schema as dataframes and stores in a dict
def read_table(table_names, schema):
    proceed = True
    try:
        dfs = {}
        for table in table_names:
            df = spark.sql(f'SELECT * FROM nhl_bronze_lh.{schema}.{table}')
            dfs[table] = df
        entry = 'success'
    except Exception as e:
        dfs = None
        proceed = False
        entry = f"Error reading staging tables: {type(e).__name__}"
    return dfs, proceed, entry

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# sorts the list of staging tables into dim and fact tables
def sort_dim_fact(table_names):
    proceed = True
    dim_tables = []
    fact_tables = []
    other_tables = []
    try:
        for table in table_names:
            if table in DIM_TABLE_CHECKS:
                dim_tables.append(table)
            elif table in FACT_TABLE_CHECKS:
                fact_tables.append(table)
            else:
                other_tables.append(table)
        entry = 'success'
        if other_tables:
            entry = f'warning, tables: [{other_tables}] are not categorized'
            print(entry)
    except Exception as e:
        dim_tables = None
        fact_tables = None
        proceed = False
        entry = f"Error sorting staging tables: {type(e).__name__}"
    return dim_tables, fact_tables, proceed, entry

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# checks for unique primary keys in the dim tables
def check_dim_pkey(dim_tables, dfs, qrt):
    """
    dim_tables: List of strings (table names)
    dfs: Dictionary of {table_name: dataframe}
    qrt: Dictionary to store the quarantine rows (duplicates)
    """
    proceed = True
    try:
        for table in dim_tables:
            df = dfs[table]
            # Ensure your dictionary 'DIM_TABLE_CHECKS' is defined globally or passed in
            pkey = DIM_TABLE_CHECKS[table] 
            
            # 1. Identify duplicates using Window
            window_spec = Window.partitionBy(pkey)
            flagged_df = df.withColumn("_key_count", F.count("*").over(window_spec))
            
            # 2. Extract problematic rows
            # We filter for > 1 to find ONLY the rows involved in a duplication
            dup_df = (flagged_df.filter(F.col("_key_count") > 1)
                        .withColumn("issue_type", F.lit("pkey_dupe"))
                        .drop("_key_count"))
            
            # 3. Create the clean version
            # Note: we use the original df to avoid keeping the internal _key_count column
            cleaned_df = df.dropDuplicates(pkey)
            
            rows = dup_df.count()
            if rows > 0:
                print(f"duplicate {pkey} found in {table}: {rows} rows")
                qrt[table] = dup_df
                # Update the dictionary with the CLEANED version for downstream use
                dfs[table] = cleaned_df 
            else:
                print(f"no duplicate {pkey} found in {table}")
        entry = 'success'
    except Exception as e:
        proceed = False
        entry = f"Error checking pkey: {type(e).__name__}"
            
    return dfs, qrt, proceed, entry

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# checks for referential integrity from the fact tables to the dim tables
def check_fact_ri(fact_tables, dfs, qrt):
    """
    fact_tables: Dict like {"game_skater_stats": {"player_info": "player_id"}}
    dfs: Dictionary of all your DataFrames
    qrt: Dictionary to store the error rows
    """
    proceed = True
    try:
        for ftable in fact_tables:
            # Start with the current version of the fact table
            working_df = dfs[ftable]
            for dtable, col in FACT_TABLE_CHECKS[ftable].items():
                dim_df = dfs[dtable]
                
                # These are rows in Fact that DON'T exist in Dim
                orphans_df = working_df.join(dim_df, on=col, how="left_anti")
                
                rows = orphans_df.count()
                
                if rows > 0:
                    print(f"RI FAILURE: {ftable} has {rows} orphans missing from {dtable} on {col}")
                    
                    # Label and store in qrt
                    report_key = f"{ftable}_{dtable}_orphans"
                    qrt[report_key] = orphans_df.withColumn("issue_type", F.lit("ri_orphan")) \
                                                .withColumn("failed_dimension", F.lit(dtable)) \
                                                .withColumn("join_key", F.lit(col))
                    
                    # We keep only rows that have a match in the Dimension table
                    working_df = working_df.join(dim_df, on=col, how="left_semi")
                else:
                    print(f"RI PASSED: {ftable} -> {dtable} ({col})")
            
            # Update the main dictionary with the filtered, "clean" DataFrame
            dfs[ftable] = working_df
        entry = 'success'
    except Exception as e:
        proceed = False
        entry = f"Error checking RI: {type(e).__name__}"
    return dfs, qrt, proceed, entry


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# checks for negative values in the positive columns for game_skater_stats and game_goalie_stats
def check_for_negative(dfs, qrt):
    """
    dfs: Dictionary of all DataFrames (skater, goalie, etc.)
    qrt: Dictionary to store the error rows (violators)
    """
    proceed = True
    entry = 'success'
    
    # Configuration mapping table name to the list of columns that must be >= 0
    checks_config = {
        'game_skater_stats': [
            'timeOnIce', 'assists', 'goals', 'shots', 'hits', 'powerPlayGoals',
            'powerPlayAssists', 'penaltyMinutes', 'faceOffWins', 'faceoffTaken',
            'takeaways', 'giveaways', 'shortHandedGoals', 'shortHandedAssists',
            'blocked', 'evenTimeOnIce', 'shortHandedTimeOnIce', 'powerPlayTimeOnIce'
        ],
        'game_goalie_stats': [
            'timeOnIce', 'assists', 'goals', 'shots', 'saves', 'powerPlaySaves',
            'shortHandedSaves', 'evenSaves', 'shortHandedShotsAgainst',
            'evenShotsAgainst', 'powerPlayShotsAgainst', 'savePercentage',
            'powerPlaySavePercentage', 'evenStrengthSavePercentage'
        ]
    }

    try:
        for table_name, cols in checks_config.items():
            if table_name not in dfs:
                continue
                
            working_df = dfs[table_name]
            
            for col_name in cols:
                # 1. Identify "Violators" (Negative values)
                # We use < 0 because 0 is usually a valid stat in hockey
                violators_df = working_df.filter(F.col(col_name) < 0)
                
                rows = violators_df.count()
                
                if rows > 0:
                    print(f"NEG VALUE FAILURE: {table_name} has {rows} negative values in {col_name}")
                    
                    # 2. Label and store in qrt (Matching your RI pattern)
                    report_key = f"{table_name}_{col_name}_negatives"
                    qrt[report_key] = violators_df.withColumn("issue_type", F.lit("negative_value")) \
                                                 .withColumn("failed_column", F.lit(col_name)) \
                                                 .withColumn("check_type", F.lit("must_be_non_negative"))
                    
                    # 3. Filter working_df to keep only valid rows (>= 0)
                    working_df = working_df.filter(F.col(col_name) >= 0)
                else:
                    print(f"NEG VALUE PASSED: {table_name} -> {col_name}")

            # Update the main dictionary with the "clean" DataFrame
            dfs[table_name] = working_df

    except Exception as e:
        proceed = False
        entry = f"Error checking Negative Values: {type(e).__name__}: {str(e)}"
        
    return dfs, qrt, proceed, entry


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# loads the working dataframe into the silver layer tables
def load_to_silver(dfs, initial_load=True):
    proceed = True
    try:
        for table_name, df in dfs.items():        
            if initial_load:
                print(f"Initial Load: Writing {table_name} to Silver...")
                # We use 'overwrite' to create the table and schema for the first time
                df.write.format("delta").mode("overwrite").save(f'{SILVER_LAKEHOUSE_ABFSS}/silver/{table_name}')

            else:
                print(f"Incremental Load: Merging {table_name} into Silver...")
                # For future runs, you would use a MERGE (Upsert) logic here
                # to prevent duplicates.
                pass
        entry = 'success'
    except Exception as e:
        proceed = False
        entry = f"Error loading to silver tables: {type(e).__name__}"
    
    return proceed, entry


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# loads the quarantined rows into staging schema
def save_qrt_to_bronze(qrt, bronze_lakehouse, schema, p_id, r_id):
    proceed = True
    try:
        for report_key, df in qrt.items():
            # 1. Add metadata so you know WHEN and WHY it was rejected
            # We reuse the add_audit_columns logic but add 'issue_type'
            rejected_df = df.withColumn("rejected_at", F.current_timestamp()) \
                            .withColumn("pipeline_id", F.lit(p_id)) \
                            .withColumn("run_id", F.lit(r_id))
            
            # 2. Define table name (e.g., bronze_lh.rejected_game_plays_orphans)
            # We prefix with 'rejected_' to keep it separate from raw data
            target_table = f"{bronze_lakehouse}.{schema}.rejected_{report_key}"
            
            print(f"Saving {rejected_df.count()} rejected records to {target_table}...")
            
            # 3. Append to the rejected table (so you have a history of all failures)
            rejected_df.write.format("delta") \
                    .mode("append") \
                    .option("mergeSchema", "true") \
                    .saveAsTable(target_table)
        entry = 'success'
    except Exception as e:
        proceed = False
        entry = f"Error saving qrt to bronze: {type(e).__name__}"
    
    return proceed, entry

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if __name__ == "__main__":

    qrt = {} # Initialize the quarantine table

    # Ingest Data
    schema = get_schema(schema_name)
    table_names = check_tables_in_staging(schema)
    log = init_sub_log_for_silver_cross_check_load(exec_start_time)
    log = update_log(log, field='stg_tables', entry=table_names)
    save_log(log, path)

    dfs, proceed, entry = read_table(table_names, schema)
    log = update_log(log, field='steps', step='read_tables', entry=entry)
    save_log(log, path)

    # sort tables
    if proceed:
        dim_tables, fact_tables, proceed, entry = sort_dim_fact(table_names)
        log = update_log(log, field='steps', step='sort_tables', entry=entry)
        save_log(log, path)

    # check if its an initial load
    if proceed:
        is_initial = check_if_initial_load()
        log = update_log(log, field='steps', step='is_initial', entry=is_initial)
        save_log(log, path)

    if is_initial:
        # Run dimension checks (primary key uniqueness)
        # This cleans 'game', 'player_info', and 'team_info'
        dfs, qrt, proceed, entry = check_dim_pkey(dim_tables, dfs, qrt)
        log = update_log(log, field='steps', step='check_dim_pkey', entry=entry)
        save_log(log, path)

        if proceed:
            # Run fact checks (referential integrity)
            dfs, qrt, proceed, entry = check_fact_ri(fact_tables, dfs, qrt)
            log = update_log(log, field='steps', step='check_fact_ri', entry=entry)
            save_log(log, path)

    
    if not is_initial:
        pass # pass for now, add in when incremental function is added.

    if proceed:
        dfs, qrt, proceed, entry = check_for_negative(dfs, qrt)
        log = update_log(log, field='steps', step='check_for_negative', entry=entry)
        save_log(log, path)

    if proceed:
        proceed, entry = save_qrt_to_bronze(qrt, BRONZE_LAKEHOUSE, schema, pipeline_id, run_id)
        log = update_log(log, field='steps', step='save_qrt', entry=entry)
        save_log(log, path)

        qrt_tables = list(qrt.keys())
        log = update_log(log, field='qrt_tables', entry=qrt_tables)
        save_log(log, path)

    if proceed:
        dfs, proceed, entry = add_audit_columns(dfs, pipeline_id, run_id)
        log = update_log(log, field='steps', step='add_audit_cols', entry=entry)
        save_log(log, path)

    if proceed:
        proceed, entry = load_to_silver(dfs, is_initial)
        log = update_log(log, field='steps', step='load_to_silver', entry=entry)
        save_log(log, path)

        silver_tables = list(dfs.keys())
        log = update_log(log, field='silver_tables', entry=silver_tables)
        save_log(log, path)

        log = update_log(log, field='proceed_to_next_pipeline_step', entry=True)
        save_log(log, path)
        print("--- PIPELINE COMPLETE ---")
    
    if not proceed:
        log = update_log(log, field='proceed_to_next_pipeline_step', entry=False)
        save_log(log, path)
        print("--- PIPELINE INCOMPLETE ---")
    
    log = update_log(log, field='exec_end_time', entry=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    save_log(log, path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
