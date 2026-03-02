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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from datetime import datetime, timezone
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# parameter variables (parameter cell)
exec_start_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
pipeline_id = 'manual'
run_id = 'manual'
file = "game_teams_stats.csv"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# non-parameter variables
STEP_NAME = 'validate_format'
LOG_FOLDER = 'Files/silver_logs'
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
    path = f"{LOG_FOLDER}/{date_executed}/{manual_run_folder}/silver_transformation/{file}/log_{exec_start_time}.txt"

else:
    path = f"{LOG_FOLDER}/{date_executed}/{pipeline_id}/{run_id}/silver_transformation/{file}/log_{exec_start_time}.txt"

path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## getting the constants from the constants notebook

# CELL ********************

%run constants

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Logging utils

# CELL ********************

def init_sub_log_for_silver_transformation(exec_start_time):
    # initialize logging parameters for bronze validation and loading function
    log = {
        'exec_start_time': exec_start_time,
        'exec_end_time': None,
        'file': None,
        'proceed_to_next_pipeline_step': False,
        'steps': {
            'expected_file': None,
            'get_schema': None,
            'read_csv': None ,
            'remove_dups': None,
            'check_headers': None,
            'check_data_types': None,
            'add_id': None,
            'create_new_staging_schema': None,
            'load_to_staging': None,
            'check_if_table_loaded': None, 
            'move_to_quarantine': None,
            'move_to_processed': None,
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

# ## Utils for this notebook

# CELL ********************

# check if file is expected
def check_if_file_expected(file, EXPECTED_FILES_LIST):
    print('checking if file name is expected...')
    proceed = True
    try:
        if file in EXPECTED_FILES_LIST:
            entry = 'success'
        else:
            proceed = False
            entry = 'failed'
    except Exception as e:
        entry = f'failed: {str(e)}'
    print(entry)
    return entry, proceed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# creating the table name
def get_table_name_from_file(file):
    table = file.rsplit(".", 1)[0]
    return table

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# creating a pyspark schema for the table based on their expected headers and data types
def get_schema(table, TABLE_STRUCTURE):
    print('checking if schema exists for this table')
    fields = []
    proceed = True
    try:
        for col, data_type in TABLE_STRUCTURE[table].items():
            fields.append(StructField(col, data_type, True))
        schema = StructType(fields)
        entry = 'success'
    except Exception as e:
        schema = None
        proceed = False
        entry = f'failed: {str(e)}'
    print(entry)
    return schema, entry, proceed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# function read csv to dataframe
def read_csv(file, schema, LANDING_FOLDER):
    print(f'Trying to read {file} with the expected schema...')
    proceed = True
    try:
        df = (spark.read
            .format("csv")
            .option("header", True)
            .option("nullValue", "NA")
            .option("nanValue", "NA")
            .option("mode", "FAILFAST") # <--- CRITICAL: Fails immediately if schema is wrong
            .schema(schema)
            .load(f"{LANDING_FOLDER}/{file}")
        )
        
        if df.isEmpty():
            entry = 'failed: File is empty'
            df = None
            proceed = False
        else:
            entry = 'success'
            
    except Exception as e:
        df = None
        # Handle the specific FAILFAST error if columns don't match
        entry = f'failed: Schema mismatch or corrupt file. {type(e).__name__}'
        proceed = False
    
    print(entry)
    return df, entry, proceed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# function to check dataframe headers against expected headers
def correct_headers(file, table, df):
    print('checking headers...')
    proceed = True
    try:
        expected_headers = list(TABLE_STRUCTURE[table].keys())
        table_headers = df.columns
        if expected_headers != table_headers:
            proceed = False
            entry = 'failed'
        else:
            entry = 'success'
    except Exception as e:
        entry = f'failed: {type(e).__name__}'
        proceed = False
    print(entry)
    return entry, proceed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# function to check dataframe datatypes against expected data types
def correct_data_types(file, table, df):
    print('checking column data types')
    proceed = True
    actual_data_types = {}
    try:
        for field in df.schema.fields:
            actual_data_types[field.name] = field.dataType
        expected_data_types = TABLE_STRUCTURE[table]
        if actual_data_types == expected_data_types:
            entry = "success"
        else:
            proceed = False
            entry = 'failed'
    except Exception as e:
        entry = f'failed: {type(e).__name__}'
        proceed = False
    print(entry)
    return entry, proceed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# function to remove duplicate rows
def remove_row_duplicates(df):
    print('removing row duplicates...')
    proceed = True
    try:
        df = df.dropDuplicates()
        entry = 'success'
    except Exception as e:
        entry = f'failed: {type(e).__name__}'
        proceed = False
    print(entry)
    return df, entry, proceed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def add_id_to_df(df):
    print('adding a monotonically increasing id...')
    proceed = True
    try:
        df = df.withColumn('index', F.monotonically_increasing_id())
        entry = 'success'
    except Exception as e:
        entry = f'failed: {type(e).__name__}'
        proceed = False
    print(entry)
    return df, entry, proceed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# function to move file to quarantine
def move_to_quarantine(file, LANDING_FOLDER, QUARANTINE_FOLDER):
    print('moving file to quarantine...')
    proceed = True
    try:
        mssparkutils.fs.mv(
            f'{LANDING_FOLDER}/{file}',
            f'{QUARANTINE_FOLDER}/{file}')
        entry = 'success'
    except Exception as e:
        entry = f'failed: {type(e).__name__}'
        proceed = False
    print(entry)
    return entry, proceed


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# function to move file to processed
def move_to_processed(file, LANDING_FOLDER, PROCESSED_FOLDER):
    print('moving file to processed...')
    proceed = True
    try:
        mssparkutils.fs.mv(
        f'{LANDING_FOLDER}/{file}',
        f'{PROCESSED_FOLDER}/{file}')
        entry = 'success'
    except Exception as e:
        entry = f'failed: {type(e).__name__}'
        proceed = False
    print(entry)
    return entry, proceed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# create a staging schema with timestamp
def create_new_schema(exec_start_time):
    print('creating a new staging schema if not exists...')
    proceed = True
    try:
        clean_timestamp = exec_start_time.replace("-", "").replace(":", "").replace("T", "_")[:13]
        schema_name = f"staging_{clean_timestamp}"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        entry = 'success'
    except Exception as e:
        entry = f'failed: {type(e).__name__}'
        proceed = False
        schema_name = None
    print(entry)
    return schema_name, entry, proceed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# load table to staging schema
def load_to_staging(table, df, schema_name):
    print("loading table to staging schema...")
    proceed = True
    try:
        df.write\
            .mode("overwrite")\
            .format("delta")\
            .saveAsTable(f"{schema_name}.{table}")
        entry = 'success'
    except Exception as e:
        entry = f'failed: {type(e).__name__}'
        proceed = False
    print(entry)
    return entry, proceed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# check if table loaded into staging
def check_table_loaded(table, schema_name):
    print('checking if table loaded into staging schema...')
    proceed = True
    try:
        df = spark.table(f"{schema_name}.{table}")
        row_count= df.count()
        if row_count > 0:
            entry = f"success. {table} loaded: {row_count} rows"
        else:
            proceed = False
            entry = f"failed. {table} is empty"
    except Exception as e:
        proceed = False
        entry = f"Error counting rows: {type(e).__name__}"
    print(entry)
    return entry, proceed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Main function

# CELL ********************

if __name__ == "__main__":
    log = init_sub_log_for_silver_transformation(exec_start_time)
    log = update_log(log, field='file', entry=file)
    
    try:
        # perform checks
        table = get_table_name_from_file(file)
        entry, proceed = check_if_file_expected(file, EXPECTED_FILES_LIST)
        log = update_log(log, field='steps', step='expected_file', entry=entry)

        if proceed:
            schema, entry, proceed = get_schema(table, TABLE_STRUCTURE)
            log = update_log(log, field='steps', step='get_schema', entry=entry)
            save_log(log, path)
        
        if proceed:
            df, entry, proceed = read_csv(file, schema, LANDING_FOLDER)
            log = update_log(log, field='steps', step='read_csv', entry=entry)
            save_log(log, path)
        
        if proceed:
            # Transformation logic stays in Spark execution plan (not yet written)
            df, entry, proceed = remove_row_duplicates(df)
            log = update_log(log, field='steps', step='remove_dups', entry=entry)
            save_log(log, path)

            entry, proceed = correct_headers(file, table, df)
            log = update_log(log, field='steps', step='check_headers', entry=entry)
            save_log(log, path)

            entry, proceed = correct_data_types(file, table, df)
            log = update_log(log, field='steps', step='check_data_types', entry=entry)
            save_log(log, path)

            df, entry, proceed = add_id_to_df(df)
            log = update_log(log, field='steps', step='add_id', entry=entry)
            save_log(log, path)

        # After passing the checks, loading into staging schema
        if proceed:
            # 1. Create Schema
            schema_name, entry, proceed = create_new_schema(exec_start_time)
            log = update_log(log, field='steps', step='create_new_staging_schema', entry=entry)
            save_log(log, path)
            log = update_log(log, field='staging_schema_name', entry=schema_name)
            save_log(log, path)
            
            if proceed:
                # 2. Persist data to staging schema
                entry, proceed = load_to_staging(table, df, schema_name)
                log = update_log(log, field='steps', step='load_to_staging', entry=entry)
                save_log(log, path)
            
            if proceed:
                # 3. Check if table loaded in staging schema
                entry, proceed = check_table_loaded(table, schema_name)
                log = update_log(log, field='steps', step='check_if_table_loaded', entry=entry)
                save_log(log, path)
            
            if proceed:
                # 4. Final Physical Action: Move the file
                entry, proceed = move_to_processed(file, LANDING_FOLDER, PROCESSED_FOLDER)
                log = update_log(log, field='steps', step='move_to_processed', entry=entry)
                save_log(log, path)
                
                # If we reached here, the entire transaction is a success
                log = update_log(log, field='proceed_to_next_pipeline_step', entry=proceed)
                save_log(log, path)
        
        # If fail any of the checks, move file to quarantine
        if not proceed:
            entry, proceed = move_to_quarantine(file, LANDING_FOLDER, QUARANTINE_FOLDER)
            log = update_log(log, field='steps', step='move_to_quarantine', entry=entry)
            save_log(log, path)
            log = update_log(log, field='proceed_to_next_pipeline_step', entry=proceed)
            save_log(log, path)

    except Exception as e:
        # If the cluster crashes or a write fails, the file remains in LANDING
        proceed = False
        log = update_log(log, field='proceed_to_next_pipeline_step', entry=proceed)
        save_log(log, path)
        print(f"Atomic Failure for {file}: {type(e).__name__}")
        # Re-raise so the Fabric Pipeline shows a failure
        raise e

    finally:
        log = update_log(log, field='exec_end_time', entry=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
        save_log(log, path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Passing parameters for next pipeline step

# CELL ********************

mssparkutils.notebook.exit(schema_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
