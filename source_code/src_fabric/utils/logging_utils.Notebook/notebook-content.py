# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Dummy notebook

# MARKDOWN ********************

# 1. get latest log if it exists
# 2. if log exists -> initialize a sub log for this notebook -> create a new step in the existing log -> add new sub log to full log
# 3. if log does not exist -> create a new log -> initialize a sub log for this notebook -> create a new step in the new log -> add new sub log to full log
# 4. save log

# CELL ********************

from datetime import datetime, timezone
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def add_to_log(full_log, sub_log, STEP_NAME):
    full_log['steps'][STEP_NAME] = sub_log
    return full_log

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_latest_log(path):
    exists = False
    log = None
    try:
        full_path = f'/lakehouse/default/{path}'
        with open(full_path, 'r') as f:
            log = json.load(f)
        exists = True
    except FileNotFoundError as e:
        print(f'Could not find an existing log file: {str(e)}')
    except Exception as e:
        print(f'Error encountered loading log file: {str(e)}')
    return exists, log

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_new_full_log(pipeline_id, run_id):
    full_log = {
        'pipeline_id': pipeline_id,
        'run_id': run_id,
        'steps': {}
    }
    print("creating a new log...")
    return full_log

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def add_new_step_log(log, step_name):
    log['steps'][step_name] = {}
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

def init_sub_log_for_validate_format_and_load_to_bronze():
    # initialize logging parameters for bronze validation and loading function
    sub_log = {
        'exec_start_time': datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        'exec_end_time': None,
        'files_all': None,
        'files_data_validated': [],
        'files_loaded': [],
        'files_failed': [],
        'files_quarantined': [],
        'errors': {
            'unexpected_filename': [],
            'read_csv_error': [],
            'schema_undefined': [],
            'load_to_table_failure': []
        }
    }
    return sub_log

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def initialize_log_for_check_load_bronze():
    # initialize logging parameters for bronze checking function
    log = {
        'exec_start_time': datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        'exec_end_time': None,
        'tables_all': None,
        'tables_confirmed_loaded': [],
        'tables_quarantined': [],
        'errors': {
            'no_data_in_tables': [],
        }
    }
    return log

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_or_add_to_log(pipeline_id, run_id, path, step_name, sub_log_func):
    exists, log = get_latest_log(path)
    if exists:
        full_log = add_new_step_log(log, STEP_NAME)
        sub_log = sub_log_func()
        full_log = add_to_log(full_log, sub_log, STEP_NAME)
    else:
        full_log = create_new_full_log(pipeline_id, run_id)
        sub_log = sub_log_func()
        full_log = add_to_log(full_log, sub_log, STEP_NAME)
    return full_log

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_to_log(full_log, field, entry, error_type=None):
    try:
        field_to_update = full_log['steps'][STEP_NAME][field]

        # for other fields
        if error_type == None:
            if field_to_update == None:
                field_to_update = entry
            else:
                field_to_update.append(entry)

        # for errors field
        else:
            if field_to_update == None:
                field_to_update = entry
            else:
                field_to_update[error_type].append(entry)
        full_log['steps'][STEP_NAME][field] = field_to_update
        print(f'    log updated with {field} entry')
        return full_log
        
    except Exception as e:
        print(f'unable to write to log: {str(e)}')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
