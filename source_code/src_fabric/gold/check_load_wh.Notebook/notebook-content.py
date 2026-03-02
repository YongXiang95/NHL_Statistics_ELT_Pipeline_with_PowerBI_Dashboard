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
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Import Functions

# CELL ********************

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants
import json
from datetime import datetime, timezone
from pyspark.sql.utils import AnalysisException

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

STEP_NAME = 'check_load_gold'
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

def check_warehouse_table_loaded(table_fqn: str):
    """
    Check if a Fabric Warehouse table/view exists and has rows.

    Returns:
        (valid: bool, msg: str, row_count: int|None)
    """
    try:
        df = spark.read.synapsesql(table_fqn)

        # Fast "has at least 1 row" check
        if df.limit(1).count() == 0:
            return False, f"{table_fqn} is empty", 0

        # Full row count (audit)
        row_count = df.count()
        return True, f"{table_fqn} loaded: {row_count} rows", int(row_count)

    except Exception as e:
        return False, f"Error reading/counting {table_fqn}: {str(e)}", None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables = [
    "nhl_gold_wh.gold.fact_goalie_stats_per_60",
    "nhl_gold_wh.gold.fact_skater_stats_per_60",
    # "NHL_Warehouse.dbo.dim_player",
    # "NHL_Warehouse.dbo.dim_team",
]

results = []

for t in tables:
    valid, msg, row_count = check_warehouse_table_loaded(t)

    status = "PASS" if valid else "FAIL"
    results.append({
        "table": t,
        "status": status,
        "message": msg,
        "row_count": row_count
    })

    print(f"[{status}] {t} → {msg}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def init_sub_log_for_check_load_gold():
    return {
        "tables_all": [],
        "tables_confirmed_loaded": [],
        "tables_quarantined": [],
        "errors": {
            "no_data_in_tables": [],
            "others": []
        },
        "exec_start_time": None,
        "exec_end_time": None
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def safe_write_to_log(log, **kwargs):
    """
    Calls write_to_log but never returns None.
    If write_to_log fails, returns the original log unchanged.
    """
    try:
        updated = write_to_log(log, **kwargs)
        return updated if updated is not None else log
    except Exception as e:
        print(f"unable to write to log: {e}")
        return log

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def utc_now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def start_execution_log(step_log: dict):
    if step_log is not None and not step_log.get("exec_start_time"):
        step_log["exec_start_time"] = utc_now_iso()

def finalize_execution_log(step_log: dict, status: str = "COMPLETED"):
    if step_log is None:
        return
    if not step_log.get("exec_end_time"):
        step_log["exec_end_time"] = utc_now_iso()
    step_log["status"] = status

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run logging_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if __name__ == "__main__":

    STEP_NAME = "check_load_gold"

    # Create or load log object
    log = create_or_add_to_log(
        pipeline_id,
        run_id,
        path,
        STEP_NAME,
        init_sub_log_for_check_load_gold
    )

    # SET exec_start_time (right after log creation, before try)
    try:
        if log is not None and "steps" in log and STEP_NAME in log["steps"]:
            # only set if empty
            if not log["steps"][STEP_NAME].get("exec_start_time"):
                log["steps"][STEP_NAME]["exec_start_time"] = utc_now_iso()
            log["steps"][STEP_NAME]["status"] = "RUNNING"
        save_log(log, path)
    except Exception as e:
        print(f"Error setting exec_start_time: {e}")
        # still try to persist whatever log exists
        try:
            save_log(log, path)
        except:
            pass

    try:
        # Define Warehouse tables explicitly (FQNs)
        warehouse_tables = [
            "nhl_gold_wh.gold.fact_goalie_stats_per_60",
            "nhl_gold_wh.gold.fact_skater_stats_per_60",
        ]

        log = safe_write_to_log(log, field="tables_all", entry=warehouse_tables)
        save_log(log, path)

        for table_fqn in warehouse_tables:
            print(f"checking warehouse table: {table_fqn}...")

            valid, msg, row_count = check_warehouse_table_loaded(table_fqn)

            if valid:
                log = safe_write_to_log(
                    log,
                    field="tables_confirmed_loaded",
                    entry={"table": table_fqn, "row_count": row_count}
                )
                save_log(log, path)
                print(f"    {msg}")

            else:
                log = safe_write_to_log(log, field="tables_quarantined", entry=table_fqn)
                save_log(log, path)
                print(f"    {msg}")

                # classify error type
                error_type = "no_data_in_tables" if msg == f"{table_fqn} is empty" else "others"

                log = safe_write_to_log(
                    log,
                    field="errors",
                    error_type=error_type,
                    entry={"table": table_fqn, "error": msg}
                )
                save_log(log, path)

        print(f"Number of warehouse tables checked: {len(warehouse_tables)}")

    except Exception as e:
        # mark failed (optional but recommended)
        try:
            if log is not None and "steps" in log and STEP_NAME in log["steps"]:
                log["steps"][STEP_NAME]["status"] = "FAILED"
        except:
            pass

        log = safe_write_to_log(
            log,
            field="errors",
            error_type="others",
            entry={"tables": "all", "error": str(e)}
        )
        save_log(log, path)
        print(f"    Error: {e}")

    finally:
        # Guard so finalize never crashes
        try:
            if log is not None and "steps" in log and STEP_NAME in log["steps"]:
                # finalize_execution_log should set exec_end_time (and maybe status)
                finalize_execution_log(log["steps"][STEP_NAME])
                save_log(log, path)
            else:
                print("Skipping finalize_execution_log: log is None or missing steps/step.")
        except Exception as e:
            print(f"Error finalizing log: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
