# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "649a3e9d-78e2-4e64-940a-997b6e3d62ff",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "8055f488-6591-481e-b384-c01054ed014c"
# META     },
# META     "environment": {
# META       "environmentId": "42c57194-1c26-4a76-b1ac-5cefd0660a3e",
# META       "workspaceId": "8055f488-6591-481e-b384-c01054ed014c"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Set Parameters

# PARAMETERS CELL ********************

items_to_optimize_vacuum = "All" # "All"/None means all schema/tables in the current lakehouse, example usage: "Schema1:Table1,Table2|Schema2"
optimize_and_vacuum = True
master_job_name = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Check if optimize_and_vacuum flag is set, if not Exit notebook

# CELL ********************

if not optimize_and_vacuum:
    notebookutils.notebook.exit("optimize_and_vacuum flag not set")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Import Libraries

# CELL ********************

%run "/Cricsheet Initialize"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Execute Optimize Vacuum

# CELL ********************

J.execute_and_log(
    function=D.process_delta_tables_maintenance,
    schema_table_map=items_to_optimize_vacuum,
    retain_hours=None,
    log_lakehouse=LAKEHOUSE,
    log_schema=LOG_SCHEMA,
    job_name=items_to_optimize_vacuum,
    job_category="Optimize and Vacuum",
    parent_job_name=master_job_name,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
