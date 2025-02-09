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

# # Import Libraries

# CELL ********************

%run "/Cricsheet Initialize"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Set Parameters

# PARAMETERS CELL ********************

semantic_model_list = "Cricsheet Model,Data Load Model" # Can pass multiple coma seperated "model1,model2"
master_job_name = "Cricsheet Master"
optimize_and_vacuum = True # Change to False to disable optimize and vaccum
items_to_optimize_vacuum = "All" # All corresponds to all tables in the current Lakehouse

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Create DAG

# CELL ********************

DAG = {
    "activities": [
        {
            "name": "Cricsheet Ingest Data", # activity name, must be unique
            "path": "Cricsheet Ingest Data", # notebook path
            "timeoutPerCellInSeconds": 1800, # max timeout for each cell, default to 90 seconds
            "args": {"master_job_name": master_job_name} # notebook parameters
        },
        {
            "name": "Cricsheet Build Facts and Dimensions",
            "path": "Cricsheet Build Facts and Dimensions",
            "timeoutPerCellInSeconds": 1800,
            "dependencies": ["Cricsheet Ingest Data"],
            "args": {"master_job_name": master_job_name}
        },
        {
            "name": "Cricsheet Optimize and Vacuum",
            "path": "Cricsheet Optimize and Vacuum",
            "timeoutPerCellInSeconds": 1800,
            "dependencies": ["Cricsheet Build Facts and Dimensions"],
            "args": {"optimize_and_vacuum": optimize_and_vacuum, "master_job_name": master_job_name, "items_to_optimize_vacuum": items_to_optimize_vacuum}
        },
        {
            "name": "Cricsheet Model Refresh",
            "path": "Cricsheet Model Refresh",
            "timeoutPerCellInSeconds": 180,
            "dependencies": ["Cricsheet Optimize and Vacuum"],
            "args": {"semantic_model_list_str": semantic_model_list, "master_job_name": master_job_name}
        }
    ]
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Execute DAG

# CELL ********************

J.execute_and_log(function=J.execute_dag,
                log_lakehouse=LAKEHOUSE, 
                log_schema=LOG_SCHEMA,
                job_name=master_job_name,
                job_category= "Orchestrator",
                dag=DAG)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
