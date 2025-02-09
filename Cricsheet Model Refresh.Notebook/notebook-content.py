# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2b4cedf2-b4c3-4bd8-a763-af94e7196059",
# META       "default_lakehouse_name": "lh_gold",
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

semantic_model_list_str = "Cricsheet Model" # Pass multiple coma seperated "model1,model2"
master_job_name = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Refresh and Wait for Dataset completion

# CELL ********************

job_category = "Semantic Model Refresh"

# Split the dataset_list string by commas and store the result as a list
semantic_model_list = semantic_model_list_str.split(",")

# Start the dataset refresh and wait for completion
P.refresh_and_wait(dataset_list=semantic_model_list,
                 logging_lakehouse=LAKEHOUSE,
                 logging_schema=LOG_SCHEMA,
                 parent_job_name=master_job_name,
                 job_category=job_category)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
