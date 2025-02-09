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

# # Import Required Libraries

# CELL ********************

import fabric_utils as U
import file_operations as L
import job_operations as J
import delta_table_operations as D
import powerbi_operations as P
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Set Constants

# CELL ********************

LAKEHOUSE = "lh_cricsheet"
STAGING_SCHEMA = "staging"
REPORTING_SCHEMA = "reporting"
LOG_SCHEMA = "logs"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for schema in [STAGING_SCHEMA, REPORTING_SCHEMA, LOG_SCHEMA]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
