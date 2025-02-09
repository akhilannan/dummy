# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "384bac83-78c6-40bc-8a72-7a384945fc51",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "6426563a-7af2-4eec-aedd-8bc29a94eb15"
# META     },
# META     "environment": {
# META       "environmentId": "a3f56da8-5f75-4a6c-94ff-2604323eb7af",
# META       "workspaceId": "6426563a-7af2-4eec-aedd-8bc29a94eb15"
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

# # Set Parameter

# PARAMETERS CELL ********************

master_job_name = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Initialize Variables

# CELL ********************

dataset_url = "https://cricsheet.org/downloads/"
dataset_file_name = "all_json"
dataset_file_extn = "zip"
dataset_full_name = dataset_file_name + "." + dataset_file_extn
raw_folder = "cricsheet_raw"
file_format = "json"
download_folder = raw_folder + "/" + dataset_file_extn
extract_folder = raw_folder + "/" + file_format
table_name = "t_cricsheet"
base_url = dataset_url + dataset_full_name
downloaded_file = download_folder + "/" + dataset_full_name
spark_raw_path = U.get_lakehouse_path(LAKEHOUSE, "spark", "Files") + "/" + extract_folder
job_category = "Ingest to Raw"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Check if Cricsheet has new matches added, else Quit

# CELL ********************

J.execute_and_log(
    function=D.compare_row_count,
    table1_lakehouse=LAKEHOUSE,
    table1_schema=STAGING_SCHEMA,
    table1_name=table_name,
    table2_lakehouse=dataset_url,
    log_lakehouse=LAKEHOUSE,
    log_schema=LOG_SCHEMA,
    job_name='Compare Row Count Bronze',
    job_category = job_category,
    parent_job_name=master_job_name
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Download zip file from Cricsheet

# CELL ********************

J.execute_and_log(
    function=L.download_data,
    url=base_url,
    lakehouse = LAKEHOUSE,
    path=download_folder,
    log_lakehouse=LAKEHOUSE,
    log_schema=LOG_SCHEMA,
    job_name='Download Cricsheet Data',
    job_category = job_category,
    parent_job_name=master_job_name
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# # Unzip Files in parallel

# CELL ********************

J.execute_and_log(
    function=L.unzip_parallel,
    lakehouse = LAKEHOUSE,
    zip_relative_path=downloaded_file,
    extract_relative_path=extract_folder,
    file_type = file_format,
    log_lakehouse=LAKEHOUSE,
    log_schema=LOG_SCHEMA,
    job_name='Unzip Files',
    job_category = job_category,
    parent_job_name=master_job_name
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Create t_cricsheet table

# CELL ********************

# Get the full file name from the input
full_file_name = F.input_file_name()

# Split the file name by "/" and get the last element which will be the file name
file_name_array = F.split(full_file_name, "/")
file_name = F.element_at(file_name_array, -1)

# Remove the ".json" extension from the file name
match_file_name = F.regexp_replace(file_name, ".json", "")

# Extract the match id from the file name as an integer
file_match_id = F.regexp_extract(match_file_name, "\d+", 0).cast("int")

# Read the json file into a spark data frame with the specified schema
cricket_df = (
  spark
  .read
  .format(file_format)
  .option("multiline", "true")
  .schema("info string, innings string")
  .load(spark_raw_path)
)

cricket_df = (
    cricket_df
    # Select the following columns from the data frame
    .select(
        # If the match file name starts with "wi_", multiply the match id by -1. This is to distinguish between Women and Mens matches
        F.when(match_file_name.like("wi_%"),file_match_id * -1).otherwise(file_match_id).alias("match_id"),
        F.col("info").alias("match_info"), 
        F.col("innings").alias("match_innings"),
        # Add a new column with the file name as a literal value
        F.lit(file_name).alias("file_name"),
        # Add a new column with the current timestamp as the last update time
        F.current_timestamp().alias("last_update_ts"))
    )
        
# Create or replace a delta table with the data frame
J.execute_and_log(
    function=D.create_or_replace_delta_table,
    df=cricket_df,
    lakehouse_name=LAKEHOUSE,
    schema_name=STAGING_SCHEMA,
    table_name=table_name,
    log_lakehouse=LAKEHOUSE,
    log_schema=LOG_SCHEMA,
    job_name=table_name,
    job_category = job_category,
    parent_job_name=master_job_name
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
