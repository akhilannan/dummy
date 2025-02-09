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

master_job_name = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Set Variables

# CELL ********************

# Define the schema for the team_player column, which is a map of team names to player names
team_player_schema = T.MapType(T.StringType(), T.ArrayType(T.StringType()))

# Define the schema for the array columns, which are arrays of strings
array_schema = T.ArrayType(T.StringType())

# Unpack the paths for the six tables to be created from the clean table path
(
    match_table_path,
    team_table_path,
    player_table_path,
    date_table_path,
    team_players_table_path,
    deliveries_table_path,
    cricsheet_table_path,
) = (
    list([LAKEHOUSE] + [tbl] + [REPORTING_SCHEMA] for tbl in [
        "t_dim_match",
        "t_dim_team",
        "t_dim_player",
        "t_dim_date",
        "t_fact_team_players",
        "t_fact_deliveries",
    ])
    + [
        [LAKEHOUSE] + ["t_cricsheet"] + [STAGING_SCHEMA]
    ]
)

# Set job category
job_category = "Build Star Schema"

# Read the cricsheet table as a delta table
raw_df = D.read_delta_table(*cricsheet_table_path)

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
    log_lakehouse=LAKEHOUSE,
    table1_lakehouse=match_table_path[0],
    table1_name=match_table_path[1],
    table1_schema=match_table_path[2],
    table2_lakehouse=cricsheet_table_path[0],
    table2_name=cricsheet_table_path[1],
    table2_schema=cricsheet_table_path[2],
    job_name='Compare Row Count Gold',
    job_category = job_category,
    parent_job_name=master_job_name
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Create Team Player Dataframe and Cache it

# CELL ********************

tpd = (
    raw_df
    # Select the match_id column and explode the players array from the match_info column
    .select("match_id", 
            F.explode(F.from_json(F.get_json_object("match_info",'$.players'), team_player_schema)))
    # Select the match_id column, rename the key column as team, and explode the value array as player_name
    .select("match_id",
            F.col("key").alias("team"),
            F.explode("value").alias("player_name"))
    # Cache the result for faster access
    .cache()
)
# Assign an alias to the temporary table
tpd = tpd.alias("tpd")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Create t_dim_player

# CELL ********************

pdf = (
    tpd
    .select("player_name")
    .distinct() # Get distinct player name
    .select(F.monotonically_increasing_id().alias("player_id"), "*") # Add a primary key column player_id with increasing IDs
    .union(spark.createDataFrame([[-1, "Extras"]])) # Append a row for extras with ID -1
)

# Write the DataFrame to a Delta table
# Create a new table if it does not exist, or append the new payers to the existing table
J.execute_and_log(
    function=D.create_or_insert_table,
    df=pdf,
    lakehouse_name=player_table_path[0],
    table_name=player_table_path[1],
    schema_name=player_table_path[2],
    primary_key = "player_id",
    merge_key = "player_name",
    log_lakehouse=LAKEHOUSE,
    job_name=player_table_path[1],
    job_category = job_category,
    parent_job_name=master_job_name
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Create t_dim_team

# CELL ********************

tdf = (
    tpd
    .select("team")
    .distinct() # Get distinct team
    .select(F.monotonically_increasing_id().alias("team_id"), "*") # Add a primary key column team_id with increasing IDs
)

# Write the DataFrame to a Delta table
# Create a new table if it does not exist, or append the new teams to the existing table
J.execute_and_log(
    function=D.create_or_insert_table,
    df=tdf,
    lakehouse_name=team_table_path[0],
    table_name=team_table_path[1],
    schema_name=team_table_path[2],
    primary_key = "team_id",
    merge_key = "team",
    log_lakehouse=LAKEHOUSE,
    job_name=team_table_path[1],
    job_category = job_category,
    parent_job_name=master_job_name
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Create Dataframe aliases and functions

# CELL ********************

# Read the player table from the delta table and assign it to pdf
pdf = D.read_delta_table(*player_table_path).alias("pdf")

# Create aliases for the player table based on different roles
pom = pdf.alias("pom") # Player of the match
bat = pdf.alias("bat") # Batsman
bwl = pdf.alias("bwl") # Bowler
fld = pdf.alias("fld") # Fielder
nsr = pdf.alias("nsr") # Non Striker
pot = pdf.alias("pot") # Player Out

# Read the team table from the delta table and assign it to tdf
tdf = D.read_delta_table(*team_table_path).alias("tdf")

# Create aliases for the team table based on different metrics
mwn = tdf.alias("mwn") # Match Winner
elm = tdf.alias("elm") # Match Winner by Eliminator
twn = tdf.alias("twn") # Toss winner
fbt = tdf.alias("fbt") # First batting team
flt = tdf.alias("flt") # First fielding team

def first_team(toss_decision, team_player_schema):
  """
  Returns the name of the first team to play based on the toss decision.

  Parameters:
  toss_decision (str): The decision of the toss winner to bat or field.

  Returns:
  str: The name of the first team to play.
  """
  # Extract the team names from the JSON column "team_players" using the schema "team_player_schema"
  teams = F.map_keys(F.from_json(F.col("team_players"), team_player_schema))
  # Assign the first and second team names to variables
  first_team_name = teams[0]
  second_team_name =  teams[1]
  # Return the name of the first team to play based on the toss decision and the toss winner
  return (F.when(F.col("toss_decision") == toss_decision, F.col("toss_winner"))
          .when((F.col("toss_winner") == first_team_name), second_team_name)
          .otherwise(first_team_name))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Create t_dim_match

# CELL ********************

# Define a list of fields to extract from the match_info column, which is a JSON string
mdf_json_fields = [
    ["dates[0]", "date", "match_date"],
    ["gender", "string", "match_gender"],
    ["season", "string", "season"],
    ["event.name", "string", "event_name"],
    ["event.group", "string", "event_group"],
    ["event.match_number", "int", "event_match_number"],
    ["city", "string", "city"],
    ["venue", "string", "venue"],
    ["officials.umpires", "string", "umpires"],
    ["team_type","string", "team_type"],
    ["match_type", "string", "match_type"],
    ["outcome.winner", "string", "match_winner"],
    ["outcome.result", "string", "match_result"],
    ["outcome.by.runs", "string", "match_won_by_runs"],
    ["outcome.by.wickets", "string", "match_won_by_wickets"],
    ["outcome.by.innings", "string", "match_won_by_innings"],
    ["outcome.eliminator", "string", "match_winner_eliminator"],
    ["outcome.method", "string", "match_result_method"],
    ["toss.winner", "string", "toss_winner"],
    ["toss.decision", "string", "toss_decision"],
    ["player_of_match[0]", "string", "player_of_match"],
    ["players", "string", "team_players"]
]

# Create a list of column expressions to select from the raw_df dataframe, using the get_json_object function to parse the match_info column
mdf_json_select_lst = [
    F.get_json_object("match_info", "$." + json[0]).cast(json[1]).alias(json[2])
    for json in mdf_json_fields
]

# Create a new dataframe by selecting the match_id and the columns from the mdf_json_select_lst list
mdf = (
  raw_df
  .select("match_id",
          *mdf_json_select_lst)
  .select("*", 
          first_team('bat', team_player_schema).alias("first_bat"),
          first_team('field', team_player_schema).alias("first_field"))
  .alias("mdf")
)

# Join the mdf dataframe with other dataframes based on the team and player names, using left outer join
mdf = ( 
  mdf
  .join(twn, twn.team == mdf.toss_winner, 'leftouter' )
  .join(mwn, mwn.team == mdf.match_winner, 'leftouter' )
  .join(elm, elm.team == mdf.match_winner_eliminator, 'leftouter' )
  .join(pom, pom.player_name == mdf.player_of_match, 'leftouter' )
  .join(fbt, fbt.team == mdf.first_bat, 'leftouter' )
  .join(flt, flt.team == mdf.first_field, 'leftouter' )
  .select("mdf.match_id",
          "mdf.match_date",
          "mdf.match_gender",
          "mdf.season",
          "mdf.event_name",
          "mdf.event_group",
          "mdf.event_match_number",
          "mdf.city",
          "mdf.venue",
          "mdf.team_type",
          "mdf.match_type",
          "mdf.match_result",
          "mdf.match_won_by_runs",
          "mdf.match_won_by_wickets",
          "mdf.match_won_by_innings",
          "mdf.match_result_method",
          F.concat_ws(", ", F.from_json("umpires", array_schema)).alias("umpires"), # Concatenate the umpires array into a string, separated by commas
          F.coalesce("mwn.team_id", "elm.team_id").alias("match_winner_id"), # Get the match winner from Eliminator in case Match Winner is empty (eg. tied matches)
          F.col("twn.team_id").alias("toss_winner_id"),
          "mdf.toss_decision",
          F.col("pom.player_id").alias("player_of_match_id"),
          F.col("fbt.team_id").alias("first_bat_id"),
          F.col("flt.team_id").alias("first_field_id"))
)

# Create or replace a delta table using the mdf dataframe
J.execute_and_log(
    function=D.create_or_replace_delta_table,
    df=mdf,
    lakehouse_name=match_table_path[0],
    table_name=match_table_path[1],
    schema_name=match_table_path[2],
    log_lakehouse=LAKEHOUSE,
    job_name=match_table_path[1],
    job_category = job_category,
    parent_job_name=master_job_name
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Create t_dim_date

# CELL ********************

# Read the delta table and assign it an alias
mdf = D.read_delta_table(*match_table_path).alias("mdf")

# Create a new dataframe with the start and end dates of each year in the match table
dte = (
    mdf
    # Select the minimum and maximum match dates and truncate them to the year level
    .select(F.trunc(F.min("match_date"), "YY").alias("start_date"),
            F.add_months(F.trunc(F.max("match_date"), "YY")-1,12).alias("end_date"))
    # Generate a sequence of dates from the start to the end date of each year
    .select(F.explode(F.sequence("start_date", "end_date")).alias("date"))
    # Extract the year, quarter, month number and month name from each date
    .select("date",
            F.year("date").alias("year"),
            F.concat(F.lit("Q"), F.quarter("date")).alias("quarter"),
            F.month("date").alias("month_number"),
            F.date_format("date", "MMM").alias("month"))
)

# Create or replace a delta table with the date dataframe at the given path
J.execute_and_log(
    function=D.create_or_replace_delta_table,
    df=dte,
    lakehouse_name=date_table_path[0],
    table_name=date_table_path[1],
    schema_name=date_table_path[2],
    log_lakehouse=LAKEHOUSE,
    job_name=date_table_path[1],
    job_category = job_category,
    parent_job_name=master_job_name
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Create t_fact_team_players

# CELL ********************

tpl = (
  tpd
  .join(tdf, tdf.team == tpd.team, 'inner' )
  .join(pdf, pdf.player_name == tpd.player_name, 'inner' )
  .join(mdf, mdf.match_id == tpd.match_id, 'inner')
  # Add all relevant foreign keys based on the joined tables
  .select("tpd.match_id", 
          "tdf.team_id", 
          "pdf.player_id",
          "mdf.match_date",
          "mdf.match_winner_id",
          "mdf.toss_winner_id",
          "mdf.player_of_match_id",
          "mdf.first_bat_id",
          "mdf.first_field_id")
)

# Create or replace a Delta table using the tpl DataFrame
J.execute_and_log(
    function=D.create_or_replace_delta_table,
    df=tpl,
    lakehouse_name=team_players_table_path[0],
    table_name=team_players_table_path[1],
    schema_name=team_players_table_path[2],
    log_lakehouse=LAKEHOUSE,
    job_name=team_players_table_path[1],
    job_category = job_category,
    parent_job_name=master_job_name
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Create t_fact_deliveries

# CELL ********************

# Define a list of fields to extract from the JSON data
dlv_json_fields = [
    ["batter", "string", "batter_name"],
    ["bowler", "string", "bowler_name"],
    ["extras.byes", "int", "byes"],
    ["extras.legbyes", "int", "leg_byes"],
    ["extras.noballs", "int", "no_balls"],
    ["extras.wides", "int", "wides"],
    ["runs.batter", "int", "batter_runs"],
    ["runs.total", "int", "total_runs"],
    ["non_striker", "string", "non_striker_name"],
    ["wickets[0].kind", "string", "wicket_kind"],
    ["wickets[0].player_out", "string", "player_out"],
    ["wickets[0].fielders[0].name", "string", "fielder_name"]
]

# Create a list of expressions to select the fields from the JSON data and cast them to the appropriate data types
dlv_json_select_lst = [
    F.get_json_object("col", "$." + json[0]).cast(json[1]).alias(json[2])
    for json in dlv_json_fields
]

# Read the raw data frame
dlv = (
  raw_df
  .repartition(200) # Repartition the data frame to 200 partitions for better performance
  .select("match_id",
          F.posexplode(F.from_json("match_innings", array_schema))) # Explode the match_innings array into rows
  .select("match_id",
          (F.col("pos") + 1).alias("innings"), # Add 1 to the position to get the innings number
          F.get_json_object("col",'$.team').alias("team"),  # Get the team name from the JSON object
          F.posexplode(F.from_json(F.get_json_object("col",'$.overs'), array_schema))) # Explode the overs array into rows
  .select("match_id",
          "innings",
          "team",
          (F.col("pos") + 1).alias("overs"), # Add 1 to the position to get the over number
          F.posexplode(F.from_json(F.get_json_object("col",'$.deliveries'), array_schema))) # Explode the deliveries array into rows
  .select("*",
          *dlv_json_select_lst) # Select all the columns and the extracted fields from the JSON data
)

# Create an alias for the data frame
dlv = dlv.alias("dlv")

# Define a window specification to partition by match_id and order by team_id
window_spec = Window.partitionBy("match_id").orderBy("team_id")

# Read fact team players
tpl = (
  D.read_delta_table(*team_players_table_path)
  .select("match_id", "team_id")
  .distinct()
  .withColumn("next_team_id", F.coalesce(F.lead("team_id", 1).over(window_spec), F.lag("team_id", 1).over(window_spec))) # Create a new column with the next team_id in the same match using the window function
  .alias("tpl")
)

# Join the data frames to get the batting and bowling team ids, and the player ids for each delivery
dlv = (
        dlv
        .join(mdf, mdf.match_id == dlv.match_id, 'inner')
        .join(tdf, tdf.team == dlv.team, 'inner')
        .join(tpl, [tpl.team_id == tdf.team_id, tpl.match_id == dlv.match_id], 'inner')
        .join(bat, bat.player_name == dlv.batter_name, 'leftouter')
        .join(bwl, bwl.player_name == dlv.bowler_name, 'leftouter')
        .join(fld, fld.player_name == dlv.fielder_name, 'leftouter')
        .join(pot, pot.player_name == dlv.player_out, 'leftouter')
        .join(nsr, nsr.player_name == dlv.non_striker_name, 'leftouter')
        .select("dlv.match_id",
                "mdf.match_date",
                F.col("tdf.team_id").alias("batting_team_id"),
                F.col("tpl.next_team_id").alias("bowling_team_id"),
                "dlv.innings",
                "dlv.overs",
                (F.col("dlv.pos") + 1).alias("balls"),
                F.col("bat.player_id").alias("batter_id"),
                F.col("nsr.player_id").alias("non_striker_id"),
                F.col("bwl.player_id").alias("bowler_id"),
                "dlv.byes",
                "dlv.leg_byes",
                "dlv.no_balls",
                "dlv.wides",
                "dlv.batter_runs",
                "dlv.total_runs",
                "dlv.wicket_kind",
                F.col("pot.player_id").alias("player_out_id"),
                F.col("fld.player_id").alias("fielder_id"))   
)

# Create or replace the delta table with the deliveries data
J.execute_and_log(
    function=D.create_or_replace_delta_table,
    df=dlv,
    lakehouse_name=deliveries_table_path[0],
    table_name=deliveries_table_path[1],
    schema_name=deliveries_table_path[2],
    log_lakehouse=LAKEHOUSE,
    job_name=deliveries_table_path[1],
    job_category = job_category,
    parent_job_name=master_job_name
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
