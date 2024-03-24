import pandas as pd
import requests
import json
import Functions.fpl_functions as fpl
import Functions.sql_functions as sql
import __main__
import dotenv
import os


##### CONFIG #####
# Loading the config json file for the script
print("Reading config file...")

try:
    with open(os.path.join(os.path.dirname(__file__), "Configuration", "fpl_config.json")) as config_file:
        config = json.load(config_file)

except Exception as e:
    print(f"Error encountered: {e}")
    exit()



##### API REQUEST #####
# Requesting the FPL API, converting it to json format and loading in the 'elements' dictionary, which contains player data
print("Making API request...")
fpl_api = requests.get('https://fantasy.premierleague.com/api/bootstrap-static/')
api_return_code = str(fpl_api)[-5:-2]

if api_return_code == "200":
    print("API request successful, retrieving data...")

else:
    print(f"API request unsuccessful, return code: {api_return_code}")
    exit()



##### DATA CLEANING #####
# Converting the json returned by the FPL API into a pandas dataframe
print("Converting API return into a dataframe...")
fpl_api = fpl_api.json()
fpl_api = fpl_api['elements']
fpl_df = pd.json_normalize(fpl_api)

# Dropping unnecessary columns, then applying dtypes
required_columns = list(config["sql_upload_config"]["required_columns"].keys())
fpl_df = fpl_df[required_columns]
fpl_df.astype(config["sql_upload_config"]["required_columns"])

# Using pd.to_numeric() to convert 'stubborn' integer columns for which astype() doesn't work
for column in config["sql_upload_config"]["to_numeric_cols"]:
    fpl_df[column] = pd.to_numeric(fpl_df[column], errors= "coerce")

# Creating the 'full_name' column for each player in the dataframe 
print("Creating 'full name' column...")
fpl_df["full_name"] = fpl_df["first_name"] + " " + fpl_df["second_name"]

# Creating the 'team_name' and 'position' columns by applying the config dictionary to the 'team' and 'element_type' columns
print("Converting team and position codes into readable forms...")

fpl_df["team_name"] = fpl_df["team"].apply(lambda row: fpl.convert_to_string(str(row), 
                                                                     config_category= "team_code", 
                                                                     config_file= config))

fpl_df["position"] = fpl_df["element_type"].apply(lambda row: fpl.convert_to_string(str(row), 
                                                                            config_category= "position_code", 
                                                                            config_file= config))



##### CALCULATIONS #####
# Calculations of 90s played, goal involvements and xGI:Cost.
fpl_df["90s"] = fpl_df["minutes"]/90
fpl_df["goal_involvements"] = fpl_df["goals_scored"] + fpl_df["assists"]
fpl_df["xgi_to_cost_ratio"] = (fpl_df["expected_goal_involvements"] * 100) / fpl_df["now_cost"]
    
# Calculating a player's 'expected' points using their 90s played, xG and xA stats.
fpl_df["minutes_xpoints"] = fpl_df["90s"] * 2
fpl_df["goal_xpoints"] = fpl_df.apply(fpl.goal_values, axis= 1)
fpl_df["assist_xpoints"] = fpl_df["expected_assists"] * 3
fpl_df["xpoints"] = fpl_df["minutes_xpoints"] + fpl_df["goal_xpoints"] + fpl_df["assist_xpoints"]
fpl_df["xpoints_to_cost_ratio"] = fpl_df["xpoints"] / fpl_df["now_cost"]

# Calculating a player's performance against their xG, xA and xGC stats.
fpl_df["performance_vs_xg"] = fpl_df["goals_scored"] - fpl_df["expected_goals"]
fpl_df["performance_vs_xa"] = fpl_df["assists"] - fpl_df["expected_assists"]
fpl_df["performance_vs_xgc"] = fpl_df["expected_goals_conceded"] - fpl_df["goals_conceded"]

# Reordering columns according to preferred order.
column_order = config["sql_upload_config"]["column_order"]
fpl_df = fpl_df[column_order]



##### UPLOAD INTO MYSQL TABLE #####
# Retrieving MySQL details from dotenv file.
DOT_ENV_FILE_PATH = os.path.join(os.path.dirname(__file__), "Authorisation", ".env")
dotenv.load_dotenv(DOT_ENV_FILE_PATH)

mysql_username = os.getenv("username")
mysql_password = os.getenv("password")
host = "localhost"
dbase_name = "FPL_Data"

# Connecting to SQL database.
connection = sql.sql_connection(host= host, user= mysql_username, passwd= mysql_password, database= dbase_name)
cursor = connection.cursor()

# Creating the SQL query required to upload data into the 'player_data' table, to be run later.
table_name = 'player_data'
columns = ', '.join(fpl_df.columns)
placeholders = ', '.join(['%s'] * len(fpl_df.columns))
insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

# Converting the rows of the FPL API dataframe into a list containing a series of tuples.
data_to_insert = [tuple(row) for row in fpl_df.itertuples(index=False, name=None)]

# Running our SQL query to insert each tuple into the table.
cursor.executemany(insert_query, data_to_insert)

# Committing changes to the database and closing the SQL cursor.
connection.commit()
cursor.close()