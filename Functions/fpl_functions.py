import pandas as pd
import mysql.connector
from mysql.connector import Error


def double_gameweek_processing(dictionary_1, dictionary_2, config_dictionary):
    '''In the event that a player has played two games in a single gameweek, this function combines data from the two matches 
    and generates a signle dataframe for the gameweek in question.
    \n
    \n Arguments:
    \n dictionary_1 (dict): The dictionary containing player performance data from the first match of the double gameweek.
    \n dictionary_2 (dict): The dictionary containing player performance data from the second match of the double gameweek.
    \n config_dictionary (dict): The dictionary which contains the dtype mapping for the dataframes created by this function.
    \n
    \n Returns:
    \n combined_df (DataFrame): Dataframe containing combined data for the two matches in the gameweek.'''


    column_dtype_mapping = config_dictionary["previous_gameweeks_config"]["double_gameweek_processing"]
    columns_to_combine = column_dtype_mapping.keys()
    
    df1 = pd.json_normalize(dictionary_1)
    df2 = pd.json_normalize(dictionary_2)

    df1 = df1.astype(column_dtype_mapping)
    df2 = df2.astype(column_dtype_mapping)

    for column in columns_to_combine:

        df1[column] = df1[column] + df2[column]

    combined_df = df1.copy()

    return combined_df


def convert_to_string(code, config_category, config_file):
    '''Returns human-readable 'team' or 'position' values from appropriate config dictionary. Defined as a function so that it 
    can be used in a lambda expression.
    \n
    \n Arguments:
    \n code (str): The numeric code used in the FPL API to refer to a player or team, converted into a string.
    \n config_category (str): The corresponding player/team dictionary in the config file.
    \n config_file (dict): The config file itself.
    \n
    \n Returns:
    \n Readable version of the player or team code.'''

    return config_file[config_category][code]


def performance_metric_calculations(dataframe):
    '''Adds a number of calculated fields to the gameweek dataframe.
    \n
    \n Arguments:
    \n dataframe (DataFrame): The merged dataframe containing player data for a given gameweek.
    \n
    \n Returns:
    \n No return, function simply adds columns to the existing dataframe.'''

    # Calculating per 90 values for single gameweek dataframes
    dataframe["goal_involvements"] = dataframe["goals_scored"] + dataframe["assists"]
    dataframe["90s"] = dataframe["minutes"] / 90
    dataframe["expected_goals_per_90"] = dataframe["expected_goals"] / dataframe["90s"]
    dataframe["expected_assists_per_90"] = dataframe["expected_assists"] / dataframe["90s"]
    dataframe["expected_goal_involvements"] = dataframe["expected_goals"] + dataframe["expected_assists"]
    dataframe["expected_goal_involvements_per_90"] = dataframe["expected_goal_involvements"] / dataframe["90s"]
    dataframe["goals_conceded_per_90"] = dataframe["goals_conceded"] / dataframe["90s"]
    dataframe["expected_goals_conceded_per_90"] = dataframe["expected_goals_conceded"] / dataframe["90s"]
    dataframe["saves_per_90"] = dataframe["saves"] / dataframe["90s"]
    dataframe["clean_sheets_per_90"] = dataframe["clean_sheets"] / dataframe["90s"]
    dataframe["xgi_to_cost_ratio"] = (dataframe["expected_goal_involvements"] * 100) / dataframe["now_cost"]

    # Calculating a player's performance against their xG, xA and xGC stats.
    dataframe["performance_vs_xg"] = dataframe["goals_scored"] - dataframe["expected_goals"]
    dataframe["performance_vs_xa"] = dataframe["assists"] - dataframe["expected_assists"]
    dataframe["performance_vs_xgc"] = dataframe["expected_goals_conceded"] - dataframe["goals_conceded"]

    # Calculation a player's 'expected points' based on their attacking output.
    dataframe["minutes_xpoints"] = dataframe["90s"] * 2
    dataframe["goal_xpoints"] = dataframe.apply(goal_values, axis= 1)
    dataframe["assist_xpoints"] = dataframe["expected_assists"] * 3
    dataframe["xpoints"] = dataframe["minutes_xpoints"] + dataframe["goal_xpoints"] + dataframe["assist_xpoints"]
    dataframe["xpoints_to_cost_ratio"] = dataframe["xpoints"] / dataframe["now_cost"]


def goal_values(row):
    '''Function used to convert a player's xG into an 'expected points' value. Called in performance_metric_calculations 
    function.'''

    if row["position"] == "FWD":
        return row["expected_goals"] * 4

    elif row["position"] == "MID":
        return row["expected_goals"] * 5

    else:
        return row["expected_goals"] * 6
    

def sql_connection(host, user, passwd, database = None):
    
    connection = None

    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            passwd=passwd,
            database=database
        )
        print("MySQL Database connection successful")

    except Error as err:
        print(f"Error: '{err}'")

    return connection