import pandas as pd
import requests
import json
import os
import Functions.fpl_functions as fpl

print("---------- SCRIPT STARTED ----------")

##### READING CONFIG FILE #####
try:
    with open(os.path.join(os.path.dirname(__file__), "Configuration", "fpl_config.json")) as config_file:
        config = json.load(config_file)

except Exception as e:
    print(f"Error encountered while reading in config file: {e}")
    print("********** SCRIPT ENDED ON ERROR **********")
    exit()



##### RETRIEVING CONFIG DATA #####
# Calling API endpoint containing general information on the current season.
general_fpl_info = requests.get('https://fantasy.premierleague.com/api/bootstrap-static/')
api_return_code = str(general_fpl_info)[-5:-2]

if api_return_code == "200":

    print("General Info API request successful")

    # Determining how many football players are in the FPL database.
    general_fpl_info = general_fpl_info.json()
    no_of_players = len(general_fpl_info['elements'])

else:
    print(f"General Info API request failed, return code: {api_return_code}")
    print("********** SCRIPT ENDED ON ERROR **********")
    exit()


# Calling API endpoint containing player data for previous gameweeks.
previous_gameweeks_data = requests.get('https://fantasy.premierleague.com/api/element-summary/1')
api_return_code = str(previous_gameweeks_data)[-5:-2]

if api_return_code == "200":

    print("Previous Gameweeks API request successful")
    
    # Determining how many gameweeks have been played of the current FPL season.
    previous_gameweeks_data = previous_gameweeks_data.json()
    no_of_gameweeks = len(previous_gameweeks_data['history'])

else:
    print(f"Previous Gameweeks API request failed, return code: {api_return_code}")
    print("********** SCRIPT ENDED ON ERROR **********")
    exit()


# Converting the general info API return to a dataframe and adding some columns ahead of a merge later on in the script.
fpl_df = pd.json_normalize(general_fpl_info['elements'])
fpl_df["full_name"] = fpl_df["first_name"] + " " + fpl_df["second_name"]

fpl_df["team_name"] = fpl_df["team"].apply(lambda row: fpl.convert_to_string(str(row), 
                                                                     config_category= "team_code", 
                                                                     config_file= config))

fpl_df["position"] = fpl_df["element_type"].apply(lambda row: fpl.convert_to_string(str(row), 
                                                                            config_category= "position_code", 
                                                                            config_file= config))



##### RETRIEVING PLAYER DATA #####
# Making an API call to retrieve data for each player and appending the data to a list of dictionaries.
print("Starting extraction of individual player data...")

player_data_dictionaries = []
for player in range(no_of_players + 1):

    player_data = requests.get(f"https://fantasy.premierleague.com/api/element-summary/{player}/")
    api_return_code = int(str(player_data)[-5:-2])

    if api_return_code != 200:
        continue

    player_data = player_data.json()
    player_data_dictionaries.append(player_data["history"])

    if player % 100 == 0 and player != 0:
        print(f"Data for {player} players extracted...")

print("Extraction of player data complete.")



##### TRANSFORMING DATA AND LOADING INTO DATABASES #####
# For loop which generates a database for each gameweek played, containing data for each player's performance in that gameweek.
print("Starting database creation for each gameweek...")

no_of_player_dictionaries = len(player_data_dictionaries)
for gameweek in range(no_of_gameweeks):

    print(f"Creating database for gameweek {gameweek + 1}...")

    player_dataframes = []
    double_gameweek = False

    for player_id in range(no_of_player_dictionaries):

        first_dictionary = player_data_dictionaries[player_id][0]
        second_dictionary = player_data_dictionaries[player_id][1] if len(player_data_dictionaries[player_id]) >= 2 else None

        if first_dictionary["round"] == gameweek + 1 and (second_dictionary and second_dictionary["round"] == gameweek + 1):
            double_gameweek = True

        if double_gameweek:
            
            # Combining data from the two matches in the double gameweek.
            player_df = fpl.double_gameweek_processing(first_dictionary, second_dictionary, config)

            player_dataframes.append(player_df)

            player_data_dictionaries[player_id].pop(0)
            player_data_dictionaries[player_id].pop(0)

            double_gameweek = False
            second_dictionary = None
        
        elif first_dictionary["round"] == gameweek + 1:

            player_df = pd.json_normalize(first_dictionary)
            player_dataframes.append(player_df)
            
            player_data_dictionaries[player_id].pop(0)

        else:
            continue
        

    gameweek_data = pd.concat(player_dataframes)

    gameweek_data = gameweek_data.rename(columns= {"element" : "id"})
    gameweek_data = gameweek_data.astype(config["previous_gameweeks_config"]["before_calculations"])

    # Merging our dataframe with selected columns from the 'general_info' dataframe.
    general_info_columns = fpl_df[["full_name", "team_name", "position", "now_cost", "id"]]
    merged_df = pd.merge(gameweek_data, general_info_columns, how= "right", on= "id")

    # Adding some columns to the dataframe containing calculations of a players performance against selected metrics.
    fpl.performance_metric_calculations(dataframe= merged_df)

    # Cleaning the database and writing it to a csv.
    column_order = list(config["previous_gameweeks_config"]["after_calculations"].keys())
    merged_df = merged_df[column_order]
    merged_df = merged_df.fillna(config["previous_gameweeks_config"]["na_columns"])
    merged_df = merged_df.astype(config["previous_gameweeks_config"]["after_calculations"])

    merged_df.to_csv(os.path.join(os.path.dirname(__file__), "Databases", f"GW{gameweek + 1}.csv"), index= False)
    print("Database successfully created.")



##### RETIRIEVING LIVE GW DATA #####
# Repeating the process performed in the above block, but for the most recent gameweek.
current_gameweek = no_of_gameweeks + 1
current_gameweek_data = requests.get(f"https://fantasy.premierleague.com/api/event/{current_gameweek}/live/")

current_gameweek_data_dict = current_gameweek_data.json()

print(f"Creating database for gameweek {current_gameweek}...")

player_dataframes = []
for player in range(no_of_players):

    player_df = pd.json_normalize(current_gameweek_data_dict['elements'][player]['stats'])
    player_df['id'] = current_gameweek_data_dict['elements'][player]['id']

    player_dataframes.append(player_df)

gameweek_data = pd.concat(player_dataframes)
gameweek_data = gameweek_data.astype(config["current_gameweek_config"]["before_calculations"])

merged_df = pd.merge(gameweek_data, general_info_columns, how="right", on= "id")

fpl.performance_metric_calculations(dataframe= merged_df)

column_order = list(config["previous_gameweeks_config"]["after_calculations"].keys())
merged_df = merged_df[column_order]
merged_df = merged_df.fillna(config["current_gameweek_config"]["na_columns"])
merged_df = merged_df.astype(config["current_gameweek_config"]["after_calculations"])

merged_df.to_csv(os.path.join(os.path.dirname(__file__), "Databases", f"GW{current_gameweek}.csv"), index= False)

print("Database successfully created.")

print("---------- SCRIPT COMPLETED ----------")