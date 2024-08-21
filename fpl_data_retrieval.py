import pandas as pd
import requests
import json
import os
from datetime import datetime
import functions.fpl_functions as fpl


print("---------- SCRIPT STARTED ----------")


# Retrieve general information about the FPL season from the API
general_fpl_info = requests.get('https://fantasy.premierleague.com/api/bootstrap-static/')
api_return_code = str(general_fpl_info)[-5:-2]

if api_return_code != '200':

    print(f"General Info API request failed, return code: {api_return_code}")
    print("********** SCRIPT ENDED ON ERROR **********")
    exit()

else:

    print("General Info API request successful")
    general_fpl_info = general_fpl_info.json()

    pass

del api_return_code



# Determine the current season
season_start_date_str = general_fpl_info['events'][0]['deadline_time']
season_start_year = datetime.strptime(season_start_date_str, '%Y-%m-%dT%H:%M:%SZ').year
season_end_year = season_start_year + 1

current_season = f'{season_start_year}-{str(season_end_year)[-2:]}'

del season_start_date_str, season_start_year, season_end_year



# Generate file paths required for the script
(
    CONFIG_JSON_FILEPATH,
    GAMEWEEK_FILES_DIRECTORY
)= fpl.pathfinder(season= current_season)



# Read in config file
try:

    with open(CONFIG_JSON_FILEPATH) as temporary_file:
        config = json.load(temporary_file)

except Exception as e:

    print(f"Error encountered while reading in config file: {e}")
    print("********** SCRIPT ENDED ON ERROR **********")
    exit()



# Determine the current gameweek
print('Checking which FPL gameweek has been most recently completed...')
no_gameweeks_completed_yet = True

for gameweek in general_fpl_info['events']:

    if not gameweek['finished']:

        no_gameweeks_completed_yet = False

        current_gameweek = gameweek['name']
        current_gameweek_number = int(current_gameweek[-2:])
        last_completed_gameweek_number = current_gameweek_number - 1
        last_completed_gameweek_string = f'Gameweek {last_completed_gameweek_number}'

        print(f'{last_completed_gameweek_string} is the most recently completed gameweek.')

        break

    else:
        continue

if no_gameweeks_completed_yet:

    print('No gameweeks have been completed yet')
    print("---------- SCRIPT COMPLETED ----------")
    exit()

else:
    pass

del gameweek, current_gameweek



# Determine which gameweeks need to be processed
missing_gameweeks_list = [
    x for x in range(1, current_gameweek_number) 
    if f'Gameweek_{x}.csv' not in os.listdir(GAMEWEEK_FILES_DIRECTORY)
]

if not missing_gameweeks_list:

    print('Files have already been generated for all completed gameweeks')
    print("---------- SCRIPT COMPLETED ----------")
    exit()

else:
    pass



# Retrieve player data for the required gameweeks from the API
gameweek_dictionary_list = []

for gameweek_number in missing_gameweeks_list:

    api_endpoint_url = f"https://fantasy.premierleague.com/api/event/{gameweek_number}/live/"
    gameweek_data = requests.get(api_endpoint_url)
    api_return_code = str(gameweek_data)[-5:-2]

    if api_return_code != '200':

        ### I can probably raise an exception here and continue with the script

        print(f'API call for last completed gameweek unsuccessful. Return code: {api_return_code}')
        print("********** SCRIPT ENDED ON ERROR **********")
        exit(1)

    else:

        gameweek_dict = gameweek_data.json()
        # gameweek_dict[f'gameweek_{gameweek_number}'] = gameweek_dict.pop('elements')
        gameweek_dictionary_list.append(gameweek_dict)

    del api_return_code, api_endpoint_url, gameweek_data, gameweek_dict



# Retrieve player details from the general_fpl_info dictionary, ahead of a dataframe join at the end of gameweek processing
player_details_df = pd.json_normalize(general_fpl_info['elements'])
player_details_df = player_details_df[config['player_details_columns_list']]
player_details_df['full_name'] = player_details_df['first_name'] + ' ' + player_details_df['second_name']

player_details_df = player_details_df.drop(
    labels= ['first_name', 'second_name'],
    axis= 1
)

player_details_df = fpl.map_column_values_to_string(
    general_info_dict= general_fpl_info,
    dataframe= player_details_df
)


# Convert gameweek dictionary into dataframe and merge with player details
for gameweek_dict, gameweek in zip(gameweek_dictionary_list, missing_gameweeks_list):

    player_dataframe_list = []

    for player in range(0, len(gameweek_dict['elements'])):

        player_id = gameweek_dict['elements'][player]['id']
        player_data_df = pd.json_normalize(gameweek_dict['elements'][player]['stats'])
        player_data_df['id'] = player_id

        player_dataframe_list.append(player_data_df)

    full_gameweek_df = pd.concat(player_dataframe_list)
    full_gameweek_df = full_gameweek_df.merge(
        right= player_details_df,
        how= 'inner',
        on= 'id'
    )

    # Clean dataframe and write to csv
    full_gameweek_df = full_gameweek_df.drop(
        labels=['bonus', 'bps', 'in_dreamteam', 'yellow_cards', 'red_cards', 'own_goals'], 
        axis= 1
    )

    full_gameweek_df = full_gameweek_df.astype(config['column_dtypes_mapper'])
    full_gameweek_df['now_cost'] = full_gameweek_df['now_cost'] / 10
    full_gameweek_df = fpl.xpoints_calculation(full_gameweek_df)
    full_gameweek_df = full_gameweek_df[config['column_reordering_list']]

    csv_filepath = os.path.join(
        os.path.dirname(__file__), 
        'player_data', 
        current_season, 
        f'Gameweek_{gameweek}.csv'
    )

    full_gameweek_df.to_csv(
        path_or_buf= csv_filepath,
        index= False
    )

    del player_id, player_data_df

### Double gameweeks will likely break this for loop, but I don't know exactly how, will need to revisit later in the season

print("Database successfully created.")
print("---------- SCRIPT COMPLETED ----------")


# # Calculations I've removed from the datbases (manually set up in Tableau)
# dataframe["xpoints_to_cost_ratio"] = dataframe["xpoints"] / dataframe["now_cost"]

# dataframe["goal_involvements"] = dataframe["goals_scored"] + dataframe["assists"]
# dataframe["90s"] = dataframe["minutes"] / 90
# dataframe["expected_goals_per_90"] = dataframe["expected_goals"] / dataframe["90s"]
# dataframe["expected_assists_per_90"] = dataframe["expected_assists"] / dataframe["90s"]
# dataframe["expected_goal_involvements"] = dataframe["expected_goals"] + dataframe["expected_assists"]
# dataframe["expected_goal_involvements_per_90"] = dataframe["expected_goal_involvements"] / dataframe["90s"]
# dataframe["goals_conceded_per_90"] = dataframe["goals_conceded"] / dataframe["90s"]
# dataframe["expected_goals_conceded_per_90"] = dataframe["expected_goals_conceded"] / dataframe["90s"]
# dataframe["saves_per_90"] = dataframe["saves"] / dataframe["90s"]
# dataframe["clean_sheets_per_90"] = dataframe["clean_sheets"] / dataframe["90s"]
# dataframe["xgi_to_cost_ratio"] = (dataframe["expected_goal_involvements"] * 100) / dataframe["now_cost"]

# dataframe["performance_vs_xg"] = dataframe["goals_scored"] - dataframe["expected_goals"]
# dataframe["performance_vs_xa"] = dataframe["assists"] - dataframe["expected_assists"]
# dataframe["performance_vs_xgc"] = dataframe["expected_goals_conceded"] - dataframe["goals_conceded"]