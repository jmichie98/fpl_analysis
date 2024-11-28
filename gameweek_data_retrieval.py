import os
import json
import requests
import pandas as pd
from datetime import datetime
import functions.fpl_functions as fpl
from functions.fpl_functions import APIError


print('---------- SCRIPT STARTED ----------')


# Retrieve general information about the FPL season from the API
print('Retrieving general information about the current FPL season...')

try:
    general_fpl_info_dict = fpl.retrieve_general_data()

except APIError as api_error:

    print(api_error)
    print('********** SCRIPT ENDED ON ERROR **********')
    exit(1)

except Exception as e:
    print(f'Unexpected error encountered while retrieving general FPL data - {e}')



# Determine the current season
print('Determining the current Premier League season...')

try:
    current_season = fpl.determine_current_season(general_fpl_info_dict= general_fpl_info_dict)

except ValueError as value_error:

    print(value_error)
    print('********** SCRIPT ENDED ON ERROR **********')
    exit(1)

except Exception as e:

    print(f'Unexpected error encountered while determining the current Premier League season - {e}')
    print('********** SCRIPT ENDED ON ERROR **********')
    exit(1)



# Generate file paths required for the script
(
    CONFIG_JSON_FILEPATH,
    GAMEWEEK_FILES_DIRECTORY
)= fpl.pathfinder(season= current_season)



# Read in config file
print('Reading in config file...')
try:

    with open(CONFIG_JSON_FILEPATH) as temporary_file:
        config = json.load(temporary_file)

except Exception as e:

    print(f'Error encountered while reading in config file: {e}')
    print('********** SCRIPT ENDED ON ERROR **********')
    exit(1)



# Determine the current gameweek
print('Checking which FPL gameweek has been most recently completed...')

try:
    last_completed_gameweek = fpl.find_last_completed_gameweek(general_fpl_info_dict= general_fpl_info_dict)

except Exception as e:

    print(f'Error encountered while identifying last completed gameweek: {e}')
    print('********** SCRIPT ENDED ON ERROR **********')
    exit(1)

if not last_completed_gameweek:

    print('No gameweeks have been completed yet')
    print('---------- SCRIPT COMPLETED ----------')
    exit(0)



# Create a directory to store the seasons data files if it doesn't exist already
season_directory_exists = os.path.exists(GAMEWEEK_FILES_DIRECTORY)

if not season_directory_exists:

    print(f'Creating directory for {current_season} season data files')
    os.makedirs(GAMEWEEK_FILES_DIRECTORY)

else:
    pass



# Determine which gameweeks need to be processed
missing_gameweeks_list = [
    x for x in range(1, last_completed_gameweek + 1) 
    if f'Gameweek_{x}.csv' not in os.listdir(GAMEWEEK_FILES_DIRECTORY)
]

if not missing_gameweeks_list:

    print('Data files have already been generated for all completed gameweeks.')
    print('---------- SCRIPT COMPLETED ----------')
    exit(0)

else:
    print(f'Data files have not been generated for gameweek(s): {missing_gameweeks_list}.')



# Retrieve player details from the general_fpl_info dictionary, ahead of a dataframe join at the end of gameweek processing
try:

    print('Retrieving general details for each player...')
    player_details_df = fpl.prepare_player_details_df(
        general_fpl_info_dict= general_fpl_info_dict,
        config_dict= config
    )

except Exception as e:

    print(f'Error encountered while retrieving player details: {e}')
    print('********** SCRIPT ENDED ON ERROR **********')
    exit(1)



# Retrieve player data for the required gameweek(s) from the API
print('Retrieving player data for the required gameweek(s) from the FPL API')

for gameweek_number in missing_gameweeks_list:

    GAMEWEEK_ENDPOINT_URL = f"https://fantasy.premierleague.com/api/event/{gameweek_number}/live/"
    gameweek_data_response = requests.get(GAMEWEEK_ENDPOINT_URL)
    api_return_code = gameweek_data_response.status_code

    if api_return_code != 200:

        # NOTE: Can probably add a warning message here, continue with the script and remove the 'failed gw' from the 
        #       list of gws to process.

        print(f'API call for last completed gameweek unsuccessful. Return code: {api_return_code}')
        print('********** SCRIPT ENDED ON ERROR **********')
        exit(1)

    else:

        gameweek_dict = gameweek_data_response.json()

    # Convert gameweek dictionary into dataframe and merge with player details
    player_dataframe_list = []
    number_of_players = len(gameweek_dict['elements'])

    for player in range(0, number_of_players):

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
        labels= config['columns_to_drop_list'], 
        axis= 1
    )

    full_gameweek_df = full_gameweek_df.astype(config['column_dtypes_mapper'])
    full_gameweek_df = fpl.attacking_score_calculation(full_gameweek_df, config)
    full_gameweek_df = full_gameweek_df[config['column_reordering_list']]

    csv_filepath = os.path.join(
        GAMEWEEK_FILES_DIRECTORY,
        f'Gameweek_{gameweek_number}.csv'
    )

    full_gameweek_df.to_csv(
        path_or_buf= csv_filepath,
        index= False
    )

### Double gameweeks will likely break this for-loop, but I don't know exactly how, will need to revisit later in the season

print('Gameweek file(s) successfully created.')
print('---------- SCRIPT COMPLETED ----------')