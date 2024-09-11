import pandas as pd
import requests
import json
import os
from datetime import datetime
import functions.fpl_functions as fpl


print('---------- SCRIPT STARTED ----------')


# Retrieve general information about the FPL season from the API
try:

    print('Making request to General Info API...')
    general_fpl_info = requests.get('https://fantasy.premierleague.com/api/bootstrap-static/')
    api_return_code = str(general_fpl_info)[-5:-2]

    if api_return_code != '200':

        raise ValueError

    else:

        print('General Info API request successful.')
        general_fpl_info = general_fpl_info.json()

        pass

except ValueError:
    
    print(f'General Info API request failed, return code: {api_return_code}')
    print('********** SCRIPT ENDED ON ERROR **********')
    exit(1)

except Exception as e:

    print(f'Error encountered while making General Info API request: {e}')
    print('********** SCRIPT ENDED ON ERROR **********')
    exit(1)
    

del api_return_code


# Determine the current season
try:

    print('Determining the current Premier League season...')
    season_start_date_str = general_fpl_info['events'][0]['deadline_time']
    season_start_year = datetime.strptime(season_start_date_str, '%Y-%m-%dT%H:%M:%SZ').year
    season_end_year = season_start_year + 1

    current_season = f'{season_start_year}-{str(season_end_year)[-2:]}'

except Exception as e:

    print(f'Error encountered while calculating current season: {e}')
    print('********** SCRIPT ENDED ON ERROR **********')
    exit(1)

del season_start_date_str, season_start_year, season_end_year


# Generate file paths required for the script
print('Retrieving file paths required for the script...')
(
    CONFIG_JSON_FILEPATH,
    GAMEWEEK_FILES_DIRECTORY
)= fpl.pathfinder(season= current_season)

PLAYER_COST_DATABASE_FILEPATH = os.path.join(GAMEWEEK_FILES_DIRECTORY, 'player_cost.csv')


# Retrieve player costs and write to csv
print('Extracting player cost information and writing to csv...')
try:

    general_fpl_info_dataframe = pd.json_normalize(general_fpl_info['elements'])
    general_fpl_info_dataframe = general_fpl_info_dataframe[['id', 'now_cost']]
    general_fpl_info_dataframe['now_cost'] = general_fpl_info_dataframe['now_cost'] / 10

    general_fpl_info_dataframe.to_csv(PLAYER_COST_DATABASE_FILEPATH, index= False)

except KeyError as e:

    print(f'Elements key is missing from the general_fpl_info dictionary: {e}')
    print('********** SCRIPT ENDED ON ERROR **********')
    exit(1)

except FileNotFoundError as e:

    print(f'Player Cost database could not be found: {e}')
    print('********** SCRIPT ENDED ON ERROR **********')
    exit(1)

except Exception as e:

    print(f'Error occurred while retrieving current player costs: {e}')
    print('********** SCRIPT ENDED ON ERROR **********')
    exit(1)

print('Database successfully updated.')
print('---------- SCRIPT COMPLETED ----------')