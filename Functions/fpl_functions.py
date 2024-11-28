import os
import requests
import pandas as pd
from datetime import datetime

class APIError(Exception):

    '''Exception class to raise in case of errors with API calls'''

    def __init__(self, status):
        self.status = status

    def __str__(self):
        return f'APIError - {self.status}'


def retrieve_general_data() -> dict:

    '''
    Retrieves general information about the current FPL season from the API, and converts it into a dictionary.
    
    Returns:
        general_fpl_info_dict - Dictionary containing general information about the current FPL season.

    Raises:
        APIError - Raised if the response code of the API call is unsuccessful.
    '''

    print('Making API call to general information endpoint...')
    GENERAL_FPL_INFO_URL = 'https://fantasy.premierleague.com/api/bootstrap-static/'

    # Make API request and raise exception if error response received    
    try:

        general_fpl_info_response = requests.get(GENERAL_FPL_INFO_URL)
        general_fpl_info_response.raise_for_status()

    except requests.exceptions.HTTPError:

        response_code = general_fpl_info_response.status_code
        raise APIError(f'Response Code: {response_code}')

    # Convert response into dictionary
    print('API call successful, converting to dictionary...')
    general_fpl_info_dict = general_fpl_info_response.json()
        
    return general_fpl_info_dict


def determine_current_season(general_fpl_info_dict: dict) -> str:

    '''
    Determines the current Premier League season, returning a string in the format "YYYY-YY".
    
    Args:
        general_fpl_info_dict - Dictionary containing general information about the current FPL season.

    Returns:
        current_season - The current Premier League season, declared as a string with format "YYYY-YY".

    Raises:
        ValueError - Raised if the relevant key in the general_fpl_info_dict is not present.
    '''

    # Check if 'events' dictionary is present
    events_dict_absent = not general_fpl_info_dict.get('events', [])

    if events_dict_absent:
        raise ValueError("ValueError - Unable to locate 'events' dictionary in the general FPL dictionary")
    
    else:
        pass

    # Parse deadline_time to determine season start date, then format as string
    season_start_date_str = general_fpl_info_dict['events'][0]['deadline_time']
    season_start_date = datetime.strptime(season_start_date_str, '%Y-%m-%dT%H:%M:%SZ')
    season_start_year = season_start_date.year
    season_end_year = season_start_year + 1

    current_season = f'{season_start_year}-{(season_end_year % 100):02d}'

    return current_season


def pathfinder(season: str) -> tuple[str, str]:
    
    '''
    Generates the file paths required for the script
    
    Args:
        season: A string indicating the current Premier League season in the format 'YYYY-YY' (e.g. '2024-25')

    Returns:
        CONFIG_JSON_FILEPATH: The full filepath to the fpl_config.json file
        GAMEWEEK_FILES_DIRECTORY: The full filepath to the folder containing the gameweek files for the current season 
    '''

    CONFIG_JSON_FILEPATH = os.path.join(
        os.path.dirname(__file__).replace('functions', ''),
        'configuration',
        'fpl_config.json'
    )

    GAMEWEEK_FILES_DIRECTORY = os.path.join(
        os.path.dirname(__file__).replace('functions', ''),
        'database_files',
        season
    )

    return (
        CONFIG_JSON_FILEPATH,
        GAMEWEEK_FILES_DIRECTORY
    )


def find_last_completed_gameweek(general_fpl_info_dict: dict) -> int:

    '''
    Identifies the most recent gameweek in the FPL calendar for which all data is finalised.

    Args:
        general_fpl_info_dict -  Dictionary containing general information about the current FPL season.

    Returns:
        last_completed_gameweek - Integer representing the last completed gameweek.
    '''

    for gameweek in general_fpl_info_dict['events']:

        gameweek_id = gameweek['id']
        gameweek_is_finished = gameweek['finished']
        gameweek_name = gameweek['name']

        # Check if the season has started
        season_not_started = (gameweek_id == 1) and (not gameweek_is_finished)

        if season_not_started:
            print('The first gameweek of the FPL season is not yet complete.')
            return None

        else:
            pass

        # Check if the season has ended
        season_has_finished = (gameweek_id == 38) and (gameweek_is_finished)

        if season_has_finished:

            last_completed_gameweek = gameweek_id
            print(f'{gameweek_name} is the most recently completed gameweek.')

            break

        else:
            pass

        # Determine most recently completed gameweek
        if not gameweek['finished']:

            last_completed_gameweek = gameweek_id - 1
            last_completed_gameweek_string = f'Gameweek {last_completed_gameweek}'

            print(f'{last_completed_gameweek_string} is the most recently completed gameweek.')

            break

    return last_completed_gameweek


def prepare_player_details_df(
        general_fpl_info_dict: dict,
        config_dict
    ) -> pd.DataFrame:

    '''
    Generates a dataframe containing the "id", "team", "element_type" and "full_name" of each player in the FPL season.

    Args:
        general_fpl_info_dict - Dictionary containing general information about the current FPL season.
        config_dict - Dictionary containing configuration info for processing of the FPL API returns.

    Returns:
        player_details_df - Dataframe containing general information about each player.
    '''

    # Convert dictionary to dataframe, filter for required columns and creating 'full name' column.
    player_details_df = pd.json_normalize(general_fpl_info_dict['elements'])
    player_details_df = player_details_df[config_dict['player_details_columns_list']]
    player_details_df['full_name'] = player_details_df['first_name'] + ' ' + player_details_df['second_name']

    player_details_df = player_details_df.drop(
        labels= ['first_name', 'second_name'],
        axis= 1
    )

    # Converting the 'team' column values from integers into readable team strings.
    player_details_df = map_integer_columns_to_string(
        general_info_dict= general_fpl_info_dict,
        dataframe= player_details_df,
        dictionary_key= 'teams',
        value_to_map_key= 'name',
        integer_column_title= 'team',
        string_column_title= 'team_name'
    )

    # Converting the 'element_type' column values from integers into readable player position strings.
    player_details_df = map_integer_columns_to_string(
        general_info_dict= general_fpl_info_dict,
        dataframe= player_details_df,
        dictionary_key= 'element_types',
        value_to_map_key= 'singular_name_short',
        integer_column_title= 'element_type',
        string_column_title= 'position'
    )

    return player_details_df


def map_integer_columns_to_string(
        general_info_dict: dict,
        dataframe: pd.DataFrame,
        dictionary_key: str,
        value_to_map_key: str,
        integer_column_title: str,
        string_column_title: str 
    ) -> pd.DataFrame:

    '''
    Converts an integer in each row of a specified column into a string according to specifications in the FPL API.

    Args:
        general_info_dict - Dictionary containing general information about the current FPL season.
        dataframe - Dataframe containing general information about each player.
        dictionary_key - The key in the general_info_dict which contains information relating to integer-string mapping.
        value_to_map_key - The key for the mapping value we wish to retrieve from the dictionary.
        integer_column_title - The name of the column we wish to convert in the dataframe.
        string_column_title - The name we wish to give the new string column.

    Returns:
        dataframe - The dataframe, with the integer columns dropped and the new string columns present.
    '''

    mapper = {}

    for dictionary in general_info_dict[dictionary_key]:

        mapping_dict = {dictionary['id'] : dictionary[value_to_map_key]}
        mapper.update(mapping_dict)

    # Using the mapper to convert the 'team' column of the player_details_df from integers into strings.
    dataframe[string_column_title] = dataframe[integer_column_title].map(mapper)

    # Dropping the old integer column
    dataframe = dataframe.drop(
        labels= integer_column_title, 
        axis=1
    )

    return dataframe


def attacking_score_calculation(
        dataframe: pd.DataFrame,
        config_dict: dict
    ) -> pd.DataFrame:

    '''
    Adds a number of calculated fields to the gameweek dataframe.
    
    Args:
        dataframe: The merged dataframe containing player data for a given gameweek.

    Returns:
        dataframe: The full_gameweek_df with an added column containing player 'Expected Points' figures
        config_dict - Dictionary containing configuration info for processing of the FPL API returns.
    '''

    # Calculation a player's 'expected points' based on their attacking output.
    dataframe['minutes_score'] = (dataframe['minutes'] / 90) * 2
    dataframe['xgoals_score'] = dataframe.apply(lambda row: goal_value_multiplier(row, config_dict), axis= 1)
    dataframe['xassist_score'] = dataframe['expected_assists'] * 3

    dataframe = dataframe.astype(
        {
            'minutes_score' : 'float64', 
            'xgoals_score' : 'float64', 
            'xassist_score' : 'float64'
        }
    )

    dataframe['attacking_score'] = dataframe['minutes_score'] + dataframe['xgoals_score'] + dataframe['xassist_score']
    dataframe['attacking_score'] = dataframe['attacking_score'].round(2)
    dataframe = dataframe.drop(['minutes_score', 'xgoals_score', 'xassist_score'], axis= 1)

    return dataframe


def goal_value_multiplier(
        row: pd.Series,
        config_dict: dict
    ) -> int:

    '''
    Converts a player's xG into an 'expected points' value, based on the points a player in their position would score per goal
    in FPL.

    Args:
        row - The player's performance data for a given gameweek.
        config_dict - Dictionary containing configuration info for processing of the FPL API returns.
    '''

    player_position = row['position']
    goal_value = config_dict['goal_values'][player_position]
    return row['expected_goals'] * goal_value
    

if __name__ == '__main__':

    data = retrieve_general_data()

    try:
        find_last_completed_gameweek(data)

    except Exception as e:
        print(e)