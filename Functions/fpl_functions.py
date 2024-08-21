import pandas as pd
import os


def pathfinder(season: str) -> tuple[str, str]:
    '''
    Generating the file paths required for the script
    
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
        'player_data',
        season
    )

    return (
        CONFIG_JSON_FILEPATH,
        GAMEWEEK_FILES_DIRECTORY
    )



def map_column_values_to_string(
        general_info_dict: dict,
        dataframe: pd.DataFrame
    ):

    # Creating a team name mapper and assigning it to the 'team' column of the player_details_df
    team_name_mapper = {}

    for team in general_info_dict['teams']:

        team_dict = {team['id'] : team['name']}
        team_name_mapper.update(team_dict)

    dataframe['team_name'] = dataframe['team'].apply(
        
        lambda row: map_string_to_int(
            code= row,
            mapper= team_name_mapper
        )
    )

    dataframe = dataframe.drop(
        labels= 'team',
        axis= 1
    )

    del team_name_mapper, team_dict

    # Creating a position mapper and assigning it to the 'element_type' column of the player_details_df
    position_mapper = {}

    for position in general_info_dict['element_types']:
        position_dict = {position['id'] : position['singular_name_short']}
        position_mapper.update(position_dict)

    dataframe['position'] = dataframe['element_type'].apply(
        
        lambda row: map_string_to_int(
            code= row,
            mapper= position_mapper
        )
    )

    dataframe = dataframe.drop(
        labels= 'element_type',
        axis= 1
    )

    del position_mapper, position_dict

    return dataframe



def map_string_to_int(
        code: int, 
        mapper: dict) -> str:
    
    '''
    Returns full 'team' or 'position' strings according to the FPL mapper.

    Args:
        code: The numeric code used in the FPL API to refer to a player's position or team.
        mapper: The mapper used to convert the integers into strings.
    
    Returns:
        coverted_string: The player's position or team code.
    '''

    converted_string = mapper[code]

    return converted_string



def xpoints_calculation(dataframe: pd.DataFrame) -> pd.DataFrame:
    '''
    Adds a number of calculated fields to the gameweek dataframe.
    
    Args:
        dataframe: The merged dataframe containing player data for a given gameweek.

    Returns:
        dataframe: The full_gameweek_df with an added column containing player 'Expected Points' figures
    '''

    # Calculation a player's 'expected points' based on their attacking output.
    dataframe['minutes_xpoints'] = (dataframe['minutes'] / 90) * 2
    dataframe['goal_xpoints'] = dataframe.apply(goal_values, axis= 1)
    dataframe['assist_xpoints'] = dataframe['expected_assists'] * 3

    dataframe = dataframe.astype(
        {
            'minutes_xpoints' : 'float64', 
            'goal_xpoints' : 'float64', 
            'assist_xpoints' : 'float64'
        }
    )

    dataframe['xpoints'] = dataframe['minutes_xpoints'] + dataframe['goal_xpoints'] + dataframe['assist_xpoints']
    dataframe = dataframe.drop(['minutes_xpoints', 'goal_xpoints', 'assist_xpoints'], axis= 1)

    return dataframe



def goal_values(row: pd.Series) -> int:
    '''
    Converts a player's xG into an 'expected points' value, based on the points a player in their position would score per goal \
    in FPL.

    Args:
        row: The player's performance data for a given gameweek.
    '''

    if row['position'] == 'FWD':
        return row['expected_goals'] * 4

    elif row['position'] == 'MID':
        return row['expected_goals'] * 5

    else:
        return row['expected_goals'] * 6