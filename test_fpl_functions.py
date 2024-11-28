import unittest
import requests
import pandas as pd
from unittest.mock import patch, Mock
import functions.fpl_functions as fpl


class TestFplFunctions(unittest.TestCase):

    
    @patch('functions.fpl_functions.requests.get')
    def test_retrieve_general_data(self, mock_get):
        
        # Test a successful API call
        mock_response_success = Mock()
        mock_response_success.status_code = 200
        mock_response_success.json.return_value = {'key' : 'value'}
        mock_get.return_value = mock_response_success

        api_response = fpl.retrieve_general_data()
        self.assertEqual(api_response, {'key' : 'value'})


        # Test an unsuccessful API call
        mock_response_error = Mock()
        mock_response_error.status_code = 500
        mock_response_error.raise_for_status.side_effect = requests.exceptions.HTTPError
        mock_get.return_value = mock_response_error

        with self.assertRaises(fpl.APIError):
            api_response = fpl.retrieve_general_data()

        mock_get.assert_called_with('https://fantasy.premierleague.com/api/bootstrap-static/')

    

    def test_determine_current_season(self):

        # Test with valid input
        general_fpl_info_dict = {
            'events' : [
                {'deadline_time' : '2024-10-28T20:00:00Z'},
                {'deadline_time' : '2025-05-11T15:00:00Z'}
            ]
        }

        current_season = fpl.determine_current_season(general_fpl_info_dict)
        expected_season = '2024-25'

        self.assertEqual(current_season, expected_season)


        # Test if correct exception is raised when field can't be located
        with self.assertRaises(ValueError) as value_error:

            test_dictionary = {'test_field' : 'test_key'}
            fpl.determine_current_season(test_dictionary)

        self.assertEqual(
            str(value_error.exception), 
            "ValueError - Unable to locate 'events' dictionary in the general FPL dictionary"
        )

    

    @patch('functions.fpl_functions.__file__', 'test/path/functions')
    def test_pathfinder(self):

        season = '2024-25'
        expected_config_filepath = 'test/path/configuration/fpl_config.json'
        expected_gameweek_directory = 'test/path/database_files/2024-25'

        config_path, gameweek_files_directory = fpl.pathfinder(season)

        self.assertEqual(config_path, expected_config_filepath)
        self.assertEqual(gameweek_files_directory, expected_gameweek_directory)


    
    def test_find_last_completed_gameweek(self):

        # Test the output for if the season is in progress
        season_in_progress_dict = {

            'events' : [
                {
                    'id' : 1, 
                    'finished' : True, 
                    'name' : 'Gameweek 1'
                },

                {
                    'id' : 2, 
                    'finished' : False, 
                    'name' : 'Gameweek 2'
                }
            ]
        }

        last_completed_gameweek = fpl.find_last_completed_gameweek(general_fpl_info_dict= season_in_progress_dict)
        expected_gameweek = 1
        self.assertEqual(last_completed_gameweek, expected_gameweek)


        # Test the output for if the season hasn't started yet
        season_not_started_dict = {

            'events' : [
                {
                    'id' : 1, 
                    'finished' : False, 
                    'name' : 'Gameweek 1'
                }
            ]
        }

        last_completed_gameweek = fpl.find_last_completed_gameweek(general_fpl_info_dict= season_not_started_dict)
        self.assertIsNone(last_completed_gameweek)


        # Test the output for if the season is finished
        season_finished_dict = {

            'events' : [
                {
                    'id' : 38, 
                    'finished' : True, 
                    'name' : 'Gameweek 38'
                }
            ]
        }

        last_completed_gameweek = fpl.find_last_completed_gameweek(general_fpl_info_dict= season_finished_dict)
        expected_gameweek = 38
        self.assertEqual(last_completed_gameweek, expected_gameweek)

    
    def test_prepare_player_details_df(self):

    
        general_fpl_info_dict = {

            'elements' : [
                {'element_type': 3, 'first_name': 'Mohamed', 'id': 328, 'now_cost': 131, 'second_name': 'Salah', 'team': 12},
                {'element_type': 4, 'first_name': 'Erling', 'id': 351, 'now_cost': 151, 'second_name': 'Haaland', 'team': 13}
            ],

            'teams' : [
                {'id' : 12, 'name' : 'Liverpool'},
                {'id' : 13, 'name' : 'Man City'}
            ],

            'element_types' : [
                {'id' : 3, 'singular_name_short' : 'MID'},
                {'id' : 4, 'singular_name_short' : 'FWD'}
            ]
        }

        config_dict = {

            'player_details_columns_list' : [
                'id',
                'team',
                'element_type', 
                'first_name', 
                'second_name'
            ]
        }

        player_details_df = fpl.prepare_player_details_df(
            general_fpl_info_dict= general_fpl_info_dict,
            config_dict= config_dict
        )

        data = {
            'id' : [328, 351],
            'full_name' : ['Mohamed Salah', 'Erling Haaland'],
            'team_name' : ['Liverpool', 'Man City'],
            'position' : ['MID', 'FWD']
        }

        expected_dataframe = pd.DataFrame(data)
        pd.testing.assert_frame_equal(player_details_df, expected_dataframe)

    
    def test_attacking_score_calculation(self):

        input_data = {
            'id' : [15, 36, 328, 351],
            'full_name' : ['David Raya Martin', 'Lucas Digne', 'Mohamed Salah', 'Erling Haaland'],
            'position' : ['GKP', 'DEF', 'MID', 'FWD'],
            'minutes' : [90, 21, 90, 90],
            'expected_goals' : [0.01, 0.42, 1.86, 0.82],
            'expected_assists' : [0.0, 0.06, 0.16, 0.03]
        }

        input_dataframe = pd.DataFrame(input_data)

        config_dict = {
            "goal_values" : {
                "GKP" : 15,
                "DEF" : 6,
                "MID" : 5,
                "FWD" : 4
            }
        }

        output_dataframe = fpl.attacking_score_calculation(input_dataframe, config_dict)

        expected_data = {
            'id' : [15, 36, 328, 351],
            'full_name' : ['David Raya Martin', 'Lucas Digne', 'Mohamed Salah', 'Erling Haaland'],
            'position' : ['GKP', 'DEF', 'MID', 'FWD'],
            'minutes' : [90, 21, 90, 90],
            'expected_goals' : [0.01, 0.42, 1.86, 0.82],
            'expected_assists' : [0.0, 0.06, 0.16, 0.03],
            'attacking_score' : [2.15, 3.17, 11.78, 5.37]
        }

        expected_dataframe = pd.DataFrame(expected_data)
        pd.testing.assert_frame_equal(output_dataframe, expected_dataframe)


if __name__ == '__main__':
    
    # unittest.main()

    test_suite = unittest.TestSuite()
    test_suite.addTest(TestFplFunctions('test_attacking_score_calculation'))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)