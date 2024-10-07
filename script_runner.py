import os
import subprocess
import functions.subprocess_functions as subprocess

print('---------- SCRIPT STARTED ----------')
print('Running gameweek_data_retrieval script...')

if os.name != 'nt':

    venv_file_path = os.path.join(
        os.path.join(
            os.path.dirname(__file__),
            'venv',
            'bin',
            'python'
        )
    )

else:

    venv_file_path = os.path.join(
    os.path.join(
        os.path.dirname(__file__),
        'venv',
        'Scripts',
        'python.exe'
        )
    )

try:

    script_filepath = os.path.join(
        os.path.dirname(__file__),
        'gameweek_data_retrieval.py'
    )

    subprocess.subprocess_runner(
        operations_list= [venv_file_path, script_filepath],
        time_limit_seconds= 60,
        script_name= 'gameweek_data_retrieval'
    )

except subprocess.TimeoutExpired as e:
    
    print(f'Timeout: {e}')

except Exception as e:

    print(f'Exception: {e}')

print('Running player_cost_retrieval script...')

try:

    script_filepath = os.path.join(
        os.path.dirname(__file__),
        'player_cost_retrieval.py'
    )

    subprocess.subprocess_runner(
        operations_list= [venv_file_path, script_filepath],
        time_limit_seconds= 60,
        script_name= 'gameweek_data_retrieval'
    )

except subprocess.TimeoutExpired as e:
    
    print(f'Timeout: {e}')

except Exception as e:

    print(f'Exception: {e}')

print('---------- SCRIPT ENDED ----------')

