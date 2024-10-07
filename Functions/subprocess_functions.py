import time
import subprocess

def subprocess_runner(
        operations_list: list,
        time_limit_seconds: int,
        script_name: str
    ):

    '''Runs a specified script as a subprocess
    
    Args:
        operations_list - List of variables to pass into the subprocess, first two items in this list should be the python environment \
        the script should be run in, followed by the filepath to the script
        time_limit_seconds - The amount of time in seconds that the script should be allowed to run before it is terminated
        script_name - The name of the script

    Returns:
        running_subprocess - 
    '''

    try:

        running_subprocess = subprocess.Popen(
            args= operations_list,
            stdout= subprocess.PIPE,
            stderr= subprocess.PIPE
        )

        running_subprocess.communicate(timeout= time_limit_seconds)
        running_subprocess.terminate()

        return

    except subprocess.TimeoutExpired as e:

        running_subprocess.terminate()
        subprocess_still_running = running_subprocess.poll() is None

        if not subprocess_still_running:
            raise subprocess.TimeoutExpired(f'Time limit was reached for {script_name}, subprocess was successfully terminated.')
        
        else:
            pass

        running_subprocess.kill()
        subprocess_still_running = running_subprocess.poll() is None

        if not subprocess_still_running:
            raise subprocess.TimeoutExpired(f'Time limit was reached for {script_name}, subprocess was successfully terminated.')
        
        else:
            raise subprocess.TimeoutExpired(f'Time limit was reached for {script_name}, subprocess could not be terminated. PID: {running_subprocess.pid}')
        
    except Exception as e:
        raise Exception(f'{e}')
    


