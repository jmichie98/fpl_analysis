The code for my Fantasy Premier League Analysis project.

fpl_data_retrieval.py aggregates player data, creating separate databases for each gameweek in the 'Databases' folder.
fpl_sql.py extracts data from the 'general' FPL API endpoint and loads it into a MySQL Database for further querying.

Both scripts are dependent on Configuration/fpl_configuration.json and on Functions/fpl_functions.py
There is a separate Functions/sql_functions.py file for the SQL upload.

fpl_sql.py also requires that a .env file be created in the 'Authorisation' folder, containing 'username' and 'password' fields for your local MySQL server.
An empty 'venv' folder has been created, along with the 'requirements.txt' folder if you wish to replicate the virtual environment in which my code has been running.
