"""
This file contains all the configuration variables to connect to the database and setup the websocket origin. All the variables are loaded from the .env file.
"""

from dotenv import load_dotenv
import os

# App Configuration
load_dotenv()

DB_HOST = os.environ.get('DB_HOST', 'localhost')
"""
The host of the database
"""
DB_PORT = os.environ.get('DB_PORT', 3333)
"""
The port of the database
"""
DB_NAME = os.environ.get('DB_NAME', 'db_name')
"""
The name of the database
"""
WEBSOCKET_ORIGIN = os.environ.get('WEBSOCKET_ORIGIN', 'localhost')
"""
The origin of the websocket
"""