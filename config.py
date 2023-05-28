from dotenv import load_dotenv
import os

# App Configuration
load_dotenv()

DB_HOST = os.environ.get('DB_HOST')
DB_PORT = int(os.environ.get('DB_PORT'))
DB_NAME = os.environ.get('DB_NAME')
WEBSOCKET_ORIGIN = os.environ.get('WEBSOCKET_ORIGIN')