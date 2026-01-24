from opensky_api import OpenSkyApi
import logging

logger = logging.getLogger("opensky_api")
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

opensky_api_secret_file = "/home/matthew/.local/share/credentials/opensky_api_key.json"

api = OpenSkyApi(client_json_path=opensky_api_secret_file)