import assets
import config
import os
from dotenv import load_dotenv
from flask import Flask
from flask_caching import Cache
from google.cloud import bigquery

load_dotenv()
PROJECT_ID = os.getenv("PROJECTID")

app = Flask(__name__)
app.config.from_object(config)
app.config["CACHE_TYPE"] = "simple"
app.config["CACHE_DEFAULT_TIMEOUT"] = 1
assets.init(app)

cache = Cache(app)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "aqandu/aqandu.json"
bq_client = bigquery.Client(project=PROJECT_ID)

from aqandu import utils
elevation_interpolator = utils.setupElevationInterpolator('elevation_map.mat')

from aqandu import api_routes, basic_routes
