import assets
import config
import os
from dotenv import load_dotenv
from flask import Flask
from google.cloud import bigquery

load_dotenv()
PROJECT_ID = os.getenv("PROJECTID")

app = Flask(__name__)
app.config.from_object(config)
assets.init(app)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "aqandu/aqandu.json"
bq_client = bigquery.Client(project=PROJECT_ID)

from aqandu import utils
elevation_interpolator = utils.setupElevationInterpolator('elevation_map.mat')

from aqandu import api_routes, basic_routes
