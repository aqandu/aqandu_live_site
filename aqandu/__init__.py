import assets
import config
import os
from dotenv import load_dotenv
from flask import Flask
from google.cloud import bigquery
import logging
import time


load_dotenv()
PROJECT_ID = os.getenv("PROJECTID")


logfile = "serve.log"
logging.basicConfig(filename=logfile, level=logging.DEBUG, format = '%(levelname)s: %(filename)s: %(message)s')
logging.info('API server started at %s', time.asctime(time.localtime()))
logging.debug('This message should go to the log file')
logging.warning('And this, too')
logging.error('And non-ASCII stuff, too, like Øresund and Malmö')

app = Flask(__name__)
app.config.from_object(config)
app.config["CACHE_TYPE"] = "simple"
app.config["CACHE_DEFAULT_TIMEOUT"] = 1
cache = Cache(app)
assets.init(app)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "aqandu/aqandu.json"
bq_client = bigquery.Client(project=PROJECT_ID)

from aqandu import utils
# WARNING - current status of the elevation_map.mat files is that longitude is the first coordinate
elevation_interpolator = utils.setupElevationInterpolator('elevation_map.mat')


from aqandu import api_routes, basic_routes
