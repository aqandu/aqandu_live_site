import assets
import config
import os
from dotenv import load_dotenv
from flask import Flask
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()
PROJECT_ID = os.getenv("PROJECTID")

app = Flask(__name__)
app.config.from_object(config)
assets.init(app)

credentials = service_account.Credentials.from_service_account_file("aqandu/aqandu.json")
bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

from aqandu import basic_routes
from aqandu import api_routes
