import json
import os
from aqandu import app, bq_client
from dotenv import load_dotenv
from flask import render_template

load_dotenv()
SENSOR_TABLE = os.getenv("BIGQ_SENSOR")

@app.route("/test")
def test():
    sensor_list = []

    query = f"SELECT * FROM `{SENSOR_TABLE}` WHERE DATE(TIMESTAMP) = '2020-03-05' LIMIT 10 "
    query_job = bq_client.query(query)
    rows = query_job.result()
    for row in rows:
        sensor_list.append({"DEVICE_ID": str(row.DEVICE_ID),
                            "LAT": row.LAT,
                            "LON": row.LON,
                            "TIMESTAMP": str(row.TIMESTAMP),
                            "PM1": row.PM1,
                            "PM25": row.PM25,
                            "PM10": row.PM10,
                            "TEMP": row.TEMP,
                            "HUM": row.HUM,
                            "NOX": row.NOX,
                            "CO": row.CO,
                            "VER": row.VER})
    json_sensors = json.dumps(sensor_list, indent=4)
    return json_sensors