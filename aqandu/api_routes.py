from datetime import datetime
import json
import logging
import os
from aqandu import app, bq_client
from dotenv import load_dotenv
from flask import request

# Load in .env and set the table name
load_dotenv()
SENSOR_TABLE = os.getenv("BIGQ_SENSOR")

# Set up logging
LOGGER = logging.getLogger('aqandu')
uncaught_LOGGER = logging.getLogger('uncaughtExcpt')

# Set the lookup for better returned variable names
varNameLookup = {
    'PM25': '\"pm2.5 (ug/m^3)\"',
    'HUM': '\"Humidity (%)\"',
    'LAT': 'Latitude',
    'LON': 'Longitude',
    'VER': '\"Sensor Version\"',
    'TEMP': '\"Temp (*C)\"',
    'PM1': '\"pm1.0 (ug/m^3)\"',
    'PM10': '\"pm10.0 (ug/m^3)\"',
    'CO': 'CO',
    'NOX': 'NOX'
}

@app.route("/api/rawDataFrom", methods = ["GET"])
def rawDataFrom():
    # Check that the arguments we want exist
    if not validateInputs(['id', 'sensorSource', 'start', 'end', 'show'], request.args):
        LOGGER.info('missing an id and/or a sensorSource and/or a start and/or an end date and/or a show')
        msg = 'Query string is missing an id and/or a sensorSource and/or a start and/or end date and/or a show'
        return msg, 400

    # Get the arguments from the query string
    id = request.args.get('id')
    sensor_source = request.args.get('sensorSource')
    start = request.args.get('start')
    end = request.args.get('end')
    show = request.args.get('show')

    # Check that the data is formatted correctly
    if not validateDate(start) or not validateDate(end):
        resp = jsonify({'message': "Incorrect date format, should be %Y-%m-%dT%H:%M:%SZ, e.g.: 2018-01-03T20:00:00Z"})
        return resp, 400

    # Define the BigQuery query
    query = (
        "SELECT * "
        f"FROM `{SENSOR_TABLE}` "
        "LIMIT 10"
    )

    # Run the query and collect the result
    sensor_list = []
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

@app.route("/api/liveSensors", methods = ["GET"])
def liveSensors():
    # Get the arguments from the query string
    sensor_type = request.args.get('sensorType')

    # Define the BigQuery query
    query = (
        "SELECT * "
        f"FROM `{SENSOR_TABLE}` "
    )

    # Run the query and collect the result
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

@app.route("/api/processedDataFrom", methods = ["GET"])
def processedDataFrom():
    # Get the arguments from the query string
    id = request.args.get('id')
    sensor_source = request.args.get('sensorSource')
    start = request.args.get('start')
    end = request.args.get('end')
    function = request.args.get('function')
    functionArg = request.args.get('functionArg')
    timeInterval = request.args.get('timeInterval')

    # Define the BigQuery query
    query = (
        "SELECT * "
        f"FROM `{SENSOR_TABLE}` "
    )

    # Run the query and collect the result
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

@app.route("/api/lastValue", methods = ["GET"])
def lastValue():
    # Get the arguments from the query string
    field_key = request.args.get('fieldKey')

    # Define the BigQuery query
    query = (
        "SELECT * "
        f"FROM `{SENSOR_TABLE}` "
    )

    # Run the query and collect the result
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

@app.route("/api/contours", methods = ["GET"])
def contours():
    # Get the arguments from the query string
    start = request.args.get('start')
    end = request.args.get('end')

    # Define the BigQuery query
    query = (
        "SELECT * "
        f"FROM `{SENSOR_TABLE}` "
    )

    # Run the query and collect the result
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

@app.route("/api/getLatestContour", methods = ["GET"])
def getLatestContour():
    # Define the BigQuery query
    query = (
        "SELECT * "
        f"FROM `{SENSOR_TABLE}` "
    )

    # Run the query and collect the result
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

@app.route("/api/getEstimatesForLocation", methods = ["GET"])
def getEstimatesForLocation():
    # Get the arguments from the query string
    location_lat = request.args.get('locationLat')
    location_lon = request.args.get('locationLon')
    start = request.args.get('start')
    end = request.args.get('end')

    # Define the BigQuery query
    query = (
        "SELECT * "
        f"FROM `{SENSOR_TABLE}` "
    )

    # Run the query and collect the result
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


# Helper function
def validateInputs(neededInputs, inputs):
    """Check that expected inputs are provided"""
    for anNeededInput in neededInputs:
        if anNeededInput not in inputs:
            return False
    return True

def validateDate(dateString):
    """Check if date string is valid"""
    try:
        if dateString != datetime.strptime(dateString, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%dT%H:%M:%SZ"):
            raise ValueError
        LOGGER.info('{} valid date, no value error'.format(dateString))
        return True
    except ValueError:
        LOGGER.info('{} not a valid date, value error'.format(dateString))
        return False