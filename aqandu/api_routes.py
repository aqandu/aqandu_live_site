from datetime import datetime, timedelta
import json
import os
from aqandu import app, bq_client, bigquery, utils, elevation_interpolator, gaussian_model_utils
from dotenv import load_dotenv
from flask import request, jsonify

# Load in .env and set the table name
load_dotenv()
SENSOR_TABLE = os.getenv("BIGQ_SENSOR")
PROJECTID = os.getenv("PROJECTID")
POLMONID = os.getenv("POLMONID")

# Set the lookup for better returned variable names
varNameLookup = {
    'DEVICE_ID': 'ID',
    'PM25': 'pm25',
    'HUM': '\"Humidity (%)\"',
    'LAT': 'Latitude',
    'LON': 'Longitude',
    'VER': '\"Sensor Version\"',
    'MODEL': '\"Sensor Model\"',
    'TEMP': '\"Temp (*C)\"',
    'PM1': '\"pm1.0 (ug/m^3)\"',
    'PM10': '\"pm10.0 (ug/m^3)\"',
    'CO': 'CO',
    'NOX': 'NOX',
    'TIMESTAMP': 'time'
}

# Example request:
# 127.0.0.1:8080/api/rawDataFrom?id=M30AEA4EF9F88&sensorSource=Purple%20Air&start=2020-03-25T00:23:51Z&end=2020-03-26T00:23:51Z&show=pm25
@app.route("/api/rawDataFrom", methods = ["GET"])
def rawDataFrom():
    # Check that the arguments we want exist
    if not validateInputs(['id', 'sensorSource', 'start', 'end', 'show'], request.args):
        msg = 'Query string is missing an id and/or a sensorSource and/or a start and/or end date and/or a show'
        return msg, 400

    # Get the arguments from the query string
    id = request.args.get('id')
    sensor_source = request.args.get('sensorSource')
    start = request.args.get('start')
    end = request.args.get('end')
    show = request.args.get('show') # Data type (should be pm25)

    # Check that the data is formatted correctly
    if not utils.validateDate(start) or not utils.validateDate(end):
        resp = jsonify({'message': f"Incorrect date format, should be {utils.DATETIME_FORMAT}, e.g.: 2018-01-03T20:00:00Z"})
        return resp, 400

    # Define the BigQuery query
    query = (
        "SELECT PM25, TIMESTAMP "
        f"FROM `{SENSOR_TABLE}` "
        f"WHERE DEVICE_ID = @id "
        f"AND TIMESTAMP >= @start "
        f"AND TIMESTAMP <= @end "
    )

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("id", "STRING", id),
            bigquery.ScalarQueryParameter("start", "TIMESTAMP", start),
            bigquery.ScalarQueryParameter("end", "TIMESTAMP", end),
        ]
    )

    # Run the query and collect the result
    measurements = []
    query_job = bq_client.query(query, job_config=job_config)
    rows = query_job.result()
    for row in rows:
        measurements.append({"pm25": row.PM25, "time": row.TIMESTAMP.strftime(utils.DATETIME_FORMAT)})
    tags = [{
        "ID": id,
        "Sensor Source": sensor_source,
        "SensorModel":"H1.2+S1.0.8",
        "time": datetime.utcnow().strftime(utils.DATETIME_FORMAT)
    }]
    return jsonify({"data": measurements, "tags": tags})

# Example request
# 127.0.0.1:8080/api/liveSensors?sensorType=all
@app.route("/api/liveSensors", methods = ["GET"])
def liveSensors():
    # Get the arguments from the query string
    sensor_type = request.args.get('sensorType')

    # Define the BigQuery query
    query = (
        "SELECT a.* "
        f"FROM `{SENSOR_TABLE}` AS a "
        "INNER JOIN ( "
            "SELECT DEVICE_ID AS ID, max(TIMESTAMP) AS LATEST_MEASUREMENT "
            f"FROM `{SENSOR_TABLE}` "
            "GROUP BY DEVICE_ID "
            ") AS b ON a.DEVICE_ID = b.ID AND a.TIMESTAMP = b.LATEST_MEASUREMENT "
    )

    # Run the query and collect the result
    sensor_list = []
    query_job = bq_client.query(query)
    rows = query_job.result()
    for row in rows:
        sensor_list.append({"ID": str(row.DEVICE_ID),
                            "Latitude": row.LAT,
                            "Longitude": row.LON,
                            "time": row.TIMESTAMP.timestamp() * 1000,
                            "pm1": row.PM1,
                            "pm25": row.PM25,
                            "pm10": row.PM10,
                            "Temperature": row.TEMP,
                            "Humidity": row.HUM,
                            "NOX": row.NOX,
                            "CO": row.CO,
                            "VER": row.VER,
                            "Sensor Source": "DAQ"})

    return jsonify(sensor_list)

# TODO: Fix this route
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

# Example request:
# 127.0.0.1:8080/api/lastValue?fieldKey=pm25
@app.route("/api/lastValue", methods = ["GET"])
def lastValue():
    # Get the arguments from the query string
    field_key = request.args.get('fieldKey')

    # Define the BigQuery query
    query = (
        "SELECT a.* "
        f"FROM `{SENSOR_TABLE}` AS a "
        "INNER JOIN ( "
            "SELECT DEVICE_ID AS ID, max(TIMESTAMP) AS LATEST_MEASUREMENT "
            f"FROM `{SENSOR_TABLE}` "
            "GROUP BY DEVICE_ID "
            ") AS b ON a.DEVICE_ID = b.ID AND a.TIMESTAMP = b.LATEST_MEASUREMENT "
    )

    # Run the query and collect the result
    sensor_list = []
    query_job = bq_client.query(query)
    rows = query_job.result()
    for row in rows:
        sensor_list.append({"ID": str(row.DEVICE_ID),
                            "Latitude": row.LAT,
                            "Longitude": row.LON,
                            "time": row.TIMESTAMP.timestamp() * 1000,
                            "pm1": row.PM1,
                            "pm25": row.PM25,
                            "pm10": row.PM10,
                            "Temperature": row.TEMP,
                            "Humidity": row.HUM,
                            "NOX": row.NOX,
                            "CO": row.CO,
                            "VER": row.VER,
                            "Sensor Source": "DAQ"})

    return jsonify(sensor_list)

# TODO: Fix this route
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

# TODO: Fix this route
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

# TODO: Fix this route
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

def request_model_data_local(lat, lon, radius, start_date, end_date):
    model_data = []
    # get the latest sensor data from each sensor
    query = (
        "SELECT * "
        "FROM ( "
            "SELECT "
                "Altitude," 
                "CO, "
                "Humidity, "
                "ID, "
                "Latitude, "
                "Longitude, "
                "PM10, "
                "PM2_5, "
                "SensorModel, "
                "Temperature, "
                "time, "
                "'AirU' AS Source "
            f"FROM `{PROJECTID}.{POLMONID}.airu_stationary` "
            "UNION ALL "
            "SELECT "
                "Altitude," 
                "CO, "
                "Humidity, "
                "ID, "
                "Latitude, "
                "Longitude, "
                "PM10, "
                "PM2_5, "
                "NULL AS SensorModel, "
                "Temperature, "
                "time, "
                "'DAQ' AS Source "
            f"FROM `{PROJECTID}.{POLMONID}.daq` "
            "UNION ALL "
            "SELECT "
                "Altitude, "
                "NULL AS CO, "
                "Humidity, "
                "ID, "
                "Latitude, "
                "Longitude, "
                "PM10, "
                "PM2_5, "
                "SensorModel, "
                "Temperature, "
                "time, "
                "'Purple Air' AS Source "
            f"FROM `{PROJECTID}.{POLMONID}.purpleair` "
        ") "
        "WHERE SQRT(POW(Latitude - @lat, 2) + POW(Longitude - @lon, 2)) <= @radius "
        "AND time > @start_date AND time < @end_date "
        "ORDER BY time ASC;"
    )

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("lat", "NUMERIC", lat),
            bigquery.ScalarQueryParameter("lon", "NUMERIC", lon),
            bigquery.ScalarQueryParameter("radius", "NUMERIC", radius),
            bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_date),
            bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end_date),
        ]
    )

    query_job = bq_client.query(query, job_config = job_config)

    if query_job.error_result:
        print(query_job.error_result)
        return "Invalid API call - check documentation.", 400
    rows = query_job.result()  # Waits for query to finish

    for row in rows:
        model_data.append({
            "Altitude": row.Altitude,
            "CO": row.CO,
            "Humidity": row.Humidity,
            "ID": row.ID,
            "Latitude": row.Latitude,
            "Longitude": row.Longitude,
            "PM10": row.PM10,
            "PM2_5": row.PM2_5,
            "SensorModel": row.SensorModel,
            "Temperature": row.Temperature,
            "time": row.time,
            "Source": row.Source,
        })

    return model_data

# Example request:
# 127.0.0.1:8080/api/request_model_data?lat=40.7688&lon=-111.8462&radius=1&start_date=2020-03-10T0:0:0&end_date=2020-03-10T0:1:0
@app.route("/api/request_model_data/", methods=['GET'])
def request_model_data():
    query_parameters = request.args
    lat = query_parameters.get('lat')
    lon = query_parameters.get('lon')
    radius = query_parameters.get('radius')
    start_date = f"{query_parameters.get('start_date')} America/Denver"
    end_date = f"{query_parameters.get('end_date')} America/Denver"

    model_data = request_model_data_local(lat, lon, radius, start_date, end_date)
    return jsonify(model_data)

# Example request:
# 127.0.0.1:8080/api/getPredictionsForLocation?lat=40.7688&lon=-111.8462&predictionsperhour=1&start_date=2020-03-10T00:00:00&end_date=2020-03-11T00:00:00
@app.route("/api/getPredictionsForLocation/", methods=['GET'])
def getPredictionsForLocation():
    # Check that the arguments we want exist
    if not validateInputs(['lat', 'lon', 'predictionsperhour', 'start_date', 'end_date'], request.args):
        return 'Query string is missing one or more of lat, lon, predictionsperhour, start_date, end_date', 400

    # step -1, parse query parameters
    try:
        query_lat = float(request.args.get('lat'))
        query_lon = float(request.args.get('lon'))
        query_frequency = float(request.args.get('predictionsperhour'))
    except ValueError:
        return 'lat, lon, predictionsperhour must be floats.', 400

    query_start_date = request.args.get('start_date')
    query_end_date = request.args.get('end_date')

    # Check that the data is formatted correctly
    if not utils.validateDate(query_start_date) or not utils.validateDate(query_end_date):
        resp = jsonify({'message': f"Incorrect date format, should be {utils.DATETIME_FORMAT}, e.g.: 2018-01-03T20:00:00Z"})
        return resp, 400

    query_start_datetime = utils.parseDateString(query_start_date)
    query_end_datetime = utils.parseDateString(query_end_date)

    print(f'Query parameters: lat={query_lat} lon={query_lon} start_date={query_start_datetime} end_date={query_end_datetime} frequency={query_frequency}')

    # step 0, load up the bounding box from file and check that request is within it
    bounding_box_vertices = utils.loadBoundingBox('bounding_box.csv')
    print(f'Loaded {len(bounding_box_vertices)} bounding box vertices.')

    if not utils.isQueryInBoundingBox(bounding_box_vertices, query_lat, query_lon):
        response = jsonify(f'400 Bad Request: The query location is outside of the bounding box.')
        response.status_code = 400
        return response

    # step 1, load up correction factors from file
    correction_factors = utils.loadCorrectionFactors('correction_factors.csv')
    print(f'Loaded {len(correction_factors)} correction factors.')

    # step 2, load up length scales from file
    length_scales = utils.loadLengthScales('length_scales.csv')
    print(f'Loaded {len(length_scales)} length scales.')

    print('Loaded length scales:', length_scales, '\n')
    length_scales = utils.getScalesInTimeRange(length_scales, query_start_datetime, query_end_datetime)
    if len(length_scales) < 1:
        response = jsonify(f'400 Bad Request: Incorrent number of length scales({len(length_scales)}) found in between {query_start_datetime} and {query_end_datetime}')
        response.status_code = 400
        return response
    
    latlon_length_scale = length_scales[0]['latlon']
    elevation_length_scale = length_scales[0]['elevation']
    time_length_scale = length_scales[0]['time']

    print(f'Using length scales: latlon={latlon_length_scale} elevation={elevation_length_scale} time={time_length_scale}')

    # step 3, query relevent data
    NUM_METERS_IN_MILE = 1609.34
    radius = latlon_length_scale/NUM_METERS_IN_MILE # convert meters to miles for db query

    radius = latlon_length_scale / 70000
    sensor_data = request_model_data_local(
                    lat=query_lat, 
                    lon=query_lon, 
                    radius=radius, 
                    start_date=query_start_datetime - timedelta(hours=time_length_scale), 
                    end_date=query_end_datetime + timedelta(hours=time_length_scale))

    unique_sensors = {datum['ID'] for datum in sensor_data}
    print(f'Loaded {len(sensor_data)} data points for {len(unique_sensors)} unique devices from bgquery.')

    # step 3.5, convert lat/lon to UTM coordinates
    try:
        utils.convertLatLonToUTM(sensor_data)
    except ValueError as err:
        response = jsonify(f'400 Bad Request: {str(err)}')
        response.status_code = 400
        return response

    sensor_data = [datum for datum in sensor_data if datum['zone_num'] == 12]

    unique_sensors = {datum['ID'] for datum in sensor_data}
    print(f'After removing points with zone num != 12: {len(sensor_data)} data points for {len(unique_sensors)} unique devices.')

    # Step 4, parse sensor type from the version
    sensor_source_to_type = {'AirU': '3003', 'Purple Air': '5003', 'DAQ': '5003'}
    for datum in sensor_data:
        datum['type'] = sensor_source_to_type[datum['Source']]
            
    print(f'Fields: {sensor_data[0].keys()}')

    # step 4.5, Data Screening
    print('Screening data')
    sensor_data = utils.removeInvalidSensors(sensor_data)

    # step 5, apply correction factors to the data
    for datum in sensor_data:
        datum['PM2_5'] = utils.applyCorrectionFactor(correction_factors, datum['time'], datum['PM2_5'], datum['type'])

    # step 6, add elevation values to the data
    for datum in sensor_data:
        if 'Altitude' not in datum:
            datum['Altitude'] = elevation_interpolator([datum['Latitude']], [datum['Longitude']])[0]

    # step 7, Create Model
    model, time_offset = gaussian_model_utils.createModel(sensor_data, latlon_length_scale, elevation_length_scale, time_length_scale)

    # step 8, get predictions from model
    query_dates = utils.interpolateQueryDates(query_start_datetime, query_end_datetime, query_frequency)
    query_elevation = elevation_interpolator([query_lat], [query_lon])[0]
    predictions = gaussian_model_utils.predictUsingModel(model, query_lat, query_lon, query_elevation, query_dates, time_offset)

    return jsonify(predictions)


# Helper function
def validateInputs(neededInputs, inputs):
    """Check that expected inputs are provided"""
    for anNeededInput in neededInputs:
        if anNeededInput not in inputs:
            return False
    return True

