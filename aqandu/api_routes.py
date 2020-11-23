from datetime import datetime, timedelta
import os
from aqandu import app, bq_client, bigquery, utils, elevation_interpolator, gaussian_model_utils
from dotenv import load_dotenv
from flask import request, jsonify
# regular expression stuff for decoding quer 
import re
import numpy as np

# Load in .env and set the table name
load_dotenv()  # Required for compatibility with GCP, can't use pipenv there
AIRU_TABLE_ID = os.getenv("AIRU_TABLE_ID")
PURPLEAIR_TABLE_ID = os.getenv("PURPLEAIR_TABLE_ID")
DAQ_TABLE_ID = os.getenv("DAQ_TABLE_ID")
SOURCE_TABLE_MAP = {
    "AirU": AIRU_TABLE_ID,
    "PurpleAir": PURPLEAIR_TABLE_ID,
    "DAQ": DAQ_TABLE_ID,
}
VALID_SENSOR_SOURCES = ["AirU", "PurpleAir", "DAQ", "all"]
TIME_KERNEL_FACTOR_PADDING = 3.0
SPACE_KERNEL_FACTOR_PADDING = 2.


@app.route("/api/rawDataFrom", methods=["GET"])
def rawDataFrom():
    # Get the arguments from the query string
    id = request.args.get('id')
    sensor_source = request.args.get('sensorSource')
    start = request.args.get('start')
    end = request.args.get('end')

    # Check ID is valid
    if id == "" or id == "undefined":
        msg = "id is invalid. It must be a string that is not '' or 'undefined'."
        return msg, 400

    # Check that the arguments we want exist
    if sensor_source not in VALID_SENSOR_SOURCES:
        msg = f"sensor_source is invalid. It must be one of {VALID_SENSOR_SOURCES}"
        return msg, 400

    # Check that the data is formatted correctly
    if not utils.validateDate(start) or not utils.validateDate(end):
        msg = "Incorrect date format, should be {utils.DATETIME_FORMAT}, e.g.: 2018-01-03T20:00:00Z"
        return msg, 400

    # Define the BigQuery query
    query = f"""
        SELECT
            PM2_5,
            time
        FROM `{SOURCE_TABLE_MAP[sensor_source]}`
        WHERE ID = @id
            AND time >= @start
            AND time <= @end
        ORDER BY time
    """

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
        measurements.append({"PM2_5": row.PM2_5, "time": row.time.strftime(utils.DATETIME_FORMAT)})
    tags = [{
        "ID": id,
        "SensorSource": sensor_source,
        "SensorModel": "H1.2+S1.0.8",
        "time": datetime.utcnow().strftime(utils.DATETIME_FORMAT)
    }]
    return jsonify({"data": measurements, "tags": tags})


@app.route("/api/liveSensors", methods=["GET"])
def liveSensors():
    # Get the arguments from the query string
    sensor_source = request.args.get('sensorSource')

    # Check that sensor_source is valid
    if sensor_source not in VALID_SENSOR_SOURCES:
        msg = f"sensor_source is invalid. It must be one of {VALID_SENSOR_SOURCES}"
        return msg, 400

    # Define the BigQuery query
    one_hour_ago = datetime.utcnow() - timedelta(hours=1)  # AirU + PurpleAir sensors have reported in the last hour
    three_hours_ago = datetime.utcnow() - timedelta(hours=3)  # DAQ sensors have reported in the 3 hours
    query_list = []

    if sensor_source == "AirU" or sensor_source == "all":
        query_list.append(
            f"""(
                SELECT a.ID, time, PM2_5, Latitude, Longitude, SensorModel, 'AirU' as SensorSource
                FROM `{AIRU_TABLE_ID}` as a
                INNER JOIN (
                    SELECT ID, max(time) AS LATEST_MEASUREMENT
                    FROM `{AIRU_TABLE_ID}`
                    WHERE time >= '{str(one_hour_ago)}'
                    GROUP BY ID
                ) AS b ON a.ID = b.ID AND a.time = b.LATEST_MEASUREMENT
                WHERE time >= '{str(one_hour_ago)}'
            )"""
        )

    if sensor_source == "PurpleAir" or sensor_source == "all":
        query_list.append(
            f"""(
                SELECT a.ID, time, PM2_5, Latitude, Longitude, '' as SensorModel, 'PurpleAir' as SensorSource
                FROM `{PURPLEAIR_TABLE_ID}` as a
                INNER JOIN (
                    SELECT ID, max(time) AS LATEST_MEASUREMENT
                    FROM `{PURPLEAIR_TABLE_ID}`
                    WHERE time >= '{str(one_hour_ago)}'
                    GROUP BY ID
                ) AS b ON a.ID = b.ID AND a.time = b.LATEST_MEASUREMENT
                WHERE time >= '{str(one_hour_ago)}'
            )"""
        )

    if sensor_source == "DAQ" or sensor_source == "all":
        query_list.append(
            f"""(
                SELECT a.ID, time, PM2_5, Latitude, Longitude, '' as SensorModel, 'DAQ' as SensorSource
                FROM `{DAQ_TABLE_ID}` as a
                INNER JOIN (
                    SELECT ID, max(time) AS LATEST_MEASUREMENT
                    FROM `{DAQ_TABLE_ID}`
                    WHERE time >= '{str(three_hours_ago)}'
                    GROUP BY ID
                ) AS b ON a.ID = b.ID AND a.time = b.LATEST_MEASUREMENT
                WHERE time >= '{str(three_hours_ago)}'
            )"""
        )

    # Build the actual query from the list of options
    query = " UNION ALL ".join(query_list)

    # Run the query and collect the result
    sensor_list = []
    query_job = bq_client.query(query)
    rows = query_job.result()
    for row in rows:
        sensor_list.append(
            {
                "ID": str(row.ID),
                "Latitude": row.Latitude,
                "Longitude": row.Longitude,
                "time": row.time,
                "PM2_5": row.PM2_5,
                "SensorModel": row.SensorModel,
                "SensorSource": row.SensorSource,
            }
        )

    return jsonify(sensor_list)


@app.route("/api/getEstimateMap", methods=["GET"])
def getEstimateMap():

    # this species grid positions should be interpolated in UTM coordinates
    # right now (Nov 2020) this is not supported.
    # might be used later in order to build grids of data in UTM coordinates -- this would depend on what the display/visualization code needs
    # after investigation, most vis toolkits support lat-lon grids of data. 
    if "UTM" in request.args:
        UTM = True
    else:
        UTM = False

    # Get the arguments from the query string
    if not UTM:
        try:
            lat_hi = float(request.args.get('lat_hi'))
            lat_lo = float(request.args.get('lat_lo'))
            lon_hi = float(request.args.get('lon_hi'))
            lon_lo = float(request.args.get('lon_lo'))
        except ValueError:
            return 'lat, lon, lat_res, be floats in the lat-lon (not UTM) case', 400
        try:
            lat_size = int(request.args.get('lat_size'))
            lon_size = int(request.args.get('lon_size'))
        except ValueError:
            return 'lat, lon, sizes must be ints (not UTM) case', 400

        lat_res = (lat_hi-lat_lo)/float(lat_size)
        lon_res = (lon_hi-lon_lo)/float(lon_size)

    query_date = request.args.get('date')
    if not utils.validateDate(query_date):
        msg = f"Incorrect date format, should be {utils.DATETIME_FORMAT}, e.g.: 2018-01-03T20:00:00Z"
        return msg, 400

    query_datetime = utils.parseDateString(query_date)

    print((
        f"Query parameters: lat_lo={lat_lo} lat_hi={lat_hi}  lon_lo={lon_lo} lon_hi={lon_hi} lat_res={lat_res} lon_res={lon_res} date={query_datetime}"
    ))

    # step 0, load up the bounding box from file and check that request is within it
    bounding_box_vertices = utils.loadBoundingBox('bounding_box.csv')
    print(f'Loaded {len(bounding_box_vertices)} bounding box vertices.')

    if not (
        utils.isQueryInBoundingBox(bounding_box_vertices, lat_lo, lon_lo) and
        utils.isQueryInBoundingBox(bounding_box_vertices, lat_lo, lon_hi) and
        utils.isQueryInBoundingBox(bounding_box_vertices, lat_hi, lon_hi) and
        utils.isQueryInBoundingBox(bounding_box_vertices, lat_hi, lon_lo)):
        return 'One of the query locations is outside of the bounding box for the database', 400

    # step 1, load up correction factors from file
    correction_factors = utils.loadCorrectionFactors('correction_factors.csv')
    print(f'Loaded {len(correction_factors)} correction factors.')

    # step 2, load up length scales from file
    length_scales = utils.loadLengthScales('length_scales.csv')
    print(f'Loaded {len(length_scales)} length scales.')

    print('Loaded length scales:', length_scales, '\n')
    length_scales = utils.getScalesInTimeRange(length_scales, query_datetime, query_datetime)
    if len(length_scales) < 1:
        msg = (
            f"Incorrect number of length scales({len(length_scales)}) "
            f"found in between {query_start_datetime} and {query_end_datetime}"
        )
        return msg, 400

    latlon_length_scale = length_scales[0]['latlon']
    elevation_length_scale = length_scales[0]['elevation']
    time_length_scale = length_scales[0]['time']

    print(
        f'Using length scales: latlon={latlon_length_scale} elevation={elevation_length_scale} time={time_length_scale}'
    )

  # step 3, query relevent data
  # for this compute a circle center at the query volume.  Radius is related to lenth scale + the size fo the box.
    lat = (lat_lo + lat_hi)/2.0
    lon = (lon_lo + lon_hi)/2.0
#    NUM_METERS_IN_MILE = 1609.34
#    radius = latlon_length_scale / NUM_METERS_IN_MILE  # convert meters to miles for db query

    UTM_N_hi, UTM_E_hi, zone_num_hi, zone_let_hi = utils.latlonToUTM(lat_hi, lon_hi)
    UTM_N_lo, UTM_E_lo, zone_num_lo, zone_let_lo = utils.latlonToUTM(lat_lo, lon_lo)
# compute the lenght of the diagonal of the lat-lon box.  This units here are **meters**
    lat_diff = UTM_N_hi - UTM_N_lo
    lon_diff = UTM_E_hi - UTM_E_lo
    radius = SPACE_KERNEL_FACTOR_PADDING*latlon_length_scale + np.sqrt(lat_diff**2 + lon_diff**2)/2.0

    if not ((zone_num_lo == zone_num_hi) and (zone_let_lo == zone_let_hi)):
        return 'Requested region spans UTM zones', 400        


#    radius = latlon_length_scale / 70000 + box_diag/2.0
    sensor_data = request_model_data_local(
        lats=lat,
        lons=lon,
        radius=radius,
        start_date=query_datetime - TIME_KERNEL_FACTOR_PADDING*timedelta(hours=time_length_scale),
        end_date=query_datetime + TIME_KERNEL_FACTOR_PADDING*timedelta(hours=time_length_scale))

    unique_sensors = {datum['ID'] for datum in sensor_data}
    print(f'Loaded {len(sensor_data)} data points for {len(unique_sensors)} unique devices from bgquery.')

    # step 3.5, convert lat/lon to UTM coordinates
    try:
        utils.convertLatLonToUTM(sensor_data)
    except ValueError as err:
        return f'{str(err)}', 400

    # Step 4, parse sensor type from the version
    sensor_source_to_type = {'AirU': '3003', 'PurpleAir': '5003', 'DAQ': '0000'}
    for datum in sensor_data:
        datum['type'] = sensor_source_to_type[datum['SensorSource']]

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
            datum['Altitude'] = elevation_interpolator([datum['Longitude']],[datum['Latitude']])[0]

    # step 7, Create Model
    model, time_offset = gaussian_model_utils.createModel(
        sensor_data, latlon_length_scale, elevation_length_scale, time_length_scale)

    
# step 8, build the grid of query locations
    if not UTM:
        lon_vector, lat_vector = utils.interpolateQueryLocations(lat_lo, lat_hi, lon_lo, lon_hi, lat_res, lon_res)
#        locations_UTM = utm.from_latlon(query_locations_latlon)
    else:
        # step 7.5, convert query box to UTM -- do the two far corners and hope for the best
#        lat_lo_UTM, lon_lo_UTM, zone_num_lo, zone_let_lo = utils.latlonToUTM(lat_lo, lon_lo)
#        lat_hi_UTM, lon_hi_UTM, zone_num_hi, zone_let_hi = utils.latlonToUTM(lat_hi, lon_hi)
#        query_locations_UTM = utils.interpolateQueryLocations(lat_lo_UTM, lat_hi_UTM, lon_lo_UTM, lon_hi_UTM, spatial_res)
#        query_locations_
        return 'UTM not yet supported', 400


#    locations_lat = locations_lat.flatten()
#    locations_lon = locations_lon.flatten()
#    print(locations_lat.shape)
#    print(locations_lon.shape)
    elevations = elevation_interpolator(lon_vector, lat_vector)
    print(elevations.shape)

    locations_lon, locations_lat = np.meshgrid(lon_vector, lat_vector)
    # print("B")
    # print(locations_lat)
    # print(locations_lon)

    locations_lat = locations_lat.flatten()
    locations_lon = locations_lon.flatten()
    elevations = elevations.flatten()
    # print("C")
    # print(locations_lat)
    # print(locations_lon)

    # print("D")
    # print(locations_lat.reshape((lat_size, lon_size)))
    # print(locations_lon.reshape((lat_size, lon_size)))


    yPred, yVar = gaussian_model_utils.estimateUsingModel(
        model, locations_lat, locations_lon, elevations, [query_datetime], time_offset)

    elevations = (elevations.reshape((lat_size, lon_size))).tolist()
    yPred = yPred.reshape((lat_size, lon_size))
    yVar = yVar.reshape((lat_size, lon_size))
    estimates = yPred.tolist()
    variances = yVar.tolist()
    print(lat_vector.tolist())
    print(lon_vector.tolist())
    return jsonify({"Elevations":elevations, "PM2.5":estimates, "PM2.5 variance":variances, "Latitudes":lat_vector.tolist(), "Longitudes":lon_vector.tolist()})
    

@app.route("/api/timeAggregatedDataFrom", methods=["GET"])
def timeAggregatedDataFrom():
    # Get the arguments from the query string
    id = request.args.get('id')
    sensor_source = request.args.get('sensorSource')
    start = request.args.get('start')
    end = request.args.get('end')
    function = request.args.get('function')
    timeInterval = request.args.get('timeInterval')  # Time interval in minutes

    SQL_FUNCTIONS = {
        "mean": "AVG",
        "min": "MIN",
        "max": "MAX",
    }

    # Check ID is valid
    if id == "" or id == "undefined":
        msg = "id is invalid. It must be a string that is not '' or 'undefined'."
        return msg, 400

    # Check that sensor_source is valid
    if sensor_source not in VALID_SENSOR_SOURCES:
        msg = f"sensor_source is invalid. It must be one of {VALID_SENSOR_SOURCES}"
        return msg, 400

    # Check aggregation function is valid
    if function not in SQL_FUNCTIONS:
        msg = f"function is not in {SQL_FUNCTIONS.keys()}"
        return msg, 400

    # Check that the data is formatted correctly
    if not utils.validateDate(start) or not utils.validateDate(end):
        msg = "Incorrect date format, should be {utils.DATETIME_FORMAT}, e.g.: 2018-01-03T20:00:00Z"
        return msg, 400

    # Define the BigQuery query
    tables_list = []
    if sensor_source == "AirU" or sensor_source == "all":
        tables_list.append(
            f"""(
                SELECT ID, time, PM2_5, Latitude, Longitude, SensorModel, 'AirU' as SensorSource
                FROM `{AIRU_TABLE_ID}`
                WHERE time >= @start
            )"""
        )

    if sensor_source == "PurpleAir" or sensor_source == "all":
        tables_list.append(
            f"""(
                SELECT ID, time, PM2_5, Latitude, Longitude, '' as SensorModel, 'PurpleAir' as SensorSource
                FROM `{PURPLEAIR_TABLE_ID}`
                WHERE time >= @start
            )"""
        )

    if sensor_source == "DAQ" or sensor_source == "all":
        tables_list.append(
            f"""(
                SELECT ID, time, PM2_5, Latitude, Longitude, '' as SensorModel, 'DAQ' as SensorSource
                FROM `{DAQ_TABLE_ID}`
                WHERE time >= @start
            )"""
        )

    query = f"""
        WITH
            intervals AS (
                SELECT
                    TIMESTAMP_ADD(@start, INTERVAL @interval * num MINUTE) AS lower,
                    TIMESTAMP_ADD(@start, INTERVAL @interval * 60* (1 + num) - 1 SECOND) AS upper
                FROM UNNEST(GENERATE_ARRAY(0,  DIV(TIMESTAMP_DIFF(@end, @start, MINUTE) , @interval))) AS num
            )
        SELECT
            CASE WHEN {SQL_FUNCTIONS.get(function)}(PM2_5) IS NOT NULL
                THEN {SQL_FUNCTIONS.get(function)}(PM2_5)
                ELSE 0
                END AS PM2_5,
            upper
        FROM intervals
            JOIN (
            {' UNION ALL '.join(tables_list)}
        ) sensors
            ON sensors.time BETWEEN intervals.lower AND intervals.upper
        WHERE ID = @id
        GROUP BY upper
        ORDER BY upper
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("id", "STRING", id),
            bigquery.ScalarQueryParameter("start", "TIMESTAMP", start),
            bigquery.ScalarQueryParameter("end", "TIMESTAMP", end),
            bigquery.ScalarQueryParameter("interval", "INT64", timeInterval),
        ]
    )

    # Run the query and collect the result
    measurements = []
    query_job = bq_client.query(query, job_config=job_config)
    rows = query_job.result()
    for row in rows:
        measurements.append({"PM2_5": row.PM2_5, "time": row.upper.strftime(utils.DATETIME_FORMAT)})

    tags = [{
        "ID": id,
        "SensorSource": sensor_source,
        "SensorModel": "H1.2+S1.0.8",
        "time": datetime.utcnow().strftime(utils.DATETIME_FORMAT)
    }]
    return jsonify({"data": measurements, "tags": tags})


# submit a query for a range of values
# Ross Nov 2020
# this has been consolidate and generalized so that multiple api calls can use the same query code
def submit_sensor_query(lat_lo, lat_hi, lon_lo, lon_hi, start_date, end_date):
    query = f"""
    SELECT *
    FROM
    (
        (
            SELECT ID, time, PM2_5, Latitude, Longitude, SensorModel, 'AirU' as SensorSource
            FROM `{AIRU_TABLE_ID}`
            WHERE time > @start_date AND time < @end_date
        )
        UNION ALL
        (
            SELECT ID, time, PM2_5, Latitude, Longitude, "" as SensorModel, 'PurpleAir' as SensorSource
            FROM `{PURPLEAIR_TABLE_ID}`
            WHERE time > @start_date AND time < @end_date
        )
        UNION ALL
        (
            SELECT ID, time, PM2_5, Latitude, Longitude, '' as SensorModel, 'DAQ' as SensorSource
            FROM `{DAQ_TABLE_ID}`
            WHERE time > @start_date AND time < @end_date
        )
    ) WHERE (Latitude <= @lat_hi) AND (Latitude >= @lat_lo) AND (Longitude <= @lon_hi) AND (Longitude >= @lon_lo) AND time > @start_date AND time < @end_date
    ORDER BY time ASC
    """

    
# Old code that does not compute distance correctly
#    WHERE SQRT(POW(Latitude - @lat, 2) + POW(Longitude - @lon, 2)) <= @radius
#    AND time > @start_date AND time < @end_date
#    ORDER BY time ASC
#    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            # bigquery.ScalarQueryParameter("lat", "NUMERIC", lat),
            # bigquery.ScalarQueryParameter("lon", "NUMERIC", lon),
            # bigquery.ScalarQueryParameter("radius", "NUMERIC", radius),
            bigquery.ScalarQueryParameter("start_date", "TIMESTAMP", start_date),
            bigquery.ScalarQueryParameter("end_date", "TIMESTAMP", end_date),
            bigquery.ScalarQueryParameter("lat_lo", "NUMERIC", lat_lo),
            bigquery.ScalarQueryParameter("lat_hi", "NUMERIC", lat_hi),
            bigquery.ScalarQueryParameter("lon_lo", "NUMERIC", lon_lo),
            bigquery.ScalarQueryParameter("lon_hi", "NUMERIC", lon_hi),
        ]
    )

    query_job = bq_client.query(query, job_config=job_config)

    if query_job.error_result:
        print(query_job.error_result)
        return "Invalid API call - check documentation.", 400
    sensor_data = query_job.result()  # Waits for query to finish

    return(sensor_data)


# could do an ellipse in lat/lon around the point using something like this
#WHERE SQRT(POW(Latitude - @lat, 2) + POW(Longitude - @lon, 2)) <= @radius
#    AND time > @start_date AND time < @end_date
#    ORDER BY time ASC
# Also could do this by spherical coordinates on the earth -- however we use a lat-lon box to save compute time on the BigQuery server

# radius should be in *meters*!!!
# this has been modified so that it now takes an array of lats/lons
# the radius parameter is not implemented in a precise manner -- rather it is converted to a lat-lon bounding box and all within that box are returned
# there could be an additional culling of sensors outside the radius done here after the query - if the radius parameter needs to be precise. 
def request_model_data_local(lats, lons, radius, start_date, end_date):
    model_data = []
    # get the latest sensor data from each sensor
    # Modified by Ross for
    ## using a bounding box in lat-lon
    if isinstance(lats, (float)):
            if isinstance(lons, (float)):
                    lat_lo, lat_hi, lon_lo, lon_hi = utils.latlonBoundingBox(lats, lons, radius)
            else:
                    return "lats,lons data structure misalignment in request sensor data", 400
    elif (isinstance(lats, (np.ndarray)) and isinstance(lons, (np.ndarray))):
        if not lats.shape == lons.shape:
            return "lats,lons data data size error", 400
        else:
            num_points = lats.shape[0]
            lat_lo, lat_hi, lon_lo, lon_hi = utils.latlonBoundingBox(lats[0], lons[0], radius)
            for i in range(1, num_points):
                lat_lo, lat_hi, lon_lo, lon_hi = utils.boundingBoxUnion((utils.latlonBoundingBox(lats[i], lons[i], radius)), (lat_lo, lat_hi, lon_lo, lon_hi))
    else:
        return "lats,lons data structure misalignment in request sensor data", 400               
   
    rows = submit_sensor_query(lat_lo, lat_hi, lon_lo, lon_hi, start_date, end_date)
    
    for row in rows:
        model_data.append({
            "ID": str(row.ID),
            "Latitude": row.Latitude,
            "Longitude": row.Longitude,
            "time": row.time,
            "PM2_5": row.PM2_5,
            "SensorModel": row.SensorModel,
            "SensorSource": row.SensorSource,
        })

    return model_data


# returns sensor data for a range of times within a distance radius (meters) of the lat-lon location.  
@app.route("/api/request_model_data", methods=['GET'])
def request_model_data():
    query_parameters = request.args
    try:
        lat = float(query_parameters.get('lat'))
        lon = float(query_parameters.get('lon'))
        radius = float(query_parameters.get('radius'))
    except ValueError:
        return 'lat, lon, radius, must be floats.', 400

    start_date = query_parameters.get('start_date')
    end_date = query_parameters.get('end_date')
    print("model requuest api with " + str(lat) + ":" + str(lon) + " and radius " + str(radius) + " and start " + str(start_date) + " and end " + str(end_date))
    # must format these for database
    start_datetime = utils.parseDateString(start_date)
    end_datetime = utils.parseDateString(end_date)
    model_data = request_model_data_local(lat, lon, radius, start_datetime, end_datetime)
    return jsonify(model_data)

# get estimates within a time frame for a single location
@app.route("/api/getEstimatesForLocation", methods=['GET'])
def getEstimatesForLocation():
    # Check that the arguments we want exist
    # if not validateInputs(['lat', 'lon', 'estimatesperhour', 'start_date', 'end_date'], request.args):
    #     return 'Query string is missing one or more of lat, lon, estimatesperhour, start_date, end_date', 400

    # step -1, parse query parameters
    try:
        query_lat = float(request.args.get('lat'))
        query_lon = float(request.args.get('lon'))
        query_rate = float(request.args.get('estimaterate'))
    except ValueError:
        return 'lat, lon, estimaterate must be floats.', 400

    query_start_date = request.args.get('start_date')
    query_end_date = request.args.get('end_date')

    # Check that the data is formatted correctly
    if not utils.validateDate(query_start_date) or not utils.validateDate(query_end_date):
        msg = f"Incorrect date format, should be {utils.DATETIME_FORMAT}, e.g.: 2018-01-03T20:00:00Z"
        return msg, 400

    query_start_datetime = utils.parseDateString(query_start_date)
    query_end_datetime = utils.parseDateString(query_end_date)

    print((
        f"Query parameters: lat={query_lat} lon={query_lon} start_date={query_start_datetime}"
        f" end_date={query_end_datetime} estimaterate={query_rate}"
    ))

    # step 0, load up the bounding box from file and check that request is within it
    bounding_box_vertices = utils.loadBoundingBox('bounding_box.csv')
    print(f'Loaded {len(bounding_box_vertices)} bounding box vertices.')

    if not utils.isQueryInBoundingBox(bounding_box_vertices, query_lat, query_lon):
        return 'The query location is outside of the bounding box.', 400

    # step 1, load up correction factors from file
    correction_factors = utils.loadCorrectionFactors('correction_factors.csv')
    print(f'Loaded {len(correction_factors)} correction factors.')

    # step 2, load up length scales from file
    length_scales = utils.loadLengthScales('length_scales.csv')
    print(f'Loaded {len(length_scales)} length scales.')

    print('Loaded length scales:', length_scales, '\n')
    length_scales = utils.getScalesInTimeRange(length_scales, query_start_datetime, query_end_datetime)
    if len(length_scales) < 1:
        msg = (
            f"Incorrect number of length scales({len(length_scales)}) "
            f"found in between {query_start_datetime} and {query_end_datetime}"
        )
        return msg, 400

    latlon_length_scale = length_scales[0]['latlon']
    elevation_length_scale = length_scales[0]['elevation']
    time_length_scale = length_scales[0]['time']

    print(
        f'Using length scales: latlon={latlon_length_scale} elevation={elevation_length_scale} time={time_length_scale}'
    )

    # step 3, query relevent data

# these conversions were when we were messing around with specifying radius in miles and so forth.      
#    NUM_METERS_IN_MILE = 1609.34
#    radius = latlon_length_scale / NUM_METERS_IN_MILE  # convert meters to miles for db query

#    radius = latlon_length_scale / 70000


# radius is in meters, as is the length scale and UTM.    
    radius = SPACE_KERNEL_FACTOR_PADDING*latlon_length_scale

    sensor_data = request_model_data_local(
        lats=query_lat,
        lons=query_lon,
        radius=radius,
        start_date=query_start_datetime - timedelta(hours=TIME_KERNEL_FACTOR_PADDING*time_length_scale),
        end_date=query_end_datetime + timedelta(hours=TIME_KERNEL_FACTOR_PADDING*time_length_scale))

    print("start date")
    print(query_start_datetime - timedelta(hours=4.0*time_length_scale))
    print("end date")
    print(query_end_datetime + timedelta(hours=4.0*time_length_scale))
    

    unique_sensors = {datum['ID'] for datum in sensor_data}
    print(f'Loaded {len(sensor_data)} data points for {len(unique_sensors)} unique devices from bgquery.')

    # step 3.5, convert lat/lon to UTM coordinates
    try:
        utils.convertLatLonToUTM(sensor_data)
    except ValueError as err:
        return f'{str(err)}', 400

    sensor_data = [datum for datum in sensor_data if datum['zone_num'] == 12]

    unique_sensors = {datum['ID'] for datum in sensor_data}
    print((
        "After removing points with zone num != 12: "
        f"{len(sensor_data)} data points for {len(unique_sensors)} unique devices."
    ))

    # Step 4, parse sensor type from the version
    sensor_source_to_type = {'AirU': '3003', 'PurpleAir': '5003', 'DAQ': '0000'}
# DAQ does not need a correction factor
    for datum in sensor_data:
        datum['type'] =  sensor_source_to_type[datum['SensorSource']]

    print(f'Fields: {sensor_data[0].keys()}')

    # step 4.5, Data Screening
    print('Screening data')
    sensor_data = utils.removeInvalidSensors(sensor_data)

    # step 5, apply correction factors to the data
    for datum in sensor_data:
        datum['PM2_5'] = utils.applyCorrectionFactor(correction_factors, datum['time'], datum['PM2_5'], datum['type'])

    # step 6, add elevation values to the data
    # NOTICE - the elevation object takes locations in the form "lon-lat"
    for datum in sensor_data:
        if 'Altitude' not in datum:
            datum['Altitude'] = elevation_interpolator([datum['Longitude']],[datum['Latitude']])[0]

    # step 7, Create Model
    model, time_offset = gaussian_model_utils.createModel(
        sensor_data, latlon_length_scale, elevation_length_scale, time_length_scale, save_matrices=True)

    # step 8, get estimates from model
    query_dates = utils.interpolateQueryDates(query_start_datetime, query_end_datetime, query_rate)
    # NOTICE - the elevation object takes locations in the form "lon-lat"
    query_elevation = elevation_interpolator(np.array([query_lon]), np.array([query_lat]))[0]
    yPred, yVar = gaussian_model_utils.estimateUsingModel(
        model, np.array([query_lat]), np.array([query_lon]), query_elevation, query_dates, time_offset, save_matrices=True)

# convert the arrays to lists of floats
    yPred = yPred.tolist()
    yVar = yVar.tolist()
    
    estimates = [
        {'PM2_5': pred, 'variance': var, 'datetime': date.strftime('%Y-%m-%d %H:%M:%S%z'), 'Latitude': query_lat, 'Longitude': query_lon, 'Elevation': query_elevation}
        for pred, var, date in zip(yPred, yVar, query_dates)
        ]

    return jsonify(estimates)


# get estimates within a time frame for a multiple locations, given as a list of lat/lons in the query parameters
### this allows multi lats/lons to be specified.  
@app.route("/api/getEstimatesForLocations", methods=['GET'])
def getEstimatesForLocations():
    # Check that the arguments we want exist
    # if not validateInputs(['lat', 'lon', 'estimatesperhour', 'start_date', 'end_date'], request.args):
    #     return 'Query string is missing one or more of lat, lon, estimatesperhour, start_date, end_date', 400

    # step -1, parse query parameters
    try:
        query_rate = float(request.args.get('estimaterate'))
    except ValueError:
        return 'estimaterate must be floats.', 400

## regular expression for floats
    regex = '[+-]?[0-9]+\.[0-9]+'
    query_lats = np.array(re.findall(regex,request.args.get('lat'))).astype(np.float)
    query_lons = np.array(re.findall(regex,request.args.get('lon'))).astype(np.float)
    if (query_lats.shape != query_lons.shape):
        return 'lat, lon must be equal sized arrays of floats:'+str(query_lats)+' ; ' + str(query_lons), 400

    num_locations = query_lats.shape[0]

    query_start_date = request.args.get('start_date')
    query_end_date = request.args.get('end_date')

    # Check that the data is formatted correctly
    if not utils.validateDate(query_start_date) or not utils.validateDate(query_end_date):
        msg = f"Incorrect date format, should be {utils.DATETIME_FORMAT}, e.g.: 2018-01-03T20:00:00Z"
        return msg, 400

    query_start_datetime = utils.parseDateString(query_start_date)
    query_end_datetime = utils.parseDateString(query_end_date)

    print((
        f"Query parameters: lat={query_lats} lon={query_lons} start_date={query_start_datetime}"
        f" end_date={query_end_datetime} estimaterate={query_rate}"
    ))

    # step 0, load up the bounding box from file and check that request is within it
    bounding_box_vertices = utils.loadBoundingBox('bounding_box.csv')
    print(f'Loaded {len(bounding_box_vertices)} bounding box vertices.')

    for i in range(num_locations):
        if not utils.isQueryInBoundingBox(bounding_box_vertices, query_lats[i], query_lons[i]):
            return 'The query location, {query_lats[i]},{query_lons[i]},  is outside of the bounding box.', 400

    # step 1, load up correction factors from file
    correction_factors = utils.loadCorrectionFactors('correction_factors.csv')
    print(f'Loaded {len(correction_factors)} correction factors.')

    # step 2, load up length scales from file
    length_scales = utils.loadLengthScales('length_scales.csv')
    print(f'Loaded {len(length_scales)} length scales.')

    print('Loaded length scales:', length_scales, '\n')
    length_scales = utils.getScalesInTimeRange(length_scales, query_start_datetime, query_end_datetime)
    if len(length_scales) < 1:
        msg = (
            f"Incorrect number of length scales({len(length_scales)}) "
            f"found in between {query_start_datetime} and {query_end_datetime}"
        )
        return msg, 400

    latlon_length_scale = length_scales[0]['latlon']
    elevation_length_scale = length_scales[0]['elevation']
    time_length_scale = length_scales[0]['time']

    print(
        f'Using length scales: latlon={latlon_length_scale} elevation={elevation_length_scale} time={time_length_scale}'
    )

    # step 3, query relevent data

# these conversions were when we were messing around with specifying radius in miles and so forth.      
#    NUM_METERS_IN_MILE = 1609.34
#    radius = latlon_length_scale / NUM_METERS_IN_MILE  # convert meters to miles for db query

#    radius = latlon_length_scale / 70000


# radius is in meters, as is the length scale and UTM.    
    radius = SPACE_KERNEL_FACTOR_PADDING*latlon_length_scale

    sensor_data = request_model_data_local(
        query_lats,
        query_lons,
        radius=radius,
        start_date=query_start_datetime - timedelta(hours=TIME_KERNEL_FACTOR_PADDING*time_length_scale),
        end_date=query_end_datetime + timedelta(hours=TIME_KERNEL_FACTOR_PADDING*time_length_scale))

    

    unique_sensors = {datum['ID'] for datum in sensor_data}
    print(f'Loaded {len(sensor_data)} data points for {len(unique_sensors)} unique devices from bgquery.')

    # step 3.5, convert lat/lon to UTM coordinates
    try:
        utils.convertLatLonToUTM(sensor_data)
    except ValueError as err:
        return f'{str(err)}', 400

    sensor_data = [datum for datum in sensor_data if datum['zone_num'] == 12]

    unique_sensors = {datum['ID'] for datum in sensor_data}
    print((
        "After removing points with zone num != 12: "
        f"{len(sensor_data)} data points for {len(unique_sensors)} unique devices."
    ))

    # Step 4, parse sensor type from the version
    sensor_source_to_type = {'AirU': '3003', 'PurpleAir': '5003', 'DAQ': '0000'}
# DAQ does not need a correction factor
    for datum in sensor_data:
        datum['type'] =  sensor_source_to_type[datum['SensorSource']]

    print(f'Fields: {sensor_data[0].keys()}')

    # step 4.5, Data Screening
    print('Screening data')
    sensor_data = utils.removeInvalidSensors(sensor_data)

    # step 5, apply correction factors to the data
    for datum in sensor_data:
        datum['PM2_5'] = utils.applyCorrectionFactor(correction_factors, datum['time'], datum['PM2_5'], datum['type'])

    # step 6, add elevation values to the data
    # NOTICE - the elevation object takes locations in the form "lon-lat"
    for datum in sensor_data:
        if 'Altitude' not in datum:
            datum['Altitude'] = elevation_interpolator([datum['Longitude']],[datum['Latitude']])[0]

    # step 7, Create Model
    model, time_offset = gaussian_model_utils.createModel(
        sensor_data, latlon_length_scale, elevation_length_scale, time_length_scale)

    # step 8, get estimates from model
    query_dates = utils.interpolateQueryDates(query_start_datetime, query_end_datetime, query_rate)

    # note - the elevation grid is the wrong way around, so you need to put in lons first
    query_elevations = elevation_interpolator(query_lons, query_lats)
    yPred, yVar = gaussian_model_utils.estimateUsingModel(
        model, query_lats, query_lons, query_elevations, query_dates, time_offset)

    num_times = len(query_dates)
    estimates = []

    for i in range(num_times):
        estimates.append(
            {'PM2_5': (yPred[:,i]).tolist(), 'variance': (yVar[:,i]).tolist(), 'datetime': query_dates[i].strftime('%Y-%m-%d %H:%M:%S%z'), 'Latitude': query_lats.tolist(), 'Longitude': query_lons.tolist(), 'Elevation': query_elevations.tolist()}
            )

    return jsonify(estimates)





