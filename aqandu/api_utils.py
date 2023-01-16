from datetime import datetime, timedelta
import os
import json
from aqandu import bq_client, bigquery, utils, gaussian_model_utils, cache, jsonutils
from aqandu import app 
from aqandu import _area_models
from dotenv import load_dotenv
from flask import request, jsonify
# regular expression stuff for decoding quer 
import re
import numpy as np
# Find timezone based on longitude and latitude
from timezonefinder import TimezoneFinder
import pandas
import math


# This is now done in the submit_sensor_query in order to support multiple regions
#AIRU_TABLE_ID = os.getenv("AIRU_TABLE_ID")
#PURPLEAIR_TABLE_ID = os.getenv("PURPLEAIR_TABLE_ID")
#DAQ_TABLE_ID = os.getenv("DAQ_TABLE_ID")
# SOURCE_TABLE_MAP = {
#     "AirU": AIRU_TABLE_ID,
#     "PurpleAir": PURPLEAIR_TABLE_ID,
#     "DAQ": DAQ_TABLE_ID,
# }
# this will now we done at query time
#VALID_SENSOR_SOURCES = ["AirU", "PurpleAir", "DAQ", "all"]
# This is in units of the kernel -- so 2.5-3.5 kernel distance should kill any interactions at the boundaries in time.
TIME_KERNEL_FACTOR_PADDING = 3.0
#TIME_KERNEL_FACTOR_PADDING = 2.5
SPACE_KERNEL_FACTOR_PADDING = 2.
MIN_ACCEPTABLE_ESTIMATE = -5.0

# the size of time sequence chunks that are used to break the eatimation/data into pieces to speed up computation
# in units of time-scale parameter
# This is a tradeoff between looping through the data multiple times and having to do the fft inversion (n^2) of large time matrices
# If the bin size is 10 mins, and the and the time scale is 20 mins, then a value of 30 would give 30*20/10, which is a matrix size of 60.  Which is not that big.  
#TIME_SEQUENCE_SIZE = 20.
TIME_SEQUENCE_SIZE = 20

# constants for outier, bad sensor removal
MAX_ALLOWED_PM2_5 = 1000.0
# constant to be used with MAD estimates
DEFAULT_OUTLIER_LEVEL = 5.0
# level below which outliers won't be removed 
MIN_OUTLIER_LEVEL = 10.0


def estimateMedianDeviation(start_date, end_date, lat_lo, lat_hi, lon_lo, lon_hi, area_model):
    with open('db_table_headings.json') as json_file:
        db_table_headings = json.load(json_file)

    area_id_strings=area_model['idstring']
    query_list = []
#loop over all of the tables associated with this area model
    for area_id_string in area_id_strings:
        time_string = db_table_headings[area_id_string]['time']
        pm2_5_string = db_table_headings[area_id_string]['pm2_5']
        lon_string = db_table_headings[area_id_string]['longitude']
        lat_string = db_table_headings[area_id_string]['latitude']
        id_string = db_table_headings[area_id_string]['id']
        table_string = os.getenv(area_id_string)

        column_string = " ".join([id_string, "AS id,", time_string, "AS time,", pm2_5_string, "AS pm2_5,", lat_string, "AS lat,", lon_string, "AS lon"])

        # if 'sensormodel' in db_table_headings[area_id_string]:
        #     sensormodel_string = db_table_headings[area_id_string]['sensormodel']
        #     column_string += ", " + sensormodel_string + " AS sensormodel"

        if 'sensormodel' in db_table_headings[area_id_string]:
            sensortype_string = db_table_headings[area_id_string]['sensormodel']
            column_string += ", " + sensortype_string + " AS sensormodel"

        where_string = ""
        if "label" in db_table_headings[area_id_string]:
            label_string = db_table_headings[area_id_string]["label"]
            column_string += ", " + label_string + " AS areamodel"
            where_string += " AND " + label_string + " = " + "'" + area_model["name"] + "'"

        query_list.append(f"""(SELECT pm2_5, id FROM (SELECT {column_string} FROM `{table_string}` WHERE (({time_string} > @start_date) AND ({time_string} < @end_date))) WHERE ((lat <= @lat_hi) AND (lat >= @lat_lo) AND (lon <= @lon_hi) AND (lon >= @lon_lo)) AND (pm2_5 < {MAX_ALLOWED_PM2_5}))""")

    query = "(" + " UNION ALL ".join(query_list) + ")"

#    query = f"SELECT PERCENTILE_DISC(pm2_5, 0.5) OVER ()  AS median FROM {query} LIMIT 1"
#    query = f"WITH all_data as {query} SELECT COUNT (DISTINCT id) as num_sensors, PERCENTILE_DISC(pm2_5, 0.0) OVER ()  AS min,  PERCENTILE_DISC(pm2_5, 0.5) OVER ()  AS median, PERCENTILE_DISC(pm2_5, 1.0) OVER ()  AS max FROM all_data LIMIT 1"
    full_query = f"WITH all_data as {query} SELECT * FROM (SELECT PERCENTILE_DISC(pm2_5, 0.5) OVER() AS median FROM all_data LIMIT 1) JOIN (SELECT COUNT(DISTINCT id) as num_sensors FROM all_data) ON TRUE"

#        query_string = f"""SELECT pm2_5 FROM (SELECT {column_string} FROM `{db_id_string}` WHERE (({time_string} > {start_date}) AND ({time_string} < {end_date}))) WHERE ((lat <= {lat_hi}) AND (lat >= {lat_lo}) AND (lon <= {lon_hi}) AND (lon >= {lon_lo})) ORDER BY time ASC"""

#    print("query is: " + full_query)

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

    query_job = bq_client.query(full_query, job_config=job_config)
    if query_job.error_result:
        app.logger.error(query_job.error_result)
        return "Invalid API call - check documentation.", 400

    median = 0.0
    MAD = 0.0
    count = 0

    median_data = query_job.result()
    for row in median_data:
        median = row.median
        count = row.num_sensors

    full_query = f"WITH all_data as {query} SELECT PERCENTILE_DISC(ABS(pm2_5 - {median}), 0.5) OVER() AS median FROM all_data LIMIT 1"
    query_job = bq_client.query(full_query, job_config=job_config)
    if query_job.error_result:
        app.logger.error(query_job.error_result)
        return "Invalid API call - check documentation.", 400
    MAD_data = query_job.result()
    for row in MAD_data:
        MAD = row.median

    return median, MAD, count


def filterUpperLowerBounds(lat_lo, lat_hi, lon_lo, lon_hi, start_date, end_date, area_model, filter_level = DEFAULT_OUTLIER_LEVEL):
        median, MAD, count = estimateMedianDeviation(start_date, end_date, lat_lo, lat_hi, lon_lo, lon_hi, area_model)
        lo = max(median - filter_level*MAD, 0.0)
        hi = min(max(median + filter_level*MAD, MIN_OUTLIER_LEVEL), MAX_ALLOWED_PM2_5)
        return lo, hi

def filterUpperLowerBoundsForArea(start_date, end_date, area_model, filter_level = DEFAULT_OUTLIER_LEVEL):
        bbox_array = np.array(area_model['boundingbox'])[:,1:3]
        lo = bbox_array.min(axis=0)
        hi = bbox_array.max(axis=0)
        median, MAD, count = estimateMedianDeviation(start_date, end_date, lo[0], hi[0], lo[1], hi[1], area_model)
        lo = max(median - filter_level*MAD, 0.0)
        hi = min(max(median + filter_level*MAD, MIN_OUTLIER_LEVEL), MAX_ALLOWED_PM2_5)
        print(f"Hi and low bounds are {hi} and {lo}")
        return lo, hi

# submit a query for a range of values
# Ross Nov 2020
# this has been consolidate and generalized so that multiple api calls can use the same query code
def submit_sensor_query(lat_lo, lat_hi, lon_lo, lon_hi, start_date, end_date, area_model, min_value, max_value):
#    print("aread_id_string: " + area_id_string)
#    db_id_string = "tetrad-296715.telemetry.slc_ut"
    with open('db_table_headings.json') as json_file:
        db_table_headings = json.load(json_file)

    area_id_strings = area_model["idstring"]
    this_area_model = area_model["name"]
        
    query_list = []
#loop over all of the tables associated with this area model
    for area_id_string in area_id_strings:
        time_string = db_table_headings[area_id_string]['time']
        pm2_5_string = db_table_headings[area_id_string]['pm2_5']
        lon_string = db_table_headings[area_id_string]['longitude']
        lat_string = db_table_headings[area_id_string]['latitude']
        id_string = db_table_headings[area_id_string]['id']
        table_string = os.getenv(area_id_string)

        column_string = " ".join([id_string, "AS id,", time_string, "AS time,", pm2_5_string, "AS pm2_5,", lat_string, "AS lat,", lon_string, "AS lon"])

        if 'sensormodel' in db_table_headings[area_id_string]:
            sensormodel_string = db_table_headings[area_id_string]['sensormodel']
            column_string += ", " + sensormodel_string + " AS sensormodel"

        if 'sensorsource' in db_table_headings[area_id_string]:
            sensorsource_string = db_table_headings[area_id_string]['sensorsource']
            column_string += ", " + sensorsource_string + " AS sensorsource"

        where_string = ""
        if "label" in db_table_headings[area_id_string]:
            label_string = db_table_headings[area_id_string]["label"]
            column_string += ", " + label_string + " AS areamodel" 
            where_string += " AND " + label_string + " = " + "'" + this_area_model + "'"

        query_list.append(f"""(SELECT * FROM (SELECT {column_string} FROM `{table_string}` WHERE (({time_string} > '{start_date}') AND ({time_string} < '{end_date}')) {where_string}) WHERE ((lat <= {lat_hi}) AND (lat >= {lat_lo}) AND (lon <= {lon_hi}) AND (lon >= {lon_lo})) AND (pm2_5 < {max_value}) AND (pm2_5 > {min_value}))""")


    query = " UNION ALL ".join(query_list) + " ORDER BY time ASC "
    print(f"submit sensor query is {query}")

#        query_string = f"""SELECT * FROM (SELECT {column_string} FROM `{db_id_string}` WHERE (({time_string} > {start_date}) AND ({time_string} < {end_date}))) WHERE ((lat <= {lat_hi}) AND (lat >= {lat_lo}) AND (lon <= {lon_hi}) AND (lon >= {lon_lo})) ORDER BY time ASC"""

    
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
        app.logger.error(query_job.error_result)
        return "Invalid API call - check documentation.", 400
    # Waits for query to finish
    sensor_data = query_job.result()

    return(sensor_data)

# could do an ellipse in lat/lon around the point using something like this
#WHERE SQRT(POW(Latitude - @lat, 2) + POW(Longitude - @lon, 2)) <= @radius
#    AND upper > @start_date AND time < @end_date
#    ORDER BY time ASC
# Also could do this by spherical coordinates on the earth -- however we use a lat-lon box to save compute time on the BigQuery server

# radius should be in *meters*!!!
# this has been modified so that it now takes an array of lats/lons
# the radius parameter is not implemented in a precise manner -- rather it is converted to a lat-lon bounding box and all within that box are returned
# there could be an additional culling of sensors outside the radius done here after the query - if the radius parameter needs to be precise. 
def request_model_data_local(lats, lons, radius, start_date, end_date, area_model, outlier_filtering = True, aggregation_interval=0):
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
    app.logger.info("Query bounding box is %f %f %f %f" %(lat_lo, lat_hi, lon_lo, lon_hi))

    if outlier_filtering:
        min_value, max_value = filterUpperLowerBounds(lat_lo, lat_hi, lon_lo, lon_hi, start_date, end_date + timedelta(hours=aggregation_interval), area_model)
    else:
        min_value = 0.0
        max_value = MAX_ALLOWED_PM2_5

    print(f"aggregation_interval in request_model is {aggregation_interval}")
    if aggregation_interval == 0:
        rows = submit_sensor_query(lat_lo, lat_hi, lon_lo, lon_hi, start_date, end_date, area_model, min_value, max_value)
    else:
        rows = submit_sensor_query_aggregate(lat_lo, lat_hi, lon_lo, lon_hi, start_date, end_date, area_model, min_value, max_value, aggregation_interval=aggregation_interval)

#    print(rows)
    for row in rows:
        new_row = {
            "ID": row.id,
            "Latitude": row.lat,
            "Longitude": row.lon,
            "time": row.time,
            "PM2_5": row.pm2_5,
            "SensorModel":row.sensormodel,
            }
        try:
            new_row["SensorSource"] = row.sensorsource
        except:
            pass
        model_data.append(new_row)
        
        #this is taken care of in the query now
        # if 'sensormodel' in row:
        #     new_row["SensorModel"] = row.sensormodel
        # else:
        #     print(f"missed sensor model for row {row}")
        #     new_row["SensorModel"] = "default"
        # try:
        #     new_row["SensorModel"] = row.sensormodel
        # except:
        #     new_row["SensorModel"] = "Default"

        # try:
        #     new_row["SensorSource"] = row.sensorsource
        # except:
        #     new_row["SensorSource"] = "Default"


    return model_data


def request_model_data_local_aggregate(lats, lons, radius, start_date, end_date, area_model, outlier_filtering = True, aggregation_interval=0.0):
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
    app.logger.info("Query bounding box is %f %f %f %f" %(lat_lo, lat_hi, lon_lo, lon_hi))

    if outlier_filtering:
        min_value, max_value = filterUpperLowerBounds(lat_lo, lat_hi, lon_lo, lon_hi, start_date, end_date, area_model)
    else:
        min_value = 0.0
        max_value = MAX_ALLOWED_PM2_5
    rows = submit_sensor_query_aggregate(lat_lo, lat_hi, lon_lo, lon_hi, start_date, end_date, area_model, min_value, max_value, aggregation_interval)

#    print(rows)
    for row in rows:
        new_row = {
            "ID": row.id,
            "Latitude": row.lat,
            "Longitude": row.lon,
            "time": row.time,
            "PM2_5": row.pm2_5,
            "SensorModel":row.sensormodel,
            "SensorSource":row.sensorsource
            }

        #this is taken care of in the query now
        # if 'sensormodel' in row:
        #     new_row["SensorModel"] = row.sensormodel
        # else:
        #     print(f"missed sensor model for row {row}")
        #     new_row["SensorModel"] = "default"
        # try:
        #     new_row["SensorModel"] = row.sensormodel
        # except:
        #     new_row["SensorModel"] = "Default"

        # try:
        #     new_row["SensorSource"] = row.sensorsource
        # except:
        #     new_row["SensorSource"] = "Default"

        model_data.append(new_row)

    return model_data


# this is a generic helper function that sets everything up and runs the model
def computeEstimatesForLocations(query_dates, query_locations, area_model, outlier_filtering = True, apply_correction_factors = True,  aggregation_interval=0, full_variance = False):
    num_locations = query_locations.shape[0]
    query_lats = query_locations[:,0]
    query_lons = query_locations[:,1]
    query_start_datetime = query_dates[0]
    query_end_datetime = query_dates[-1]

    elevation_interpolator = jsonutils.buildAreaElevationInterpolator(area_model['elevationfile'])    
    query_elevations = np.array([elevation_interpolator(this_row[1], this_row[0])[0] for this_row in query_locations])
    
    # step 0, load up the bounding box from file and check that request is within it

    # for i in range(num_locations):
    #     if not jsonutils.isQueryInBoundingBox(area_model['boundingbox'], query_lats[i], query_lons[i]):
    #         app.logger.error(f"The query location, {query_lats[i]},{query_lons[i]},  is outside of the bounding box.")
    #         return np.full((query_lats.shape[0], len(query_dates)), 0.0), np.full((query_lats.shape[0], len(query_dates)), np.nan), ["Query location error" for i in query_dates]


    # setup the return values in case of error
    error_estimates_return = np.full((query_lats.shape[0], len(query_dates)), 0.0)
    if full_variance == False:
        error_variance_return = np.full((query_lats.shape[0], len(query_dates)), np.inf)
    else:
        error_variance_return = np.identity(query_lats.shape[0] * len(query_dates))*np.inf
    error_elevation_return = np.full(query_lats.shape[0], 0.0)
    
    # step 2, load up length scales from file
    latlon_length_scale, time_length_scale, elevation_length_scale = jsonutils.getLengthScalesForTime(area_model['lengthscales'], query_start_datetime)
    if latlon_length_scale == None:
            app.logger.error("No length scale found between dates {query_start_datetime} and {query_end_datetime}")
            return error_estimates_return, error_variance_return, error_elevation_return, ["Length scale parameter error" for i in range(len(query_dates))]
    app.logger.debug("Loaded length scales: space=" + str(latlon_length_scale) + " time=" + str(time_length_scale) + " elevation=" + str(elevation_length_scale))

    app.logger.debug(f'Using length scales: latlon={latlon_length_scale} elevation={elevation_length_scale} time={time_length_scale}')

    # step 3, query relevent data

# these conversions were when we were messing around with specifying radius in miles and so forth.      
#    NUM_METERS_IN_MILE = 1609.34
#    radius = latlon_length_scale / NUM_METERS_IN_MILE  # convert meters to miles for db query

#    radius = latlon_length_scale / 70000


# radius is in meters, as is the length scale and UTM.    
    radius = SPACE_KERNEL_FACTOR_PADDING*latlon_length_scale

    print(f"aggregation_interval ins computeEstimatesForLocations is {aggregation_interval}")
    if aggregation_interval == 0:
        sensor_data = request_model_data_local(
            query_lats,
            query_lons,
            radius,
            query_start_datetime - timedelta(hours=TIME_KERNEL_FACTOR_PADDING*time_length_scale),
            query_end_datetime+ timedelta(hours=TIME_KERNEL_FACTOR_PADDING*time_length_scale),
            area_model, outlier_filtering)
    else:
        sensor_data = request_model_data_local(
            query_lats,
            query_lons,
            radius,
            query_start_datetime,
            query_end_datetime,
            area_model, outlier_filtering, aggregation_interval = aggregation_interval)
        
    #            query_end_datetime 


    unique_sensors = {datum['ID'] for datum in sensor_data}
    print(f" this is the unique sensor data {unique_sensors}")
    app.logger.info(f'Loaded {len(sensor_data)} data points for {len(unique_sensors)} unique devices from bgquery.')

    # step 3.5, convert lat/lon to UTM coordinates
    try:
        utils.convertLatLonToUTM(sensor_data)
    except ValueError as err:
        app.logger.error(str(err))
        return error_estimates_return, error_variance_return, error_elevation_return, ["Failure to convert lat/lon" for i in range(len(query_dates))]

# legacy code forcing sensors to like in UTM zone...
#    sensor_data = [datum for datum in sensor_data if datum['zone_num'] == 12]

    unique_sensors = {datum['ID'] for datum in sensor_data}
    app.logger.info((
#        "After removing points with zone num != 12: "
        "got " f"{len(sensor_data)} data points for {len(unique_sensors)} unique devices."
    ))

    # Step 4, parse sensor type from the version
#    sensor_source_to_type = {'AirU': '3003', 'PurpleAir': '5003', 'DAQ': '0000', 'Default':'Default'}
# DAQ does not need a correction factor
#    for datum in sensor_data:
#        datum['type'] =  sensor_source_to_type[datum['SensorSource']]

    if len(sensor_data) > 0:
        app.logger.info(f'Fields: {sensor_data[0].keys()}')
    else:
        app.logger.info(f'Got zero sensor data')
        return error_estimates_return, error_variance_return, error_elevation_return, ["Zero sensor data" for i in range(len(query_dates))]

    # step 4.5, Data Screening
#    print('Screening data')
    sensor_data = utils.removeInvalidSensors(sensor_data)

    # step 5, apply correction factors to the data
    if apply_correction_factors:
        for datum in sensor_data:
            datum['PM2_5'] = jsonutils.applyCorrectionFactor(area_model['correctionfactors'], datum['time'], datum['PM2_5'], datum['SensorModel'])

    # step 6, add elevation values to the data
    # NOTICE - the elevation object takes locations in the form "lon-lat"
    # this seems redundant since elevations are passed in...
    # elevation_interpolator = jsonutils.buildAreaElevationInterpolator(area_model['elevationfile'])
    for datum in sensor_data:
        if 'Altitude' not in datum:
            datum['Altitude'] = elevation_interpolator([datum['Longitude']],[datum['Latitude']])[0]

    # This does the calculation in one step --- old method --- less efficient.  Below we break it into pieces.  Remove this once the code below (step 7) is fully tested.
    # step 7, get estimates from model
    # # step 8, Create Model
    # model, time_offset = gaussian_model_utils.createModel(
    #     sensor_data, latlon_length_scale, elevation_length_scale, time_length_scale)
    # # step 9, Compute estimates
    # yPred, yVar = gaussian_model_utils.estimateUsingModel(
    #     model, query_lats, query_lons, query_elevations, query_dates, time_offset)

    time_padding = timedelta(hours=TIME_KERNEL_FACTOR_PADDING*time_length_scale)
    time_sequence_length = timedelta(hours = TIME_SEQUENCE_SIZE*time_length_scale)
    if aggregation_interval == 0:
        sensor_sequence, query_sequence = utils.chunkTimeQueryData(query_dates, time_sequence_length, time_padding)
    else:
        sensor_sequence, query_sequence = utils.chunkTimeQueryDataAggregate(query_dates, aggregation_interval)
    yPred = np.empty((num_locations, 0))
    yVar = np.empty((num_locations, 0))
    status = []
    # this is already handled better up above
    # if len(sensor_data) == 0:
    #     status = "0 sensors/measurements"
    #     return 
    for i in range(len(query_sequence)):
    # step 7, Create Model
        if aggregation_interval == 0.0:
            model, time_offset, model_status = gaussian_model_utils.createModel(
                sensor_data, latlon_length_scale, elevation_length_scale, time_length_scale, sensor_sequence[i][0], sensor_sequence[i][1], save_matrices=True)
        else:
            # if there is aggregation let the sensor noise be zero (because of averaging over time)
            model, time_offset, model_status = gaussian_model_utils.createModel(
                sensor_data, latlon_length_scale, elevation_length_scale, time_length_scale, sensor_sequence[i][0], sensor_sequence[i][1], save_matrices=True, sensor_noise = 0.0)
            
        # check to see if there is a valid model
        if (model == None):
            yPred_tmp = np.full((query_lats.shape[0], len(query_sequence[i])), 0.0)
            yVar_tmp = np.full((query_lats.shape[0], len(query_sequence[i])), np.nan)
            status_estimate_tmp = [model_status for i in range(len(query_sequence[i]))]
        else:
            yPred_tmp, yVar_tmp, status_estimate_tmp = gaussian_model_utils.estimateUsingModel(
                model, query_lats, query_lons, query_elevations, query_sequence[i], time_offset, save_matrices=True, full_variance=full_variance)
        # put the estimates together into one matrix
        yPred = np.concatenate((yPred, yPred_tmp), axis=1)
        yVar = np.concatenate((yVar, yVar_tmp), axis=1)
        status = status + status_estimate_tmp

    if np.min(yPred) < MIN_ACCEPTABLE_ESTIMATE:
        app.logger.warn("got estimate below level " + str(MIN_ACCEPTABLE_ESTIMATE))

    status_extras = ""
    if apply_correction_factors:
        status_extras += ", correction factors applied"
    else:
        status_extras += "no correction applied"
    if (aggregation_interval > 0):
        status += f", aggregation of {aggregation_interval} hours"
    # document that the data has been aggrevated
    status = [this_status + status_extras for this_status in status]
        
# Here we clamp values to ensure that small negative values to do not appear
    yPred = np.clip(yPred, a_min = 0., a_max = None)

    return yPred, yVar, query_elevations, status
    
# submit a query for a range of values
# Ross Nov 2020
# this has been consolidate and generalized so that multiple api calls can use the same query code
def submit_sensor_query_aggregate(lat_lo, lat_hi, lon_lo, lon_hi, start_date, end_date, area_model, min_value, max_value, aggregation_interval):
#    print("aread_id_string: " + area_id_string)
#    db_id_string = "tetrad-296715.telemetry.slc_ut"
    with open('db_table_headings.json') as json_file:
        db_table_headings = json.load(json_file)

    area_id_strings = area_model["idstring"]
    this_area_model = area_model["name"]

    #        print(area_model)
    # this logic adjusts for the two cases, where you have different tables for each source or one table for all sources
    # get all of the sources if you need to
    sources = area_model["idstring"]


    end_interval = (end_date + timedelta(hours = int(aggregation_interval))).strftime(utils.DATETIME_FORMAT[0])
    tables_list = []

    for area_id_string in sources:
        empty_query = False
        time_string = db_table_headings[area_id_string]['time']
        pm2_5_string = db_table_headings[area_id_string]['pm2_5']
        lon_string = db_table_headings[area_id_string]['longitude']
        lat_string = db_table_headings[area_id_string]['latitude']
        id_string = db_table_headings[area_id_string]['id']
        table_string = os.getenv(area_id_string)

        #            column_string = ", ".join([id_string + " AS ID", time_string + " AS time", pm2_5_string + " AS pm2_5", lat_string + " AS lat", lon_string+" AS lon","'" + this_area + "'" + " AS areamodel", model_string + " AS sensormodel"])
        # area model gets taken care of below
        # grouping on other things that we might need
        extra_group_string = ""
        column_string = ", ".join([id_string + " AS id", time_string + " AS sensor_time", pm2_5_string + " AS pm2_5", lat_string + " AS lat", lon_string+" AS lon"])
        # put together a separate query for all of the specified sources
        #            group_string = ", ".join(["ID", "pm2_5", "lat", "lon", "area_model", "sensormodel"])

        if 'sensormodel' in db_table_headings[area_id_string]:
            sensormodel_string = db_table_headings[area_id_string]['sensormodel']
            column_string += ", " + sensormodel_string + " AS sensormodel"
            extra_group_string += ", " + " sensormodel"

        if 'sensorsource' in db_table_headings[area_id_string]:
            sensorsource_string = db_table_headings[area_id_string]['sensorsource']
            column_string += ", " + sensorsource_string + " AS sensorsource"
            extra_group_string += ", " + " sensorsource"

        table_string = os.getenv(area_id_string)
        where_string = f"pm2_5 < {max_value} AND pm2_5 > {min_value} AND sensor_time >= '{start_date}' AND sensor_time <= '{end_interval}' AND ((lat <= {lat_hi}) AND (lat >= {lat_lo}) AND (lon <= {lon_hi}) AND (lon >= {lon_lo})) AND (pm2_5 < {max_value}) AND (pm2_5 > {min_value})"

        # This is to cover the case where the different regions are in the same database/table and distinguised by different labels
        if "label" in db_table_headings[area_id_string]:
            label_string = db_table_headings[area_id_string]['label']
            column_string += ", " + label_string + " AS areamodel"
        column_string += ", " + "'" + this_area_model + "'" + " AS areamodel"
        this_query = f"""(SELECT * FROM (SELECT {column_string} FROM `{table_string}`) WHERE ({where_string}))"""
        tables_list.append(this_query)

    query = f"""
        WITH
             intervals AS (
                 SELECT
                     TIMESTAMP_ADD('{start_date}', INTERVAL {aggregation_interval} * 60 * num MINUTE) AS time,
                     TIMESTAMP_ADD('{start_date}', INTERVAL {aggregation_interval} * 3600* (1 + num) - 1 SECOND) AS upper
                 FROM UNNEST(GENERATE_ARRAY(0,  DIV(TIMESTAMP_DIFF('{end_date}', '{start_date}', MINUTE) , {aggregation_interval}))) AS num
             )
         SELECT  AVG(pm2_5) AS pm2_5, time, id, lat, lon {extra_group_string}
         FROM intervals
             JOIN (
             {' UNION ALL '.join(tables_list)}
         ) sensors
             ON sensors.sensor_time BETWEEN intervals.time AND intervals.upper
         GROUP BY time, id, lat, lon {extra_group_string}
         ORDER BY time"""

    print(f"aggregate query is {query}")
    query_job = bq_client.query(query)
    if query_job.error_result:
        app.logger.error(query_job.error_result)
        return "Invalid API call - check documentation.", 400
    # Waits for query to finish
    sensor_data = query_job.result()
    return(sensor_data)

