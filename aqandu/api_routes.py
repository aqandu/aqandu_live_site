from datetime import datetime, timedelta
import os
import json
from aqandu import app, bq_client, bigquery, utils, gaussian_model_utils, cache, jsonutils
from aqandu import _area_models
from dotenv import load_dotenv
from flask import request, jsonify
# regular expression stuff for decoding quer 
import re
import numpy as np
# Find timezone based on longitude and latitude
from timezonefinder import TimezoneFinder
import pandas



# Load in .env and set the table name
load_dotenv()  # Required for compatibility with GCP, can't use pipenv there

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
TIME_KERNEL_FACTOR_PADDING = 3.0
SPACE_KERNEL_FACTOR_PADDING = 2.
MIN_ACCEPTABLE_ESTIMATE = -5.0

# the size of time sequence chunks that are used to break the eatimation/data into pieces to speed up computation
# in units of time-scale parameter
# This is a tradeoff between looping through the data multiple times and having to do the fft inversion (n^2) of large time matrices
# If the bin size is 10 mins, and the and the time scale is 20 mins, then a value of 30 would give 30*20/10, which is a matrix size of 60.  Which is not that big.  
TIME_SEQUENCE_SIZE = 20.

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

        if 'sensormodel' in db_table_headings[area_id_string]:
            sensormodel_string = db_table_headings[area_id_string]['sensormodel']
            column_string += ", " + sensormodel_string + " AS sensormodel"

        if 'sensortype' in db_table_headings[area_id_string]:
            sensortype_string = db_table_headings[area_id_string]['sensortype']
            column_string += ", " + sensortype_string + " AS sensortype"

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
        return lo, hi

    
@app.route("/api/estimateSummaryStatistics", methods=["GET"])
def estimateSummaryStatistics():
    start = request.args.get('startTime')
    end = request.args.get('endTime')
    lat_hi = float(request.args.get('latHi'))
    lat_lo = float(request.args.get('latLo'))
    lon_hi = float(request.args.get('lonHi'))
    lon_lo = float(request.args.get('lonLo'))
    area_model = jsonutils.getAreaModelByLocation(_area_models, lat=lat_hi, lon=lon_lo)
    if area_model == None:
        msg = f"The query location, lat={lat_hi}, lon={lon_lo}, and/or area string {area_string} does not have a corresponding area model"
        return msg, 400
    start_date = jsonutils.parseDateString(start, area_model['timezone'])
    end_date = jsonutils.parseDateString(end, area_model['timezone'])

#    print(filterUpperLowerBoundsForArea(start_date, end_date, area_model))

    median, MAD, count = estimateMedianDeviation(start_date, end_date, lat_lo, lat_hi, lon_lo, lon_hi, area_model)
    
#    for row in median_data:
#        print(f"min = {row.min}")
#        print(f"median = {row.median}")
#        print(f"max = {row.max}")
#        print(f"num sensors = {row.num_sensors}")
    summary = {"Median":median, "Count":count, "MAD":MAD}
    
    return(jsonify(summary))


@app.route("/api/getSensorData", methods=["GET"])
def getSensorData():
    # Get the arguments from the query string
    id = request.args.get('id')
    sensor_source = request.args.get('sensorSource')
    start = request.args.get('startTime')
    end = request.args.get('endTime')
    if "noCorrection" in request.args:
        apply_correction = False
    else:
        apply_correction = True
        
    # Check ID is valid
    if id == "" or id == "undefined":
        id = None
#        msg = "id is invalid. It must be a string that is not '' or 'undefined'."
#        return msg, 400

    if "areamodel" in request.args:
        area_string = request.args.get('areaModel')
    else:
        area_string = "all"

    # check if sensor_source is specified
    # If not, default to all
    if sensor_source == "" or sensor_source == "undefined" or sensor_source==None:
        # Check that the arguments we want exist
        sensor_source = "all"


    # Check that the data is formatted correctly
    if not utils.validateDate(start) or not utils.validateDate(end):
        msg = f"Incorrect date format, should be {utils.DATETIME_FORMAT}, e.g.: 2018-01-03T20:00:00Z"
        return msg, 400

    # Define the BigQuery query

    # set up the areas so that it does one or all
    # first, how many area tables are we searching...
    if area_string == "all":
        areas = _area_models.keys()
    else:
        areas = [area_string]

    with open('db_table_headings.json') as json_file:
        db_table_headings = json.load(json_file)
        
    query_list = []

    for this_area in areas:
#        print(f"this_area is {this_area}")
        area_model = _area_models[this_area]
#        print(area_model)
        # this logic adjusts for the two cases, where you have different tables for each source or one table for all sources
        # get all of the sources if you need to
        source_query = ""
        if (sensor_source == "all"):
            # easy case, query all tables with no source requirement
            sources = area_model["idstring"]
        elif "sourcetablemap" in area_model:
            # if it's organized by table, then get the right table (or nothing)
            if sensor_source in area_model["sourcetablemap"]:
                sources = area_model["sourcetablemap"][sensor_source]
            else:
                sources = None
        else:
            # sources are not organized by table.  Get all the tables and add a boolean to check for the source
            sources = area_model["idstring"]
            source_query = f" AND sensorsource = @sensor_source"

        for area_id_string in sources:
            empty_query = False
            time_string = db_table_headings[area_id_string]['time']
            pm2_5_string = db_table_headings[area_id_string]['pm2_5']
            lon_string = db_table_headings[area_id_string]['longitude']
            lat_string = db_table_headings[area_id_string]['latitude']
            id_string = db_table_headings[area_id_string]['id']
            model_string = db_table_headings[area_id_string]['sensormodel']
            table_string = os.getenv(area_id_string)

            column_string = " ".join([id_string, "AS ID,", time_string, "AS time,", pm2_5_string, "AS pm2_5,", lat_string, "AS lat,", lon_string, "AS lon,", "'" + this_area + "'", "AS area_model,", model_string, "AS sensormodel"])
            # put together a separate query for all of the specified sources
            table_string = os.getenv(area_id_string)

            if "sensorsource" in db_table_headings[area_id_string]:
                sensor_string = db_table_headings[area_id_string]['sensorsource']
                column_string += ", " + sensor_string + " AS sensorsource"
            elif (not source_query==""):
            # if you are looking for a particular sensor source, but that's not part of the tables info, then the query is not going to return anything
                empty_query = True


                # for efficiency, don't do the query if the sensorsource is needed by not available

            where_string = "time >= @start AND time <= @end"
            if id != None:
                where_string  += " AND ID = @id"
            where_string += source_query

            this_query = f"""(SELECT * FROM (SELECT {column_string} FROM `{table_string}`) WHERE ({where_string}))"""

            if not empty_query:
                query_list.append(this_query)

        query = " UNION ALL ".join(query_list) + " ORDER BY time ASC "

    job_config = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("id", "STRING", id),
                        bigquery.ScalarQueryParameter("sensor_source", "STRING", sensor_source),
            bigquery.ScalarQueryParameter("start", "TIMESTAMP", start),
            bigquery.ScalarQueryParameter("end", "TIMESTAMP", end),
        ])

    # Run the query and collect the result
    measurements = []
    query_job = bq_client.query(query, job_config=job_config)
#    rows = query_job.result()
    df = query_job.to_dataframe()
    status_data = ["No correction"]*df.shape[0]
    df["status"] = status_data

#    df.append(bigquery.SchemaField("status", "STRING"))

#    rows.append(bigquery.SchemaField("status", "STRING"))
# apply correction factors unless otherwise noted
    if apply_correction:
        for idx, datum in df.iterrows():
            df.at[idx, 'pm2_5'], df.at[idx, 'status'] = jsonutils.applyCorrectionFactor(_area_models[datum["area_model"]]['correctionfactors'], datum['time'], datum['pm2_5'], datum['sensormodel'], status=True)
#    else:
#        datum['status'] = "No correction"
        
    for idx, row in df.iterrows():
        measurements.append({"Sensor source": row["sensorsource"], "Sensor ID": row["ID"], "PM2_5": row["pm2_5"], "Time": row["time"].strftime(utils.DATETIME_FORMAT), "Latitude": row["lat"], "Longitude": row["lon"], "Status": row["status"]})
    # tags = [{
    #     "ID": id,
    #     "SensorSource": sensor_source,
    #     "time": datetime.utcnow().strftime(utils.DATETIME_FORMAT)
    # }]
    # return jsonify({"data": measurements, "tags": tags})
    return jsonify(measurements)


@app.route("/api/getLiveSensors", methods=["GET"])
@cache.cached(timeout=59, query_string=True)
def getLiveSensors():
    # Get the arguments from the query string
    sensor_source = request.args.get('sensorSource')
    if "areaModel" in request.args:
        area_string = request.args.get('areaModel')
    else:
        area_string = "all"
    if "noCorrection" in request.args:
        apply_correction = False
    else:
        apply_correction = True
    if "flagOutliers" in request.args:
        flag_outliers = True
    else:
        flag_outliers = False


    # check if sensor_source is specified
    # If not, default to all
    if sensor_source == "" or sensor_source == "undefined" or sensor_source==None:
        # Check that the arguments we want exist
        sensor_source = "all"

    # Define the BigQuery query
    now = datetime.utcnow()
    one_hour_ago = now - timedelta(hours=1)  # AirU + PurpleAir sensors have reported in the last hour
    three_hours_ago = now - timedelta(hours=3)  # DAQ sensors have reported in the 3 hours
    query_list = []

    if area_string == "all":
        areas = _area_models.keys()
    else:
        if area_string in _area_models:
            areas = [area_string]
        else:
            msg = f"The area string {area_string} does not have a corresponding area model"
            return msg, 400

    with open('db_table_headings.json') as json_file:
        db_table_headings = json.load(json_file)
        
    query_list = []

    for this_area in areas:
        need_source_query = False
        area_model = _area_models[this_area]
#        print(area_model)
        # this logic adjusts for the two cases, where you have different tables for each source or one table for all sources
        # get all of the sources if you need to
        source_query = "TRUE"
        if (sensor_source == "all"):
            # easy case, query all tables with no source requirement
            sources = area_model["idstring"]
        elif "sourcetablemap" in area_model:
            # if it's organized by table, then get the right table (or nothing)
            if sensor_source in area_model["sourcetablemap"]:
                sources = area_model["sourcetablemap"][sensor_source]
            else:
                sources = None
        else:
            # sources are not organized by table.  Get all the tables and add a boolean to check for the source
            sources = area_model["idstring"]
#            source_query = f"{sensor_string} = @sensor_source"
            need_source_query = True

        for area_id_string in sources:
            where_string = " WHERE TRUE"
            empty_query = False
            time_string = db_table_headings[area_id_string]['time']
            pm2_5_string = db_table_headings[area_id_string]['pm2_5']
            lon_string = db_table_headings[area_id_string]['longitude']
            lat_string = db_table_headings[area_id_string]['latitude']
            id_string = db_table_headings[area_id_string]['id']
            model_string = db_table_headings[area_id_string]['sensormodel']
            table_string = os.getenv(area_id_string)

            column_string = ", ".join([id_string + " AS ID", time_string + " AS time", pm2_5_string + " AS pm2_5", lat_string + " AS lat", lon_string+" AS lon", model_string + " AS sensormodel"])
            # put together a separate query for all of the specified sources
            group_string = ", ".join(["ID", "pm2_5", "lat", "lon", "area_model", "sensormodel"])
            table_string = os.getenv(area_id_string)

            if "sensorsource" in db_table_headings[area_id_string]:
                sensor_string = db_table_headings[area_id_string]['sensorsource']
                column_string += ", " + sensor_string + " AS sensorsource"
                group_string += ", sensorsource"
                if need_source_query:
                    source_query = f"{sensor_string} = '{sensor_source}'"
            elif need_source_query:
                # if you are looking for a particular sensor source, but that's not part of the tables info, then the query is not going to return anything
                empty_query = True

                # This is to cover the case where the different regions are in the same database/table and distinguised by different labels
            if "label" in db_table_headings[area_id_string]:
                label_string = db_table_headings[area_id_string]['label']
                column_string += ", " + label_string + " AS area_model"
                if area_model != "all":
                    where_string += " AND " + label_string + " = " + "'" + this_area + "'"
#                    where_string += " AND area_model = " + this_area
            else:
                column_string += ", " + this_area + "'" + " AS area_model"

            where_string += " AND " + source_query

            this_query = f"""(WITH a AS (SELECT {column_string} FROM `{table_string}` {where_string}),  b AS (SELECT {id_string} AS ID, max({time_string})  AS LATEST_MEASUREMENT FROM `{table_string}` WHERE {time_string} >= '{str(one_hour_ago)}' GROUP BY {id_string}) SELECT * FROM a INNER JOIN b ON a.time = b.LATEST_MEASUREMENT and b.ID = a.ID)"""
#            this_query = f"""(SELECT * FROM (SELECT {column_string}, max({time_string}) AS LATEST_MEASUREMENT FROM `{table_string}` WHERE {time_string} >= '{str(one_hour_ago)}' GROUP BY {group_string}) WHERE LATEST_MEASUREMENT > '{str(one_hour_ago)}')"""
            
#            print(this_query)

            if not empty_query:
                query_list.append(this_query)

    # if sensor_source == "AirU" or sensor_source == "all":
    #     query_list.append(
    #         f"""(
    #             SELECT a.ID, time, PM2_5, Latitude, Longitude, SensorModel, 'AirU' as SensorSource
    #             FROM `{AIRU_TABLE_ID}` as a
    #             INNER JOIN (
    #                 SELECT ID, max(time) AS LATEST_MEASUREMENT
    #                 FROM `{AIRU_TABLE_ID}`
    #                 WHERE time >= '{str(one_hour_ago)}'
    #                 GROUP BY ID
    #             ) AS b ON a.ID = b.ID AND a.time = b.LATEST_MEASUREMENT
    #             WHERE time >= '{str(one_hour_ago)}'
    #         )"""
    #     )

    # if sensor_source == "PurpleAir" or sensor_source == "all":
    #     query_list.append(
    #         f"""(
    #             SELECT a.ID, time, PM2_5, Latitude, Longitude, '' as SensorModel, 'PurpleAir' as SensorSource
    #             FROM `{PURPLEAIR_TABLE_ID}` as a
    #             INNER JOIN (
    #                 SELECT ID, max(time) AS LATEST_MEASUREMENT
    #                 FROM `{PURPLEAIR_TABLE_ID}`
    #                 WHERE time >= '{str(one_hour_ago)}'
    #                 GROUP BY ID
    #             ) AS b ON a.ID = b.ID AND a.time = b.LATEST_MEASUREMENT
    #             WHERE time >= '{str(one_hour_ago)}'
    #         )"""
    #     )

    # if sensor_source == "DAQ" or sensor_source == "all":
    #     query_list.append(
    #         f"""(
    #             SELECT a.ID, time, PM2_5, Latitude, Longitude, '' as SensorModel, 'DAQ' as SensorSource
    #             FROM `{DAQ_TABLE_ID}` as a
    #             INNER JOIN (
    #                 SELECT ID, max(time) AS LATEST_MEASUREMENT
    #                 FROM `{DAQ_TABLE_ID}`
    #                 WHERE time >= '{str(three_hours_ago)}'
    #                 GROUP BY ID
    #             ) AS b ON a.ID = b.ID AND a.time = b.LATEST_MEASUREMENT
    #             WHERE time >= '{str(three_hours_ago)}'
    #         )"""
    #     )

    # Build the actual query from the list of options
    query = " UNION ALL ".join(query_list)

    print(query)
    # Run the query and collect the result
    query_job = bq_client.query(query)
#    rows = query_job.result()
    df = query_job.to_dataframe()
    status_data = [[]]*df.shape[0]
    df["status"] = status_data
    
    if flag_outliers:
        filters = {}
        for this_area in areas:
            area_model = _area_models[this_area]
            lo_filter, hi_filter = filterUpperLowerBoundsForArea(str(one_hour_ago), str(now), area_model)
            filters[this_area] = (lo_filter,hi_filter)
        for idx, datum in df.iterrows():
            this_lo, this_hi = filters[datum["area_model"]]
            this_data = datum['pm2_5']
            if  this_data < 0.0:
                df.at[idx, 'status'] = df.at[idx, 'status'] + ["No data"]
            elif (this_data < this_lo) or (this_data > this_hi):
                df.at[idx, 'status'] = df.at[idx, 'status'] + ["Outlier"]
                

    if apply_correction:
        for idx, datum in df.iterrows():
            df.at[idx, 'pm2_5'], this_status= jsonutils.applyCorrectionFactor(_area_models[datum["area_model"]]['correctionfactors'], datum['time'], datum['pm2_5'], datum['sensormodel'], status=True)
            df.at[idx, 'status'] = df.at[idx, 'status'] + [this_status]
    else:
        for idx, datum in df.iterrows():
            df.at[idx, 'status'] += ["No correction"]

    sensor_list = []
    for idx, row in df.iterrows():
        sensor_list.append(
            {
                "Sensor ID": str(row["ID"]),
                "Latitude": row["lat"],
                "Longitude": row["lon"],
                "Time": row["time"],
                "PM2_5": row["pm2_5"],
                "Sensor model": row["sensormodel"],
                "Sensor source": row["sensorsource"],
                "Status":row["status"]
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
            lat_hi = float(request.args.get('latHi'))
            lat_lo = float(request.args.get('latLo'))
            lon_hi = float(request.args.get('lonHi'))
            lon_lo = float(request.args.get('lonLo'))
        except ValueError:
            return 'lat, lon, lat_res, be floats in the lat-lon (not UTM) case', 400
        try:
            lat_size = int(request.args.get('latSize'))
            lon_size = int(request.args.get('lonSize'))
        except ValueError:
            return 'lat, lon, sizes must be ints (not UTM) case', 400

        lat_res = (lat_hi-lat_lo)/float(lat_size)
        lon_res = (lon_hi-lon_lo)/float(lon_size)

    query_date = request.args.get('time')
    if query_date == None:
        query_startdate = request.args.get('startTime')
        query_enddate = request.args.get('endTime')
        if (query_startdate == None) or (query_enddate == None):
            return 'requires valid date or start/end', 400
        datesequence=True
        try:
            query_rate = float(request.args.get('timeInterval', 0.25))
        except ValueError:
            return 'timeInterval must be floats.', 400
    else:
        datesequence=False

    if "areaModel" in request.args:
        area_string = request.args.get('areaModel')
    else:
        area_string = None

    area_model = jsonutils.getAreaModelByLocation(_area_models, lat=lat_hi, lon=lon_lo, string = area_string)
    if area_model == None:
        msg = f"The query location, lat={lat_hi}, lon={lon_lo}, and/or area string {area_string} does not have a corresponding area model"
        return msg, 400


    app.logger.info((
        f"Query parameters: lat_lo={lat_lo} lat_hi={lat_hi}  lon_lo={lon_lo} lon_hi={lon_hi} lat_res={lat_res} lon_res={lon_res}"
    ))

        
# build the grid of query locations
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

    elevations = area_model['elevationinterpolator'](lon_vector, lat_vector)
    locations_lon, locations_lat = np.meshgrid(lon_vector, lat_vector)
    query_lats = locations_lat.flatten()
    query_lons= locations_lon.flatten()
    query_elevations = elevations.flatten()
    query_locations = np.column_stack((query_lats, query_lons))

# deal with single or time sequences.
    if not datesequence:
        if query_date == "now":
             query_date = (datetime.now()).strftime(jsonutils.DATETIME_FORMAT)
        query_datetime = jsonutils.parseDateString(query_date, area_model['timezone'])
        if query_datetime == None:
            msg = f"The query {query_date} is not a recognized date/time format or specify 'now'; see also https://www.cl.cam.ac.uk/~mgk25/iso-time.html.  Default time zone is {area_model['timezone']}"
            return msg, 400
        query_dates = np.array([query_datetime])
    else:
        query_start_datetime = jsonutils.parseDateString(query_startdate, area_model['timezone'])
        query_end_datetime = jsonutils.parseDateString(query_enddate, area_model['timezone'])
        if query_start_datetime == None or query_end_datetime == None:
            msg = f"The query ({query_startdate}, {query_enddate}) is not a recognized date/time format; see also https://www.cl.cam.ac.uk/~mgk25/iso-time.html.  Default time zone is {area_model['timezone']}"
            return msg, 400
        query_dates = utils.interpolateQueryDates(query_start_datetime, query_end_datetime, query_rate)


#   # step 3, query relevent data
#   # for this compute a circle center at the query volume.  Radius is related to lenth scale + the size fo the box.
#     lat = (lat_lo + lat_hi)/2.0
#     lon = (lon_lo + lon_hi)/2.0
# #    NUM_METERS_IN_MILE = 1609.34
# #    radius = latlon_length_scale / NUM_METERS_IN_MILE  # convert meters to miles for db query

#     UTM_N_hi, UTM_E_hi, zone_num_hi, zone_let_hi = utils.latlonToUTM(lat_hi, lon_hi)
#     UTM_N_lo, UTM_E_lo, zone_num_lo, zone_let_lo = utils.latlonToUTM(lat_lo, lon_lo)
# # compute the lenght of the diagonal of the lat-lon box.  This units here are **meters**
#     lat_diff = UTM_N_hi - UTM_N_lo
#     lon_diff = UTM_E_hi - UTM_E_lo
#     radius = SPACE_KERNEL_FACTOR_PADDING*latlon_length_scale + np.sqrt(lat_diff**2 + lon_diff**2)/2.0

#     if not ((zone_num_lo == zone_num_hi) and (zone_let_lo == zone_let_hi)):
#         return 'Requested region spans UTM zones', 400        

    yPred, yVar, status = computeEstimatesForLocations(query_dates, query_locations, query_elevations, area_model)
    
    # yPred, yVar = gaussian_model_utils.estimateUsingModel(
    #     model, locations_lat, locations_lon, elevations, [query_datetime], time_offset)

    num_times = len(query_dates)

    elevations = (elevations).tolist()
    yPred = yPred.reshape((lat_vector.shape[0], lon_vector.shape[0], num_times))
    yVar = yVar.reshape((lat_vector.shape[0], lon_vector.shape[0], num_times))
    estimates = yPred.tolist()
    variances = yVar.tolist()
    return_object = {"Area model": area_model["note"],"Elevations":elevations,"Latitudes":lat_vector.tolist(), "Longitudes":lon_vector.tolist()
                         }

    estimates = []
    for i in range(num_times):
        estimates.append(
            {'PM2_5': (yPred[:,:,i]).tolist(), 'Variance': (yVar[:,:,i]).tolist(), 'Time': query_dates[i].strftime('%Y-%m-%d %H:%M:%S%z'), 'Status': status[i]}
            )

    return_object['estimates'] = estimates
    return jsonify(return_object)
    

@app.route("/api/getTimeAggregatedData", methods=["GET"])
def getTimeAggregatedData():
    # this is used to convert the parameter terms to those used in the database
    group_tags = {"id":"ID", "sensorModel":"sensormodel", "area":"areamodel"}
    # Get the arguments from the query string
    id = request.args.get('id')
    sensor_source = request.args.get('sensorSource')
    start = request.args.get('startTime')
    end = request.args.get('endTime')
    function = request.args.get('function')
    if "timeInterval" in request.args:
        timeInterval = int(request.args.get('timeInterval'))  # Time interval in minutes
    else:
        timeInterval = 60
    if "applyCorrection" in request.args:
        apply_correction = True
    else:
        apply_correction = False

    if "groupBy" in request.args:
        group_by = request.args.get("groupBy")
        if group_by in group_tags:
            group_string = f", {group_tags[group_by]}"
        else:
            msg = "Ground must be one of id, sensorSource, area"
            return msg, 400
    else:
        group_string = ""
        group_by == None

# if you are going to apply correction, you need to have data grouped by area and sensortype
    if apply_correction:
        if group_by == "id" or group_by == None:
            group_string = ", " + ", ".join(list(group_tags.values())[0:3])
        elif group_by == "sensorModel":
            group_string = ", ".join([group_string, group_tags["area"]])
        elif group_by == "area":
            group_string = ", ".join([group_string, group_tags["sensorModel"]])
        else:
            app.logger.warn("got a problem with the groupby logic")
            group_string = ""
            



    # Check ID is valid
    if id == "" or id == "undefined":
        id = None
#        msg = "id is invalid. It must be a string that is not '' or 'undefined'."
#        return msg, 400

    if "areaModel" in request.args:
        area_string = request.args.get('areaModel')
    else:
        area_string = "all"

    # check if sensor_source is specified
    # If not, default to all
    if sensor_source == "" or sensor_source == "undefined" or sensor_source==None:
        # Check that the arguments we want exist
        sensor_source = "all"


    SQL_FUNCTIONS = {
        "mean": "AVG",
        "min": "MIN",
        "max": "MAX",
    }

    # Check aggregation function is valid
    if function not in SQL_FUNCTIONS:
        msg = f"function is not in {SQL_FUNCTIONS.keys()}"
        return msg, 400

    # Check that the data is formatted correctly
    if not utils.validateDate(start) or not utils.validateDate(end):
        msg = "Incorrect date format, should be {utils.DATETIME_FORMAT}, e.g.: 2018-01-03T20:00:00Z"
        return msg, 400

    time_tmp = utils.parseDateString(end)
    end_interval = (time_tmp + timedelta(minutes = int(timeInterval))).strftime(utils.DATETIME_FORMAT)


    # Define the BigQuery query

    if area_string == "all":
        areas = _area_models.keys()
    else:
        areas = [area_string]

    with open('db_table_headings.json') as json_file:
        db_table_headings = json.load(json_file)


    tables_list = []

    
    for this_area in areas:
        need_source_query = False
        area_model = _area_models[this_area]
#        print(area_model)
        # this logic adjusts for the two cases, where you have different tables for each source or one table for all sources
        # get all of the sources if you need to
        source_query = ""
        if (sensor_source == "all"):
            # easy case, query all tables with no source requirement
            sources = area_model["idstring"]
        elif "sourcetablemap" in area_model:
            # if it's organized by table, then get the right table (or nothing)
            if sensor_source in area_model["sourcetablemap"]:
                sources = area_model["sourcetablemap"][sensor_source]
            else:
                sources = None
        else:
            # sources are not organized by table.  Get all the tables and add a boolean to check for the source
            sources = area_model["idstring"]
            need_source_query = True
#            source_

        for area_id_string in sources:
            empty_query = False
            time_string = db_table_headings[area_id_string]['time']
            pm2_5_string = db_table_headings[area_id_string]['pm2_5']
            lon_string = db_table_headings[area_id_string]['longitude']
            lat_string = db_table_headings[area_id_string]['latitude']
            id_string = db_table_headings[area_id_string]['id']
            model_string = db_table_headings[area_id_string]['sensormodel']
            table_string = os.getenv(area_id_string)

#            column_string = ", ".join([id_string + " AS ID", time_string + " AS time", pm2_5_string + " AS pm2_5", lat_string + " AS lat", lon_string+" AS lon","'" + this_area + "'" + " AS areamodel", model_string + " AS sensormodel"])
# area model gets taken care of below
            column_string = ", ".join([id_string + " AS ID", time_string + " AS time", pm2_5_string + " AS pm2_5", lat_string + " AS lat", lon_string+" AS lon", model_string + " AS sensormodel"])
            # put together a separate query for all of the specified sources
#            group_string = ", ".join(["ID", "pm2_5", "lat", "lon", "area_model", "sensormodel"])
            table_string = os.getenv(area_id_string)

            if "sensorsource" in db_table_headings[area_id_string]:
                sensor_string = db_table_headings[area_id_string]['sensorsource']
                column_string += ", " + sensor_string + " AS sensorsource"
                if need_source_query:
                    query = f" AND sensor_string = {sensor_source}"
            elif need_source_query:
                # if you are looking for a particular sensor source, but that's not part of the tables info, then the query is not going to return anything
                empty_query = True

                
            where_string = f"pm2_5 < {MAX_ALLOWED_PM2_5} AND time >= @start AND time <= '{end_interval}'"
            if id != None:
                where_string  += " AND ID = @id"
            where_string += source_query

                # This is to cover the case where the different regions are in the same database/table and distinguised by different labels
            if "label" in db_table_headings[area_id_string]:
                label_string = db_table_headings[area_id_string]['label']
                column_string += ", " + label_string + " AS areamodel"
                if area_model != "all":
#                    where_string += " AND " + label_string + " = " + "'" + this_area + "'"
                    where_string += " AND areamodel = " + "'" + this_area + "'"
            else:
                column_string += ", " + "'" + this_area + "'" + " AS areamodel"

            this_query = f"""(SELECT * FROM (SELECT {column_string} FROM `{table_string}`) WHERE ({where_string}))"""
        

            if not empty_query:
                tables_list.append(this_query)


    query = f"""
        WITH
            intervals AS (
                SELECT
                    TIMESTAMP_ADD(@start, INTERVAL @interval * num MINUTE) AS lower,
                    TIMESTAMP_ADD(@start, INTERVAL @interval * 60* (1 + num) - 1 SECOND) AS upper
                FROM UNNEST(GENERATE_ARRAY(0,  DIV(TIMESTAMP_DIFF(@end, @start, MINUTE) , @interval))) AS num
            )
        SELECT
            CASE WHEN {SQL_FUNCTIONS.get(function)}(pm2_5) IS NOT NULL
                THEN {SQL_FUNCTIONS.get(function)}(pm2_5)
                ELSE 0
                END AS PM2_5,
            upper  {group_string}
        FROM intervals
            JOIN (
            {' UNION ALL '.join(tables_list)}
        ) sensors
            ON sensors.time BETWEEN intervals.lower AND intervals.upper
        GROUP BY upper {group_string}
        ORDER BY upper"""

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

    if group_string == "":
        for row in rows:
            if apply_correction:
                new_pm2_5, status = jsonutils.applyCorrectionFactor(_area_models[row.areamodel]['correctionfactors'], row.upper, row.PM2_5, row.sensormodel, status=True)
            else:
                new_pm2_5 = row.PM2_5
                status = "Not corrected"
            measurements.append({"PM2_5": new_pm2_5, "time":  (row.upper + timedelta(seconds=1)).strftime(utils.DATETIME_FORMAT), "Status": status})
    else:
        for row in rows:
            if apply_correction:
                new_pm2_5, status = jsonutils.applyCorrectionFactor(_area_models[row.areamodel]['correctionfactors'], row.upper, row.PM2_5, row.sensormodel, status=True)
            else:
                new_pm2_5 = row.PM2_5
                status = "Not corrected"
            measurements.append({"PM2_5": new_pm2_5, "Time": (row.upper + timedelta(seconds=1)).strftime(utils.DATETIME_FORMAT), group_by: row[group_tags[group_by]], "Area model":row.areamodel, "Status":status})

    return jsonify({"data": measurements})


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

        if 'sensortype' in db_table_headings[area_id_string]:
            sensortype_string = db_table_headings[area_id_string]['sensortype']
            column_string += ", " + sensortype_string + " AS sensortype"

        where_string = ""
        if "label" in db_table_headings[area_id_string]:
            label_string = db_table_headings[area_id_string]["label"]
            column_string += ", " + label_string + " AS areamodel" 
            where_string += " AND " + label_string + " = " + "'" + this_area_model + "'"

        query_list.append(f"""(SELECT * FROM (SELECT {column_string} FROM `{table_string}` WHERE (({time_string} > '{start_date}') AND ({time_string} < '{end_date}')) {where_string}) WHERE ((lat <= {lat_hi}) AND (lat >= {lat_lo}) AND (lon <= {lon_hi}) AND (lon >= {lon_lo})) AND (pm2_5 < {max_value}) AND (pm2_5 > {min_value}))""")


    query = " UNION ALL ".join(query_list) + " ORDER BY time ASC "

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
#    AND time > @start_date AND time < @end_date
#    ORDER BY time ASC
# Also could do this by spherical coordinates on the earth -- however we use a lat-lon box to save compute time on the BigQuery server

# radius should be in *meters*!!!
# this has been modified so that it now takes an array of lats/lons
# the radius parameter is not implemented in a precise manner -- rather it is converted to a lat-lon bounding box and all within that box are returned
# there could be an additional culling of sensors outside the radius done here after the query - if the radius parameter needs to be precise. 
def request_model_data_local(lats, lons, radius, start_date, end_date, area_model, outlier_filtering = True):
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
    rows = submit_sensor_query(lat_lo, lat_hi, lon_lo, lon_hi, start_date, end_date, area_model, min_value, max_value)

#    print(rows)
    for row in rows:
        new_row = {
            "ID": row.id,
            "Latitude": row.lat,
            "Longitude": row.lon,
            "time": row.time,
            "PM2_5": row.pm2_5,
            }
        if 'sensormodel' in row:
            new_row["SensorModel"] = row.sensormodel
        else:
            new_row["SensorModel"] = "Default"

        if 'sensorsource' in row:
            new_row["SensorModel"] = row.sensorsource
        else:
            new_row["SensorSource"] = "Default"

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


# returns sensor data for a range of times within a distance radius (meters) of the lat-lon location.
# notice the times are assumed to be mountain time....
@app.route("/api/getLocalSensorData", methods=['GET'])
def getLocalSensorData():
    query_parameters = request.args
    try:
        lat = float(query_parameters.get('lat'))
        lon = float(query_parameters.get('lon'))
        radius = float(query_parameters.get('radius'))
    except ValueError:
        return 'lat, lon, radius, must be floats.', 400

    start_date = query_parameters.get('startTime')
    end_date = query_parameters.get('endTime')
#    print("model requuest api with " + str(lat) + ":" + str(lon) + " and radius " + str(radius) + " and start " + str(start_date) + " and end " + str(end_date))
    # must format these for database
    tf = TimezoneFinder()
    start_datetime = jsonutils.parseDateString(start_date, tf.timezone_at(lng=lon, lat=lat))
    end_datetime = jsonutils.parseDateString(end_date, tf.timezone_at(lng=lon, lat=lat))
    if start_datetime == None or end_datetime == None:
        msg = f"The query ({start_date}, {end_date}) is not a recognized date/time format; see also https://www.cl.cam.ac.uk/~mgk25/iso-time.html.  Default time zone is {area_model['timezone']}"
        return msg, 400


    area_model = jsonutils.getAreaModelByLocation(_area_models, lat, lon)
    model_data = request_model_data_local(lat, lon, radius, start_datetime, end_datetime, area_model, outlier_filtering = False)
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
        query_rate = float(request.args.get('timeInterval'))
    except ValueError:
        return 'lat, lon, estimatesrate must be floats.', 400

    query_start_date = request.args.get('startTime')
    query_end_date = request.args.get('endTime')

    # Check that the data is formatted correctly
    if not utils.validateDate(query_start_date) or not utils.validateDate(query_end_date):
        msg = f"Incorrect date format, should be {utils.DATETIME_FORMAT}, e.g.: 2018-01-03T20:00:00Z"
        return msg, 400


    area_model = jsonutils.getAreaModelByLocation(_area_models, query_lat, query_lon)
    if area_model == None:
        msg = f"The query location, lat={query_lat}, lon={query_lon}, does not have a corresponding area model"
        return msg, 400

    query_start_datetime = jsonutils.parseDateString(query_start_date, area_model['timezone'])
    query_end_datetime = jsonutils.parseDateString(query_end_date, area_model['timezone'])
    if query_start_datetime == None or query_end_datetime == None:
        msg = f"The query ({query_start_date}, {query_end_date}) is not a recognized date/time format; see also https://www.cl.cam.ac.uk/~mgk25/iso-time.html.  Default time zone is {area_model['timezone']}"
        return msg, 400

    query_dates = utils.interpolateQueryDates(query_start_datetime, query_end_datetime, query_rate)
    query_elevations = area_model['elevationinterpolator'](query_lon, query_lat)
    query_locations = np.column_stack((np.array((query_lat)), np.array((query_lon))))

    app.logger.info(
        "Query parameters: lat= %f lon= %f start_date= %s end_date=%s estimatesrate=%f hours/estimate" %(query_lat, query_lon, query_start_datetime, query_end_datetime, query_rate))

    yPred, yVar, status = computeEstimatesForLocations(query_dates, query_locations, query_elevations, area_model)
    
# convert the arrays to lists of floats

#
# this index of the "0" in the first index of yPred and yVar has to do with how the data is stored and returned by the model.  Could be avoided with a tranpose of the returned data?
#
    num_times = len(query_dates)
    estimates = []
    for i in range(num_times):
        estimates.append(
            {'PM2_5': (yPred[0, i]), 'Variance': (yVar[0, i]), 'Time': query_dates[i].strftime('%Y-%m-%d %H:%M:%S%z'), 'Latitude': query_lat, 'Longitude': query_lon, 'Elevation': query_elevations[0], 'Status': status[i]}
            )

    # estimates = [
    #     {'PM2_5': pred, 'variance': var, 'datetime': date.strftime('%Y-%m-%d %H:%M:%S%z'), 'Latitude': query_lat, 'Longitude': query_lon, 'Elevation': query_elevation}
    #     for pred, var, date in zip(yPred, yVar, query_dates)
    #     ]

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
        query_rate = float(request.args.get('timeInterval'))
    except ValueError:
        return 'estimatesrate must be floats.', 400

## regular expression for floats
    regex = '[+-]?[0-9]+\.[0-9]+'
    query_lats = np.array(re.findall(regex,request.args.get('lat'))).astype(np.float)
    query_lons = np.array(re.findall(regex,request.args.get('lon'))).astype(np.float)
    if (query_lats.shape != query_lons.shape):
        return 'lat, lon must be equal sized arrays of floats:'+str(query_lats)+' ; ' + str(query_lons), 400

    num_locations = query_lats.shape[0]

    query_start_date = request.args.get('startTime')
    query_end_date = request.args.get('endTime')

    # Check that the data is formatted correctly
    if not utils.validateDate(query_start_date) or not utils.validateDate(query_end_date):
        msg = f"Incorrect date format, should be {utils.DATETIME_FORMAT}, e.g.: 2018-01-03T20:00:00Z"
        return msg, 400


    area_model = jsonutils.getAreaModelByLocation(_area_models, query_lats[0], query_lons[0])
    if area_model == None:
        msg = f"The query location, lat={query_lats[0]}, lon={query_lons[0]}, does not have a corresponding area model"
        return msg, 400

    query_start_datetime = jsonutils.parseDateString(query_start_date, area_model['timezone'])
    query_end_datetime = jsonutils.parseDateString(query_end_date, area_model['timezone'])
    if query_start_datetime == None or query_end_datetime == None:
        msg = f"The query ({query_start_date}, {query_end_date}) is not a recognized date/time format; see also https://www.cl.cam.ac.uk/~mgk25/iso-time.html.  Default time zone is {area_model['timezone']}"
        return msg, 400


#    print((
#        f"Query parameters: lat={query_lats} lon={query_lons} start_date={query_start_datetime}"
#        f" end_date={query_end_datetime} estimatesrate={query_rate}"
#    ))

################  start of generic code
### we know:     query locations, query start date, query end data
    query_dates = utils.interpolateQueryDates(query_start_datetime, query_end_datetime, query_rate)
    query_locations = np.column_stack((query_lats, query_lons))
# note - the elevation grid is the wrong way around, so you need to put in lons first
    
    query_elevations = np.array([area_model['elevationinterpolator'](this_lon, this_lat)[0] for this_lat, this_lon in zip(query_lats, query_lons)])
    
    yPred, yVar, status = computeEstimatesForLocations(query_dates, query_locations, query_elevations, area_model)

    num_times = len(query_dates)
    data_out = {'Latitude': query_lats.tolist(), 'Longitude': query_lons.tolist(), 'Elevation': query_elevations.tolist()}
    estimates = []

    for i in range(num_times):
        estimates.append(
            {'PM2_5': (yPred[:,i]).tolist(), 'Variance': (yVar[:,i]).tolist(), 'Time': query_dates[i].strftime('%Y-%m-%d %H:%M:%S%z'), 'Status': status[i]}
            )
    data_out["Estimates"] = estimates
    return jsonify(data_out)


# this is a generic helper function that sets everything up and runs the model
def computeEstimatesForLocations(query_dates, query_locations, query_elevations, area_model, outlier_filtering = True):
    num_locations = query_locations.shape[0]
    query_lats = query_locations[:,0]
    query_lons = query_locations[:,1]
    query_start_datetime = query_dates[0]
    query_end_datetime = query_dates[-1]

    # step 0, load up the bounding box from file and check that request is within it

    # for i in range(num_locations):
    #     if not jsonutils.isQueryInBoundingBox(area_model['boundingbox'], query_lats[i], query_lons[i]):
    #         app.logger.error(f"The query location, {query_lats[i]},{query_lons[i]},  is outside of the bounding box.")
    #         return np.full((query_lats.shape[0], len(query_dates)), 0.0), np.full((query_lats.shape[0], len(query_dates)), np.nan), ["Query location error" for i in query_dates]

    # step 2, load up length scales from file

    latlon_length_scale, time_length_scale, elevation_length_scale = jsonutils.getLengthScalesForTime(area_model['lengthscales'], query_start_datetime)
    if latlon_length_scale == None:
            app.logger.error("No length scale found between dates {query_start_datetime} and {query_end_datetime}")
            return np.full((query_lats.shape[0], query_dates.shape[0]), 0.0), np.full((query_lats.shape[0], query_dates.shape[0]), np.nan), ["Length scale parameter error" for i in range(query_dates.shape[0])]
    app.logger.debug("Loaded length scales: space=" + str(latlon_length_scale) + " time=" + str(time_length_scale) + " elevation=" + str(elevation_length_scale))


    app.logger.debug(f'Using length scales: latlon={latlon_length_scale} elevation={elevation_length_scale} time={time_length_scale}')

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
            radius,
            query_start_datetime - timedelta(hours=TIME_KERNEL_FACTOR_PADDING*time_length_scale),
            query_end_datetime + timedelta(hours=TIME_KERNEL_FACTOR_PADDING*time_length_scale),
            area_model, outlier_filtering)

    unique_sensors = {datum['ID'] for datum in sensor_data}
    app.logger.info(f'Loaded {len(sensor_data)} data points for {len(unique_sensors)} unique devices from bgquery.')

    # step 3.5, convert lat/lon to UTM coordinates
    try:
        utils.convertLatLonToUTM(sensor_data)
    except ValueError as err:
        app.logger.error(str(err))
        return np.full((query_lats.shape[0], query_dates.shape[0]), 0.0), np.full((query_lats.shape[0], query_dates.shape[0]), np.nan), ["Failure to convert lat/lon" for i in range(query_dates.shape[0])]

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
        return np.full((query_lats.shape[0], query_dates.shape[0]), 0.0), np.full((query_lats.shape[0], query_dates.shape[0]), np.nan), ["Zero sensor data" for i in range(query_dates.shape[0])]

    # step 4.5, Data Screening
#    print('Screening data')
    sensor_data = utils.removeInvalidSensors(sensor_data)

    # step 5, apply correction factors to the data
    for datum in sensor_data:
        datum['PM2_5'] = jsonutils.applyCorrectionFactor(area_model['correctionfactors'], datum['time'], datum['PM2_5'], datum['SensorModel'])

    # step 6, add elevation values to the data
    # NOTICE - the elevation object takes locations in the form "lon-lat"
    # this seems redundant since elevations are passed in...
    for datum in sensor_data:
        if 'Altitude' not in datum:
            datum['Altitude'] = area_model['elevationinterpolator']([datum['Longitude']],[datum['Latitude']])[0]

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
    sensor_sequence, query_sequence = utils.chunkTimeQueryData(query_dates, time_sequence_length, time_padding)

    yPred = np.empty((num_locations, 0))
    yVar = np.empty((num_locations, 0))
    status = []
    if len(sensor_data) == 0:
        status = "0 sensors/measurements"
        return 
    for i in range(len(query_sequence)):
    # step 7, Create Model
        model, time_offset, model_status = gaussian_model_utils.createModel(
            sensor_data, latlon_length_scale, elevation_length_scale, time_length_scale, sensor_sequence[i][0], sensor_sequence[i][1], save_matrices=True)
        # check to see if there is a valid model
        if (model == None):
            yPred_tmp = np.full((query_lats.shape[0], len(query_sequence[i])), 0.0)
            yVar_tmp = np.full((query_lats.shape[0], len(query_sequence[i])), np.nan)
            status_estimate_tmp = [model_status for i in range(len(query_sequence[i]))]
        else:
            yPred_tmp, yVar_tmp, status_estimate_tmp = gaussian_model_utils.estimateUsingModel(
                model, query_lats, query_lons, query_elevations, query_sequence[i], time_offset, save_matrices=True)
        # put the estimates together into one matrix
        yPred = np.concatenate((yPred, yPred_tmp), axis=1)
        yVar = np.concatenate((yVar, yVar_tmp), axis=1)
        status = status + status_estimate_tmp

    if np.min(yPred) < MIN_ACCEPTABLE_ESTIMATE:
        app.logger.warn("got estimate below level " + str(MIN_ACCEPTABLE_ESTIMATE))
        
# Here we clamp values to ensure that small negative values to do not appear
    yPred = np.clip(yPred, a_min = 0., a_max = None)

    return yPred, yVar, status





