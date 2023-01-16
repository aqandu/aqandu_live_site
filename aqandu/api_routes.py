
######  FIX THESE
# Time sequence size
# apply_correction_factors

from datetime import datetime, timedelta
import os
import json
from aqandu import app, bq_client, bigquery, utils, gaussian_model_utils, cache, jsonutils, api_utils
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


# Load in .env and set the table name
load_dotenv()  # Required for compatibility with GCP, can't use pipenv there


    
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

    median, MAD, count = api_utils.estimateMedianDeviation(start_date, end_date, lat_lo, lat_hi, lon_lo, lon_hi, area_model)
    
#    for row in median_data:
#        print(f"min = {row.min}")
#        print(f"median = {row.median}")
#        print(f"max = {row.max}")
#        print(f"num sensors = {row.num_sensors}")
    summary = {"Median":median, "Count":count, "MAD":MAD}
    
    return(jsonify(summary))


@app.route("/api/getCorrectionFactors", methods=["GET"])
def getCorrectionFactors():
# Get the arguments from the query string
    area_string = request.args.get('areaModel', default=None)
    time = request.args.get('time', default=None)

    if area_string == "all" or area_string==None:
        areas = _area_models.keys()
    elif area_string in _area_models.keys():
        areas = [area_string]
    else:
        msg = f"Specific areaModel {area_string} not available, options are {_area_models.keys()}"
        return msg, 400
    all_factors = {}
    for area in areas:
        area_model = _area_models[area]
        factors = area_model['correctionfactors']
        if time != None:
            area_factors = {}
            if time == "now":
                this_time = (datetime.now()).strftime(jsonutils.DATETIME_FORMAT[0])
            else:
                this_time = jsonutils.parseDateString(time, area_model['timezone'])
            for this_type in factors:
                for i in range(len(factors[this_type])):
                    if (factors[this_type][i]['starttime'] <= this_time and factors[this_type][i]['endtime'] > this_time) or (factors[this_type][i]['starttime'] == "default"):
                        area_factors[this_type] = factors[this_type][i]
                        break
            all_factors[area] = area_factors
        else:
            all_factors[area] = factors
    return(jsonify(all_factors))


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

    if "areaModel" in request.args:
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

    print(f"Areas are {areas} and areaModel is {area_string}")

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

            column_string = " ".join([id_string, "AS ID,", time_string, "AS time,", pm2_5_string, "AS pm2_5,", lat_string, "AS lat,", lon_string, "AS lon,", model_string, "AS sensormodel"])
            # put together a separate query for all of the specified sources
            table_string = os.getenv(area_id_string)

            if "sensorsource" in db_table_headings[area_id_string]:
                sensor_string = db_table_headings[area_id_string]['sensorsource']
                column_string += ", " + sensor_string + " AS sensorsource"
            elif (not source_query==""):
            # if you are looking for a particular sensor source, but that's not part of the tables info, then the query is not going to return anything
                empty_query = True

            where_string = "time >= @start AND time <= @end"
            if id != None:
                where_string  += " AND ID = @id"
            where_string += source_query


                # This is to cover the case where the different regions are in the same database/table and distinguised by different labels
            if "label" in db_table_headings[area_id_string]:
                label_string = db_table_headings[area_id_string]['label']
                column_string += ", " + label_string + " AS area_model"
                if area_model != "all":
                    where_string += " AND " + "area_model" + " = " + "'" + this_area + "'"
            else:
                column_string += ", " + "'" + this_area + "'" + " AS area_model"

                
                # for efficiency, don't do the query if the sensorsource is needed by not available


            this_query = f"""(SELECT * FROM (SELECT {column_string} FROM `{table_string}`) WHERE ({where_string}))"""

            if not empty_query:
                query_list.append(this_query)

    query = " UNION ALL ".join(query_list) + " ORDER BY time ASC "
    print(f"getSensorData query is {query}")

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
        measurements.append({"Sensor source": row["sensorsource"], "Sensor ID": row["ID"], "PM2_5": row["pm2_5"], "Time": row["time"].strftime(utils.DATETIME_FORMAT[0]), "Latitude": row["lat"], "Longitude": row["lon"], "Status": row["status"]})
    # tags = [{
    #     "ID": id,
    #     "SensorSource": sensor_source,
    #     "time": datetime.utcnow().strftime(utils.DATETIME_FORMAT)
    # }]
    # return jsonify({"data": measurements, "tags": tags})
    return jsonify(measurements)

@app.route("/api/getSensorLocations", methods=["GET"])
@cache.cached(timeout=59, query_string=True)
def getSensorLocations():
    if "areaModel" in request.args:
        area_string = request.args.get('areaModel')
    else:
        area_string = "all"

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
        area_model = _area_models[this_area]
#        print(area_model)
        # this logic adjusts for the two cases, where you have different tables for each source or one table for all sources
        # get all of the sources if you need to
        sources = area_model["idstring"]

        now = datetime.utcnow()
        one_hour_ago = now - timedelta(hours=1)  # AirU + PurpleAir sensors have reported in the last hour
        one_week_ago = now - timedelta(days=7)  # AirU + PurpleAir sensors have reported in the last hour

        for area_id_string in sources:
            where_string = " WHERE TRUE"
            time_string = db_table_headings[area_id_string]['time']
            lon_string = db_table_headings[area_id_string]['longitude']
            lat_string = db_table_headings[area_id_string]['latitude']
            id_string = db_table_headings[area_id_string]['id']
            table_string = os.getenv(area_id_string)

            column_string = ", ".join([id_string + " AS ID", time_string + " AS time", lat_string + " AS lat", lon_string+" AS lon"])
            # put together a separate query for all of the specified sources
            table_string = os.getenv(area_id_string)

                # This is to cover the case where the different regions are in the same database/table and distinguised by different labels
            if "label" in db_table_headings[area_id_string]:
                label_string = db_table_headings[area_id_string]['label']
                column_string += ", " + label_string + " AS area_model"
                if area_model != "all":
                    where_string += " AND " + label_string + " = " + "'" + this_area + "'"
#                    where_string += " AND area_model = " + this_area
            else:
                column_string += ", " + "'" + this_area + "'" + " AS area_model"

            #where_string += f"AND {time_string} >= '{str(one_hour_ago)}'"
            where_string += f" AND {time_string} >= '{str(one_week_ago)}'"

        # Define the BigQuery query
            
            this_query = f"""(WITH a AS (SELECT {column_string} FROM `{table_string}` {where_string}),  b AS (SELECT {id_string} AS ID, max({time_string})  AS LATEST_MEASUREMENT FROM `{table_string}` GROUP BY {id_string}) SELECT * FROM a INNER JOIN b ON a.time = b.LATEST_MEASUREMENT and b.ID = a.ID)"""
#            this_query = f"""(SELECT * FROM (SELECT {column_string}, max({time_string}) AS LATEST_MEASUREMENT FROM `{table_string}` WHERE {time_string} >= '{str(one_hour_ago)}' GROUP BY {group_string}) WHERE LATEST_MEASUREMENT > '{str(one_hour_ago)}')"""
#            print(this_query)

            query_list.append(this_query)


    # Build the actual query from the list of options
    query = " UNION ALL ".join(query_list)

    print(query)
    # Run the query and collect the result
    query_job = bq_client.query(query)
    df = query_job.to_dataframe()
    sensor_list = {}
    for idx, row in df.iterrows():
        sensor_list[row["ID"]] = {"Latitude": row["lat"], "Longitude": row["lon"], "Time": row["time"]}
    return jsonify(sensor_list)

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

    if area_string == "all":
        areas = _area_models.keys()
    else:
        if area_string in _area_models:
            areas = [area_string]
        else:
            msg = f"The area string {area_string} does not have a corresponding area model"
            return msg, 400

    if "time" in request.args:
        if area_string != "all":
            now = jsonutils.parseDateString(request.args.get('time'), _area_models[areas[0]]['timezone'])
        else:
            now = jsonutils.parseDateString(request.args.get('time'))
    else:
        now = datetime.utcnow()

    # Define the BigQuery query
    one_hour_ago = now - timedelta(hours=1)  # AirU + PurpleAir sensors have reported in the last hour
    three_hours_ago = now - timedelta(hours=3)  # DAQ sensors have reported in the 3 hours
    query_list = []


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
                column_string += ", " + "'" + this_area + "'" + " AS area_model"

            where_string += f" AND {source_query} AND {time_string} >= '{str(one_hour_ago)}' AND {time_string} <= '{str(now)}'  "

            this_query = f"""(WITH a AS (SELECT {column_string} FROM `{table_string}` {where_string}),  b AS (SELECT {id_string} AS ID, max({time_string})  AS LATEST_MEASUREMENT FROM `{table_string}` WHERE {time_string} >= '{str(one_hour_ago)}' AND {time_string} <= '{str(now)}' GROUP BY {id_string}) SELECT * FROM a INNER JOIN b ON a.time = b.LATEST_MEASUREMENT and b.ID = a.ID)"""
#            this_query = f"""(SELECT * FROM (SELECT {column_string}, max({time_string}) AS LATEST_MEASUREMENT FROM `{table_string}` WHERE {time_string} >= '{str(one_hour_ago)}' GROUP BY {group_string}) WHERE LATEST_MEASUREMENT > '{str(one_hour_ago)}')"""



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
    print(f"liveSensors query {query}")


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
            if  this_data < 0.0 or math.isnan(this_data):
                df.at[idx, 'status'] = df.at[idx, 'status'] + ["No data"]
            elif (this_data < this_lo) or (this_data > this_hi) or np.isnan(this_data):
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
    aggregation_interval = request.args.get('aggregateInterval')
    if "fullVariance" in request.args:
        full_variance = True
    else:
        full_variance = False
    
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

#    elevation_interpolator = jsonutils.buildAreaElevationInterpolator(area_model['elevationfile'])
#   elevations = elevation_interpolator(lon_vector, lat_vector)
    locations_lon, locations_lat = np.meshgrid(lon_vector, lat_vector)
    query_lats = locations_lat.flatten()
    query_lons= locations_lon.flatten()
#    query_elevations = elevations.flatten()
    query_locations = np.column_stack((query_lats, query_lons))

# deal with single or time sequences.
    if not datesequence:
        if query_date == "now":
             query_date = (datetime.now()).strftime(jsonutils.DATETIME_FORMAT[0])
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

    if (aggregation_interval == None):
        yPred, yVar, query_elevations, status = api_utils.computeEstimatesForLocations(query_dates, query_locations, area_model,  full_variance = full_variance)
    else:
        aggregation_interval = int(aggregation_interval)
        yPred, yVar, query_elevations, status = api_utils.computeEstimatesForLocations(query_dates, query_locations, area_model, aggregation_interval=aggregation_interval, full_variance = full_variance)

    print(f"max estimate is {np.max(yPred)} and min is {np.min(yPred)}")
    # yPred, yVar = gaussian_model_utils.estimateUsingModel(
    #     model, locations_lat, locations_lon, elevations, [query_datetime], time_offset)

    num_times = len(query_dates)

    print(f"elevations shape {query_elevations.shape}")
    query_elevations = query_elevations.reshape((lat_vector.shape[0], lon_vector.shape[0]))
    print(f"elevations shape {query_elevations.shape}")
    yPred = yPred.reshape((lat_vector.shape[0], lon_vector.shape[0], num_times))
    print(f"yPred shape {yPred.shape}")
    if not full_variance:
        yVar = yVar.reshape((lat_vector.shape[0], lon_vector.shape[0], num_times))
    else:
        yVar = yVar.reshape((lat_vector.shape[0]*lon_vector.shape[0], lat_vector.shape[0]*lon_vector.shape[0], num_times))

    estimates = yPred.tolist()
    variances = yVar.tolist()
    elevations = query_elevations.tolist()

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
    if "id" in request.args:
        id = request.args.get('id')
    else:
        id = "all"
    if "sensorSource" in request.args:
        sensor_source = request.args.get('sensorSource')
    else:
        sensor_source = "all"
    start = request.args.get('startTime')
    end = request.args.get('endTime')
    function = request.args.get('function')
    if "timeInterval" in request.args:
        timeInterval = int(request.args.get('timeInterval'))  # Time interval in minutes
    else:
        timeInterval = 60
    if "noCorrection" in request.args:
        apply_correction = False
    else:
        apply_correction = True

    if "groupBy" in request.args:
        group_by = request.args.get("groupBy")
        if group_by in group_tags:
            group_string = f", {group_tags[group_by]}"
        else:
            msg = "Ground must be one of id, sensorSource, area"
            return msg, 400
    else:
        group_string = ""
        group_by = None

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
            



#        msg = "id is invalid. It must be a string that is not '' or 'undefined'."
#        return msg, 400

    if "areaModel" in request.args:
        area_string = request.args.get('areaModel')
        if not area_string in _area_models:
            msg = f"Invalid area model {area_string} - options are: {_area_models.keys()}"
            return msg, 400
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
    end_interval = (time_tmp + timedelta(minutes = int(timeInterval)))

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

                
            where_string = f"pm2_5 < {api_utils.MAX_ALLOWED_PM2_5} AND time >= @start AND time <= '{end_interval}'"
            if id != "all":
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

    print(f"aggregate query is {query}")
    
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
            new_row = {"PM2_5": new_pm2_5, "time":  (row.upper + timedelta(seconds=1)).strftime(utils.DATETIME_FORMAT[0]), "Status": status}
            if id != "all":
                new_row["id"] = id
            if sensor_source != "all":
                new_row["Sensor source"] = sensor_source
            measurements.append(new_row)
    else:
        for row in rows:
            if apply_correction:
                new_pm2_5, status = jsonutils.applyCorrectionFactor(_area_models[row.areamodel]['correctionfactors'], row.upper, row.PM2_5, row.sensormodel, status=True)
            else:
                new_pm2_5 = row.PM2_5
                status = "Not corrected"
            new_row = {"PM2_5": new_pm2_5, "Time": (row.upper + timedelta(seconds=1)).strftime(utils.DATETIME_FORMAT[0]), "Status":status}
            if group_by != None:
                new_row[group_by] = row[group_tags[group_by]]
                # if each ID is presented, also present their locations
            # if group_by == "id":
            #     new_row["Latitude"] = row.lat
            #     new_row["Longitude"] = row.lon
# if a specific ID is presented, present it's location as well
            if id != "all":
                new_row["id"] = id
                # new_row["Latitude"] = row.lat
                # new_row["Longitude"] = row.lon
            if sensor_source != "all":
                new_row["Sensor source"] = sensor_source
            measurements.append(new_row)

    return jsonify(measurements)


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
    model_data = api_utils.request_model_data_local(lat, lon, radius, start_datetime, end_datetime, area_model, outlier_filtering = False)
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
    # elevation_interpolator = jsonutils.buildAreaElevationInterpolator(area_model['elevationfile'])
    # query_elevations = elevation_interpolator(query_lon, query_lat)
    query_locations = np.column_stack((np.array((query_lat)), np.array((query_lon))))

    app.logger.info(
        "Query parameters: lat= %f lon= %f start_date= %s end_date=%s estimatesrate=%f hours/estimate" %(query_lat, query_lon, query_start_datetime, query_end_datetime, query_rate))

    yPred, yVar, query_elevations, status = api_utils.computeEstimatesForLocations(query_dates, query_locations, area_model)
    
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

    print("about to do lats/lons")
## regular expression for floats
    regex = '[+-]?[0-9]+\.[0-9]+'
    query_lats = np.array(re.findall(regex,request.args.get('lat'))).astype(np.float)
    query_lons = np.array(re.findall(regex,request.args.get('lon'))).astype(np.float)
    if (query_lats.shape != query_lons.shape):
        print(f"lat, lon must be equal sized arrays of floats:'+{query_lats.shape} and {query_lons.shape}")
        return 'lat, lon must be equal sized arrays of floats:'+str(query_lats)+' ; ' + str(query_lons), 400



    num_locations = query_lats.shape[0]
    print(f"num locations is {num_locations}")
    query_start_date = request.args.get('startTime')
    query_end_date = request.args.get('endTime')

    # Check that the data is formatted correctly
    if not utils.validateDate(query_start_date) or not utils.validateDate(query_end_date):
        msg = f"Incorrect date format, should be {utils.DATETIME_FORMAT}, e.g.: 2018-01-03T20:00:00Z"
        return msg, 400

    print("about to do area model")
    area_model = jsonutils.getAreaModelByLocation(_area_models, query_lats[0], query_lons[0])
    if area_model == None:
        msg = f"The query location, lat={query_lats[0]}, lon={query_lons[0]}, does not have a corresponding area model"
        return msg, 400
    print("done get area model")
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
    print("about to do query dates")
    query_dates = utils.interpolateQueryDates(query_start_datetime, query_end_datetime, query_rate)
    query_locations = np.column_stack((query_lats, query_lons))
# note - the elevation grid is the wrong way around, so you need to put in lons first

#    elevation_interpolator = jsonutils.buildAreaElevationInterpolator(area_model['elevationfile'])    
#    query_elevations = np.array([elevation_interpolator(this_lon, this_lat)[0] for this_lat, this_lon in zip(query_lats, query_lons)])
    yPred, yVar, query_elevations, status = api_utils.computeEstimatesForLocations(query_dates, query_locations, area_model)
    num_times = len(query_dates)
    data_out = {'Latitude': query_lats.tolist(), 'Longitude': query_lons.tolist(), 'Elevation': query_elevations.tolist()}
    estimates = []

    for i in range(num_times):
        estimates.append(
            {'PM2_5': (yPred[:,i]).tolist(), 'Variance': (yVar[:,i]).tolist(), 'Time': query_dates[i].strftime('%Y-%m-%d %H:%M:%S%z'), 'Status': status[i]}
            )
    data_out["Estimates"] = estimates
    return jsonify(data_out)






