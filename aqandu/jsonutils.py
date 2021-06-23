from matplotlib.path import Path
import json
import pytz
from datetime import datetime
from dateutil.tz import tzoffset
from dateutil.parser import parse
from dateutil.utils import default_tzinfo
from dateutil import tz
import numpy as np
import logging
from scipy import interpolate
from scipy.io import loadmat

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

JAN_FIRST = datetime(2000, 1, 1, 0, 0, 0, 0, pytz.timezone('UTC'))
JAN_LAST = datetime(2100, 1, 1, 0, 0, 0, 0, pytz.timezone('UTC'))

def parseDateString(datetime_string, dflt_tz_string=None):
    if (dflt_tz_string == None):
        try:
            return parse(datetime_string)
        except:
            return None
    else:
        dflt_tz = tz.gettz(dflt_tz_string)
        try:
            return default_tzinfo(parse(datetime_string), dflt_tz)
        except:
            return None

def loadBoundingBox(bbox_info):
        rows = [row for row in bbox_info]
        bounding_box_vertices = [(index, float(row['Latitude']), float(row['Longitude'])) for row, index in zip(rows, range(len(rows)))]
        return bounding_box_vertices

def loadCorrectionFactors(cfactor_info, dflt_tz_string=None):
    cfactors = {}
    for sensor_type in cfactor_info:
        sensorDict = []
        for row in cfactor_info[sensor_type]:
# need to deal with default values
            new_row = {}
            if (row['starttime'] != 'default'):
                new_row['starttime'] = parseDateString(row['starttime'], dflt_tz_string)
                new_row['endtime'] = parseDateString(row['endtime'], dflt_tz_string)
            new_row['slope'] = float(row['slope'])
            new_row['intercept'] = float(row['intercept'])
            new_row['note'] = row['note']
            sensorDict.append(new_row)
# put the default at the end of the list -- the system will use whichever one hits first
        for row in cfactor_info[sensor_type]:
            if (row['starttime'] == 'default'):
                new_row['starttime'] = JAN_FIRST
                new_row['endtime'] = JAN_LAST
                new_row['slope'] = float(row['slope'])
                new_row['intercept'] = float(row['intercept'])
                new_row['note'] = row['note']
                sensorDict.append(new_row)
        cfactors[sensor_type] = sensorDict
    return cfactors

# put the default at the end of the list -- the system will use whichever one hits first
def loadLengthScales(length_info, dflt_tz_string=None):
    lengthScaleArray = []
    for row in length_info:
        new_row = {}
        if (row['starttime'] != 'default'):
            new_row['starttime'] = parseDateString(row['starttime'], dflt_tz_string)
            new_row['endtime'] = parseDateString(row['endtime'], dflt_tz_string)
            new_row['Space'] = float(row['Space'])
            new_row['Time'] = float(row['Time'])
            new_row['Elevation'] = float(row['Elevation'])
            lengthScaleArray.append(new_row)
    for row in length_info:
        if (row['starttime'] == 'default'):
            new_row['starttime'] = JAN_FIRST
            new_row['endtime'] = JAN_LAST
            new_row['Space'] = float(row['Space'])
            new_row['Time'] = float(row['Time'])
            new_row['Elevation'] = float(row['Elevation'])
            lengthScaleArray.append(new_row)
    return lengthScaleArray


def isQueryInBoundingBox(bounding_box_vertices, query_lat, query_lon):
    verts = [(0, 0)] * len(bounding_box_vertices)
    for elem in bounding_box_vertices:
        verts[elem[0]] = (elem[2], elem[1])
    # Add first vertex to end of verts so that the path closes properly
    verts.append(verts[0])
    codes = [Path.MOVETO]
    codes += [Path.LINETO] * (len(verts) - 2)
    codes += [Path.CLOSEPOLY]
    boundingBox = Path(verts, codes)
    return boundingBox.contains_point((query_lon, query_lat))

def getAreaModelByLocation(area_models, lat=0.0, lon=0.0, string=None):
    if (string == None):
        for key in area_models:
            if (isQueryInBoundingBox(area_models[key]['boundingbox'], lat, lon)):
                return area_models[key]
    else:
        try:
            return area_models[string]
        except:
            logging.warn("Got bad request for area by string: " + str(string))

    logging.warn("Query location "+str(lat)+ "," + str(lon) + " not in any known model area")
    return None

def buildAreaModelsFromJson(json_data):
    area_models = {}
    for key in json_data:
        this_model = {}
        this_model['name'] = json_data[key]['Name']
        this_model['timezone'] = json_data[key]['Timezone']
        this_model['idstring'] = json_data[key]['ID String']
        this_model['elevationfile'] = json_data[key]['Elevation File']
        this_model['note'] = json_data[key]['Note']
#delay loading elev maps to save memory
#        this_model['elevationinterpolator'] = buildAreaElevationInterpolator(json_data[key]['Elevation File'])
        this_model['elevationfile'] = json_data[key]['Elevation File']
        this_model['boundingbox'] = loadBoundingBox(json_data[key]['Boundingbox'])
        this_model['correctionfactors'] = loadCorrectionFactors(json_data[key]['Correction Factors'],json_data[key]['Timezone'])
        this_model['lengthscales'] = loadLengthScales(json_data[key]['Length Scales'], json_data[key]['Timezone'])
        if 'Source table map' in json_data[key]:
            this_model['sourcetablemap'] = json_data[key]['Source table map']
        # else:
        #     this_model['sourcetablemap'] = None
        area_models[key] = this_model
    return area_models

# note this can be very slow -- need 
def applyCorrectionFactor(factors, data_timestamp, data, sensor_type, status=False):
    if sensor_type in factors:
        this_type = sensor_type
    elif "default" in factors:
        this_type = "default"
    default_idx = -1
    for i in range(len(factors[this_type])):
            if factors[this_type][i]['starttime'] <= data_timestamp and factors[this_type][i]['endtime'] > data_timestamp:
                if not status:
                    return np.maximum(data * factors[this_type][i]['slope'] + factors[this_type][i]['intercept'], 0.0)
                else:
                    # print(f"factor type is {factor_type} and case {i}")
                    # print(factors[factor_type][i])
                    return np.maximum(data * factors[this_type][i]['slope'] + factors[this_type][i]['intercept'], 0.0), factors[this_type][i]['note']

            if factors[this_type][i]['starttime'] == "default":
                default_idx = i
    if default_idx >= 0:
        return np.maximum(data * factors[this_type][default_idx]['slope'] + factors[this_type][default_idx]['intercept'], 0.0), factors[this_type][i]['note']
        
    if not status:
        return data
    else:
        return data, "no correction"

def getLengthScalesForTime(length_scales_array, datetime):
    default_idx = -1
    for i in range(len(length_scales_array)):
        if length_scales_array[i]['starttime'] == "default":
            default_idx = i
        elif length_scales_array[i]['starttime'] <= datetime and length_scales_array[i]['endtime'] > datetime:
            return length_scales_array[i]['Space'], length_scales_array[i]['Time'], length_scales_array[i]['Elevation']
    if default_idx >= 0:
        return length_scales_array[default_idx]['Space'], length_scales_array[default_idx]['Time'], length_scales_array[default_idx]['Elevation']
    logging.warn("failure to find length scale in area model: " + str(area_model["Note"]))
    return None, None, None
    
def buildAreaElevationInterpolator(filename):
    data = loadmat(filename)
    elevation_grid = data['elevs']
    gridLongs = data['lons']
    gridLats = data['lats']
    # np.savetxt('grid_lons.txt',gridLongs)
    # np.savetxt('elev_grid.txt', elevation_grid)
    return interpolate.interp2d(gridLongs, gridLats, elevation_grid, kind='linear', fill_value=0.0)

#print(default_tzinfo(parse('2017-11-01T00:00:00Z'), dflt_tz))
#print(default_tzinfo(parse('2017-11-01T00:00:00'), dflt_tz))
