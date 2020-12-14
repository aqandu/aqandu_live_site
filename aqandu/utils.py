from datetime import datetime, timedelta
import pytz
import utm
from matplotlib.path import Path
import numpy as np
from scipy import interpolate
from scipy.io import loadmat
import csv


DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def validateDate(dateString):
    """Check if date string is valid"""
    try:
        return dateString == datetime.strptime(dateString, DATETIME_FORMAT).strftime(DATETIME_FORMAT)
    except ValueError:
        return False


def parseDateString(datetime_string):
    """Parse date string into a datetime object"""
    return datetime.strptime(datetime_string, DATETIME_FORMAT).astimezone(pytz.timezone('US/Mountain'))

#  this breaks the time part of the  eatimation/data into pieces to speed up computation
# sequence_size_mins
# assumes query_dates are sorted
def chunkTimeQueryData(query_dates, time_sequence_size, time_padding):
    # Careful.  Is the padding in date-time or just integer minutes.
    start_date = query_dates[0]
    end_date = query_dates[-1]
    query_length = (end_date - start_date)
#    query_length_mins = query_length.total_seconds()/60
    num_short_queries = int(query_length/time_sequence_size)
# cover the special corner case where the time series is shorter than the specified chunk size
    if (num_short_queries == 0):
        query_time_sequence = []
        query_time_sequence.append(query_dates)
    else:
        short_query_length = query_length/num_short_queries
        time_index = 0
        query_time_sequence = []
        for i in range(0,num_short_queries - 1):
            query_time_sequence.append([])
            while query_dates[time_index] < start_date + (i+1)*short_query_length:
                query_time_sequence[-1].append(query_dates[time_index])
                time_index += 1
# put the last sequence in place
        query_time_sequence.append([])
        for i in range(time_index,len(query_dates)):
            query_time_sequence[-1].append(query_dates[time_index])
            time_index += 1

# now build the endpoints we will need for the sensor data that feeds the estimates of each of these ranges of queries (they overlap)
    sensor_time_sequence = []
    for i in range(len(query_time_sequence)):
        sensor_time_sequence.append([query_time_sequence[i][0] - time_padding, query_time_sequence[i][-1] + time_padding])

    return sensor_time_sequence, query_time_sequence

# Load up elevation grid
# BE CAREFUL - this object, given the way the data is saved, seems to talk "lxbon-lat" order
def setupElevationInterpolator(filename):
    data = loadmat(filename)
    elevation_grid = data['elevs']
    gridLongs = data['gridLongs']
    gridLats = data['gridLats']
    np.savetxt('grid_lats.txt',gridLats)
    np.savetxt('grid_lons.txt',gridLongs)
    np.savetxt('elev_grid.txt', elevation_grid)
    print(gridLongs.shape)
    print(gridLats.shape)
    print(elevation_grid.shape)
    return interpolate.interp2d(gridLongs, gridLats, elevation_grid, kind='cubic')


def loadBoundingBox(filename):
    with open(filename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        rows = [row for row in csv_reader][1:]
        bounding_box_vertices = [(index, float(row[1]), float(row[2])) for row, index in zip(rows, range(len(rows)))]
        return bounding_box_vertices


def loadCorrectionFactors(filename):
    with open(filename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        rows = [row for row in csv_reader]
        header = rows[0]
        rows = rows[1:]
        correction_factors = []
        for row in rows:
            rowDict = {name: elem for elem, name in zip(row, header)}
            rowDict['start_date'] = parseDateString(rowDict['start_date'])
            rowDict['end_date'] = parseDateString(rowDict['end_date'])
            rowDict['1003_slope'] = float(rowDict['1003_slope'])
            rowDict['1003_intercept'] = float(rowDict['1003_intercept'])
            rowDict['3003_slope'] = float(rowDict['3003_slope'])
            rowDict['3003_intercept'] = float(rowDict['3003_intercept'])
            rowDict['5003_slope'] = float(rowDict['5003_slope'])
            rowDict['5003_intercept'] = float(rowDict['5003_intercept'])
            correction_factors.append(rowDict)
        return correction_factors


def loadLengthScales(filename):
    with open(filename) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        rows = [row for row in csv_reader]
        header = rows[0]
        rows = rows[1:]
        length_scales = []
        for row in rows:
            rowDict = {name: elem for elem, name in zip(row, header)}
            rowDict['start_date'] = parseDateString(rowDict['start_date'])
            rowDict['end_date'] = parseDateString(rowDict['end_date'])
            rowDict['latlon'] = float(rowDict['latlon'])
            rowDict['elevation'] = float(rowDict['elevation'])
            rowDict['time'] = float(rowDict['time'])
            length_scales.append(rowDict)
        return length_scales


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


def removeInvalidSensors(sensor_data):
    # sensor is invalid if its average reading for any day exceeds 350 ug/m3
    epoch = datetime(1970, 1, 1)
    epoch = pytz.timezone('US/Mountain').localize(epoch)
    dayCounts = {}
    dayReadings = {}
    for datum in sensor_data:
        pm25 = datum['PM2_5']
        datum['daysSinceEpoch'] = (datum['time'] - epoch).days
        key = (datum['daysSinceEpoch'], datum['ID'])
        if key in dayCounts:
            dayCounts[key] += 1
            dayReadings[key] += pm25
        else:
            dayCounts[key] = 1
            dayReadings[key] = pm25

    # get days that had higher than 350 avg reading
    keysToRemove = [key for key in dayCounts if (dayReadings[key] / dayCounts[key]) > 350]
    keysToRemoveSet = set()
    for key in keysToRemove:
        keysToRemoveSet.add(key)
        keysToRemoveSet.add((key[0] + 1, key[1]))
        keysToRemoveSet.add((key[0] - 1, key[1]))

    print(f'Removing these days from data due to exceeding 350 ug/m3 avg: {keysToRemoveSet}')
    sensor_data = [datum for datum in sensor_data if (datum['daysSinceEpoch'], datum['ID']) not in keysToRemoveSet]

    # TODO NEEDS TESTING!
    # 5003 sensors are invalid if Raw 24-hour average PM2.5 levels are > 5 ug/m3
    # AND the two sensors differ by more than 16%
    sensor5003Locations = {
        datum['ID']: (datum['utm_x'], datum['utm_y']) for datum in sensor_data if datum['type'] == '5003'
    }
    sensorMatches = {}
    for sensor in sensor5003Locations:
        for match in sensor5003Locations:
            if sensor5003Locations[sensor] == sensor5003Locations[match] and sensor != match:
                sensorMatches[sensor] = match
                sensorMatches[match] = sensor

    keysToRemoveSet = set()
    for key in dayReadings:
        sensor = key[1]
        day = key[0]
        if sensor in sensorMatches:
            match = sensorMatches[sensor]
            reading1 = dayReadings[key] / dayCounts[key]
            key2 = (day, match)
            if key2 in dayReadings:
                reading2 = dayReadings[key2] / dayCounts[key2]
                difference = abs(reading1 - reading2)
                maximum = max(reading1, reading2)
                if min(reading1, reading2) > 5 and difference / maximum > 0.16:
                    keysToRemoveSet.add(key)
                    keysToRemoveSet.add((key[0] + 1, key[1]))
                    keysToRemoveSet.add((key[0] - 1, key[1]))
                    keysToRemoveSet.add(key2)
                    keysToRemoveSet.add((key2[0] + 1, key2[1]))
                    keysToRemoveSet.add((key2[0] - 1, key2[1]))

    print((
        "Removing these days from data due to pair of 5003 sensors with both > 5 "
        f"daily reading and smaller is 16% different reading from larger : {keysToRemoveSet}"
    ))
    sensor_data = [datum for datum in sensor_data if (datum['daysSinceEpoch'], datum['ID']) not in keysToRemoveSet]

    # * Otherwise just average the two readings and correct as normal.
    return sensor_data


def applyCorrectionFactor(factors, data_timestamp, data, sensor_type):
    for factor in factors:
        factor_start = factor['start_date']
        factor_end = factor['end_date']
        if factor_start <= data_timestamp and factor_end > data_timestamp:
            if sensor_type == '1003':
                return data * factor['1003_slope'] + factor['1003_intercept']
            elif sensor_type == '3003':
                return data * factor['3003_slope'] + factor['3003_intercept']
            elif sensor_type == '5003':
                return data * factor['5003_slope'] + factor['5003_intercept']
###  print('\nNo correction factor found for ', data_timestamp)
#  no correction factor will be considered identity
    return data


def getScalesInTimeRange(scales, start_time, end_time):
    relevantScales = []
    for scale in scales:
        scale_start = scale['start_date']
        scale_end = scale['end_date']
        if start_time < scale_end and end_time >= scale_start:
            relevantScales.append(scale)
    return relevantScales


def interpolateQueryDates(start_datetime, end_datetime, period):
    query_dates = []
    query_date = start_datetime
    while query_date <= end_datetime:
        query_dates.append(query_date)
        query_date = query_date + timedelta(hours=period)

    return query_dates

# Not yet sure if this is needed
# build a grid of coordinates that will consistute the "map"
#def interpolateQueryLocationsUTM(lat_lo, lat_hi, lon_lo, lon_hi, spatial_res): 
    # # create the north sound and east west locations in UTM coordinates
    # E_range = np.arrange(lon_low, lon_hi, spatial_res)
    # N_range = np.arrange(lat_low, lat_hi, spatial_res)
    # return np.meshgrid(E_range, N_range)

# build a grid of coordinates that will consistute the "map"  - used for getEstimateMap() in the api
def interpolateQueryLocations(lat_lo, lat_hi, lon_lo, lon_hi, lat_res, lon_res): 
#    lat_step = (lat_hi-lat_low)/float(lat_size)
#    lon_step = (lon_hi-lon_low)/float(lon_size)
    lat_range = np.arange(lon_lo, lon_hi, lon_res)
    lon_range = np.arange(lat_lo, lat_hi, lat_res)
    return lat_range, lon_range
#    return np.meshgrid(lat_range, lon_range)




# computes an approximate latlon bounding box that includes the given point and all points within the radius of distance_meters.  Used to limit the query of "relevant sensors".  Note the return order...
def latlonBoundingBox(lat, lon, distance_meters):
    E, N, zone_num, zone_let  = utm.from_latlon(lat, lon)
    lat_lo, lon_tmp = utm.to_latlon(E, N-distance_meters, zone_num, zone_let)
    lat_hi, lon_tmp = utm.to_latlon(E, N+distance_meters, zone_num, zone_let)
    lat_tmp, lon_lo = utm.to_latlon(E-distance_meters, N, zone_num, zone_let)
    lat_tmp, lon_hi = utm.to_latlon(E+distance_meters, N, zone_num, zone_let)
    print(lat_lo, lat_hi, lon_lo, lon_hi)
    return lat_lo, lat_hi, lon_lo, lon_hi

# when you have multiple queries at once, you need to build bounding boxes that include all of the sensors
def boundingBoxUnion(bbox1, bbox2):
    return min(bbox1[0], bbox2[0]), max(bbox1[1], bbox2[1]), min(bbox1[2], bbox2[2]), max(bbox1[3], bbox2[3])


# convenience/wrappers for the utm toolbox
def latlonToUTM(lat, lon):
    return utm.from_latlon(lat, lon)

def UTM(E, N, zone_num, zone_let):
    return utm.to_latlon(E, N)

def convertLatLonToUTM(sensor_data):
    for datum in sensor_data:
        datum['utm_x'], datum['utm_y'], datum['zone_num'], zone_let = latlonToUTM(datum['Latitude'], datum['Longitude'])
