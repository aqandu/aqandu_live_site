from datetime import datetime, timedelta
import pytz
import utm
from matplotlib.path import Path

def isQueryInBoundingBox(bounding_box_vertices, query_lat, query_lon):
    verts = [(0, 0)] * len(bounding_box_vertices)
    for elem in bounding_box_vertices:
        verts[elem[0]] = (elem[2], elem[1])
    # Add first vertex to end of verts so that the path closes properly
    verts.append(verts[0])
    codes = [Path.MOVETO]
    codes += [Path.LINETO]*(len(verts)-2)
    codes += [Path.CLOSEPOLY]
    boundingBox = Path(verts, codes)
    return boundingBox.contains_point((query_lon, query_lat))


def removeInvalidSensors(sensor_data):
    # sensor is invalid if its average reading for any day exceeds 350 ug/m3
    epoch = datetime(1970,1,1)
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
        keysToRemoveSet.add((key[0]+1, key[1]))
        keysToRemoveSet.add((key[0]-1, key[1]))

    print(f'Removing these days from data due to exceeding 350 ug/m3 avg: {keysToRemoveSet}')
    sensor_data = [datum for datum in sensor_data if (datum['daysSinceEpoch'], datum['ID']) not in keysToRemoveSet]


    # TODO NEEDS TESTING!
    # 5003 sensors are invalid if Raw 24-hour average PM2.5 levels are > 5 ug/m3 AND the two sensors differ by more than 16%
    sensor5003Locations = {datum['ID']: (datum['utm_x', datum['utm_y']]) for datum in sensor_data if datum['type'] == '5003'}
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
                    keysToRemoveSet.add((key[0]+1, key[1]))
                    keysToRemoveSet.add((key[0]-1, key[1]))
                    keysToRemoveSet.add(key2)
                    keysToRemoveSet.add((key2[0]+1, key2[1]))
                    keysToRemoveSet.add((key2[0]-1, key2[1]))

    print(f'Removing these days from data due to pair of 5003 sensors with both > 5 daily reading and smaller is 16% different reading from larger : {keysToRemoveSet}')
    sensor_data = [datum for datum in sensor_data if (datum['daysSinceEpoch'], datum['ID']) not in keysToRemoveSet]

    # * Otherwise just average the two readings and correct as normal.
    return sensor_data

def applyCorrectionFactor(factors, data_timestamp, data, sensor_type):
    for factor in factors:
        factor_start = factor['start_date']
        factor_end = factor['end_date']
        if factor_start <= data_timestamp and factor_end > data_timestamp:
            if sensor_type == '1003':
                return data*factor['1003_slope'] + factor['1003_intercept']
            elif sensor_type == '3003':
                return data*factor['3003_slope'] + factor['3003_intercept']
            elif sensor_type == '5003':
                return data*factor['5003_slope'] + factor['5003_intercept']
    print('\nNo correction factor found for ', data_timestamp)
    return data


def getScalesInTimeRange(scales, start_time, end_time):
    relevantScales = []
    for scale in scales:
        scale_start = scale['start_date']
        scale_end = scale['end_date']
        if start_time < scale_end and end_time >= scale_start:
            relevantScales.append(scale)
    return relevantScales


def parseDateTimeParameter(datetime_string):
    datetime_string = datetime_string.replace('/', ' ')
    # datetime.strptime(datetime_string, '%Y-%m-%d %H:%M:%S').astimezone(pytz.timezone('US/Mountain'))
    try:
        return datetime.strptime(datetime_string, '%Y-%m-%d %H:%M:%S%z').astimezone(pytz.timezone('US/Mountain'))
    except:
        try:
            # assume mountain time if no time zone provided
            return datetime.strptime(datetime_string, '%Y-%m-%d %H:%M:%S').astimezone(pytz.timezone('US/Mountain'))
        except:
            return None


def interpolateQueryDates(start_datetime, end_datetime, frequency):
    query_dates = []
    query_date = start_datetime
    while query_date <= end_datetime:
        query_dates.append(query_date)
        query_date = query_date + timedelta(hours=frequency)

    return query_dates


def latlonToUTM(lat, lon):
    return utm.from_latlon(lat, lon)


def convertLatLonToUTM(sensor_data):
    provided_utm_zones = set()
    for datum in sensor_data:
        datum['utm_x'], datum['utm_y'], datum['zone_num'], zone_let = latlonToUTM(datum['Latitude'], datum['Longitude'])
    #     provided_utm_zones.add(zone_num)

    # if len(provided_utm_zones) is not 1:
    #     raise ValueError(f'The Provided data must fall into the same UTM zone but it does not! UTM zones provided: {provided_utm_zones}')
