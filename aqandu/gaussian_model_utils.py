from datetime import datetime
import pytz
import numpy
from aqandu import gaussian_model
from aqandu import utils
import torch
import statistics
import logging


JANUARY1ST = datetime(2000, 1, 1, 0, 0, 0, 0, pytz.timezone('UTC'))
TIME_COORDINATE_BIN_NUMBER_KEY = 'time_coordinate_bin_number'

##  These are indices into the sensor map
UTM_X_INDEX = 0
UTM_Y_INDEX = 1
ELEV_INDEX = 2
# index for this sensor into the data matrix
SPACE_COORD_INDEX = 3
# A map that returns a set of measurements at a given time
TIME_MAP_INDEX = 4
# An array of time measurements after processing sets
# indices for this array are same as data matrix
TIME_ARRAY_INDEX = 5
###################

# some variables in binning and correcting data
SENSOR_INTERPOLATE_DISTANCE = 6
NUM_MINUTES_PER_BIN = 8
# this is the percentage of sensors recording measurements within a time slice/bin before a warning is reported
TIME_SLICE_MIN_SENSOR_RATE = 0.65
# a general number for reporting warnings about sensors that fail to report, interpolate, etc.
FRACTION_SIGNIFICANT_FAILS = 0.30


def getTimeCoordinateBin(datetime, time_offset=0):
    delta = datetime - JANUARY1ST
    # this bin needs to be set to be a fraction of the time scale.
    # this is kind of an important variable and should probably not be hard coded like this
    # NUM_MINUTES_PER_BIN = 8
    # convert from seconds to minutes
    tmp = delta.total_seconds() / 60
    # round down to the nearest multiple of NUM_MINUTES_PER_BIN
    tmp = float(int(tmp/NUM_MINUTES_PER_BIN))*NUM_MINUTES_PER_BIN
    # convert from minutes to hours
    bin_number = float(tmp/60)
#    bin_number = float(int((delta.total_seconds()/60)/NUM_MINUTES_PER_BIN) / 60 * NUM_MINUTES_PER_BIN)
    return bin_number - time_offset


def convertToTimeCoordinatesVector(dates, time_offset):
    return [getTimeCoordinateBin(date, time_offset=time_offset) for date in dates]


def createTimeVector(sensor_data, time_lo_bound = -1.0, time_hi_bound = -1.0):
    time_coordinates = set()

# check to see if there are bounds set on this query (typically to improve efficiency).
    time_bounds = not ( (time_lo_bound == -1.0) or (time_hi_bound == -1.0) )

    lowest_bin_number = None

    for datum in sensor_data:
        this_time = datum['time']
#  you only fill in the bin stuff for the dates that are in range
        if (not time_bounds) or ((this_time >= time_lo_bound) and (this_time <= time_hi_bound)):
            bin_number = getTimeCoordinateBin(this_time)
            time_coordinates.add(bin_number)
            datum[TIME_COORDINATE_BIN_NUMBER_KEY] = bin_number

            if lowest_bin_number is None or bin_number < lowest_bin_number:
                lowest_bin_number = bin_number

#  you only fill in the bin stuff for the dates that are in range
    for datum in sensor_data:
        this_time = datum['time']
        if (not time_bounds) or ((this_time >= time_lo_bound) and (this_time <= time_hi_bound)):
            datum[TIME_COORDINATE_BIN_NUMBER_KEY] -= lowest_bin_number

    time_coordinates = [bin_number - lowest_bin_number for bin_number in time_coordinates]
    time_coordinates.sort()
    
#    print("time1")
#    print(time_coordinates)
#    time_coordinates.sort()
    time_coordinates = numpy.expand_dims(numpy.asarray(time_coordinates), axis=1)
    

    return time_coordinates, lowest_bin_number


# organize the measurement data into time bins for each sensor
def assignTimeData(sensor_data, device_location_map, time_offset, time_lo_bound = -1.0, time_hi_bound = -1.0):
# This loads the device_location_map with a set of bins, and each bin contains all of the measurements associated with that bin and that device.  Later we will average these or choose one of them (median)
    time_bounds = not ( (time_lo_bound == -1.0) or (time_hi_bound == -1.0) )
    for datum in sensor_data:
        this_time = datum['time']
        if (not time_bounds) or ((this_time >= time_lo_bound) and (this_time <= time_hi_bound)):
            bin_number = getTimeCoordinateBin(this_time) - time_offset
            this_id = datum['ID']
            if not (bin_number in device_location_map[this_id][TIME_MAP_INDEX]):
                device_location_map[this_id][TIME_MAP_INDEX].update({bin_number:{datum['PM2_5']}})
            else:
                device_location_map[this_id][TIME_MAP_INDEX][bin_number].add(datum['PM2_5'])
    # this is already done in createTimeVector() -- do we need this
    # datum[TIME_COORDINATE_BIN_NUMBER_KEY] = bin_number

    # for key in device_location_map.keys():
    #     loc = device_location_map[key]
    #     if loc[SPACE_COORD_INDEX] <= 5:
    #         print(loc)
    #         print(loc[SPACE_COORD_INDEX])
    #         print(loc[UTM_X_INDEX])
    #         print(loc[TIME_MAP_INDEX])



def createSpaceVector(sensor_data):
    for datum in sensor_data:
        if datum['ID'] not in device_location_map:
            device_location_map[datum['ID']] = (datum['utm_x'], datum['utm_y'], datum['Altitude'])

    space_coordinates = numpy.ndarray(shape=(0, 3), dtype=float)
    for key in device_location_map.keys():
        loc = device_location_map[key]
        toadd = numpy.asarray([loc[0], loc[1], loc[2]])
        toadd = numpy.expand_dims(toadd, axis=0)
        space_coordinates = numpy.append(space_coordinates, toadd, axis=0)
        device_location_map[key] = space_coordinates.shape[0] - 1

    return space_coordinates, device_location_map

# this builds up the first instance of the device_location_map
# sucessive calls will process and fill in the time data
# differs from the first instance, above, in that it stores for each sensor the list of times that it is available.  This is used later for filling in data
def createSpaceVector2(sensor_data, time_array_size):
    device_location_map = {}

    # the time array allows us to keep track of how many entries this sensor has within each time bin
    for datum in sensor_data:
        if datum['ID'] not in device_location_map:
            # for the meaning of these entries, see the index set at top of file
            device_location_map[datum['ID']] = [datum['utm_x'], datum['utm_y'], datum['Altitude'], -1, {}, numpy.full((time_array_size), -1.0)]

    space_coordinates = numpy.ndarray(shape=(0, 3), dtype=float)
    for key in device_location_map.keys():
        loc = device_location_map[key]
        toadd = numpy.asarray([loc[0], loc[1], loc[2]])
        toadd = numpy.expand_dims(toadd, axis=0)
        space_coordinates = numpy.append(space_coordinates, toadd, axis=0)
#     this redefines the map to have a unique number -- can be used in debugging.
        device_location_map[key][SPACE_COORD_INDEX] = space_coordinates.shape[0] - 1

    # for key in device_location_map.keys():
    #     loc = device_location_map[key]
    #     if loc[SPACE_COORD_INDEX] <= 5:
    #         print(loc)
    #         print(loc[SPACE_COORD_INDEX])
    #         print(loc[UTM_X_INDEX])
    #         print(loc[TIME_MAP_INDEX])
        
    return space_coordinates, device_location_map


# used for debugging - you can use the "save_matrices" flag to get intermediate data to files. 
def saveMatrixToFile(matrix, filename):
    with open(filename, 'w') as output_file:
        for row in matrix:
            for col in row:
                print(f'{col:0.2f}\t', end='', file=output_file)
            print(file=output_file)


# goal is to fill in zero/bad elements in between two values
# only do short distances, e.g.  1 > <  SENSOR_INTERPOLATE_DISTANCE (missing bins)
def interpolateBadElements(matrix, bad_value = 0):
    num_interp_fails = 0
    for row in matrix:
        prevValueIndex = None
        this_fail = False
        for i in range(row.shape[0]):
            if row[i] != bad_value:
                if prevValueIndex is None:
                    prevValueIndex = i
# this takes care of the boundary at the beginning of the time sequence
                    if (i > 0) and (i < SENSOR_INTERPOLATE_DISTANCE):
                        row[0:i] = row[i]
                else:
                    curValueIndex = i
                    distance = curValueIndex - prevValueIndex
                    if (distance > 1):
                        if (distance < SENSOR_INTERPOLATE_DISTANCE):
                        # interpolate zeros between prev and cur
                            terp = numpy.interp(range(prevValueIndex + 1, curValueIndex), [prevValueIndex, curValueIndex], [row[prevValueIndex], row[curValueIndex]])
                            row[prevValueIndex + 1:curValueIndex] = terp
                            prevValueIndex = curValueIndex
                        else:
                            this_fail = True
        if this_fail:
            num_interp_fails += 1
        # take care of bad values and end of time range
        if row[-1] == bad_value:
            curValueIndex = row.shape[0]-1
            if (prevValueIndex != None):
                distance = curValueIndex - prevValueIndex
                if (distance < SENSOR_INTERPOLATE_DISTANCE):            
                    row[prevValueIndex+1: curValueIndex+1] = row[prevValueIndex]
            else:
                logging.debug("got full row of bad indices?" + str(row))
        if (float(num_interp_fails)/matrix.shape[0]) > FRACTION_SIGNIFICANT_FAILS:
            logging.warn("got %d interp failures out of %d sensors", num_interp_fails, matrix.shape[0])
            saveMatrixToFile(matrix, 'failed_interp_matrix.txt')            
                
        

def trimBadEdgeElements(matrix, time_coordinates, bad_value=-1):
    # record index of edge values for each row
    firstValues = {}
    lastValues = {}
    for col_index in range(matrix.shape[1]):
        inverse_col_index = -1 - col_index
        for row_index in range(matrix.shape[0]):
            if row_index not in firstValues:
                if matrix[row_index][col_index] != bad_value:
                    firstValues[row_index] = col_index
            if row_index not in lastValues:
                if matrix[row_index, inverse_col_index] != bad_value:
                    lastValues[row_index] = inverse_col_index
        if len(firstValues) == matrix.shape[0] and len(lastValues) == matrix.shape[0]:
            break
    maxFirstValue = max(firstValues.values())
    minLastValue = min(lastValues.values())
    minLastValue = matrix.shape[1] + minLastValue

    # limit matrix to the range:
    matrix = matrix[:, maxFirstValue:minLastValue]
    time_coordinates = time_coordinates[maxFirstValue:minLastValue]

    return matrix, time_coordinates

# if a sensor doesn't have enough data then it gets taken out of calculations
def removeBadSensors(data_matrix, space_coordinates, ratio):
    toKeep = [(numpy.count_nonzero(row != -1.0) / len(row)) > ratio for row in data_matrix]
    fraction_removed = (1.0 - float(numpy.sum(toKeep))/float(data_matrix.shape[0]))
    if (fraction_removed > FRACTION_SIGNIFICANT_FAILS):
        logging.warn("Removed %f percent of sensors due to insufficient measurements", 100.0*fraction_removed)
        logging.debug("time slices in remove data to keep " + str(toKeep))
    else:
        logging.info("Removed %f percent of sensors due to insufficient measurements", 100.0*fraction_removed)
    data_matrix = data_matrix[toKeep]
    space_coordinates = space_coordinates[toKeep]
    return data_matrix, space_coordinates


# for debuging to check raw data against matrix data
# generally not needed
def getSensorIDByUTMCoords(sensor_data, utm_x, utm_y):
    target_sensor_id = -1
    for datum in sensor_data:
        if utm_x==datum['utm_x'] and utm_y==datum['utm_y']:
            target_sensor_id = datum['ID']
    return target_sensor_id

# for debuging to check raw data against matrix data
# generally not needed
def getSensorIDByMatrixPosition(sensor_data, data_matrix, row):
    return getSensorIDByUTMCoords(sensor_data, data_matrix[row, 0], data_matrix[row, 1])
    
def setupDataMatrix(sensor_data, space_coordinates, time_coordinates, device_location_map):
#    data_matrix = numpy.zeros(shape=(space_coordinates.shape[0], time_coordinates.shape[0]))
    shape=(space_coordinates.shape[0], time_coordinates.shape[0])
    data_matrix = numpy.full(shape, -1.0)
    for datum in sensor_data:
        date_index = numpy.nonzero(time_coordinates == datum[TIME_COORDINATE_BIN_NUMBER_KEY])[0][0]
        location_index = device_location_map[datum['ID']]
        # bound sensor data below by 0
        data_matrix[location_index][date_index] = datum['PM2_5'] if datum['PM2_5'] >= 0 else 0

#    saveMatrixToFile(data_matrix, '1matrix.txt')
    interpolateBadElements(data_matrix,-1)
#    saveMatrixToFile(data_matrix, '2interpolated.txt')
    data_matrix, space_coordinates = removeBadSensors(data_matrix, space_coordinates, 0.6)
#    saveMatrixToFile(data_matrix, '3matrix_removed_bad.txt')

#  This is important because bins on either end of the time range might not have enough measurements
#    data_matrix, time_coordinates = trimBadEdgeElements(data_matrix, time_coordinates)

    #    saveMatrixToFile(data_matrix, '4matrixtrimmed.txt')

# for debugging report id of last sensor in matrix - to get raw data
    # print("ID of last sensor is")
    # print(space_coordinates[space_coordinates.shape[0]-1, :])
    # print(getSensorIDByMatrixPosition(sensor_data, space_coordinates, (space_coordinates.shape[0]-1)))

    return data_matrix, space_coordinates, time_coordinates


# this is a new version of the setupDataMatrix which organizes things to that one can remove the bad sensors in different ways
def setupDataMatrix2(sensor_data, space_coordinates, time_coordinates, device_location_map):
#    data_matrix = numpy.zeros(shape=(space_coordinates.shape[0], time_coordinates.shape[0]))
    data_shape=(space_coordinates.shape[0], time_coordinates.shape[0])
    data_matrix = numpy.full(data_shape, -1.0)

    # THIS IS THE OLD METHOD -- STILL USED ABOVE
    # for datum in sensor_data:
    #     date_index = numpy.nonzero(time_coordinates == datum[TIME_COORDINATE_BIN_NUMBER_KEY])[0][0]
    #     location_index = device_location_map[datum['ID']]
    #     # bound sensor data below by 0
    #     data_matrix[location_index][date_index] = datum['PM2_5'] if datum['PM2_5'] >= 0 else 0

    # print("setupdatamatrix")
    # for key in device_location_map.keys():
    #     loc = device_location_map[key]
    #     if loc[SPACE_COORD_INDEX] <= 5:
    #         print(loc)
    #         print(loc[SPACE_COORD_INDEX])
    #         print(loc[UTM_X_INDEX])
    #         print(loc[TIME_MAP_INDEX])

#    print(time_coordinates.shape)
    for key in device_location_map.keys():
        device = device_location_map[key]
        space_index = device[SPACE_COORD_INDEX]
        # this looping through time is now done in the compute time arrays routine
        # then we can clean up the sensors based on lack of data beforehand
#        for i in range(time_coordinates.shape[0]):
            # this is a set of measurements for this device in this time bin -- create a list and find median
            # time offset is needed because of the way the location_map stores time values -- it creates them before the mininum value is set.  This should be cleaned up later
#            measurement_set = (device[TIME_MAP_INDEX]).get(time_coordinates[i][0], -1)
#            if (measurement_set != -1) and (len(measurement_set) > 0):
#                data_matrix[space_index][i] = statistics.median(list(measurement_set))
                # else there is no data and we leave the value at the initialized value above for later processing
        time_data_array = device[TIME_ARRAY_INDEX]
#        for i in range(time_coordinates.shape[0]):
#            data_matrix[space_index,i] = time_data_array[i,0]
        data_matrix[space_index,:] = time_data_array

    saveMatrixToFile(data_matrix, '1matrix.txt')
    numpy.savetxt('1matrix.csv', data_matrix, delimiter=',')
    interpolateBadElements(data_matrix, -1)
    saveMatrixToFile(data_matrix, '2interpolated.txt')
    numpy.savetxt('2interpolated.csv', data_matrix, delimiter=',')
    data_matrix, space_coordinates = removeBadSensors(data_matrix, space_coordinates, 0.75)
    saveMatrixToFile(data_matrix, '3matrix_removed_bad.txt')
    numpy.savetxt('3removed_bad.csv', data_matrix, delimiter=',')
    # fill in missing readings using the average values for each time slice

#  This is important because bins on either end of the time range might not have enough measurements
#    data_matrix, time_coordinates = trimBadEdgeElements(data_matrix, time_coordinates)
#    saveMatrixToFile(data_matrix, '4matrixtrimmed.txt')
#    numpy.savetxt('4matrixtrimmed.csv', data_matrix, delimiter=',')

    data_matrix = fillInMissingReadings(data_matrix, -1)
    saveMatrixToFile(data_matrix, '4matrix_filled_bad.txt')
    numpy.savetxt('4filled_bad.csv', data_matrix, delimiter=',')
    # fill in missing readings using the average values for each time slice

#    data_matrix, time_coordinates = trimEdgeZeroElements(data_matrix, time_coordinates)
#    saveMatrixToFile(data_matrix, '4matrixtrimmed.txt')

# for debugging report id of last sensor in matrix - to get raw data
    # print("ID of last sensor is")
    # print(space_coordinates[space_coordinates.shape[0]-1, :])
    # print(getSensorIDByMatrixPosition(sensor_data, space_coordinates, (space_coordinates.shape[0]-1)))

    return data_matrix, space_coordinates, time_coordinates



#####  fill in missing values with time averages.
# keep an eye out for time slices with too few good values and fill those in (print warning)
def fillInMissingReadings(data_matrix, bad_value = 0.):
    data_mask = (data_matrix != bad_value)
    data_counts = numpy.sum(data_mask, 0)
    if (float(numpy.min(data_counts))/float(data_matrix.shape[0]) < TIME_SLICE_MIN_SENSOR_RATE):
        logging.warn("WARNING: got time slice with too few data sensor values with value " + str(float(numpy.min(data_counts))/float(data_matrix.shape[0])) + " and index "  + str(numpy.nonzero((data_counts/data_matrix.shape[0]) < 0.75)))
    sum_tmp = numpy.sum(numpy.multiply(data_matrix,data_mask), 0)
    time_averages = numpy.divide(sum_tmp, data_counts, out=numpy.zeros_like(sum_tmp), where=(data_counts!=0))
#    print("time_averages is " + str(time_averages))
    for idx in numpy.ndindex(data_matrix.shape):
        if data_matrix[idx] == bad_value:
            data_matrix[idx] = time_averages[idx[1]]
    # it = numpy.nditer(data_matrix, flags=['multi_index'], op_flags=['readwrite'])
    # for data_value in it:
    #     if data_value == bad_value:
    #         print("got bad value at " + str(it.multi_index) + " filling in with " + str(time_averages[it.multi_index[1]]))
    #         data_value = time_averages[it.multi_index[1]]
    # it.close()
    return data_matrix


# this fills in the PM2.5 values for each sensor over an array of times into the correct field of the device location map -- which holds a lot of stuff about each sensor
def computeTimeArrays(sensor_data, device_location_map, time_coordinates):
    #    print(time_coordinates.shape)
    for key in device_location_map.keys():
        device = device_location_map[key]
        for i in range(time_coordinates.shape[0]):
            # this is a set of measurements for this device in this time bin -- create a list and find median
            # time offset is needed because of the way the location_map stores time values -- it creates them before the mininum value is set.  This should be cleaned up later
            measurement_set = (device[TIME_MAP_INDEX]).get(time_coordinates[i][0], -1)
            if (measurement_set != -1) and (len(measurement_set) > 0):
                device[TIME_ARRAY_INDEX][i] = statistics.median(list(measurement_set))
                # else there is no data and we leave the value at the initialized value above for later processing

# look a the time arrays and flat sensors with too much missing data for removal
# not needed because taken care of in the dataMatrix routine
# def flagUnderperformingSensors(device_location_map):

# creates the gaussian_model object and loads it with the sensor data
# Nov 2020 : This has been modified so that it takes bounds on the times considered.  This is for use in breaking up long time sequences into smaller chunks for efficiency
def createModel(sensor_data, latlon_length_scale, elevation_length_scale, time_length_scale, time_lo_bound = -1.0, time_hi_bound = -1.0, save_matrices=False):

    time_coordinates, time_offset = createTimeVector(sensor_data, time_lo_bound, time_hi_bound)
##    space_coordinates, device_location_map = createSpaceVector(sensor_data)
# this builds up the first instance of the device_location_map
# sucessive calls will process and fill in the time data
    space_coordinates, device_location_map = createSpaceVector2(sensor_data, time_coordinates.shape[0])
    assignTimeData(sensor_data, device_location_map, time_offset, time_lo_bound, time_hi_bound)
    computeTimeArrays(sensor_data, device_location_map, time_coordinates)
#    data_matrix, space_coordinates, time_coordinates = setupDataMatrix(sensor_data, space_coordinates, time_coordinates, device_location_map)
    data_matrix, space_coordinates, time_coordinates = setupDataMatrix2(sensor_data, space_coordinates, time_coordinates, device_location_map)

    space_coordinates = torch.tensor(space_coordinates)     # convert data to pytorch tensor
    time_coordinates = torch.tensor(time_coordinates)   # convert data to pytorch tensor
    data_matrix = torch.tensor(data_matrix)   # convert data to pytorch tensor

    model = gaussian_model.gaussian_model(space_coordinates, time_coordinates, data_matrix,
                                          latlon_length_scale=float(latlon_length_scale),
                                          elevation_length_scale=float(elevation_length_scale),
                                          time_length_scale=float(time_length_scale),
                                          noise_variance=36.0, signal_variance=400.0)

    if save_matrices:
        numpy.savetxt('space_coords.csv', space_coordinates, delimiter=',')
        numpy.savetxt('time_coords.csv', time_coordinates, delimiter=',')
        numpy.savetxt('PM_data.csv', data_matrix, delimiter=',')
        numpy.savetxt('latlon_scale.csv', numpy.full([1], latlon_length_scale), delimiter=',')
        numpy.savetxt('time_scale.csv', numpy.full([1], time_length_scale), delimiter=',')
        numpy.savetxt('elevation_scale.csv', numpy.full([1], elevation_length_scale), delimiter=',')

    return model, time_offset


# Ross changed this to do the formatting in the api_routes call instead of here
def estimateUsingModel(model, lats, lons, elevations, query_dates, time_offset, save_matrices=False):

    # converts from absolute dates to the local time coordinate system (in hours).  time_offset is the date of the first bin in the sensor data
    time_coordinates = convertToTimeCoordinatesVector(query_dates, time_offset)
    x, y, zone_num, zone_let = utils.latlonToUTM(lats, lons)

    # assume locations are arrays
    # to allow for special case where float locations are given instead of arrays
    # float_case = False
    # if isinstance(lats, (float)):
    #         if isinstance(lons, (float)) and isinstance(elevations, (float)):
    #             space_coordinates = numpy.ndarray(shape=(0, 3), dtype=float)
    #             toadd = numpy.asarray([x, y, elevations])
    #             toadd = numpy.expand_dims(toadd, axis=0)
    #             space_coordinates = numpy.append(space_coordinates, toadd, axis=0)
    #             float_case = True
    #         else:
    #                 return "lats,lons data structure misalignment in request sensor data", 400
    # elif (isinstance(lats, (numpy.ndarray)) and isinstance(lons, (numpy.ndarray)) and isinstance(elevations, (numpy.ndarray))):
    #     if not (lats.shape == lons.shape) or not (lats.shape == elevations.shape):
    #         return "lats,lons, elevations data size error", 400
    #     else:
    #         space_coordinates = numpy.column_stack((x, y, elevations))
    # else:
    #     print(lats.shape)
    #     print(lons.shape)
    #     print(elevations.shape)
    #     return "lats,lons data structure misalignment in request sensor vector data", 400

    space_coordinates = numpy.column_stack((x, y, elevations))

    query_space = torch.tensor(space_coordinates)
    query_dates2 = numpy.transpose(numpy.asarray([time_coordinates]))
    query_time = torch.tensor(query_dates2)

    if save_matrices:
        numpy.savetxt('query_space_coords.csv', space_coordinates, delimiter=',')
        numpy.savetxt('query_time_coords.csv', query_time, delimiter=',')
    
    yPred, yVar = model(query_space, query_time)
    yPred = yPred.numpy()
    yVar = yVar.numpy()

    return yPred, yVar

# 
# this kind of formatting of data is now done in the API (api_routes), because it will get formatted differently for different types of queries. 
#
# # put the data in a list of dictionaries
#     num_times = len(query_dates)
#     estimates = []
#     if (float_case):
#         for i in range(num_times):
#             estimates.append(
#                 {'PM2_5': (yPred[:,i]).tolist(), 'variance': (yVar[:,i]).tolist(), 'datetime': query_dates[i].strftime('%Y-%m-%d %H:%M:%S%z'), 'Latitude': lats, 'Longitude': lons, 'Altitude': elevations}
#                 )
#     else:
#         for i in range(num_times):
#             estimates.append(
#                 {'PM2_5': (yPred[:,i]).tolist(), 'variance': (yVar[:,i]).tolist(), 'datetime': query_dates[i].strftime('%Y-%m-%d %H:%M:%S%z'), 'Latitude': lats.tolist(), 'Longitude': lons.tolist(), 'Altitude': elevations.tolist()}
#                 )
#     return estimates


# does only a single lat lon -- WORKS
# the version above allows for return of an array of lat-lons -- which is more general and needed. 
# def estimateUsingModel(model, lat, lon, elevation, query_dates, time_offset, save_matrices=False):

#     time_coordinates = convertToTimeCoordinatesVector(query_dates, time_offset)

#     x, y, zone_num, zone_let = utils.latlonToUTM(lat, lon)
#     space_coordinates = numpy.ndarray(shape=(0, 3), dtype=float)
#     toadd = numpy.asarray([x, y, elevation])
#     toadd = numpy.expand_dims(toadd, axis=0)
#     space_coordinates = numpy.append(space_coordinates, toadd, axis=0)

#     query_space = torch.tensor(space_coordinates)
#     query_dates2 = numpy.transpose(numpy.asarray([time_coordinates]))
#     query_time = torch.tensor(query_dates2)

#     if save_matrices:
#         numpy.savetxt('query_space_coords.csv', space_coordinates, delimiter=',')
#         numpy.savetxt('query_time_coords.csv', query_time, delimiter=',')
    
#     yPred, yVar = model(query_space, query_time)
#     yPred = yPred.numpy()
#     yVar = yVar.numpy()
#     yPred = [float(value) for value in yPred[0]]
#     yVar = [float(value) for value in yVar[0]]

#     estimates = [
#         {'PM2_5': pred, 'variance': var, 'datetime': date.strftime('%Y-%m-%d %H:%M:%S%z'), 'Latitude': lat, 'Longitude': lon, 'Altitude': elevation}
#         for pred, var, date in zip(yPred, yVar, query_dates)
#     ]

#     return estimates
