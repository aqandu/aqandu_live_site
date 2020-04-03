from datetime import datetime
import pytz
import numpy
from aqandu import gaussian_model
from aqandu import utils
import torch


JANUARY1ST = datetime(2000, 1, 1, 0, 0, 0, 0, pytz.timezone('UTC'))
TIME_COORDINATE_BIN_NUMBER_KEY = 'time_coordinate_bin_number'


def getTimeCoordinateBin(datetime, time_offset = 0):
    delta = datetime - JANUARY1ST
    NUM_MINUTES_PER_BIN = 10
    bin_number = float(int(delta.total_seconds() / 60 / NUM_MINUTES_PER_BIN) / 60 * NUM_MINUTES_PER_BIN)
    return bin_number - time_offset


def convertToTimeCoordinatesVector(dates, time_offset):
    return [getTimeCoordinateBin(date, time_offset=time_offset) for date in dates]


def createTimeVector(sensor_data):
    time_coordinates = set()

    lowest_bin_number = None

    for datum in sensor_data:
        bin_number = getTimeCoordinateBin(datum['time'])
        time_coordinates.add(bin_number)
        datum[TIME_COORDINATE_BIN_NUMBER_KEY] = bin_number

        if lowest_bin_number is None or bin_number < lowest_bin_number:
            lowest_bin_number = bin_number

    for datum in sensor_data:
        datum[TIME_COORDINATE_BIN_NUMBER_KEY] -= lowest_bin_number
        #print(f"{datum['date_time']} -> {datum[TIME_COORDINATE_BIN_NUMBER_KEY]}") 

    time_coordinates = [bin_number - lowest_bin_number for bin_number in time_coordinates]
    time_coordinates.sort()
    #print(time_coordinates)
    time_coordinates = numpy.expand_dims(numpy.asarray(time_coordinates), axis=1)

    return time_coordinates, lowest_bin_number


def createSpaceVector(sensor_data):
    device_location_map = {}

    for datum in sensor_data:
        if datum['ID'] not in device_location_map:
            device_location_map[datum['ID']] = (datum['utm_x'], datum['utm_y'], datum['Altitude'])
    
    #print(device_location_map)

    #print('SPACE COORDS')
    space_coordinates = numpy.ndarray(shape=(0, 3), dtype=float)
    for key in device_location_map.keys():
        loc = device_location_map[key]
        toadd = numpy.asarray([loc[0], loc[1], loc[2]])
        toadd = numpy.expand_dims(toadd, axis=0)
        space_coordinates = numpy.append(space_coordinates, toadd, axis=0)
        device_location_map[key] = space_coordinates.shape[0] - 1
    
    # print(device_location_map)
    # print(space_coordinates)
    # print(space_coordinates.shape)
    return space_coordinates, device_location_map


def saveMatrixToFile(matrix, filename):
    with open(filename, 'w') as output_file:
        for row in matrix:
            for col in row:
                print(f'{col:0.2f}\t', end='', file=output_file)
            print(file=output_file)
    

# goal is to fill in zero elements in between two values
def interpolateZeroElements(matrix):
    row_index = 0
    for row in matrix:
        row_index += 1
        prevValueIndex = None
        for i in range(row.shape[0]):
            if row[i] != 0:
                if prevValueIndex is None:
                    prevValueIndex = i
                else:
                    curValueIndex = i
                    distance = curValueIndex - prevValueIndex
                    if distance > 1:
                        # interpolate zeros between prev and cur
                        terp = numpy.interp(range(prevValueIndex+1, curValueIndex), [prevValueIndex, curValueIndex], [row[prevValueIndex], row[curValueIndex]])
                        row[prevValueIndex+1:curValueIndex] = terp
                    prevValueIndex = curValueIndex
    

def trimEdgeZeroElements(matrix, time_coordinates):
    # record index of edge values for each row
    firstValues = {}
    lastValues = {}
    for col_index in range(matrix.shape[1]):
        inverse_col_index = -1-col_index
        for row_index in range(matrix.shape[0]):
            if row_index not in firstValues:
                if matrix[row_index][col_index] != 0:
                    firstValues[row_index] = col_index
            if row_index not in lastValues:
                if matrix[row_index, inverse_col_index] != 0:
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


def removeBadSensors(data_matrix, space_coordinates, ratio):
    toKeep = [(numpy.count_nonzero(row) / len(row)) > ratio for row in data_matrix]
    data_matrix = data_matrix[toKeep]
    space_coordinates = space_coordinates[toKeep]
    return data_matrix, space_coordinates


def setupDataMatrix(sensor_data, space_coordinates, time_coordinates, device_location_map):
    data_matrix = numpy.zeros(shape=(space_coordinates.shape[0], time_coordinates.shape[0]))
    for datum in sensor_data:
        date_index = numpy.nonzero(time_coordinates == datum[TIME_COORDINATE_BIN_NUMBER_KEY])[0][0]
        location_index = device_location_map[datum['ID']]
        # bound sensor data below by 0
        data_matrix[location_index][date_index] = datum['PM2_5'] if datum['PM2_5'] >= 0 else 0
    
    saveMatrixToFile(data_matrix, '1matrix.txt')
    interpolateZeroElements(data_matrix)
    saveMatrixToFile(data_matrix, '2interpolated.txt')
    data_matrix, space_coordinates = removeBadSensors(data_matrix, space_coordinates, 0.6)
    saveMatrixToFile(data_matrix, '3matrix_removed_bad.txt')
    data_matrix, time_coordinates = trimEdgeZeroElements(data_matrix, time_coordinates)
    saveMatrixToFile(data_matrix, '4matrixtrimmed.txt')
    return data_matrix, space_coordinates, time_coordinates


def createModel(sensor_data, latlon_length_scale, elevation_length_scale, time_length_scale):

    time_coordinates, time_offset = createTimeVector(sensor_data)
    space_coordinates, device_location_map = createSpaceVector(sensor_data)
    data_matrix, space_coordinates, time_coordinates = setupDataMatrix(sensor_data, space_coordinates, time_coordinates, device_location_map)
    
    space_coordinates = torch.tensor(space_coordinates)     #convert data to pytorch tensor
    time_coordinates = torch.tensor(time_coordinates)   #convert data to pytorch tensor
    data_matrix = torch.tensor(data_matrix)   #convert data to pytorch tensor

    # print(f'space_coordinates: {space_coordinates.shape}, time_coordinates: {time_coordinates.shape}, data: {data_matrix.shape}')
    # print(data_matrix)
    # print(space_coordinates)
    # print(time_coordinates)
    model = gaussian_model.gaussian_model(space_coordinates, time_coordinates, data_matrix,
             latlong_length_scale=float(latlon_length_scale),
             elevation_length_scale=float(elevation_length_scale),
             time_length_scale=float(time_length_scale),
             noise_variance=0.1)
             

    return model, time_offset


def predictUsingModel(model, lat, lon, elevation, query_dates, time_offset):

    time_coordinates = convertToTimeCoordinatesVector(query_dates, time_offset)
    
    x, y, zone_num, zone_let = utils.latlonToUTM(lat, lon)
    space_coordinates = numpy.ndarray(shape=(0, 3), dtype=float)
    toadd = numpy.asarray([x, y, elevation])
    toadd = numpy.expand_dims(toadd, axis=0)
    space_coordinates = numpy.append(space_coordinates, toadd, axis=0)

    query_space = torch.tensor(space_coordinates)
    query_dates2 = numpy.transpose(numpy.asarray([time_coordinates]))
    query_time = torch.tensor(query_dates2)

    yPred, yVar = model(query_space, query_time)
    yPred = yPred.numpy()
    yVar = yVar.numpy()
    yPred = [float(value) for value in yPred[0]]
    yVar = [float(value) for value in yVar[0]]

    predictions = [{'PM2_5': pred, 'variance':var, 'datetime':date.strftime('%Y-%m-%d %H:%M:%S%z'), 'Latitude':lat, 'Longitude':lon, 'Altitude':elevation} for pred, var, date in zip(yPred, yVar, query_dates)]
    
    return predictions
