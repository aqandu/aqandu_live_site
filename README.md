# AQandU
These are instructions for setting up the Python Virtual Environment and frontend of AQandU. We use Python 3 at its latest version (on GCP) which, at the time of writing, is 3.7. These instructions assume that you have python 3.7 and pip installed locally.

## Table of Contents

1. [Development Environment Quick Start](#development-environment-quick-start)
1. [Deploying In Production](#deploying-in-production)
1. [Route Documentation](#route-documentation)

  
## Development Environment Quick Start

This project uses `pipenv` for python package version management, so make sure you have that installed. If you need instructions for setting it up, check [here](https://pipenv.pypa.io/en/latest/install/#installing-pipenv). Once  `pipenv` is installed, you can set up a virtual environment and install all python dependencies with `pipenv install`.

Now, copy the .env.prod to .env using `cp .env.prod .env` to use the bigquery database. You may need to acquire this file from an admin.

Next, we need to generate some flask assets with `pipenv run build-assets`. Then you may launch the application with `pipenv run serve`.


## Deploying in Production

To deploy the application, you have to use the command line and the gcloud tools. Once you have the production config (from Jack or another admin) and you've set up gcloud cli with the correct default project, run the following commands:

```
cp config.production.py config.py
gcloud app deploy app.yaml
```

This will start building the containers that serve the website. You can check for a successful deployment from the app engine versions dashboard in GCP. The app usually builds and deploys within a few minutes, but sometimes, Google can be a little slow with the building.

**NOTE**: If you're getting `Error Response: [4] DEADLINE_EXCEEDED` then you need to increase the timeout for the build to 20 minutes using `gcloud config set app/cloud_build_timeout 1200`.

# AQandU
These are instructions for setting up the Python Virtual Environment and frontend of AQandU. We use Python 3 at its latest version (on GCP) which, at the time of writing, is 3.7. These instructions assume that you have python 3.7 and pip installed locally.

## Table of Contents

1. [Development Environment Quick Start](#development-environment-quick-start)
1. [Deploying In Production](#deploying-in-production)
1. [Route Documentation](#route-documentation)

  
## Development Environment Quick Start

This project uses `pipenv` for python package version management, so make sure you have that installed. If you need instructions for setting it up, check [here](https://pipenv.pypa.io/en/latest/install/#installing-pipenv). Once  `pipenv` is installed, you can set up a virtual environment and install all python dependencies with `pipenv install`.

Now, copy the .env.prod to .env using `cp .env.prod .env` to use the bigquery database. You may need to acquire this file from an admin.

Next, we need to generate some flask assets with `pipenv run build-assets`. Then you may launch the application with `pipenv run serve`.


## Deploying in Production

To deploy the application, you have to use the command line and the gcloud tools. Once you have the production config (from Jack or another admin) and you've set up gcloud cli with the correct default project, run the following commands:

```
cp config.production.py config.py
gcloud app deploy app.yaml
```

This will start building the containers that serve the website. You can check for a successful deployment from the app engine versions dashboard in GCP. The app usually builds and deploys within a few minutes, but sometimes, Google can be a little slow with the building.

**NOTE**: If you're getting `Error Response: [4] DEADLINE_EXCEEDED` then you need to increase the timeout for the build to 20 minutes using `gcloud config set app/cloud_build_timeout 1200`.

## Route Documentation 

There are several routes set up for accessing the data. Here are the names, allowed methods, parameters, and descriptions:

- Name:`/api/rawDataFrom`
  - Allowed Methods: `GET`
  - Parameters:
      - Required:  
           `id`: A sensor id.  
           `sensor_source`: A sensor source. One of ["AirU", "DAQ", "PurpleAir", "all"].  
           `start`: A datetime string in the format "%Y-%m-%dT%H:%M:%SZ".  
           `end`: A datetime string in the format "%Y-%m-%dT%H:%M:%SZ".  
  - Description: Returns the raw, unaggregated data for a sensor. This is the data shown on the timeline view at the bottom of the main page.
  - Return: A JSON response that looks like {data: [], tags: []} where the data Array is an Object[] where each Object has the following keys (PM2_5, time).
  - Example:
    ```
    curl '127.0.0.1:8080/api/rawDataFrom?id=M9884E31FEBEE&sensorSource=AirU&start=2020-07-06T22:14:00Z&end=2020-07-07T22:14:00Z'
    ```

- Name:`/api/liveSensors`
  - Allowed Methods: `GET`
  - Parameters:
      - Required:  
           `sensor_source`: A sensor source. One of ["AirU", "DAQ", "PurpleAir", "all"].  
  - Description: Returns data from either all live sensors or all live sensors from one source. 
  - Return: A JSON response that looks like [] where the Array is an Object[] where each Object has the following keys (ID, Latitude, Longitude, time, PM2_5, SensorModel, SensorSource).
  - Example:
    ```
    curl '127.0.0.1:8080/api/liveSensors?sensorSource=PurpleAir'
    ```

- Name:`/api/timeAggregatedDataFrom`
  - Allowed Methods: `GET`
  - Parameters:
      - Required:  
           `id`: A sensor id.  
           `sensor_source`: A sensor source. One of ["AirU", "DAQ", "PurpleAir", "all"].  
           `start`: A datetime string in the format "%Y-%m-%dT%H:%M:%SZ".  
           `end`: A datetime string in the format "%Y-%m-%dT%H:%M:%SZ".  
           `function`: One of ["mean", "min", "max"], which correspond to the SQL functions AVG(), MIN(), and MAX(), respectively.
           `timeInterval`: Integer number of minutes between each aggregation. E.g. 5 will give the aggregated value every 5 minutes.  
  - Description: Returns data from either all live sensors or all live sensors from one source, aggregated as mean, min, or max, by a defined time interval in minutes.
  - Return: A JSON response that looks like {data: [], tags: []} where the data Array is an Object[] where each Object has the following keys (PM2_5, time).
  - Example:
    ```
    curl '127.0.0.1:8080/api/timeAggregatedDataFrom?id=M9884E31FEBEE&sensorSource=AirU&start=2020-07-04T22:14:00Z&end=2020-07-07T22:14:00Z&function=mean&timeInterval=5'
    ```

- Name:`/api/request_model_data`
  - Allowed Methods: `GET`
  - Parameters:
      - Required:  
           `lat`: Latitude.  
           `lon`: Longitude.  
           `radius`: radius around lat, lon in degrees.  
           `start_date`: A datetime string in the format "%Y-%m-%dT%H:%M:%SZ".  
           `end_date`: A datetime string in the format "%Y-%m-%dT%H:%M:%SZ".
  - Description: Get model data for the estimates route.
  - Return: Array of Objects with keys (ID, Latitude, Longitude, time, PM2_5, SensorModel, SensorSource)
  - Example:
    ```
    curl '127.0.0.1:8080/api/request_model_data?lat=40.7688&lon=-111.8462&radius=1&start_date=2020-06-30T00:00:00Z&end_date=2020-07-01T00:01:00Z'
    ```

- Name:`/api/getEstimatesForLocation`
  - Allowed Methods: `GET`
  - Parameters:
      - Required:  
           `lat`: Latitude.  
           `lon`: Longitude.  
           `estimatesrate`: integer number of estimates per hour.  
           `start_date`: A datetime string in the format "%Y-%m-%dT%H:%M:%SZ".  
           `end_date`: A datetime string in the format "%Y-%m-%dT%H:%M:%SZ".
  - Description: Generate estimated pm2.5 for arbitrary locations downtown SLC.
  - Return: Array of Objects with keys (Elevation, Latitude, Longitude, PM2_5, datetime, variance).
  - Example:
    ```
    curl
    '127.0.0.1:8080/api/getEstimatesForLocation?lat=40.7688&lon=-111.8462&estimatesrate=0.5&start_date=2020-06-30T00:00:00Z&end_date=2020-07-01T00:01:00Z'
    ```


- Name:`/api/getEstimatesForLocations`
  - Allowed Methods: `GET`
  - Parameters:
      - Required:  
           `lat`: Latitude1,Latitude2,....  
           `lon`: Longitude1,Longitude2,....  
           `estimatesrate`: interval (in hours) between estimates.  
           `start_date`: A datetime string in the format "%Y-%m-%dT%H:%M:%SZ".  
           `end_date`: A datetime string in the format "%Y-%m-%dT%H:%M:%SZ".
  - Description: Generate estimated pm2.5 for arbitrary lists of locations
    downtown SLC returned as array with size based on # of locations given.
  - Return: Array of Objects with keys (Elevation, Latitude, Longitude, PM2_5, datetime, variance).
  - Example:
    ```
    curl
    '127.0.0.1:8080/api/getEstimatesForLocation?lat=40.7688,40.7698&lon=-111.8462,-111.8472&estimatesrate=0.5&start_date=2020-06-30T00:00:00Z&end_date=2020-07-01T00:01:00Z'
    ```

- Name:`/api/getEstimateMap`
  - Allowed Methods: `GET`
  - Parameters:
      - Required:  
			`lat_lo`: Latitude.
			`lat_hi`: Latitude.
			`lon_lo`: Longitude.
			`lon_hi`: Longitude.
			`lon_size`: Integer.
			`lon_size`: Integer.
			`date`: A datetime string in the format "%Y-%m-%dT%H:%M:%SZ".  

  - Description: Generate estimated pm2.5 for grid of locations within
    the box given by hi and lo lats and lons, with the specified size
    (which determines resolution) at the given date/time.
  - Return: Array of Objects(lists) with keys (Latitudes(vector), Longitude(vector),
    Elevations(array), PM2_5(array), PM2.5 variance(array)).
  - Example:
    ```
    curl
    ''http://127.0.0.1:8080/api/getEstimateMap?lat_lo=40.733534&lat_hi=40.780421&lon_lo=-111.906754&lon_hi=-111.846383&lat_size=100&lon_size=100&date=2019-01-04T00:08:00Z
    ```
 
