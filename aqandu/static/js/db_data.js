/* global XMLHttpRequest:true */
/* eslint no-undef: "error" */

var baseURL = '/api';


function generateURL(route, parameters) {
  let urlRoute = `${baseURL}${route}`;
  
  if (route === '/rawDataFrom') {
    url = `${urlRoute}?id=${parameters.id}&sensorSource=${parameters.sensorSource}&start=${parameters.start}&end=${parameters.end}&show=${parameters.show}`;
  } else if (route === '/liveSensors') {
    url = `${urlRoute}?sensorType=${parameters.type}`;
  } else if (route === '/timeAggregatedDataFrom') {
    url = `${urlRoute}?id=${parameters.id}&sensorSource=${parameters.sensorSource}&start=${parameters.start}&end=${parameters.end}&function=${parameters.function}&functionArg=${parameters.functionArg}&timeInterval=${parameters.timeInterval}`;
  } else if (route === '/lastValue') {
    url = `${urlRoute}`;
  } else if (route === '/contours') {
    url = `${urlRoute}?start=${parameters.start}&end=${parameters.end}`;
  } else if (route === '/getLatestContour') {
    url = `${urlRoute}`;
  } else if (route === '/getEstimatesForLocation') {
    url = `${urlRoute}?locationLat=${parameters.location.lat}&locationLon=${parameters.location.lng}&start=${parameters.start}&end=${parameters.end}`;
  }

  return url;
}

function getDataFromDB(anURL) {
  return new Promise((resolve, reject) => {
    const method = 'GET';
    const async = true;
    const request = new XMLHttpRequest();

    request.open(method, anURL, async); // true => request is async
    // If the request returns successfully, then resolve the promise
    request.onreadystatechange = function () {
      if (request.readyState === 4) {
        if (request.status === 200) {
          const response = JSON.parse(request.responseText);
          resolve(response);
        } else {
          console.log(request.responseText)
          reject(JSON.parse(request.responseText));
        }
      } else {
        console.log("xhr processing going on");
      }
    }

    request.send();
  });
}
