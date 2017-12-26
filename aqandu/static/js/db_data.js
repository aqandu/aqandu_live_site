/* global XMLHttpRequest:true */
/* eslint no-undef: "error" */


// TODO move from http to https

var baseURL = 'http://air.eng.utah.edu';


function generateURL(anEndpoint, route, parameters) { // eslint-disable-line no-unused-vars
  let url = '';
  if (route === '/rawDataFrom?') {
    url = `${baseURL}${anEndpoint}${route}id=${parameters.id}&sensorSource=${parameters.sensorSource}&start=${parameters.start}&end=${parameters.end}&show=${parameters.show}`;
  } else if (route === '/liveSensors') {
    url = `${baseURL}${anEndpoint}${route}/${parameters.type}`;
  } else if (route === '/processedDataFrom?') {
    url = `${baseURL}${anEndpoint}${route}id=${parameters.id}&start=${parameters.start}&end=${parameters.end}&function=${parameters.function}&functionArg=${parameters.functionArg}&timeInterval=${parameters.timeInterval}`;
  } else if (route === '/lastValue') {
    url = `${baseURL}${anEndpoint}${route}?fieldKey=${parameters.fieldKey}`;
  }

  return url;
}

function getDataFromDB(anURL) { // eslint-disable-line no-unused-vars
  return new Promise((resolve, reject) => {
    const method = 'GET';
    const async = true;
    const request = new XMLHttpRequest();

    request.open(method, anURL, async); // true => request is async

    // If the request returns succesfully, then resolve the promise
    request.onreadystatechange = function processingResponse() {
      if (request.readyState === 4 && request.status === 200) {
        const response = JSON.parse(request.responseText);
        resolve(response);
      }

      // If request has an error, then reject the promise
      request.onerror = function showWarning(e) {
        console.log('Something went wrong....');
        reject(e);
      };
    };

    request.send();
  });
}
