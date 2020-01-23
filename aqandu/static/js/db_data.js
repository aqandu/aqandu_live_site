/* global XMLHttpRequest:true */
/* eslint no-undef: "error" */


// TODO move from http to https

var baseURL = 'https://air.eng.utah.edu';


function generateURL(anEndpoint, route, parameters) { // eslint-disable-line no-unused-vars
  let url = '';
  if (route === '/rawDataFrom?') {
    url = `${baseURL}${anEndpoint}${route}id=${parameters.id}&sensorSource=${parameters.sensorSource}&start=${parameters.start}&end=${parameters.end}&show=${parameters.show}`;
  } else if (route === '/liveSensors') {
    url = `${baseURL}${anEndpoint}${route}/${parameters.type}`;
  } else if (route === '/processedDataFrom?') {
    url = `${baseURL}${anEndpoint}${route}id=${parameters.id}&sensorSource=${parameters.sensorSource}&start=${parameters.start}&end=${parameters.end}&function=${parameters.function}&functionArg=${parameters.functionArg}&timeInterval=${parameters.timeInterval}`;
  } else if (route === '/lastValue') {
    url = `${baseURL}${anEndpoint}${route}?fieldKey=${parameters.fieldKey}`;
  } else if (route === '/contours') {
    url = `${baseURL}${anEndpoint}${route}?start=${parameters.start}&end=${parameters.end}`;
  } else if (route === '/getLatestContour') {
    url = `${baseURL}${anEndpoint}${route}`;
  } else if (route === '/getEstimatesForLocation') {
    url = `${baseURL}${anEndpoint}${route}?location_lat=${parameters.location.lat}&location_lng=${parameters.location.lng}&start=${parameters.start}&end=${parameters.end}`;
  }



  return url;
}

// var promiseObj = new Promise(function(resolve, reject){
//    var xhr = new XMLHttpRequest();
//    xhr.open(methodType, url, true);
//    xhr.send();
//    xhr.onreadystatechange = function(){
//    if (xhr.readyState === 4){
//       if (xhr.status === 200){
//          console.log("xhr done successfully");
//          var resp = xhr.responseText;
//          var respJson = JSON.parse(resp);
//          resolve(respJson);
//       } else {
//          reject(xhr.status);
//          console.log("xhr failed");
//       }
//    } else {
//       console.log("xhr processing going on");
//    }
// }
// console.log("request sent successfully");
// });
// return promiseObj;
// }
function getDataFromDB(anURL) { // eslint-disable-line no-unused-vars
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

    // // If request has an error, then reject the promise
    // request.onerror = function showWarning(e) {
    //   console.log('Something went wrong....');
    //   reject(e);
    // };

    console.log("request sent successfully");
  });
}
