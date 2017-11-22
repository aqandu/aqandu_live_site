// TODO move from http to https

var baseURL = 'http://air.eng.utah.edu';


function generateURL(anEndpoint, route, parameters) {

    url = '';
    if (route === '/rawDataFrom?') {
        url = baseURL + anEndpoint + route + 'id=' + parameters['id'] + '&start=' + parameters['start'] + '&end=' + parameters['end'] + '&show=' + parameters['show'];
    } else if (route === '/liveSensors') {
        url = baseURL + anEndpoint + route;
    }

    return url;
}

function getDataFromDB(anURL) {

    return new Promise((resolve, reject) => {

        let method = "GET";
        let async = true;
        let request = new XMLHttpRequest();

        request.open(method, anURL, async); // true => request is async

        // If the request returns succesfully, then resolve the promise
        request.onreadystatechange = function() {
            if (request.readyState == 4 && request.status == 200) {
                let response = JSON.parse(request.responseText);
                resolve(response);
            }

            // If request has an error, then reject the promise
            request.onerror = function(e, i) {

                console.log("Something went wrong....");
                reject(e);
            };
        };

        request.send();
    });
}
