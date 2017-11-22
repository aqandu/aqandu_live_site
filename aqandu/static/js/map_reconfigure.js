//setting dates for timeline and for ajax calls
var today = new Date().toISOString().substr(0, 19) +"Z";
var date = new Date();
date.setDate(date.getDate()-1);
yesterday = date.toISOString().substr(0, 19) +"Z";

var x = d3.scaleTime().domain([new Date (yesterday), new Date (today)]);
var lineColor = d3.scaleOrdinal(d3.schemeCategory10);
var y = d3.scaleLinear().domain([0.0, 120.0]);

var sensLayer = L.layerGroup();
var heat = L.heatLayer();

var dbEndpoint = "/dbapi/api";
var liveSensorURL = generateURL(dbEndpoint, '/liveSensors', null);


getDataFromDB(liveSensorURL).then(data => {

    response = data.map(function(d) {

        if (d["Sensor Source"] === "Purple Air") {
            d['pm25'] = conversionPM(d['pm25'], d['Sensor Model']);
        }

        return d
    });

    sensorLayer(response);

}).catch(function(err){

    alert("error, request failed!");
    console.log("Error: ", err)
});


var margin = {
  top: 20,
  right: 30,
  bottom: 30,
  left: 40
};

function setupMap () {
  var map = L.map('map', {
    center: [40.7608, -111.8910],
    zoom: 13
  });
  // load a tile layer
  L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token=pk.eyJ1Ijoic2tpdHJlZSIsImEiOiJjajUyb2l0YzQwaHJwMnFwMTNhdGwxMGx1In0.V5OuKXRdmwjq4Lk3o8me1A', {
    maxZoom: 18,
    id: 'mapbox.streets',
    accessToken: 'pk.eyJ1Ijoic2tpdHJlZSIsImEiOiJjajUydDkwZjUwaHp1MzJxZHhkYnl3eTd4In0.TdQB-1U_ID-37stKON_osw'
  }).addTo(map);

  var legend = L.control({position: 'bottomright'});

  legend.onAdd = function (map) {
    this._div = L.DomUtil.create('div', 'legend');
    this.update(this._div);
    return this._div;
  };

  legend.update = function (thediv) {
    // TODO: draw the legend
    var d3div = d3.select(thediv);
    var dataLabel = ["Utah", "PurpleAir", "Mesowest", "DAQ"];
    var labels = d3div.selectAll('div').data(dataLabel);
    labels.exit().remove();
    var labelsEnter = labels.enter().append('div');
    labels = labels.merge(labelsEnter);
    labels.text(d => d);
    labels.on('mouseover', d => {
      // Add 'unhovered' class to all dots
      d3.select('#map').selectAll('.dot').classed('unhovered', true);
      // Remove 'unhovered' class from dots whose class matches the hovered legend entry
      d3.select('#map').selectAll('.' + d).classed('unhovered', false);
      // So at this point, only stuff that wasn't hovered will have the unhovered class
    });
    labels.on('mouseout', d => {
      // Remove 'unhovered' class from all dots
      d3.select('#map').selectAll('.dot').classed('unhovered', false);
    });
    return thediv;
  }

  legend.addTo(map);
  return map;
}

var map = setupMap();
var lineArray = [];

window.onload = window.onresize = function () {
  setUp();
  // TODO: call the render function(s)
  //  L.imageOverlay('overlay1.png', [[40.795925, -111.998256], [40.693031, -111.827190]], {
  // 		opacity: 0.5,
  // 		interactive: true,
  // 	}).addTo(map);
}

function distance(lat1, lon1, lat2, lon2) {
  var p = 0.017453292519943295;    // Math.PI / 180
  var c = Math.cos;
  var a = 0.5 - c((lat2 - lat1) * p)/2 +
  c(lat1 * p) * c(lat2 * p) *
  (1 - c((lon2 - lon1) * p))/2;

  return 12742 * Math.asin(Math.sqrt(a)); // 2 * R; R = 6371 km
}

function findDistance(r, mark){
  var lt = mark.getLatLng().lat;
  var lng = mark.getLatLng().lng;
  var closestsensor = null;
  var sensorobject = null;

  r.forEach(function (item){
    if (item["Latitude"] !== null && item["Longitude"] !== null) {
      var d = distance(lt, lng, parseFloat(item["Latitude"]), parseFloat(item["Longitude"]));
      //compare old distance to new distance. Smaller = closestsensor
      if (closestsensor === null) {
        closestsensor = d; //distance
        sensorobject = item; //data object
      } else {
        if (closestsensor > d) {
          closestsensor = d;
          sensorobject = item;
        }
      }
    }
  });
  return sensorobject;
}


function findCorners(ltlg) {
  var cornerarray = [];
  lt = ltlg.lat;
  lg = ltlg.lng;

  var lt1 = lt - 5.0;
  cornerarray.push(lt1);
  var lt2 = lt + 5.0;
  cornerarray.push(lt2);
  var lg1 = lg - 5.0;
  cornerarray.push(lg1);
  var lg2 = lg + 5.0;
  cornerarray.push(lg2);

  return cornerarray;
}

function findNearestSensor(cornerarray, mark, callback) {

    getDataFromDB(liveSensorURL).then(data => {

        response = data.map(function(d) {

            // return only location and ID
            var newD = {};
            newD['ID'] = d['ID'];
            newD['Latitude'] = d['Latitude'];
            newD['Longitude'] = d['Longitude'];

            return newD;
        });

        var closest = findDistance(response, mark); // returns closest sensor using distance equation
        callback(closest);

    }).catch(function(err){

        alert("error, request failed!");
        console.log("Error: ", err);
        console.warn(arguments);
    });
}


// function addData (sensorData){
//   sensorData = sensorData.results[0].series[0];
//   var chartLabel = sensorData.values[0][sensorData.columns.indexOf('ID')];
//   var markrname = sensorData.values[0][sensorData.columns.indexOf('ID')]; //what shows up in the marker on click (name of sensor)
//   var timeColumn = sensorData.columns.indexOf('time');
//   var pm25Column = sensorData.columns.indexOf('pm2.5 (ug/m^3)');
//
//   sensorData = sensorData.values.map(function (d) {
//     return {
//       id: markrname,
//       time: new Date(d[timeColumn]),
//       pm25: d[pm25Column]
//     };
//   }).filter(function (d) {
//     return d.pm25 === 0 || !!d.pm25;  // forces NaN, null, undefined to be false, all other values to be true
//   });
//
//   lineArray.push({
//     id: markrname,
//     sensorData: sensorData
//   }); //pushes data for this specific line to an array so that there can be multiple lines updated dynamically on Click
//   drawChart();
// }


function preprocessDBData(id, sensorData) {

  sensorData = sensorData.map(function (d) {
    return {
      id: id,
      time: new Date(d['time']),
      pm25: d['pm2.5 (ug/m^3)']
    };
  }).filter(function (d) {
    return d.pm25 === 0 || !!d.pm25;  // forces NaN, null, undefined to be false, all other values to be true
  });

  lineArray.push({
    id: id,
    sensorData: sensorData
  }); //pushes data for this specific line to an array so that there can be multiple lines updated dynamically on Click

  drawChart();
}


function drawChart (){
  var svg = d3.select("div svg"); // TODO: this isn't specific enough...
  var bounds = svg.node().getBoundingClientRect();
  var width = bounds.width,
  height = bounds.height;
  // Scale the range of the data
  var valueline = d3.line()
  .x(function (d) {
    return x(d.time);
  })
  .y(function (d) {
    return y(d.pm25);
  })
  //mike bostock's code
  var voronoi = d3.voronoi()
  .x(function(d) { return x(d.time); })
  .y(function(d) { return y(d.pm25); })
  .extent([[-margin.left, -margin.top], [width + margin.right, height + margin.bottom]]);

  // adds the svg attributes to container
  let lines = svg.select('#lines').selectAll('path').data(lineArray, function (d) {
    return d.id;
  }); //any path in svg is selected then assigns the data from the array
  lines.exit().remove(); //remove any paths that have been removed from the array that no longer associated data
  let linesEnter = lines.enter().append("path"); // looks at data not associated with path and then pairs it
  lines = linesEnter.merge(lines); //combines new path/data pairs with previous, unremoved data

  lines.attr('transform', 'translate(' + margin.left + ',' + margin.top + ')')
  .attr("d", d => { return valueline(d.sensorData); })
  .attr("class", d => 'line-style line' + d.id)
  .attr("stroke", d => lineColor(d.id)); //no return for d function, see above for example

  var focus = svg.select(".focus");

  function mouseover(d) { //d is voronoi paths
    let hoveredLine = svg.select('.line' + d.data.id);
    hoveredLine.classed("hover", true);
    // Sneaky hack to bump hoveredLine to be the last child of its parent;
    // in SVG land, this means that hoveredLine will jump to the foreground
    //.node() gets the dom element (line element), then when you append child to the parent that it already has, it bumps updated child to the front
    hoveredLine.node().parentNode.appendChild(hoveredLine.node());
    console.log(d.data.time)
    focus.attr("transform", "translate(" + (x(d.data.time) + margin.left) + "," + (y(d.data.pm25)+ margin.top) + ")"); //x and y gets coordinates from values, which we can then change with margin
    focus.select("text").text(d.data.id);
  }

  function mouseout(d) {
    let hoveredLine = svg.select('.line' + d.data.id);
    hoveredLine.classed("hover", false);
    focus.attr("transform", "translate(-100,-100)");
  }
  console.log(lineArray);
  var listOfLists = lineArray.map(function(d) {
    return d.sensorData;
  });
  var listOfPoints = d3.merge(listOfLists);
  var voronoiPolygons = voronoi.polygons(listOfPoints);

  var voronoiGroup = svg.select(".voronoi")
    .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');
  var voronoiPaths = voronoiGroup.selectAll("path")
    .data(voronoiPolygons);
  voronoiPaths.exit().remove();
  var voronoiPathsEnter = voronoiPaths.enter().append("path");
  voronoiPaths = voronoiPaths.merge(voronoiPathsEnter);
  voronoiPaths.attr("d", function(d) { return d ? "M" + d.join("L") + "Z" : null; })
    .on("mouseover", mouseover) //I need to add a name for this
    .on("mouseout", mouseout);

  // // adds the svg attributes to container
  let labels = svg.select("#legend").selectAll("text").data(lineArray, d => d.id); //any path in svg is selected then assigns the data from the array
  labels.exit().remove(); //remove any paths that have been removed from the array that no longer associated data
  // let labelEnter = labels.enter().append("text"); // looks at data not associated with path and then pairs it
  // labels = labelEnter.merge(labels); //combines new path/data pairs with previous, unremoved data
  //
  // //set up the legend later
  //  labels.attr("x", margin.left + width/2)
  //  .attr("y", margin.top)
  //  .attr("text-decoration")
  //  .attr("text-anchor", "middle")
  //  .attr("font-family", "verdana")
  //  .text(d => d.id);
}


function makeGraph(mark){
  findNearestSensor(findCorners(mark.getLatLng()), mark, function (sensor) {
    mark = mark.bindPopup('<p>'+ sensor["ID"] +'</p>').openPopup();

    var theRoute = '/rawDataFrom?';
    var parameters = {'id': sensor["ID"], 'start': yesterday, 'end': today, 'show': 'pm2.5'};

    var url = generateURL(dbEndpoint, theRoute, parameters);

    getDataFromDB(url).then(data => {

        // console.log(data)
        preprocessDBData(sensor["ID"], data)

    }).catch(function(err){

        alert("error, request failed!");
        console.log("Error: ", err)
    });
  });
}

var markr = null;

function onMapClick(e) {
  markr = new L.marker(e.latlng)
  .addTo(map)

  makeGraph(markr);
}


function setUp(){
  var div = d3.select(".timeline");
  var bounds = div.node().getBoundingClientRect();
  var svgWidth = bounds.width;
  var svgHeight = 200;
  var width = svgWidth - margin.left - margin.right;
  var height = svgHeight - margin.top - margin.bottom;
  var svg = div.select("svg") //sets size of svgContainer

  x.range([0, width]);
  y.range([height, 0]);

  svg.attr("width", svgWidth)
  .attr("height", svgHeight);

  var xAxis = d3.axisBottom(x).ticks(9);
  var yAxis = d3.axisLeft(y).ticks(7);

  svg.select(".x.axis") // Add the X Axis
  .attr("transform", "translate(" + margin.left + "," + (margin.top + height) + ")")
  .call(xAxis);

  svg.select(".x.label")      // text label for the x axis
  .attr("transform", "translate(" + (width / 2) + " ," + (height + margin.bottom + 15) + ")")
  .style("text-anchor", "middle")
  .text("Time");

  svg.select(".y.axis") // Add the Y Axis
  .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')')
  .call(yAxis);

  svg.select(".y.label")
  .attr("transform", "rotate(-90)")
  .attr("y", 0) // rotated! x is now y!
  .attr("x", 0 - (height / 2))
  .attr("dy", "1em")
  .style("text-anchor", "middle")
  .text("PM 2.5 ug/m3");
}

//map.on('click', onMapClick);

/* this is for parsing through Amir's data
and then building a heat map from the data */

lonPromise = getData("leaflet/sample-data/XGPS1.csv");
latPromise = getData("leaflet/sample-data/XGPS2.csv");
pmValPromise = getData("leaflet/sample-data/YPRED.csv");

lvArray = []; //locations + values array
Promise.all([lonPromise, latPromise, pmValPromise]) //Promise.all waits for all the other promises to finish
.then(function (promiseResults) { //once they are finished, the .THEN tells it the next step (a function)
  var lon = promiseResults[0].trim().split('\n');
  lon = lon[0].split(',').map(value => Number(value));
  var lat = promiseResults[1].trim().split('\n');
  lat = lat.map(row => Number(row.split(',')[0]));
  var pmVal = promiseResults[2].split('\n');
  var results = [];

  if (pmVal.length !== lat.length) {
    throw new Error('wrong number of lat coordinates');
  }

  pmVal.forEach((row, latIndex) => {
    row = row.split(',');
    if (row.length <= 1) {
      return;
    }
    if (row.length !== lon.length) {
      throw new Error('wrong number of lon coordinates');
    }
    row.forEach((value, lonIndex) => {
      results.push({
        lat: lat[latIndex],
        lon: lon[lonIndex],
        pmVal: Number(value)
      });
    });
  });
  makeHeat(results);
});



function getData(strng){
  return new Promise(function (resolve, reject) { //use a promise as a place holder until a promise is fulfilled (resolve)
    d3.text(strng, function(data){
      // console.log(strng, data)
      resolve(data);
    });
  });
}

function sensorLayer(response){
  response.forEach(function (item) {
    var dotIcon = {
      iconSize:     [20, 20], // size of the icon
      iconAnchor:   [20, 20], // point of the icon which will correspond to marker's location
      popupAnchor:  [0, 0], // point from which the popup should open relative to the iconAnchor
      html: ''
    };
    if (item["Latitude"] !== null && item["Longitude"] !== null) {
      let classList = 'dot';
      if (item["pm25"] <= 50){
        classList += ' green ';
      }
      else if (item["pm25"] > 50 && item["pm25"]<= 100){
        classList = ' yellow ';
      }
      else if (item["pm25"] > 100 && item["pm25"]<= 150){
        classList = ' orange ';
      }
      else{
        classList = ' red ';
      }
      // throw away the spaces in the sensor name string so we have a valid class name
      classList += item["Sensor Source"].replace(/ /g, '');
      dotIcon.className = classList;

      var mark = new L.marker(
        L.latLng(
          parseFloat(item["Latitude"]),
          parseFloat(item["Longitude"])
        ), { icon: L.divIcon(dotIcon) }
      ).addTo(sensLayer).bindPopup("Sensor " + item["ID"]).on('click', onClick);
    }
  });
}

var overlayMaps = {
  "SensLayer": sensLayer
};

sensLayer.addTo(map);
L.control.layers(null, overlayMaps).addTo(map);

function onClick(e){
  makeGraph(this);
}

/* example of the leaflet heatmap plugin, possibly for
modification for interpolation between points.
*/
function makeHeat(results) {
  results = results.map(function (p) { return [p["lat"], p["lon"] ,(p["pmVal"])/100 ]});
  heat = L.heatLayer(results).addTo(map);
}

function clearData(){
  // lineArray.forEach( // TODO clear the markers from the map )
  lineArray = []; //this empties line array so that new lines can now be added
  d3.selectAll("#lines").html('');  // in theory, we should just call drawChart again
  d3.selectAll(".voronoi").html('');
}


/* converts pm2.5 purpleAir to pm2.5 to federal reference method in microgram/m^3 so that the data is "consistent"
only used when data is from purpleAir sensors. There are two different kinds of sensors, thus two different conversions
for sensors pms1003:
PM2.5,TEOM =−54.22405ln(0.98138−0.00772PM2.5,PMS1003)
for sensors pms5003:
PM2.5,TEOM =−64.48285ln(0.97176−0.01008PM2.5,PMS5003)
*/
function conversionPM(pm, sensorModel) {

  if (sensorModel != null) {
      var model = sensorModel.split('+')[0];

      var pmv = 0;
      if (model === 'PMS5003') {
          // console.log('PMS5003')
          pmv = (-1) * 64.48285 * Math.log(0.97176 - (0.01008 * pm));
      } else if (model === 'PMS1003') {
          // console.log('PMS1003')
          pmv = (-1) * 54.22405 * Math.log(0.98138 - (0.00772 * pm));
      }
  } else {
      // console.log(sensorModel + ' no model?');
      pmv = pm;
  }

  return pmv;
}
