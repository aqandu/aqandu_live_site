/* global d3 L:true */
/* eslint no-undef: "error" */
/* eslint no-mixed-operators: ["error", {"allowSamePrecedence": true}] */

// setting dates for timeline and for ajax calls
const today = new Date().toISOString().substr(0, 19) + 'Z';
const date = new Date();
date.setDate(date.getDate() - 1);
const yesterday = date.toISOString().substr(0, 19) + 'Z';

const x = d3.scaleTime().domain([new Date(yesterday), new Date(today)]);
const lineColor = d3.scaleOrdinal(d3.schemeCategory10);
const y = d3.scaleLinear().domain([0.0, 120.0]);

const sensLayer = L.layerGroup();
// const heat = L.heatLayer();

const epaColors = ['green', 'yellow', 'orange', 'red', 'veryUnhealthyRed', 'hazardousRed', 'noColor'];

const margin = {
  top: 20,
  right: 30,
  bottom: 30,
  left: 40,
};

let lineArray = [];

const dbEndpoint = '/dbapi/api';
const liveSensorURL = generateURL(dbEndpoint, '/liveSensors', null);
const lastPM25ValueURL = generateURL(dbEndpoint, '/lastValue', {'fieldKey': 'pm25'});

let theMap;


$(function() {
  window.onload = window.onresize = function () {
    setUp();
    // TODO: call the render function(s)
    //  L.imageOverlay('overlay1.png', [[40.795925, -111.998256], [40.693031, -111.827190]], {
    // 		opacity: 0.5,
    // 		interactive: true,
    // 	}).addTo(map);
  }

  theMap = setupMap();

  drawSensorOnMap();

  // there is new data every minute for a sensor in the db
  setInterval('updateDots()', 60000);

  // TODO can this layer stuff be made simpler??
  // TO ADD THE LAYER ICON BACK uncomment the following lines and the line L.control.layers(null, overlayMaps).addTo(theMap);
  // var overlayMaps = {
  //   "SensLayer": sensLayer
  // };

  sensLayer.addTo(theMap);
  // L.control.layers(null, overlayMaps).addTo(theMap);

});


/**
 * [setUp description]
 */
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
     .attr("class", "timeline")
     .attr("transform", "translate(" + (width / 2) + " ," + (height + margin.bottom + 15) + ")")
     .style("text-anchor", "middle")
     .text("Time");

  svg.select(".y.axis") // Add the Y Axis
     .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')')
     .call(yAxis);

  svg.select(".y.label")
     .attr("class", "timeline")
     .attr("transform", "rotate(-90)")
     .attr("y", 0) // rotated! x is now y!
     .attr("x", 0 - (height / 2))
     .attr("dy", "1em")
     .style("text-anchor", "middle")
     .text("PM 2.5 µg/m\u00B3");
}


/**
 * [setupMap description]
 * @return {[type]} [description]
 */
function setupMap() {
  const slcMap = L.map('SLC-map', {
    center: [40.7608, -111.8910],
    zoom: 13
  });

  // load a tile layer
  L.tileLayer('https://api.tiles.mapbox.com/v4/{id}/{z}/{x}/{y}.png?access_token=pk.eyJ1Ijoic2tpdHJlZSIsImEiOiJjajUyb2l0YzQwaHJwMnFwMTNhdGwxMGx1In0.V5OuKXRdmwjq4Lk3o8me1A', {
    maxZoom: 18,
    id: 'mapbox.streets',
    accessToken: 'pk.eyJ1Ijoic2tpdHJlZSIsImEiOiJjajUydDkwZjUwaHp1MzJxZHhkYnl3eTd4In0.TdQB-1U_ID-37stKON_osw'
  }).addTo(slcMap);

  // disabling zooming when scrolling down the page (https://gis.stackexchange.com/questions/111887/leaflet-mouse-wheel-zoom-only-after-click-on-map)
  slcMap.scrollWheelZoom.disable();
  slcMap.on('focus', () => { slcMap.scrollWheelZoom.enable(); });
  slcMap.on('blur', () => { slcMap.scrollWheelZoom.disable(); });

  const legend = L.control({position: 'bottomright'});

  legend.onAdd = function () {
    this._div = L.DomUtil.create('div', 'legend');
    this.update(this._div);
    return this._div;
  };

  legend.update = function (thediv) {
    // TODO: draw the legend
    var d3div = d3.select(thediv);
    d3div.append('span')
         .attr("class", "legendTitle")
         .text('Sensor types:')

    var dataLabel = ["airu", "PurpleAir", "Mesowest", "DAQ"];
    var labels = d3div.selectAll('label').data(dataLabel);
    labels.exit().remove();
    var labelsEnter = labels.enter().append('label')
                                    .attr("class", "sensorType");
    labels = labels.merge(labelsEnter);
    labels.text(d => d);

    labels.on('mouseover', d => {
      // Add 'unhovered' class to all dots
      d3.select('#SLC-map').selectAll('.dot:not(noColor)').classed('unhovered', true);
      d3.select('#SLC-map').selectAll('.dot:not(noColor)').classed('colored-border', false);
      // Remove 'unhovered' class from dots whose class matches the hovered legend entry
      d3.select('#SLC-map').selectAll('.' + d + ':not(noColor)').classed('unhovered', false);
      d3.select('#SLC-map').selectAll('.' + d + ':not(noColor)').classed('colored-border', true);
      // So at this point, only stuff that wasn't hovered will have the unhovered class
    });

    labels.on('mouseout', d => {
      // Remove 'unhovered' class from all dots
      d3.select('#SLC-map').selectAll('.dot:not(noColor)').classed('unhovered', false);
      d3.select('#SLC-map').selectAll('.dot:not(noColor)').classed('colored-border', false);
    });

    return thediv;
  }

  legend.addTo(slcMap);

  // adding color legend
  var colorLegend = L.control({position: 'bottomleft'});

	colorLegend.onAdd = function () {

		var div = L.DomUtil.create('div', 'colorLegend'),
			grades = [0, 12, 35.4, 55.4, 150.4, 250.4],
			colorLabels = [],
			from, to;

    colorLabels.push("<span class='legendTitle'>PM2.5 levels:</span>");

		for (var i = 0; i < grades.length; i++) {
			from = grades[i];
			to = grades[i + 1];

			colorLabels.push(
				'<label><i class="' + getColor(from + 1) + '"></i> ' +
        // from + (to ? ' &ndash; ' + to + ' µg/m<sup>3</sup> ' : ' µg/m<sup>3</sup> +'));
        (to ? from + ' &ndash; ' + to + ' µg/m<sup>3</sup></label>' : 'above ' + from + ' µg/m<sup>3</sup></label>'));
		}

		div.innerHTML = colorLabels.join('<br>');
		return div;
	};

	colorLegend.addTo(slcMap);

  return slcMap;
}


/**
 * Querys db to get the live sensors -- sensors that have data since yesterday beginnning of day
 * @return {[type]} [description]
 */
function drawSensorOnMap() {
  getDataFromDB(liveSensorURL).then((data) => {
    const response = data.map((d) => {
      if (d['Sensor Source'] === 'Purple Air') {
        d.pm25 = conversionPM(d.pm25, d['Sensor Model']);
      }

      return d
    });

    sensorLayer(response);

    }).catch((err) => {
      alert('error, request failed!');
      console.log('Error: ', err)
  });
}


/**
 * [sensorLayer description]
 * @param  {[type]} response [description]
 * @return {[type]}          [description]
 */
function sensorLayer(response){
  response.forEach(function (item) {

    var dotIcon = {
      iconSize:     [20, 20], // size of the icon
      iconAnchor:   [10, 10], // point of the icon which will correspond to marker's location
      popupAnchor:  [0, -5], // point from which the popup should open relative to the iconAnchor
      html: ''
    };

    if (item["Latitude"] !== null && item["Longitude"] !== null) {
      let classList = 'dot';
      let theColor = getColor(item["pm25"]);
      // console.log(item["ID"] + ' ' + theColor + ' ' + item["pm25"])
      classList = classList + ' ' + theColor + ' ';

      // throw away the spaces in the sensor name string so we have a valid class name
      classList += item["Sensor Source"].replace(/ /g, '');
      // classList += ' ' + item['ID'];
      dotIcon.className = classList;

      var mark = new L.marker(
        L.latLng(
          parseFloat(item["Latitude"]),
          parseFloat(item["Longitude"])
        ),
        { icon: L.divIcon(dotIcon) }
      ).addTo(sensLayer);

      mark.id = item['ID'];

      mark.bindPopup(
        L.popup({closeButton: false, className: 'sensorInformationPopup'}).setContent('<span class="popup">' + item["Sensor Source"] + ': ' + item["ID"] + '</span>'))
      // mark.bindPopup(popup)

      mark.on('click', populateGraph)
      mark.on('mouseover', function(e) {
        // console.log(e.target.id)
        this.openPopup();
      });
      mark.on('mouseout', function(e) {
        this.closePopup();
      });
    }
  });
}


function updateDots() {
  console.log('updating')

  getDataFromDB(lastPM25ValueURL).then((data) => {

    // apply conversion for purple air
    Object.keys(data).forEach(function(key) {
        console.log(key, data[key]);
        let sensorModel = data[key]['Sensor Model']
        data[key]['last'] = conversionPM(data[key]['last'], sensorModel);
    });

    sensLayer.eachLayer(function(layer) {
      console.log(layer.id)
      let currentPM25 = data[layer.id].last;

      let theColor = getColor(currentPM25);

      console.log(layer.id + ' ' + theColor + ' ' + currentPM25)
      $(layer._icon).removeClass(epaColors.join(' '))
      $(layer._icon).addClass(theColor)
    });

  }).catch((err) => {
    alert("error, request failed!");
    console.log("Error: ", err);
    console.warn(arguments);
  });
}

// 0 - 12 ug/m^3 is green
// 12.1 - 35.4 ug/m^3 is yellow
// 35.5 - 55.4 ug/m^3 is orange
// 55.5 - 150.4 ug/m^3 is red
// 150.5 - 250.4 ug/m^3 is veryUnhealthyRed
// above 250.5 ug/m^3 is hazardousRed
function getColor(currentValue) {
  let theColor;
  if (currentValue <= 12) {
    theColor = 'green';
  } else if (currentValue > 12 && currentValue <= 35.4) {
    theColor = 'yellow';
  } else if (currentValue > 35.4 && currentValue <= 55.4) {
    theColor = 'orange';
  } else if (currentValue > 55.4 && currentValue <= 150.4) {
    theColor = 'red';
  } else if (currentValue > 150.4 && currentValue <= 250.4) {
    theColor = 'veryUnhealthyRed';
  } else if (isNaN(currentValue)) {     // dealing with NaN values
    theColor = 'noColor';
  } else {
    theColor = 'hazardousRed';
  }

  return theColor;
}


// var map = setupMap();

function distance(lat1, lon1, lat2, lon2) {
  const p = 0.017453292519943295; // Math.PI / 180
  const c = Math.cos;
  const a = 0.5 - c((lat2 - lat1) * p) / 2 +
  c(lat1 * p) * c(lat2 * p) *
  (1 - c((lon2 - lon1) * p)) / 2;

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

  getDataFromDB(liveSensorURL).then((data) => {

    response = data.map((d) => {
      // return only location and ID
      const newD = {};
      newD.ID = d.ID;
      newD.Latitude = d.Latitude;
      newD.Longitude = d.Longitude;
      newD.SensorSource = d['Sensor Source']

      return newD;
    });

    var closest = findDistance(response, mark); // returns closest sensor using distance equation
    callback(closest);
  }).catch((err) => {
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
  const processedSensorData = sensorData.map((d) => {
    return {
      id: id,
      time: new Date(d.time),
      // pm25: d['pm2.5 (ug/m^3)']
      pm25: d['pm25']
    };
  }).filter((d) => {
    return d.pm25 === 0 || !!d.pm25; // forces NaN, null, undefined to be false, all other values to be true
  });

  var present = false;
  for (var i = 0; i < lineArray.length; i++) {
    if (lineArray[i].id === id) {
      present = true;
      break;
    }
  }

  if (!present) {
    console.log('not in there yet');
    var newLine = {id: id, sensorData: processedSensorData};

    // pushes data for this specific line to an array so that there can be multiple lines updated dynamically on Click
    lineArray.push(newLine)

    drawChart();
  }
}


function drawChart (){
  var svg = d3.select("div svg"); // TODO: this isn't specific enough...
  var bounds = svg.node().getBoundingClientRect();
  var width = bounds.width;
  var height = bounds.height;

  // Scale the range of the data
  var valueline = d3.line()
    .x(function (d) {
      return x(d.time);
    })
    .y(function (d) {
      return y(d.pm25);
    });

  //mike bostock's code
  var voronoi = d3.voronoi()
    .x(function(d) { return x(d.time); })
    .y(function(d) { return y(d.pm25); })
    .extent([[-margin.left, -margin.top], [width + margin.right, height + margin.bottom]]);

  // adds the svg attributes to container
  var lines = svg.select('#lines').selectAll('path')
    .data(lineArray, function (d) {
      return d.id;
    }); //any path in svg is selected then assigns the data from the array

  lines.exit().remove(); //remove any paths that have been removed from the array that no longer associated data

  // var linesEnter = lines.enter().append("path"); // looks at data not associated with path and then pairs it
  // lines = linesEnter.merge(lines); //combines new path/data pairs with previous, unremoved data

  lines.enter().append("path") // looks at data not associated with path and then pairs it
       .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')')
       .attr("d", d => { return valueline(d.sensorData); })
       .attr("class", d => 'line-style line' + d.id)
       .attr("id", function(d) { return 'line_' + d.id; })
       .attr("stroke", d => lineColor(d.id)); //no return for d function, see above for example

  var focus = svg.select(".focus");

  function mouseover(d) { //d is voronoi paths
    let hoveredLine = svg.select('.line' + d.data.id);
    hoveredLine.classed("hover", true);
    // Sneaky hack to bump hoveredLine to be the last child of its parent;
    // in SVG land, this means that hoveredLine will jump to the foreground
    //.node() gets the dom element (line element), then when you append child to the parent that it already has, it bumps updated child to the front
    hoveredLine.node().parentNode.appendChild(hoveredLine.node());
    // console.log(d.data.time)
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

  // adds the svg attributes to container
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


function getGraphData(mark){
  findNearestSensor(findCorners(mark.getLatLng()), mark, function (sensor) {
    // mark = mark.bindPopup('<p>'+ sensor["ID"] +'</p>').openPopup();

    // get the data displayed in the timeline
    // var linesInTimeline = d3.select('svg').select('#lines').selectAll('path').data();
    //
    // // check if clicked sensor ID is already there
    // var present = false;
    // for (var i = 0; i < linesInTimeline.length; i++) {
    //   if (linesInTimeline[i].id === sensor['ID']) {
    //     present = true;
    //     break;
    //   }
    // }
    //
    // if (!present) {
      var aggregation = false;

      let theRoute = '';
      let parameters = {};
      if (!aggregation) {
        theRoute = '/rawDataFrom?';
        parameters = {'id': sensor['ID'], 'sensorSource': sensor['SensorSource'], 'start': yesterday, 'end': today, 'show': 'pm25'};
      } else if (aggregation) {
        theRoute = '/processedDataFrom?';
        parameters = {'id': sensor['ID'], 'start': yesterday, 'end': today, 'function': 'mean', 'functionArg': 'pm25', 'timeInterval': '10m'};
      }

      var url = generateURL(dbEndpoint, theRoute, parameters);
      console.log(url)
      getDataFromDB(url).then(data => {

          // console.log(data)
          preprocessDBData(sensor["ID"], data)

      }).catch(function(err){

          alert("error, request failed!");
          console.log("Error: ", err)
      });
    // }
  });
}

var markr = null;

// called when clicked somewhere on the map
// function onMapClick(e) {
//   markr = new L.marker(e.latlng).addTo(map)
//
//   makeGraph(markr);
// }




//map.on('click', onMapClick);

/* this is for parsing through Amir's data
and then building a heat map from the data */

// lonPromise = getData("leaflet/sample-data/XGPS1.csv");
// latPromise = getData("leaflet/sample-data/XGPS2.csv");
// pmValPromise = getData("leaflet/sample-data/YPRED.csv");
//
// lvArray = []; //locations + values array
// Promise.all([lonPromise, latPromise, pmValPromise]) //Promise.all waits for all the other promises to finish
// .then(function (promiseResults) { //once they are finished, the .THEN tells it the next step (a function)
//   var lon = promiseResults[0].trim().split('\n');
//   lon = lon[0].split(',').map(value => Number(value));
//   var lat = promiseResults[1].trim().split('\n');
//   lat = lat.map(row => Number(row.split(',')[0]));
//   var pmVal = promiseResults[2].split('\n');
//   var results = [];
//
//   if (pmVal.length !== lat.length) {
//     throw new Error('wrong number of lat coordinates');
//   }
//
//   pmVal.forEach((row, latIndex) => {
//     row = row.split(',');
//     if (row.length <= 1) {
//       return;
//     }
//     if (row.length !== lon.length) {
//       throw new Error('wrong number of lon coordinates');
//     }
//     row.forEach((value, lonIndex) => {
//       results.push({
//         lat: lat[latIndex],
//         lon: lon[lonIndex],
//         pmVal: Number(value)
//       });
//     });
//   });
//   makeHeat(results);
// });



function getData(strng){
  return new Promise(function (resolve, reject) { //use a promise as a place holder until a promise is fulfilled (resolve)
    d3.text(strng, function(data){
      // console.log(strng, data)
      resolve(data);
    });
  });
}




// sensLayer.addTo(theMap);
// L.control.layers(null, overlayMaps).addTo(theMap);


function populateGraph(e) {

  if (d3.select(this._icon).classed('sensor-selected')) {
    // if dot already selected
    let clickedDotID = this.id
    // d3.select("#line_" + clickedDotID).remove();
    lineArray = lineArray.filter(line => line.id != clickedDotID);
    drawChart();
    d3.select(this._icon).classed('sensor-selected', false);
  } else {
    // only add the timeline if dot has usable data
    if (!d3.select(this._icon).classed('noColor')) {
      d3.select(this._icon).classed('sensor-selected', true);
      getGraphData(this);
    }
  }
}


// function showID() {
//   console.log(this)
//   var dotCenter = [this._latlng.lat, this._latlng.long]
//   var endPoint = [this._latlng.lat + 0.002, this._latlng.long  + 0.004]
//
//   L.polyline([
//     dotCenter,
//     endPoint
//   ]).addTo(map);
//
// }

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

  d3.selectAll ('.dot').classed('sensor-selected', false);
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
