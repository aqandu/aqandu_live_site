/* global d3 L:true */
/* eslint no-undef: "error" */
/* eslint no-mixed-operators: ["error", {"allowSamePrecedence": true}] */

// var imageUrl = 'static/aqconference_colorband.png';
// var imageUrl = 'static/test.svg'


// setting dates for timeline and for ajax calls
const todayDate = new Date();
const today = todayDate.toISOString().substr(0, 19) + 'Z';
const date = new Date();
date.setDate(date.getDate() - 1);
let pastDate = date.toISOString().substr(0, 19) + 'Z';

// the axis transformation
let x = d3.scaleTime().domain([new Date(pastDate), new Date(today)]);
const y = d3.scaleLinear().domain([0.0, 150.0]);

let showSensors = true;

const slcMap = L.map('SLC-map', {
    // center: [40.7608, -111.8910],
    center: [40.748808, -111.8896],
    zoom: 13,
    contextmenu: true,
    contextmenuWidth: 140,
    contextmenuItems: [{
      text: 'Create Marker',
      callback: createNewMarker
    }]
  });

const sensLayer = L.layerGroup();

const epaColors = ['green', 'yellow', 'orange', 'red', 'veryUnhealthyRed', 'hazardousRed', 'noColor'];

const margin = {
  top: 10,
  right: 50,
  bottom: 40,
  left: 50,
};

// holds the timeseries lines in the timeline graph
let lineArray = [];

let theContours = [];
let liveSensors = [];
let liveSensorsData = [];

const dbEndpoint = '/dbapi/api';
// const liveSensorURL_purpleAir = generateURL(dbEndpoint, '/liveSensors', {'type': 'purpleAir'});
const liveSensorURL_airU = generateURL(dbEndpoint, '/liveSensors', {'type': 'airU'});
const liveSensorURL_all = generateURL(dbEndpoint, '/liveSensors', {'type': 'all'});
const lastPM25ValueURL = generateURL(dbEndpoint, '/lastValue', {'fieldKey': 'pm25'});
// const contoursURL = generateURL(dbEndpoint, '/contours', null);
const lastContourURL = generateURL(dbEndpoint, '/getLatestContour', null);


let theMap;

let liveAirUSensors = [];

let whichTimeRangeToShow = 1;

let currentlySelectedDataSource = 'none';

// ids for the double clicks on the map
let latestGeneratedID = -1;

let dotsUpdateID;
let sensorUpdateID;
let contourUpdateID;


// function run when page has finished loading all DOM elements and they are ready to use
$(function() {
  // startTheWholePage(imageUrl)
  startTheWholePage()
});


//
function startTheWholePage() {

  // $(document).ready(init);
  // init()
  setUpTimeline();
  // don't use setUpTimeline() because it's only the reference to a function that is needed
  window.onresize = setUpTimeline;

  // theMap = setupMap(imageUrl);
  theMap = setupMap();

  sensLayer.addTo(theMap);

  // from https://github.com/aratcliffe/Leaflet.contextmenu/issues/37
  slcMap.contextmenu.disable();

  // shows either the sensors or the contours
  showMapDataVis();

  // // TODO can this layer stuff be made simpler??
  // // TO ADD THE LAYER ICON BACK uncomment the following lines and the line
  // var overlayMaps = {
  //   "Show Sensor": sensLayer
  // };
  //
  // L.control.layers(null, overlayMaps, {position: 'verticalcentertopleft'}).addTo(theMap);




  // preventing click on timeline to generate map event (such as creating dot for getting AQ)
  var timelineDiv = L.DomUtil.get('timeline');
  L.DomEvent.disableClickPropagation(timelineDiv);
  L.DomEvent.on(timelineDiv, 'mousewheel', L.DomEvent.stopPropagation);

  var legendDiv = L.DomUtil.get('legend');
  L.DomEvent.disableClickPropagation(legendDiv);
  L.DomEvent.on(legendDiv, 'mousewheel', L.DomEvent.stopPropagation);

  var reappearingButtonDiv = L.DomUtil.get('openLegendButton');
  L.DomEvent.disableClickPropagation(reappearingButtonDiv);
  L.DomEvent.on(reappearingButtonDiv, 'mousewheel', L.DomEvent.stopPropagation);

  $('#openTimelineControlButton').hide();


  // TODO the unclick
  // titleDataSource.on('mouseout', d => {
  //   $('.tooltip').removeClass('show')
  //   $('.tooltip').addClass('hidden')
  // });


  // for the screenscapture thing
  // bottomLeftCorner = {'lat': 40.598850, 'lng': -112.001349}
  // topRightCorner = {'lat': 40.810476, 'lng': -111.713403}
  // $('#timeline').hide();
  // $('nav').hide();
  // $('.legend').hide();
};


// function init() {
//
//
//
//   setUpTimeline();
// }


function showMapDataVis() {

  if (showSensors) {
    // showSensors is true show only sensor, not the contours

    clearInterval(contourUpdateID)

    // get and set the last sensor data
    drawSensorOnMap();

    hideSlider();

    // there is new data every minute for a sensor in the db
    dotsUpdateID = setInterval('updateDots()', 60000);  // 60'000 = 60'000 miliseconds = 60 seconds = 1 min
    sensorUpdateID = setInterval('updateSensors()', 300000); // update every 5min

  } else {
    // showSensors is false show only contours, not the sensors

    clearInterval(dotsUpdateID)
    clearInterval(sensorUpdateID)

    getContourData();

    // get and set the last contour
    getDataFromDB(lastContourURL).then(data => {

      console.log(data)
      setContour(slcMap, data);

    }).catch(function(err){

      alert("error, request failed!");
      console.log("Error: ", err)

    });

    showSlider();

    // get all sensor data for interactive brushing
    // getAllSensorData()

    contourUpdateID = setInterval('updateContour()', 300000); // update every 5min


  }

}


function getAggregation(timeRange) {

  if (timeRange === 1) {
    return false;
  } else {
    return true;
  }
}


// function getClosest(num, ar) {
//   var contourArray = ar;
//   contourArray.sort((a, b) => (new Date(a.time) > new Date(b.time)) ? 1 : ((new Date(b.time) > new Date(a.time)) ? -1 : 0))
//   if (num < new Date(contourArray[0].time)) {
//     // console.log('lowest');
//     return [contourArray[0], contourArray[0]];
//   } else if (num > new Date(contourArray[contourArray.length - 1].time)) {
//     // console.log('highest')
//     return [contourArray[contourArray.length - 1], contourArray[contourArray.length - 1]];
//   } else {
//     // console.log('inbetween')
//     return contourArray.sort((a, b) => Math.abs(new Date(a.time) - new Date(num)) - Math.abs(new Date(b.time) - new Date(num))).slice(0, 2);
//   }
// }

function getClosest(aDate, contourArray) {

  if (aDate < new Date(contourArray[0].time)) {
    return [contourArray[0], contourArray[0]];
  } else if (aDate > new Date(contourArray[contourArray.length - 1].time)) {
    return [contourArray[contourArray.length - 1], contourArray[contourArray.length - 1]];
  } else {

    // contourArray is sorted acending in time
    var previousElement;
    for (let element of contourArray) {
    // contourArray.forEach(function(element, i) {
      if (aDate < new Date(element.time)) {
        return [previousElement, element];
      }

      previousElement = element;
    }
  }
}


/**
 * setting up the timeline view
 */
function setUpTimeline() {

  // TIMELINE UI

  // sets the from date for the timeline when the radio button is changed
  $('#timelineControls input[type=radio]').on('change', function() {
    whichTimeRangeToShow = parseInt($('[name="timeRange"]:checked').val());

    let newDate = new Date(today);  // use "today" as the base date
    newDate.setDate(newDate.getDate() - whichTimeRangeToShow);
    pastDate = newDate.toISOString().substr(0, 19) + 'Z';

    // refresh x
    x = d3.scaleTime().domain([new Date(pastDate), new Date(today)]);
    setUpTimeline();  // TODO is there a better way than this circular calling


    // which IDs are there
    let lineData = [];
    lineArray.forEach(function(aLine) {
      let theAggregation = getAggregation(whichTimeRangeToShow);

      lineData.push({id: aLine.id, sensorSource: aLine.sensorSource, aggregation: theAggregation})
    });

    clearData(true);

    lineData.forEach(function(aLine) {
      reGetGraphData(aLine.id, aLine.sensorSource, aLine.aggregation);
    });


    if (!showSensors) {
      getContourData();
    } else {
      // need to do the same for the sensors TODO
      console.log('nothing there yet')
    }

  });

  // add the submit event
  $('#sensorDataSearchForm').on('submit', function(e) {
      e.preventDefault();  //prevent form from submitting
      document.getElementById('errorInformation').textContent = ''
      let data = $("#sensorDataSearchForm :input").serializeArray();
      console.log(data[0].value);

      let anAggregation = getAggregation(whichTimeRangeToShow);
      reGetGraphData(data[0].value, 'airu', anAggregation);

      // if the sensor is visible on the map, mark it as selected
      sensLayer.eachLayer(function(layer) {
        if (layer.id === data[0].value) {
          d3.select(layer._icon).classed('sensor-selected', true)
        }
      });
  });

  // TIMELINE

  var timelineDIV = d3.select("#timeline");
  var bounds = timelineDIV.node().getBoundingClientRect();
  var svgWidth = bounds.width;
  // var svgHeight = 340;
  var svgHeight = bounds.height;
  var width = svgWidth - margin.left - margin.right;    // right: 50, left: 50,
  var height = svgHeight - margin.top - margin.bottom - 18;  // top: 10,   bottom: 40,
  var svg = timelineDIV.select("svg") // sets size of svgContainer

  var formatSliderDate = d3.timeFormat('%a %d %I %p');
  var formatSliderHandler = d3.timeFormat('%a %m/%d %I:%M%p');

  x.range([0, width]);
  y.range([height, 0]);

  // adding the slider
  var slider = d3.select("#slider")
                 .attr("transform", "translate(50, 10)");

  slider.selectAll("line").remove();

  slider.append("line")
        .attr("class", "track")
        .attr("x1", x.range()[0])
        .attr("x2", x.range()[1])
      .select(function() { return this.parentNode.appendChild(this.cloneNode(true)); })
        .attr("class", "track-inset")
      .select(function() { return this.parentNode.appendChild(this.cloneNode(true)); })
        .attr("class", "track-overlay")
        .call(d3.drag()
            .on("start.interrupt", function() { slider.interrupt(); })
            .on("start drag", function(d) {

              console.log(d3.event.x)
              var currentDate = x.invert(d3.event.x);
              console.log(currentDate)

              // set the contour
              // theContours.sort((a, b) => Math.abs(new Date(a.time) - new Date(num)) - Math.abs(new Date(b.time) - new Date(num)))
              // var upperAndLowerBound = getClosest(currentDate, theContours.reverse());
              var upperAndLowerBound = getClosest(currentDate, theContours);
              console.log(upperAndLowerBound);


              var roundedDate
              if ((new Date(currentDate) - new Date(upperAndLowerBound[0].time)) >= (new Date(upperAndLowerBound[1].time) - new Date(currentDate))) {
                roundedDate = upperAndLowerBound[1]
              } else {
                roundedDate = upperAndLowerBound[0]
              }

              setContour(slcMap, roundedDate);

// the get all sensor data piece
              // // set the sensorData
              // var sensorValuesForGivenTime = {};
              // liveSensorsData.forEach(function(aLiveSensorData) {
              //   var upperAndLowerBoundSensorData = getClosest(currentDate, aLiveSensorData['pmData'].reverse());
              //
              //   var roundedDateSensorData
              //   if ((new Date(currentDate) - new Date(upperAndLowerBoundSensorData[0].time)) >= (new Date(upperAndLowerBoundSensorData[1].time) - new Date(currentDate))) {
              //     roundedDateSensorData = upperAndLowerBoundSensorData[1]
              //   } else {
              //     roundedDateSensorData = upperAndLowerBoundSensorData[0]
              //   }
              //
              //   // console.log(aLiveSensorData.id + ' -- ' + roundedDateSensorData.time + ': ' + roundedDateSensorData.pm25);
              //
              //   // setContour(slcMap, roundedDateSensorData);
              //   sensorValuesForGivenTime[aLiveSensorData.id] = {'time': roundedDateSensorData.time, 'pm25': roundedDateSensorData.pm25, 'sensorSource': aLiveSensorData.sensorSource, 'sensorModel': aLiveSensorData.sensorModel};
              // });
              //
              // setDotValues(sensorValuesForGivenTime);

              console.log(roundedDate)
              sliderHandle.attr('cx', x(new Date(roundedDate.time)));

              slider.select('#contourTime').attr("transform", "translate(" + (x(new Date(roundedDate.time)) - 50) + "," + 18 + ")")
                                           .text(formatSliderHandler(new Date(roundedDate.time)) );

              // theContours.forEach(function(element, index) {
              //   if currentDate >= element.time and currentDate < theContours[index+1]
              // })

              // setContour(slcMap, d.data);

              // x.invert(d3.event.x);
            }));

  slider.select('.ticks').remove();

  var trackOverlay = slider.insert("g", ".track-overlay")
        .attr("class", "ticks")
        .attr("transform", "translate(0," + 18 + ")")
      .selectAll("text")
      .data(x.ticks(9))
      .enter().append("text")
           .attr("x", x)
           .attr("text-anchor", "middle")
           .text(function(d) { return formatSliderDate(d); });

  slider.select("circle").remove();
  slider.select("#contourTime").remove();

  slider.insert("text", ".track-overlay")
        .attr("id", "contourTime");

  var sliderHandle = slider.insert("circle", ".track-overlay")
                           .attr("class", "handle")
                           .attr("r", 9);

  sliderHandle.attr('cx', x(todayDate));

  if (showSensors) {
    hideSlider();
  } else {
    showSlider();
  }



  // adding the graph
  // var graph = d3.select('#graph')
  d3.select('#graph')
    .attr("transform", "translate(0, 20)")

  svg.attr("width", svgWidth)
     .attr("height", svgHeight);

  // the color bands
  svg.select('#colorBands').selectAll('path').remove(); // added else when resizing it would add the bands all over again

  svg.select('#colorBands').append("path")
                .attr("d", getColorBandPath(0, 12))
                .style("opacity", 0.1)
                // .style("stroke", "rgb(0,228,0)")
                // .style("fill", "rgb(0,228,0)");
                .style("stroke", "rgb(166, 217, 106)")
                .style("fill", "rgb(166, 217, 106)");


  svg.select('#colorBands').append("path")
                .attr("d", getColorBandPath(12, 35.4))
                .style("opacity", 0.1)
                // .style("stroke", "rgb(255,255,0)")
                // .style("fill", "rgb(255,255,0)");
                .style("stroke", "rgb(255, 255, 191)")
                .style("fill", "rgb(255, 255, 191)");


  svg.select('#colorBands').append("path")
                .attr("d", getColorBandPath(35.4, 55.4))
                .style("opacity", 0.1)
                // .style("stroke", "rgb(255,126,0)")
                // .style("fill", "rgb(255,126,0)");
                .style("stroke", "rgb(253, 174, 97)")
                .style("fill", "rgb(253, 174, 97)");

  svg.select('#colorBands').append("path")
                .attr("d", getColorBandPath(55.4, 150.4))
                .style("opacity", 0.1)
                // .style("stroke", "rgb(255,0,0)")
                // .style("fill", "rgb(255,0,0)");
                .style("stroke", "rgb(215, 25, 28)")
                .style("fill", "rgb(215, 25, 28)");


  var xAxis = d3.axisBottom(x).ticks(9);
  var yAxis = d3.axisLeft(y).ticks(7);

  svg.select(".x.axis") // Add the X Axis
     .attr("transform", "translate(" + margin.left + "," + (margin.top + height) + ")")
     .call(xAxis);

  svg.select(".x.label")      // text label for the x axis
     .attr("class", "timeline")
     .attr("transform", "translate(" + (width / 2) + " ," + (height + margin.bottom) + ")")
     .style("text-anchor", "middle");
     // .text("Time");

  svg.select(".y.axis") // Add the Y Axis
     .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')')
     .call(yAxis);

  svg.select(".y.label")    // text label for the y axis
     .attr("class", "timeline")
     .attr("transform", "rotate(-90)")
     .attr("y", 0) // rotated! x is now y!
     .attr("x", 0 - (height / 2))
     .attr("dy", "1em")
     .style("text-anchor", "middle")
     .text("PM2.5 µg/m\u00B3");

  // disable map panning on timeline
  document.getElementById('timeline').addEventListener('mouseover', function () {
    theMap.dragging.disable();
  });

  document.getElementById('timeline').addEventListener('mouseout', function () {
    theMap.dragging.enable();
  });
}


function getColorBandPath(yStart, yEnd) {
  return "M" + (margin.left + x(x.domain()[0])) + "," + (margin.top + y(yStart)) +
         "L" + (margin.left + x(x.domain()[0])) + "," + (margin.top + y(yEnd)) +
         "L" + (margin.left + x(x.domain()[1])) + "," + (margin.top + y(yEnd)) +
         "L" + (margin.left + x(x.domain()[1])) + "," + (margin.top + y(yStart));
}


// Create additional control placeholders
// https://stackoverflow.com/questions/33614912/how-to-locate-leaflet-zoom-control-in-a-desired-position
function addControlPlaceholders(map) {
	var corners = map._controlCorners;
  var l = 'leaflet-';
  var container = map._controlContainer;

  function createCorner(vSide, hSide) {
    var className = l + vSide + ' ' + l + hSide;

    corners[vSide + hSide] = L.DomUtil.create('div', className, container);
  }

  createCorner('verticalcentertop', 'left');
  createCorner('verticalcentertop', 'right');

  createCorner('verticalcenterbottom', 'left');
  createCorner('verticalcenterbottom', 'right');
}


/**
 * setting up the leaflet map view and the UI elements for the map
 * @return {[type]} [description]
 */
function setupMap() {

  //beginning of Peter's code (how to use StamenTileLayer)
  var bottomLayer = new L.StamenTileLayer("toner");
  slcMap.addLayer(bottomLayer);

  var topPane = slcMap.createPane('leaflet-top-pane', slcMap.getPanes().mapPane);
  var topLayerLines = new L.StamenTileLayer('toner-lines');
  var topLayerLabels = new L.StamenTileLayer('toner-labels');
  slcMap.addLayer(topLayerLines);
  slcMap.addLayer(topLayerLabels);
  topPane.appendChild(topLayerLines.getContainer());
  topPane.appendChild(topLayerLabels.getContainer());
  topLayerLabels.setZIndex(9);
  topLayerLines.setZIndex(9);

  // 40.70159
  // imageBounds = [[40.70159, -112.058312], [40.84339186094368, -111.8185385553846]];
  imageBounds = [[40.598850, -112.001349], [40.810476, -111.713403]];
  // imageBounds = [[40.70159, -112.058312], [40.8433918609, -111.8109267]];
  // L.imageOverlay(imageUrl, imageBounds, {
  //    opacity: 0.8,
  //    interactive: true,
  //  }).addTo(slcMap);


  L.svg().addTo(slcMap);


  // var mapSVG = d3.select("#SLC-map").select("svg.leaflet-zoom-animated");
  // var mapSVG_g = mapSVG.select("g");

  // disabling zooming when scrolling down the page (https://gis.stackexchange.com/questions/111887/leaflet-mouse-wheel-zoom-only-after-click-on-map)
  slcMap.scrollWheelZoom.disable();
  slcMap.on('focus', () => { slcMap.scrollWheelZoom.enable(); });
  slcMap.on('blur', () => { slcMap.scrollWheelZoom.disable(); });

  // disabling zooming when double clicking
  // slcMap.doubleClickZoom.disable();

  // adding new placeholders for leaflet controls
  addControlPlaceholders(slcMap);

// TODO to be deleted
  // // legend reappearing button
  // var reappearControlContainer = L.control({position: 'verticalcentertopright'});
  // reappearControlContainer.onAdd = function () {
  //
  //   var reappearingButton = document.createElement('div');
  //   reappearingButton.setAttribute('class', 'closeButton');
  //   reappearingButton.setAttribute('id', 'openLegendButton');
  //
  //   var i_reappearingButton = document.createElement('i');
  //   i_reappearingButton.setAttribute('class', 'aqu_icon fas fa-list fa-2x')
  //   reappearingButton.appendChild(i_reappearingButton);
  //
  //   return reappearingButton
  // }
  //
  // reappearControlContainer.addTo(slcMap);
  //
  // $('#openLegendButton').hide();

  createCustomButton('verticalcentertopright', 'customButton', 'openLegendButton', 'fa-list', true)

// TODO to be deleted
  // // timeline reappearing button
  // var reappearTimelineControlContainer = L.control({position: 'verticalcenterbottomleft'});
  // reappearTimelineControlContainer.onAdd = function () {
  //
  //   var reappearingTimelineButton = document.createElement('div');
  //   reappearingTimelineButton.setAttribute('class', 'closeButton');
  //   reappearingTimelineButton.setAttribute('id', 'openTimelineButton');
  //
  //   var i_reappearingTimelineButton = document.createElement('i');
  //   i_reappearingTimelineButton.setAttribute('class', 'aqu_icon fas fa-list fa-2x')
  //   reappearingTimelineButton.appendChild(i_reappearingTimelineButton);
  //
  //   return reappearingTimelineButton
  // }
  //
  // reappearTimelineControlContainer.addTo(slcMap);
  //
  // $('#openTimelineButton').hide();

  createCustomButton('verticalcenterbottomleft', 'customButton', 'openTimelineButton', 'fa-list', true);

  createCustomButton('verticalcentertopleft', 'customButton', 'changeOverlay', 'fa-layer-group', false);



  // adding the legend container
  var legendControl = L.control({position: 'verticalcentertopright'});

  legendControl.onAdd = function () {

    // adding color legend
    var legendContainer = L.DomUtil.create('div', 'legend');
    legendContainer.setAttribute('id', 'legend');

    var colorLegend = L.DomUtil.create('div', 'colorLegend');
    colorLegend.setAttribute('id', 'colorLegend');
    legendContainer.appendChild(colorLegend);

    // close button
    var closeButtonContainer = document.createElement('div');
    closeButtonContainer.setAttribute('class', 'customButton');
    closeButtonContainer.setAttribute('id', 'closeLegendButton');
    legendContainer.appendChild(closeButtonContainer);

    var closeButton_a = document.createElement('a');
    // closeButton_a.setAttribute('class', 'customButton');
    closeButton_a.setAttribute('href', "#");
    var createAText = document.createTextNode('X');
    closeButton_a.appendChild(createAText);
    closeButtonContainer.appendChild(closeButton_a);



    // var grades = [0, 12, 35.4, 55.4, 150.4, 250.4];
    // var grades = [0.0, 4.0, 8.0, 12.0, 19.8, 27.6, 35.4, 42.1, 48.7, 55.4, 150.4, 250.4]

    // reversed both grades and colors array
    var grades = [4, 8, 12, 20, 28, 35, 42, 49, 55, 150, 250, 350].reverse()
    var colors = ['green1', 'green2', 'green3', 'yellow1', 'yellow2', 'yellow3', 'orange1', 'orange2', 'orange3', 'red1', 'veryUnhealthyRed1', 'hazardousRed1'].reverse()
    // var colorLabels = [];
    // var from;
    // var to;

    var title = document.createElement('span');
    title.setAttribute("id", 'PM25level');
    title.setAttribute("class", "legendTitle");
    colorLegend.appendChild(title);

    var labelContainer = L.DomUtil.create('div');
    labelContainer.setAttribute('id', 'labelContainer');
    colorLegend.appendChild(labelContainer);

    var theTitleContent = document.createTextNode("PM2.5 [µg/m\u00B3]:");
    title.appendChild(theTitleContent);

    // create colored rectangle
    var lastElement;
    colors.forEach(function(aColor, index) {
      var tmp = document.createElement('div');
      tmp.setAttribute("class", "colorLegendLabel");

      var colorDiv = document.createElement('div');
      colorDiv.setAttribute("id", aColor);
      colorDiv.setAttribute("class", "colorbar " + aColor);
      tmp.appendChild(colorDiv);

      var span = document.createElement('span');
      span.setAttribute("class", "tickLegend");
      span.setAttribute("id", 'tickLegend_' + grades[index])
      span.textContent = "\u2014 " + grades[index];
      tmp.appendChild(span);

      lastElement = tmp;

      labelContainer.appendChild(tmp);
    })

    var lastSpan = document.createElement('span');
    lastSpan.setAttribute("class", "tickLegend");
    lastSpan.setAttribute("id", 'tickLegend_0')
    lastSpan.textContent = "\u2014 0";
    lastElement.appendChild(lastSpan);


    var hr = L.DomUtil.create('hr', 'theHR');
    legendContainer.appendChild(hr);

    // adding data source legend
    var datasourceLegend = L.DomUtil.create('div', 'datasourceLegend');
    legendContainer.appendChild(datasourceLegend);

    var d3div = d3.select(datasourceLegend);
    var titleDataSource = d3div.append('span')
         .attr('id', 'datasource')
         .attr('class', 'legendTitle')
         .html('Data sources:');


    var dataLabel = ["airu", "PurpleAir", "Mesowest", "DAQ"];
    var labels = d3div.selectAll('label').data(dataLabel);
    labels.exit().remove();
    var labelsEnter = labels.enter()
                           .append('label')
                           .attr("class", "sensorType")

    labels = labels.merge(labelsEnter);
    labels.text(d => d);

    labels.insert('span', function() {
            return this.childNodes[0];
          })
          .classed('notSelectedLabel', true)
          .append('i')
          .attr('class', 'fas fa-circle');

    labels.append('span')
      .attr("id", d => 'numberof_' + d);

    labels.on('click', d => {
      if (currentlySelectedDataSource != 'none') {
      // element in sensor type legend has been clicked (was already selected) or another element has been selected

        d3.select('.clickedLegendElement').classed('clickedLegendElement', false)
        d3.select('.selectedLabel').classed('notSelectedLabel', true);
        d3.select('.selectedLabel').classed('selectedLabel', false);
        if (currentlySelectedDataSource === d) {
          // remove notPartOfGroup class
          // remove colored-border-selected class
          d3.select('#SLC-map').selectAll('.dot:not(noColor)').classed('notPartOfGroup', false);
          d3.select('#SLC-map').selectAll('.dot:not(noColor)').classed('partOfGroup-border', false);

          currentlySelectedDataSource = 'none'
        } else {
          // moved from one element to another without first unchecking it

          d3.select(d3.event.currentTarget).classed('clickedLegendElement', true)
          d3.select(d3.event.currentTarget).select('span').classed('notSelectedLabel', false);
          d3.select(d3.event.currentTarget).select('span').classed('selectedLabel', true);

          d3.select('#SLC-map').selectAll('.dot:not(noColor)').classed('notPartOfGroup', true);
          d3.select('#SLC-map').selectAll('.dot:not(noColor)').classed('partOfGroup-border', false);
          d3.select('#SLC-map').selectAll('.' + d + ':not(noColor)').classed('notPartOfGroup', false);
          d3.select('#SLC-map').selectAll('.' + d + ':not(noColor)').classed('partOfGroup-border', true);

          currentlySelectedDataSource = d
        }

      } else {
        // add the notPartOfGroup class to all dots, then remove it for the ones that are actually notPartOfGroup
        // remove partOfGroup-border for all dots and add it only for the selected ones

        d3.select(d3.event.currentTarget).classed('clickedLegendElement', true)
        d3.select(d3.event.currentTarget).select('span').classed('notSelectedLabel', false);
        d3.select(d3.event.currentTarget).select('span').classed('selectedLabel', true);

        d3.select('#SLC-map').selectAll('.dot:not(noColor)').classed('notPartOfGroup', true);
        d3.select('#SLC-map').selectAll('.dot:not(noColor)').classed('partOfGroup-border', false);
        d3.select('#SLC-map').selectAll('.' + d + ':not(noColor)').classed('notPartOfGroup', false);
        d3.select('#SLC-map').selectAll('.' + d + ':not(noColor)').classed('partOfGroup-border', true);

        currentlySelectedDataSource = d;
      }
    });

    return legendContainer;
  };

  legendControl.addTo(slcMap);


  $('#closeLegendButton').on("click", function() {
    console.log('hiding legend');
    $('.legend').hide();
    $('#openLegendButton').show();
  });

  $('#openLegendButton').on("click", function() {
    console.log('showing color legend');
    $('.legend').show();
    $('#openLegendButton').hide();
  });


  $('#closeTimelineControlButton').on("click", function() {
    console.log('hiding timeline controls');
    $('#timelineControls').hide();
    $('#openTimelineControlButton').show();
  });

  $('#openTimelineControlButton').on("click", function() {
    console.log('showing the controls for the timeline');
    $('#openTimelineControlButton').hide();
    $('#timelineControls').show();
  })


  $('#closeTimelineButton').on("click", function() {
    console.log('hiding timeline');
    // $('#timelineControls').hide();
    $('#timeline').hide();
    $('#openTimelineButton').show();
  });

  $('#openTimelineButton').on("click", function() {
    console.log('showing timeline');
    $('#timeline').show();
    $('#openTimelineButton').hide();
  });


  // change the overlay
  $('#changeOverlay').on("click", function() {
    console.log('change the overlay');

    flipMapDataVis();
  });

  // Change the position of the Zoom Control to a newly created placeholder.
  // slcMap.zoomControl.setPosition('verticalcenterbottomright');
  slcMap.zoomControl.setPosition('verticalcentertopleft');

  return slcMap;
}


function createCustomButton(thePosition, buttonClass, buttonID, faIcon, hideButton) {

  var buttonControlContainer = L.control({position: thePosition});
  buttonControlContainer.onAdd = function () {

    var aButton = document.createElement('div');
    aButton.setAttribute('class', buttonClass);
    aButton.setAttribute('id', buttonID);

    var iButton = document.createElement('i');
    iButton.setAttribute('class', 'aqu_icon fas ' + faIcon + ' fa-2x')
    aButton.appendChild(iButton);

    return aButton
  }

  buttonControlContainer.addTo(slcMap);

  if (hideButton) {
    // hide the button
    $('#' + buttonID).hide();
  }

}


function setContour(theMap, theContourData) {

  var contours = [];
  var allContours = theContourData.contour;
  for (var key in allContours) {
    if (allContours.hasOwnProperty(key)) {
        // console.log(key, allContours[key]);
        var theContour = allContours[key];
        var aContour = theContour.path;
        aContour.level = theContour.level;
        aContour.k = theContour.k;

        contours.push(aContour);
    }
  }

  contours.sort(function(a,b) {
      return b.level - a.level;
  });

  // var levelColours = ['#a6d96a', '#ffffbf', '#fdae61', '#d7191c', '#bd0026', '#a63603'];
  var levelColours = ['#31a354', '#a1d99b', '#e5f5e0', '#ffffcc', '#ffeda0', '#fed976', '#feb24c', '#fd8d3c', '#fc4e2a', '#e31a1c', '#bd0026', '#800026'];
  var defaultContourColor = 'black';
  var defaultContourWidth = 1;

  var mapSVG = d3.select("#SLC-map").select("svg.leaflet-zoom-animated");
  var g = mapSVG.select("g");  //.attr("class", "leaflet-zoom-hide").attr('opacity', 0.8);

  // var contourPath = g.selectAll("path")
  //         .data(contours)
  //       .enter().append("path")
  //       .style("fill", function(d, i) { return levelColours[d.level];})
  //       .style("stroke", defaultContourColor)
  //       .style('stroke-width', defaultContourWidth)
  //       .style('opacity', 1)
  //       .on('mouseover', function(d) {
  //           d3.select(this).style('stroke', 'black');
  //       })
  //       .on('mouseout', function(d) {
  //           d3.select(this).style('stroke', defaultContourColor);
  //       });

  var contourPath = g.selectAll("path")
          .data(contours, function(d) { return d; });

  contourPath.style("fill", function(d, i) { return levelColours[d.level];})
            // .style("stroke", defaultContourColor)
            // .style('stroke-width', defaultContourWidth)
            .style('opacity', 1)
            .on('mouseover', function(d) {
                d3.select(this).style('stroke', 'black');
            })
            .on('mouseout', function(d) {
                d3.select(this).style('stroke', defaultContourColor);
            });

  var contourEnter = contourPath.enter().append("path")
    // .merge(contourPath)
      // .attr("d", function(d) {
      //   var pathStr = d.map(function(d1) {
      //     var point = theMap.latLngToLayerPoint(new L.LatLng(d1[1], d1[2]));
      //     return d1[0] + point.x + "," + point.y;
      //   }).join('');
      //   return pathStr;
      // })
      .style("fill", function(d, i) { return levelColours[d.level];})
      // .style("stroke", defaultContourColor)
      // .style('stroke-width', defaultContourWidth)
      .style('opacity', 1)
      .on('mouseover', function(d) {
          d3.select(this).style('stroke', 'black');
      })
      .on('mouseout', function(d) {
          d3.select(this).style('stroke', defaultContourColor);
      });

  contourPath.exit().remove();

  function resetView() {
    console.log('reset:', theMap.options.center);
    contourEnter.attr("d", function(d) {
      var pathStr = d.map(function(d1) {
        var point = theMap.latLngToLayerPoint(new L.LatLng(d1[1], d1[2]));
        return d1[0] + point.x + "," + point.y;
      }).join('');

      //console.log('d', d);

      return pathStr;
    });
  }

  // slcMap.on("viewreset", resetView);
  theMap.on("zoom", resetView);

  resetView();
}


/**
 * Querys db to get the live sensors -- sensors that have data since yesterday beginnning of day
 * @return {[type]} [description]
 */
function drawSensorOnMap() {

  $('#SLC-map').LoadingOverlay("show");

  getDataFromDB(liveSensorURL_all).then((data) => {
    console.log("initial data :" + data.length)

    // let timeNow = Date.now();
    // // let sensorWithDataInLast5min = data
    // let sensorWithDataInLast5min = data.filter(sensor => Math.abs(timeNow - sensor['time'])/(1000*60) <= 5.0)
    // console.log("filtered data :" + sensorWithDataInLast5min.length)

    var numberOfPurpleAir = data.filter(sensor => sensor['Sensor Source'] === 'Purple Air').length;
    $('#numberof_PurpleAir').html(numberOfPurpleAir);

    var numberOfAirU = data.filter(sensor => sensor['Sensor Source'] === 'airu').length;
    $('#numberof_airu').html(numberOfAirU);

    var numberOfMesowest = data.filter(sensor => sensor['Sensor Source'] === 'Mesowest').length;
    $('#numberof_Mesowest').html(numberOfMesowest);

    var numberOfDAQ = data.filter(sensor => sensor['Sensor Source'] === 'DAQ').length;
    $('#numberof_DAQ').html(numberOfDAQ);

    const response = data.map((d) => {
      d.pm25 = conversionPM(d.pm25, d['Sensor Source'], d['Sensor Model']);

      return d
    });

    sensorLayer(response);

    data.forEach(function(aSensor) {
      liveSensors.push({'id': aSensor.ID.split(' ').join('_'), 'sensorSource': aSensor['Sensor Source']});
    });

// removed this call to get all sensor Data TODO
    // getAllSensorData();

    $('#SLC-map').LoadingOverlay("hide");

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
function sensorLayer(response) {
  response.forEach(createMarker);
}


function sensorLayerDebugging(response) {
  response.forEach(createMarkerDebugging);
}

// layer with the marks where people clicked
function sensorLayerRandomMarker(response) {
  response.forEach(createRandomClickMarker);
}



function createMarker(markerData) {
  var dotIcon = {
    iconSize:     [20, 20], // size of the icon
    iconAnchor:   [10, 10], // point of the icon which will correspond to marker's location
    popupAnchor:  [0, -5], // point from which the popup should open relative to the iconAnchor
    html: ''
  };

  // console.log(markerData);

  let sensorSource = markerData['Sensor Source'];
  console.log(sensorSource);

  if (markerData.Latitude !== null && markerData.Longitude !== null) {
    let classList = 'dot';
    let currentPM25 = markerData.pm25;

    // if (markerData.time != undefined) {
    let currentTime = new Date().getTime();
    let timeLastMeasurement = markerData.time;
    let minutesINBetween = (currentTime - timeLastMeasurement) / (1000 * 60);

    // console.log(currentTime)
    // console.log(timeLastMeasurement)
    // console.log(minutesINBetween)
    //
    // if (minutesINBetween =='undefined') {
    //   console('ahhhhhh')
    // }

    let theColor = displaySensor(sensorSource, minutesINBetween, currentPM25)

    // if (markerData['Sensor Source'] === 'airu') {
    //   if (minutesINBetween < 5.0) {
    //     theColor = getColor(currentPM25);
    //   } else {
    //     theColor = 'noColor';
    //   }
    // } else {
    //   theColor = getColor(currentPM25);
    // }

    // let theColor = getColor(markerData["pm25"]);
    // console.log(item["ID"] + ' ' + theColor + ' ' + item["pm25"])
    classList = classList + ' ' + theColor + ' ';

    // throw away the spaces in the sensor name string so we have a valid class name
    classList += sensorSource.replace(/ /g, '');
    // classList += ' ' + item['ID'];
    dotIcon.className = classList;

    var mark = new L.marker(
      L.latLng(
        parseFloat(markerData.Latitude),
        parseFloat(markerData.Longitude)
      ),
      { icon: L.divIcon(dotIcon) }
    ).addTo(sensLayer);

    mark.id = markerData['ID'];
    if (sensorSource == "airu") {
      liveAirUSensors.push(markerData.ID)
    }

    mark.bindPopup(
      L.popup({closeButton: false, className: 'sensorInformationPopup'}).setContent('<span class="popup">' + sensorSource + ': ' + markerData.ID + '</span>'))
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
}


function createMarkerDebugging(markerData) {
  var dotIcon = {
    iconSize:     [20, 20], // size of the icon
    iconAnchor:   [10, 10], // point of the icon which will correspond to marker's location
    popupAnchor:  [0, -5], // point from which the popup should open relative to the iconAnchor
    html: ''
  };

  console.log(markerData);

  if (markerData.Latitude !== null && markerData.Longitude !== null) {
    let classList = 'dot';

    let theColor = 'hazardousRed'

    // let theColor = getColor(markerData["pm25"]);
    // console.log(item["ID"] + ' ' + theColor + ' ' + item["pm25"])
    classList = classList + ' ' + theColor + ' ';

    dotIcon.className = classList;

    var mark = new L.marker(
      L.latLng(
        parseFloat(markerData.Latitude),
        parseFloat(markerData.Longitude)
      ),
      { icon: L.divIcon(dotIcon) }
    ).addTo(sensLayer);

    mark.id = 'sensorLayerDebugging';

  }
}


function createRandomClickMarker(markerData) {
  var dotIcon = {
    iconSize:     [20, 20], // size of the icon
    iconAnchor:   [10, 10], // point of the icon which will correspond to marker's location
    popupAnchor:  [0, -5], // point from which the popup should open relative to the iconAnchor
    html: ''
  };

  console.log(markerData);

  if (markerData.Latitude !== null && markerData.Longitude !== null) {
    let classList = 'dot';

    // let theColor = 'hazardousRed'
    let theColor = 'dblclickOnMap'

    // let theColor = getColor(markerData["pm25"]);
    // console.log(item["ID"] + ' ' + theColor + ' ' + item["pm25"])
    classList = classList + ' ' + theColor + ' ';

    dotIcon.className = classList;

    var mark = new L.marker(
      L.latLng(
        parseFloat(markerData.Latitude),
        parseFloat(markerData.Longitude)
      ),
      { icon: L.divIcon(dotIcon) }
    ).addTo(sensLayer);

    // mark.id = 'sensorLayerRandomMarker';
    mark.id = markerData['ID'];

    mark.bindPopup(
      L.popup({closeButton: false, className: 'sensorInformationPopup'}).setContent('<span class="popup">' + markerData["Sensor Source"] + ': ' + markerData.ID + '</span>'));

    // set the border of created marker to selected
    d3.select(mark._icon).classed('sensor-selected', true);

    mark.on('mouseover', function(e) {
      this.openPopup();
    });

    mark.on('mouseout', function(e) {
      this.closePopup();
    });

  }
}


// get the data for the contours between start and end
function getContourData() {

  $('#SLC-map').LoadingOverlay("show");

  console.log(pastDate)
  console.log(today)

  // get difference between the dates in displays
  var diffDays = Math.ceil((new Date(today)-new Date(pastDate))/(1000*60*60*24));
  console.log(diffDays);

  // if more than 1 day, load each day separately
  var contoursURL = '';
  var endDate = today;
  // let chain = Promise.resolve([]);
  // let theContours = []
  var listOfPromises = [];
  for (let i = 1; i <= diffDays; i++) {

      var anIntermediateDate = new Date(today);
      anIntermediateDate.setDate(anIntermediateDate.getDate() - i);
      var startDate = anIntermediateDate.toISOString().substr(0, 19) + 'Z';

      console.log(startDate)
      console.log(endDate)



      var contoursURL = generateURL(dbEndpoint, '/contours', {'start': startDate, 'end': endDate});
      console.log(contoursURL);

      listOfPromises.push(getDataFromDB(contoursURL));

      endDate = startDate
  }

  console.log(listOfPromises)

  Promise.all(listOfPromises).then(result => {
    theContours = result.flat();
    theContours.sort((a, b) => (new Date(a.time) > new Date(b.time)) ? 1 : ((new Date(b.time) > new Date(a.time)) ? -1 : 0));

    console.log(theContours);
    $('#SLC-map').LoadingOverlay("hide");
  })


  // var contoursURL = generateURL(dbEndpoint, '/contours', {'start': pastDate, 'end': today})
  //
  // getDataFromDB(contoursURL).then(data => {
  //
  //   console.log(contoursURL)
  //
  //   console.log('contour data')
  //   console.log(data)
  //   theContours = data
  //   // theContours.sort((a, b) => Math.abs(new Date(a.time) - new Date(b.time)) - Math.abs(new Date(b.time) - new Date(a.time)))
  //   // process contours data
  //   // setContour(slcMap, data);
  //
  //   theContours.sort((a, b) => (new Date(a.time) > new Date(b.time)) ? 1 : ((new Date(b.time) > new Date(a.time)) ? -1 : 0));
  //
  //   $('#SLC-map').LoadingOverlay("hide");
  //
  // }).catch(err => {
  //
  //     alert("error, request failed!");
  //     console.log("Error: ", err)
  // });
}


// get through all live sensors and load their data (past 24hours)
function getAllSensorData() {

  liveSensors.forEach(function(aLiveSensor) {

    var route = '/rawDataFrom?';
    var parameters = {'id': aLiveSensor.id, 'sensorSource': aLiveSensor.sensorSource, 'start': pastDate, 'end': today, 'show': 'pm25'};

    var aSensorRawDataURL = generateURL(dbEndpoint, route, parameters)

    getDataFromDB(aSensorRawDataURL).then(data => {

      console.log(aSensorRawDataURL)

      console.log('a sensor data')
      console.log(data)
      liveSensorsData.push({'id': data.tags[0]['ID'], 'pmData': data.data, 'sensorModel': data.tags[0]['Sensor Model'], 'sensorSource': data.tags[0]['Sensor Source']})
      // theContours.sort((a, b) => Math.abs(new Date(a.time) - new Date(b.time)) - Math.abs(new Date(b.time) - new Date(a.time)))
      // process contours data
      // setContour(slcMap, data);

    }).catch(function(err){

        alert("error, request failed!");
        console.log("Error: ", err)
    });


  })


}


function updateDots() {
  console.log('updating the dots current value');

  getDataFromDB(lastPM25ValueURL).then((data) => {

    // apply conversion for purple air
    Object.keys(data).forEach(function(key) {
        // console.log(key, data[key]);
        let sensorModel = data[key]['Sensor Model'];
        let sensorSource = data[key]['Sensor Source'];
        // console.log(conversionPM(data[key]['last'], sensorModel))
        data[key]['last'] = conversionPM(data[key]['last'], sensorSource, sensorModel);
        // console.log(data[key]['last'])
    });

    sensLayer.eachLayer(function(layer) {
      // if (layer.id !== "sensorLayerRandomMarker") {
      // if (data[layer.id]['Sensor Source'] !== "sensorLayerRandomMarker") {
      if (data[layer.id] !== undefined) {
        let currentTime = new Date().getTime()
        let timeLastMeasurement = new Date(data[layer.id].time).getTime();
        let minutesINBetween = (currentTime - timeLastMeasurement) / (1000 * 60);

        let currentPM25 = data[layer.id].last;
        let theSensorSource = data[layer.id]['Sensor Source']

        let theColor = displaySensor(theSensorSource, minutesINBetween, currentPM25)
        // let theColor = 'noColor';
        // if (data[layer.id]['Sensor Source'] === 'airu' || data[layer.id]['Sensor Source'] === 'Purple Air') {
        //   if (minutesINBetween < 10.0) {
        //     theColor = getColor(currentPM25);
        //   }
        //   // else {
        //   //   theColor = 'noColor';
        //   // }
        // } else if (data[layer.id]['Sensor Source'] === 'DAQ') {
        //
        //   if (minutesINBetween < 180.0) {
        //     theColor = getColor(currentPM25);
        //   }
        //   // else {
        //   //   theColor = 'noColor';
        //   // }
        // } else if (data[layer.id]['Sensor Source'] === 'Mesowest') {
        //
        //   if (minutesINBetween < 20.0) {
        //     theColor = getColor(currentPM25);
        //   }
        //   // else {
        //   //   theColor = 'noColor';
        //   // }
        // }

        // console.log(layer.id + ' ' + theColor + ' ' + currentPM25)
        $(layer._icon).removeClass(epaColors.join(' '))
        $(layer._icon).addClass(theColor)
      }
    });

  }).catch((err) => {
    alert("error, request failed!");
    console.log("Error: ", err);
    console.warn(arguments);
  });
}

// // gets object of sensor values for a given timestamp and sets the sensors dots to the right color
// function setDotValues(sensorValues) {
//
//   sensLayer.eachLayer(function(layer) {
//     if (sensorValues[layer.id] !== undefined) {
//       // let currentTime = new Date().getTime()
//       // let timeLastMeasurement = new Date(data[layer.id].time).getTime();
//       // let minutesINBetween = (currentTime - timeLastMeasurement) / (1000 * 60);
//
//       let pm25Value = conversionPM(sensorValues[layer.id].pm25, sensorValues[layer.id].sensorSource, sensorValues[layer.id].sensorModel)
//
//       let theColor = getColor(pm25Value);
//       // if (data[layer.id]['Sensor Source'] === 'airu') {
//       //     theColor = getColor(currentPM25);
//       // } else {
//       //   theColor = getColor(currentPM25);
//       // }
//
//       // console.log(layer.id + ' ' + theColor + ' ' + currentPM25)
//       $(layer._icon).removeClass(epaColors.join(' '))
//       $(layer._icon).addClass(theColor)
//     }
//   });
// }


function updateSensors() {
  console.log('updating the sensors: adding new airUs if available');

  getDataFromDB(liveSensorURL_airU).then((data) => {

    var numberOfAirUOut = data.length;
    $('#numberof_airu').html(numberOfAirUOut);

    const response = data.filter((d) => {

      if (!liveAirUSensors.includes(d.ID)) {
        return d;
      }
    });

    sensorLayer(response);

    }).catch((err) => {
      alert('error, request failed!');
      console.log('Error: ', err)
  });
}


// updates the contours
function updateContour() {
  console.log('updating the contours');

  getDataFromDB(lastContourURL).then(data => {

    console.log(data)
    // process contours data
    setContour(slcMap, data);

  }).catch(function(err){

      // alert("error, request failed!");
      console.log("Error when updating the contour: ", err)
  });
}


function displaySensor(aSensorSource, timePassedSinceLastDataValue, aCurrentValue) {

  let theColor = 'noColor';
  let calculatedColor = getColor(aCurrentValue);

  if (aSensorSource === 'airu' || aSensorSource === 'Purple Air') {
    if (timePassedSinceLastDataValue <= 10.0) {
      theColor = calculatedColor;
    }
  } else if (aSensorSource === 'DAQ') {

    if (timePassedSinceLastDataValue <= 180.0) {
      theColor = calculatedColor;
    }
  } else if (aSensorSource === 'Mesowest') {

    if (timePassedSinceLastDataValue <= 20.0) {
      theColor = calculatedColor;
    }
  } else {
    console.log('displaySensor: forgotten a case!!');
  }

  return theColor;
}


// 0 - 12 ug/m^3 is green
// 12.1 - 35.4 ug/m^3 is yellow
// 35.5 - 55.4 ug/m^3 is orange
// 55.5 - 150.4 ug/m^3 is red
// 150.5 - 250.4 ug/m^3 is veryUnhealthyRed
// above 250.5 ug/m^3 is hazardousRed
function getColor(currentValue) {
  let theColor;
  // if (currentValue <= 12) {
  //   theColor = 'green';
  // } else if (currentValue > 12 && currentValue <= 35.4) {
  //   theColor = 'yellow';
  // } else if (currentValue > 35.4 && currentValue <= 55.4) {
  //   theColor = 'orange';
  // } else if (currentValue > 55.4 && currentValue <= 150.4) {
  //   theColor = 'red';
  // } else if (currentValue > 150.4 && currentValue <= 250.4) {
  //   theColor = 'veryUnhealthyRed';
  // } else if (isNaN(currentValue)) {     // dealing with NaN values
  //   theColor = 'noColor';
  // } else {
  //   theColor = 'hazardousRed';
  // }

  if (currentValue <= 4) {
    theColor = 'green1';
  } else if (currentValue > 4 && currentValue <= 8) {
    theColor = 'green2';
  } else if (currentValue > 8 && currentValue <= 12) {
    theColor = 'green3';
  } else if (currentValue > 12 && currentValue <= 19.8) {
    theColor = 'yellow1';
  } else if (currentValue > 19.8 && currentValue <= 27.6) {
    theColor = 'yellow2';
  } else if (currentValue > 27.6 && currentValue <= 35.4) {
    theColor = 'yellow3';
  } else if (currentValue > 35.4 && currentValue <= 42.1) {
    theColor = 'orange1';
  } else if (currentValue > 42.1 && currentValue <= 48.7) {
    theColor = 'orange2';
  } else if (currentValue > 48.7 && currentValue <= 55.4) {
    theColor = 'orange3';
  } else if (currentValue > 55.4 && currentValue <= 150.4) {
    theColor = 'red1';
  } else if (currentValue > 150.4 && currentValue <= 250.4) {
    theColor = 'veryUnhealthyRed1';
  } else if (isNaN(currentValue)) {     // dealing with NaN values
    theColor = 'noColor';
  } else {
    theColor = 'hazardousRed1';
  }


  return theColor;
}


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


// function findNearestSensor(cornerarray, mark, callback) {
//
//   getDataFromDB(liveSensorURL_all).then((data) => {
//
//     response = data.map((d) => {
//       // return only location and ID
//       const newD = {};
//       newD.ID = d.ID;
//       newD.Latitude = d.Latitude;
//       newD.Longitude = d.Longitude;
//       newD.SensorSource = d['Sensor Source']
//
//       return newD;
//     });
//
//     var closest = findDistance(response, mark); // returns closest sensor using distance equation
//     callback(closest);
//   }).catch((err) => {
//     alert("error, request failed!");
//     console.log("Error: ", err);
//     console.warn(arguments);
//   });
// }


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

// from https://stackoverflow.com/questions/3224834/get-difference-between-2-dates-in-javascript --> by Shyam Habarakada
function dateDiffInSeconds(a, b) {
  const msPerSec = 1000;
  // Discard the time and time-zone information.
  const utc1 = Date.UTC(a.getFullYear(), a.getMonth(), a.getDate());
  const utc2 = Date.UTC(b.getFullYear(), b.getMonth(), b.getDate());

  return Math.floor((utc2 - utc1) / msPerSec);
}


function preprocessDBData(id, sensorData) {

  let sanitizedID = id.split(' ').join('_')

  let tags = sensorData["tags"][0];
  let sensorSource = tags["Sensor Source"];
  let sensorModel = tags["Sensor Model"];

  let processedSensorData = sensorData["data"].map((d) => {
    return {
      id: sanitizedID,  // make out of id "Rose Park", "Rose_Park"
      time: new Date(d.time),
      // pm25: d['pm25']
      pm25: conversionPM(d.pm25, sensorSource, sensorModel)
    };
  });

  // let processedSensorDataWithNullValues = [];
  // processedSensorData.forEach(function(item, index) {
  //   let diffInSec = dateDiffInSeconds(item[time], processedSensorData[index+1][time]);
  //
  //   if (diffInSec >= 180) {
  //     processedSensorDataWithNullValues.push(item)
  //
  //
  //     processedSensorDataWithNullValues.push(item)
  //   } else {
  //
  //   }
  // });

  // .filter((d) => {
  //   return d.pm25 === 0 || !!d.pm25; // forces NaN, null, undefined to be false, all other values to be true
  // });

  var present = false;
  for (var i = 0; i < lineArray.length; i++) {
    if (lineArray[i].id === sanitizedID) {
      present = true;
      break;
    }
  }

  if (!present) {
    console.log('not in there yet');
    var newLine = {id: sanitizedID, sensorSource: sensorSource, sensorData: processedSensorData};

    // pushes data for this specific line to an array so that there can be multiple lines updated dynamically on Click
    lineArray.push(newLine)

    drawChart();
  }
}


function drawChart() {

  var svg = d3.select("#timeline svg");
  var bounds = svg.node().getBoundingClientRect();
  var width = bounds.width;
  var height = bounds.height;

  var formatDate = d3.timeFormat('%a %m/%d/%Y');
  var formatTime = d3.timeFormat('%I:%M%p');
  // Mon Jan 29 2018 15:01:16 GMT-0700 (MST)
  // var timestampPrser = d3.timeParse'(%a %b %Y %H:%M:%S GMT-0700 (MST)');
  var s = d3.formatSpecifier("f");
  s.precision = d3.precisionFixed(0.01);
  var pmFormat = d3.format(s);

  // Scale the range of the data
  var valueline = d3.line().defined(function (d) { return d.pm25; })
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
       .attr("class", d => {
         console.log(d.id);
         return 'line-style line' + d.id; })
       .attr("id", function(d) { return 'line_' + d.id; });
       // .attr("stroke", d => lineColor(d.id)); //no return for d function, see above for example

  var focus = svg.select(".focus");
  var dateFocus = svg.select(".dateFocus");



  function mouseover(d) { //d is voronoi paths

    // iterate over the layers and get the right one
    sensLayer.eachLayer(function(layer) {
      if (layer.id === d.data.id) {
        layer.openPopup();
      }
    });

    let hoveredLine = svg.select('.line' + d.data.id);
    hoveredLine.classed("hover", true);
    // Sneaky hack to bump hoveredLine to be the last child of its parent;
    // in SVG land, this means that hoveredLine will jump to the foreground
    //.node() gets the dom element (line element), then when you append child to the parent that it already has, it bumps updated child to the front
    hoveredLine.node().parentNode.appendChild(hoveredLine.node());
    // console.log(d.data.time)
    focus.attr("transform", "translate(" + (x(d.data.time) + margin.left) + "," + (y(d.data.pm25) + margin.top) + ")"); //x and y gets coordinates from values, which we can then change with margin
    // focus.select("text").text(d.data.id);
    // focus.select("text").text(formatTime(d.data.time) + ': ' + d.data.pm25 + ' µg/m\u00B3');
    focus.select("text").text(pmFormat(d.data.pm25) + ' µg/m\u00B3');

    // date focus
    dateFocus.attr("transform", "translate(" + (x(d.data.time) + margin.left) + "," + (y(2) + margin.top) + ")");
    dateFocus.select("rect").attr('x', -1);
    dateFocus.select("rect").attr('height', 9);
    dateFocus.select("rect").attr('width', 2);

    // the date
    dateFocus.select("#focusDate").text(formatDate(d.data.time));
    dateFocus.select("#focusDate").attr('text-anchor', 'middle');
    dateFocus.select("#focusDate").attr('y', '30');

    // the time
    dateFocus.select("#focusTime").text(formatTime(d.data.time));
    dateFocus.select("#focusTime").attr('text-anchor', 'middle');
    dateFocus.select("#focusTime").attr('y', '40');

    // setContour(slcMap, d.data);
  }

  function mouseout(d) {

    // close the popup
    sensLayer.eachLayer(function(layer) {
      if (layer.id === d.data.id) {
        layer.closePopup();
      }
    });

    let hoveredLine = svg.select('.line' + d.data.id);
    hoveredLine.classed("hover", false);
    focus.attr("transform", "translate(-100,-100)");

    // clear the focus
    d3.select('#focusTime').text('')
    d3.select('#focusDate').text('')
    d3.select('.dateFocus rect').attr('x',null)
    d3.select('.dateFocus rect').attr('width',null)
    d3.select('.dateFocus rect').attr('height',null)
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


// function getGraphData(mark, aggregation) {
//   findNearestSensor(findCorners(mark.getLatLng()), mark, function (sensor) {
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

      // var aggregation = false;

function getGraphData(sensorID, sensorSource, aggregation) {

      let theRoute = '';
      let parameters = {};
      if (!aggregation) {
        theRoute = '/rawDataFrom?';
        parameters = {'id': sensorID, 'sensorSource': sensorSource, 'start': pastDate, 'end': today, 'show': 'pm25'};
      } else if (aggregation) {
        theRoute = '/processedDataFrom?';
        parameters = {'id': sensorID, 'sensorSource': sensorSource, 'start': pastDate, 'end': today, 'function': 'mean', 'functionArg': 'pm25', 'timeInterval': '5m'}; // 60m
      } else {
        console.log('hmmmm problem');
      }

      var url = generateURL(dbEndpoint, theRoute, parameters);
      console.log(url)

      getDataFromDB(url).then(data => {

          preprocessDBData(sensorID, data)

      }).catch(function(err){

          alert("error, request failed!");
          console.log("Error: ", err)
      });
    // }
  // });
}


function reGetGraphData(theID, theSensorSource, aggregation) {

  let theRoute = '';
  let parameters = {};
  if (!aggregation) {
    theRoute = '/rawDataFrom?';
    parameters = {'id': theID, 'sensorSource': theSensorSource, 'start': pastDate, 'end': today, 'show': 'pm25'};
  } else if (aggregation) {
    theRoute = '/processedDataFrom?';
    parameters = {'id': theID, 'sensorSource': theSensorSource, 'start': pastDate, 'end': today, 'function': 'mean', 'functionArg': 'pm25', 'timeInterval': '5m'}; // 60min
  } else {
    console.log('hmmmm problem');
  }

  var url = generateURL(dbEndpoint, theRoute, parameters);
  console.log(url)
  getDataFromDB(url).then(data => {

      preprocessDBData(theID, data)

  }).catch(function(err){

      $("#errorInformation").html(err['message'])
      // alert("error, request failed!");
      console.log("Error: ", err)
  });

}

// var markr = null;

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


function populateGraph() {

  // unclick the sensor type legend
  if (currentlySelectedDataSource != 'none') {
    d3.select('.clickedLegendElement').classed('clickedLegendElement', false)
    // remove notPartOfGroup class
    // remove colored-border-selected class
    d3.select('#SLC-map').selectAll('.dot:not(noColor)').classed('notPartOfGroup', false);
    d3.select('#SLC-map').selectAll('.dot:not(noColor)').classed('partOfGroup-border', false);
  }


  if (d3.select(this._icon).classed('sensor-selected')) {
    // if dot already selected
    let clickedDotID = this.id.split(' ').join('_');
    // d3.select("#line_" + clickedDotID).remove();
    lineArray = lineArray.filter(line => line.id != clickedDotID);
    drawChart();
    d3.select(this._icon).classed('sensor-selected', false);

  } else {
    // only add the timeline if dot has usable data
    if (!d3.select(this._icon).classed('noColor')) {
      d3.select(this._icon).classed('sensor-selected', true);

      let aggregation = getAggregation(whichTimeRangeToShow);

      // get the sensor source

      if (d3.select(this._icon).attr("class").split(" ").includes("airu")) {
        getGraphData(this.id, "airu", aggregation);
      } else {
        getGraphData(this.id, "Purple Air", aggregation);
      }


    }
  }
}


function clearData(changingTimeRange) {
  // lineArray.forEach( // TODO clear the markers from the map )
  lineArray = []; //this empties line array so that new lines can now be added
  d3.selectAll("#lines").html('');  // in theory, we should just call drawChart again
  d3.selectAll(".voronoi").html('');

  if (!changingTimeRange) {
    d3.selectAll('.dot').classed('sensor-selected', false);
  }

  // clear the focus
  d3.select('#focusTime').text('');
  d3.select('#focusDate').text('');
  d3.select('.dateFocus rect').attr('x',null);
  d3.select('.dateFocus rect').attr('width',null);
  d3.select('.dateFocus rect').attr('height',null);

  // reset the search box field
  document.getElementById('sensorDataSearch').value = '';
  document.getElementById('errorInformation').textContent = ''

  // remove the dblclick markers
  sensLayer.eachLayer(function(layer) {
    if (layer.id.split('_')[0] === 'personalMarker') {
      slcMap.removeLayer(layer);
      sensLayer.removeLayer(layer);
    }
  });
}


/* converts pm2.5 purpleAir to pm2.5 to federal reference method in microgram/m^3 so that the data is "consistent"
only used when data is from purpleAir sensors. There are two different kinds of sensors, thus two different conversions
for sensors pms1003:
PM2.5,TEOM =−54.22405ln(0.98138−0.00772PM2.5,PMS1003)
for sensors pms5003:
PM2.5,TEOM =−64.48285ln(0.97176−0.01008PM2.5,PMS5003)
*/
function conversionPM(pm, sensorSource, sensorModel) {

  var pmv = null;

  if (pm != null) {
    // if pm is null keep it null
    if (sensorSource != 'airu' ) {

      let model = null;
      if (sensorModel != null) {
        model = sensorModel.split('+')[0];
      }

      // var pmv = 0;
      if (model === 'PMS5003') {
        // console.log('PMS5003')
        // pmv = (-1) * 64.48285 * Math.log(0.97176 - (0.01008 * pm));
        // pmv = 0.7778*pm + 2.6536; // until October 10, 2018

        // pmv = (0.432805631 * pm) + 3.316987; // wildfire
        pmv = (0.713235898 * pm) + 1.032516; // winter

      } else if (model === 'PMS1003') {
        // console.log('PMS1003')
        // pmv = (-1) * 54.22405 * Math.log(0.98138 - (0.00772 * pm));
        // pmv = 0.5431*pm + 1.0607; // until October 10, 2018

        // pmv = (0.418860234 * pm) + 4.630728956; // wildfire
        pmv = (0.574723564 * pm) + 2.205862689; //  winter
      } else {
        pmv = pm;
      }
    } else {
      // console.log(sensorModel + ' no model?');
      // airu
      // pmv = pm;

      // airu calibration
      // pmv = 0.8582*pm + 1.1644; // until October 10, 2018
      // pmv = (0.448169438 * pm) + 5.885118729; // wildfire
      pmv = (0.460549385 * pm) + 3.343513586; // winter
    }
  }

  return pmv;
}


function showCoordinates(e) {
  alert(e.latlng);
}


function createNewMarker(location) {

  var clickLocation = location.latlng;
  console.log(clickLocation);

  // creating the ID for the marker
  var markerID = latestGeneratedID + 1;
  latestGeneratedID = markerID;
  markerID = 'personalMarker_' + markerID;


  // create Dot
  var randomClickMarker = [{'ID': markerID, 'Sensor Source': 'sensorLayerRandomMarker', 'Latitude': String(clickLocation['lat']), 'Longitude': String(clickLocation['lng'])}]
  sensorLayerRandomMarker(randomClickMarker)


  var estimatesForLocationURL = generateURL(dbEndpoint, '/getEstimatesForLocation', {"location": {'lat': clickLocation['lat'], 'lng': clickLocation['lng']}, 'start': pastDate, 'end': today})

  getDataFromDB(estimatesForLocationURL).then(data => {

    console.log(data);
    // adding the 4 selected corner points to do bilinear interpolation
    // cornerMarkers = [data['leftBottomCorner'], data['leftTopCorner'], data['rightBottomCorner'], data['rightTopCorner']]
    //
    // corners = cornerMarkers.map(function(aMarker) {
    //   return {'Latitude': String(aMarker['lat']), 'Longitude': String(aMarker['lng']), 'Sensor Source': 'airu', 'pm25': 150}
    // })

    // sensorLayerDebugging(corners)

    // parse the incoming bilinerar interpolated data
    var processedData = data.map((d) => {
      return {
        id: markerID,
        time: new Date(d.time),
        pm25: d.pm25,
        contour: d.contour          // DO I NEED THIS TODO
      };
    }).filter((d) => {
      return d.pm25 === 0 || !!d.pm25; // forces NaN, null, undefined to be false, all other values to be true
    });

    // var newLine = {id: id, sensorSource: sensorSource, sensorData: processedSensorData};
    var newLine = {id: markerID, sensorSource: 'sensorLayerRandomMarker', sensorData: processedData};

    // pushes data for this specific line to an array so that there can be multiple lines updated dynamically on Click
    lineArray.push(newLine)

    drawChart();

    // return d

  }).catch((err) => {
    alert('error, request failed!');
    console.log('Error: ', err)
  });
}


function flipMapDataVis() {

  if (showSensors) {
    showSensors = false;

    // allow marker creation contextual menu
    slcMap.contextmenu.enable();

    // theMap.removeLayer(sensLayer);
    sensLayer.eachLayer(function(aLayer) {
      theMap.removeLayer(aLayer);
      sensLayer.removeLayer(aLayer);
    });

  } else {
    showSensors = true;

    slcMap.contextmenu.disable();

    clearMapSVG();

  }

  showMapDataVis();
}


function clearMapSVG() {
  var mapSVG = d3.select("#SLC-map").select("svg.leaflet-zoom-animated");
  mapSVG.select("g").selectAll('path').remove();
}


function hideSlider() {
  d3.select("#slider").classed('hide', true);
}


function showSlider() {
  d3.select("#slider").classed('hide', false);
}
