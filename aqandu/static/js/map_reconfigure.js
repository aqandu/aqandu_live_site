// Set global variables
const sensLayer = L.layerGroup();
const epaColors = ['green', 'yellow', 'orange', 'red', 'veryUnhealthyRed', 'hazardousRed', 'noColor'];
const margin = {
  top: 10,
  right: 50,
  bottom: 40,
  left: 50,
};
const slcMap = L.map('SLC-map', {
  center: [40.748808, -111.8896],
  zoom: 13,
  contextmenu: true,
  contextmenuWidth: 140,
  contextmenuItems: [{
    text: 'Create Marker',
    callback: createNewMarker
  }]
});
const liveSensorURL_all = generateURL('/liveSensors', { 'type': 'all' });
const lastContourURL = generateURL('/getLatestContour');

let theMap;
let whichTimeRangeToShow = 1;
let currentlySelectedDataSource = 'none';
let latestGeneratedID = -1;
let dotsUpdateID;
let contourUpdateID;
let showSensors = true;
let lineArray = [];
let theContours = [];
let liveSensors = [];

// Set dates for timeline and for ajax calls
const todaysDate = new Date();
let pastDate = new Date(todaysDate - whichTimeRangeToShow * 86400000);

// Timeline axis definitions
let x = d3.scaleTime().domain([pastDate, todaysDate]);
const y = d3.scaleLinear().domain([0.0, 150.0]);

// function run when page has finished loading all DOM elements and they are ready to use
$(function () {
  startTheWholePage()
});

function startTheWholePage() {
  // TODO: Find a better place for this
  // sets the from date for the timeline when the radio button is changed
  $('#timelineControls input[type=radio]').on('change', function () {
    whichTimeRangeToShow = parseInt($(`[name="timeRange"]:checked`).val());

    // Update the pastDate and update the timeline axis
    pastDate = new Date(todaysDate - whichTimeRangeToShow * 86400000);
    x = d3.scaleTime().domain([pastDate, todaysDate]);

    clearData(true)

    // TODO: Fix how the data is retrieve and pushed to the lineArray
    lineArray.forEach((d) => {
      lineArray.splice(lineArray.indexOf(d), 1);
      getGraphData(d.id, d.sensorSource, getAggregation(whichTimeRangeToShow));
    });

    // If not showSensors, get historic contour data
    if (!showSensors) {
      getContourData();
    }

    setUpTimeline();
  });

  setUpTimeline();
  window.onresize = setUpTimeline;

  theMap = setupMap();
  sensLayer.addTo(theMap);
  
  // shows either the sensors or the contours
  showMapDataVis();

  // preventing click on timeline to generate map event (such as creating dot for getting AQ)
  const timelineDiv = L.DomUtil.get('timeline');
  L.DomEvent.disableClickPropagation(timelineDiv);
  L.DomEvent.on(timelineDiv, 'mousewheel', L.DomEvent.stopPropagation);

  const legendDiv = L.DomUtil.get('legend');
  L.DomEvent.disableClickPropagation(legendDiv);
  L.DomEvent.on(legendDiv, 'mousewheel', L.DomEvent.stopPropagation);

  const reappearingButtonDiv = L.DomUtil.get('openLegendButton');
  L.DomEvent.disableClickPropagation(reappearingButtonDiv);
  L.DomEvent.on(reappearingButtonDiv, 'mousewheel', L.DomEvent.stopPropagation);

  $('#openTimelineControlButton').hide();
};

// Render the map visualization
function showMapDataVis() {
  // If showSensors is true show only sensor, not the contours
  if (showSensors) {
    clearInterval(contourUpdateID)

    // Get and set the last sensor data
    drawSensorOnMap();

    hideSlider();
  } else {
    // If showSensors is false show only contours, not the sensors
    clearInterval(dotsUpdateID)

    getContourData();

    // Get and set the last contour
    getDataFromDB(lastContourURL).then(data => {
      setContour(slcMap, data);
    }).catch(function (err) {
      console.error('Error: ', err)
    });

    showSlider();

    // Update the vis every 5 minutes
    contourUpdateID = setInterval('updateContour()', 300000);
  }
}

function getAggregation(timeRange) {
  return timeRange !== 1
}

function getClosest(aDate, contourArray) {
  if (aDate < new Date(contourArray[0].time)) {
    return [contourArray[0], contourArray[0]];
  } else if (aDate > new Date(contourArray[contourArray.length - 1].time)) {
    return [contourArray[contourArray.length - 1], contourArray[contourArray.length - 1]];
  } else {
    // contourArray is sorted ascending in time
    var previousElement;
    for (let element of contourArray) {
      if (aDate < new Date(element.time)) {
        return [previousElement, element];
      }
      previousElement = element;
    }
  }
}

// Set up the timeline view
function setUpTimeline() {
  // TIMELINE UI

  // Add the submit event
  $('#sensorDataSearchForm').on('submit', function (e) {
    e.preventDefault();  //prevent form from submitting
    document.getElementById('errorInformation').textContent = ''
    let data = $('#sensorDataSearchForm :input').serializeArray();

    let anAggregation = getAggregation(whichTimeRangeToShow);
    getGraphData(data[0].value, 'AirU', anAggregation);

    // If the sensor is visible on the map, mark it as selected
    sensLayer.eachLayer(function (layer) {
      if (layer.id === data[0].value) {
        d3.select(layer._icon).classed('sensor-selected', true)
      }
    });
  });

  // TIMELINE

  const timelineDIV = d3.select('#timeline');
  const bounds = timelineDIV.node().getBoundingClientRect();
  const svgWidth = bounds.width;
  const svgHeight = bounds.height;
  const width = svgWidth - margin.left - margin.right;
  const height = svgHeight - margin.top - margin.bottom - 18;
  const svg = timelineDIV.select('svg') // Set size of svgContainer

  const formatSliderDate = d3.timeFormat('%a %d %I %p');
  const formatSliderHandler = d3.timeFormat('%a %m/%d %I:%M%p');

  x.range([0, width]);
  y.range([height, 0]);

  // adding the slider
  const slider = d3.select('#slider')
    .attr('transform', 'translate(50, 10)');

  slider.selectAll('line').remove();

  slider.append('line')
    .attr('class', 'track')
    .attr('x1', x.range()[0])
    .attr('x2', x.range()[1])
    .select(function () { return this.parentNode.appendChild(this.cloneNode(true)); })
    .attr('class', 'track-inset')
    .select(function () { return this.parentNode.appendChild(this.cloneNode(true)); })
    .attr('class', 'track-overlay')
    .call(d3.drag()
      .on('start.interrupt', function () { slider.interrupt(); })
      .on('start drag', (d) => {
        var currentDate = x.invert(d3.event.x);
        var upperAndLowerBound = getClosest(currentDate, theContours);
        var roundedDate
        if ((new Date(currentDate) - new Date(upperAndLowerBound[0].time)) >= (new Date(upperAndLowerBound[1].time) - new Date(currentDate))) {
          roundedDate = upperAndLowerBound[1]
        } else {
          roundedDate = upperAndLowerBound[0]
        }
        setContour(slcMap, roundedDate);
        sliderHandle.attr('cx', x(new Date(roundedDate.time)));
        slider.select('#contourTime').attr('transform', 'translate(' + (x(new Date(roundedDate.time)) - 50) + ',' + 18 + ')')
          .text(formatSliderHandler(new Date(roundedDate.time)));
      }));

  slider.select('.ticks').remove();

  slider.insert('g', '.track-overlay')
    .attr('class', 'ticks')
    .attr('transform', 'translate(0,' + 18 + ')')
    .selectAll('text')
    .data(x.ticks(9))
    .enter().append('text')
    .attr('x', x)
    .attr('text-anchor', 'middle')
    .text((d) => formatSliderDate(d));

  slider.select('circle').remove();
  slider.select('#contourTime').remove();

  slider.insert('text', '.track-overlay')
    .attr('id', 'contourTime');

  var sliderHandle = slider.insert('circle', '.track-overlay')
    .attr('class', 'handle')
    .attr('r', 9);

  sliderHandle.attr('cx', x(todaysDate));

  if (showSensors) {
    hideSlider();
  } else {
    showSlider();
  }

  // adding the graph
  d3.select('#graph')
    .attr('transform', 'translate(0, 20)')

  svg.attr('width', svgWidth)
    .attr('height', svgHeight);

  // the color bands
  svg.select('#colorBands').selectAll('path').remove(); // added else when resizing it would add the bands all over again

  svg.select('#colorBands').append('path')
    .attr('d', getColorBandPath(0, 12))
    .style('opacity', 0.1)
    .style('stroke', 'rgb(166, 217, 106)')
    .style('fill', 'rgb(166, 217, 106)');

  svg.select('#colorBands').append('path')
    .attr('d', getColorBandPath(12, 35.4))
    .style('opacity', 0.1)
    .style('stroke', 'rgb(255, 255, 191)')
    .style('fill', 'rgb(255, 255, 191)');

  svg.select('#colorBands').append('path')
    .attr('d', getColorBandPath(35.4, 55.4))
    .style('opacity', 0.1)
    .style('stroke', 'rgb(253, 174, 97)')
    .style('fill', 'rgb(253, 174, 97)');

  svg.select('#colorBands').append('path')
    .attr('d', getColorBandPath(55.4, 150.4))
    .style('opacity', 0.1)
    .style('stroke', 'rgb(215, 25, 28)')
    .style('fill', 'rgb(215, 25, 28)');

  // Add axes
  svg.select('.x.axis') // Add the X Axis
    .attr('transform', 'translate(' + margin.left + ',' + (margin.top + height) + ')')
    .call(d3.axisBottom(x).ticks(9));

  svg.select('.x.label')      // text label for the x axis
    .attr('class', 'timeline')
    .attr('transform', 'translate(' + (width / 2) + ' ,' + (height + margin.bottom) + ')')
    .style('text-anchor', 'middle');

  svg.select('.y.axis') // Add the Y Axis
    .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')')
    .call(d3.axisLeft(y).ticks(7));

  svg.select('.y.label')    // text label for the y axis
    .attr('class', 'timeline')
    .attr('transform', 'rotate(-90)')
    .attr('y', 0) // rotated! x is now y!
    .attr('x', 0 - (height / 2))
    .attr('dy', '1em')
    .style('text-anchor', 'middle')
    .text('PM2.5 µg/m\u00B3');

  // disable map panning on timeline
  document.getElementById('timeline').addEventListener('mouseover', function () {
    theMap.dragging.disable();
  });

  document.getElementById('timeline').addEventListener('mouseout', function () {
    theMap.dragging.enable();
  });
}

function getColorBandPath(yStart, yEnd) {
  return `M ${margin.left + x(x.domain()[0])},${margin.top + y(yStart)} 
    L ${margin.left + x(x.domain()[0])},${margin.top + y(yEnd)} 
    L ${margin.left + x(x.domain()[1])},${margin.top + y(yEnd)} 
    L ${margin.left + x(x.domain()[1])},${margin.top + y(yStart)}`;
}

// Create additional control placeholders
// https://stackoverflow.com/questions/33614912/how-to-locate-leaflet-zoom-control-in-a-desired-position
function addControlPlaceholders(map) {
  const corners = map._controlCorners;

  function createCorner(vSide, hSide) {
    var className = `leaflet-${vSide} leaflet-${hSide}`;
    corners[vSide + hSide] = L.DomUtil.create('div', className, map._controlContainer);
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
  var bottomLayer = new L.StamenTileLayer('toner');
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

  imageBounds = [[40.598850, -112.001349], [40.810476, -111.713403]];

  L.svg().addTo(slcMap);

  // disabling zooming when scrolling down the page (https://gis.stackexchange.com/questions/111887/leaflet-mouse-wheel-zoom-only-after-click-on-map)
  slcMap.scrollWheelZoom.disable();
  slcMap.on('focus', () => { slcMap.scrollWheelZoom.enable(); });
  slcMap.on('blur', () => { slcMap.scrollWheelZoom.disable(); });

  // adding new placeholders for leaflet controls
  addControlPlaceholders(slcMap);

  createCustomButton('verticalcentertopright', 'customButton', 'openLegendButton', 'fa-list', true)
  createCustomButton('verticalcenterbottomleft', 'customButton', 'openTimelineButton', 'fa-list', true);
  createCustomButton('verticalcentertopleft', 'customButton', 'changeOverlay', 'fa-layer-group', false);

  // adding the legend container
  var legendControl = L.control({ position: 'verticalcentertopright' });
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
    closeButton_a.setAttribute('href', '#');
    var createAText = document.createTextNode('X');
    closeButton_a.appendChild(createAText);
    closeButtonContainer.appendChild(closeButton_a);

    // reversed both grades and colors array
    var grades = [4, 8, 12, 20, 28, 35, 42, 49, 55, 150, 250, 350].reverse()
    var colors = ['green1', 'green2', 'green3', 'yellow1', 'yellow2', 'yellow3', 'orange1', 'orange2', 'orange3', 'red1', 'veryUnhealthyRed1', 'hazardousRed1'].reverse()

    var title = document.createElement('span');
    title.setAttribute('id', 'PM25level');
    title.setAttribute('class', 'legendTitle');
    colorLegend.appendChild(title);

    var labelContainer = L.DomUtil.create('div');
    labelContainer.setAttribute('id', 'labelContainer');
    colorLegend.appendChild(labelContainer);

    var theTitleContent = document.createTextNode('PM2.5 [µg/m\u00B3]:');
    title.appendChild(theTitleContent);

    // create colored rectangle
    var lastElement;
    colors.forEach(function (aColor, index) {
      var tmp = document.createElement('div');
      tmp.setAttribute('class', 'colorLegendLabel');

      var colorDiv = document.createElement('div');
      colorDiv.setAttribute('id', aColor);
      colorDiv.setAttribute('class', `colorBar ${aColor}`);
      tmp.appendChild(colorDiv);

      var span = document.createElement('span');
      span.setAttribute('class', 'tickLegend');
      span.setAttribute('id', 'tickLegend_' + grades[index])
      span.textContent = '\u2014 ' + grades[index];
      tmp.appendChild(span);

      lastElement = tmp;
      labelContainer.appendChild(tmp);
    })

    var lastSpan = document.createElement('span');
    lastSpan.setAttribute('class', 'tickLegend');
    lastSpan.setAttribute('id', 'tickLegend_0')
    lastSpan.textContent = '\u2014 0';
    lastElement.appendChild(lastSpan);

    var hr = L.DomUtil.create('hr', 'theHR');
    legendContainer.appendChild(hr);

    // adding data source legend
    var dataSourceLegend = L.DomUtil.create('div', 'dataSourceLegend');
    legendContainer.appendChild(dataSourceLegend);

    var d3div = d3.select(dataSourceLegend);
    var titleDataSource = d3div.append('span')
      .attr('id', 'dataSource')
      .attr('class', 'legendTitle')
      .html('Data sources:');

    var dataLabel = ['AirU', 'PurpleAir', 'DAQ'];
    var labels = d3div.selectAll('label').data(dataLabel);
    labels.exit().remove();
    var labelsEnter = labels.enter()
      .append('label')
      .attr('class', 'sensorType')

    labels = labels.merge(labelsEnter);
    labels.text(d => d);

    labels.insert('span', function () {
      return this.childNodes[0];
    })
      .classed('notSelectedLabel', true)
      .append('i')
      .attr('class', 'fas fa-circle');

    labels.append('span')
      .attr('id', d => 'numberOf_' + d);

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
          // moved from one element to another without first un-checking it
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

  $('#closeLegendButton').on('click', function () {
    $('.legend').hide();
    $('#openLegendButton').show();
  });

  $('#openLegendButton').on('click', function () {
    $('.legend').show();
    $('#openLegendButton').hide();
  });

  $('#closeTimelineControlButton').on('click', function () {
    $('#timelineControls').hide();
    $('#openTimelineControlButton').show();
  });

  $('#openTimelineControlButton').on('click', function () {
    $('#openTimelineControlButton').hide();
    $('#timelineControls').show();
  })

  $('#closeTimelineButton').on('click', function () {
    $('#timeline').hide();
    $('#openTimelineButton').show();
  });

  $('#openTimelineButton').on('click', function () {
    $('#timeline').show();
    $('#openTimelineButton').hide();
  });

  // change the overlay
  $('#changeOverlay').on('click', function () {
    flipMapDataVis();
  });

  slcMap.zoomControl.setPosition('verticalcentertopleft');
  return slcMap;
}

function createCustomButton(thePosition, buttonClass, buttonID, faIcon, hideButton) {
  var buttonControlContainer = L.control({ position: thePosition });
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
      var theContour = allContours[key];
      var aContour = theContour.path;
      aContour.level = theContour.level;
      aContour.k = theContour.k;

      contours.push(aContour);
    }
  }

  contours.sort(function (a, b) {
    return b.level - a.level;
  });

  var levelColours = ['#31a354', '#a1d99b', '#e5f5e0', '#ffffcc', '#ffeda0', '#fed976', '#feb24c', '#fd8d3c', '#fc4e2a', '#e31a1c', '#bd0026', '#800026'];
  var defaultContourColor = 'black';
  var defaultContourWidth = 1;

  var mapSVG = d3.select('#SLC-map').select('svg.leaflet-zoom-animated');
  var g = mapSVG.select('g');

  var contourPath = g.selectAll('path')
    .data(contours, function (d) { return d; });

  contourPath.style('fill', function (d, i) { return levelColours[d.level]; })
    // .style('stroke', defaultContourColor)
    // .style('stroke-width', defaultContourWidth)
    .style('opacity', 1)
    .on('mouseover', function (d) {
      d3.select(this).style('stroke', 'black');
    })
    .on('mouseout', function (d) {
      d3.select(this).style('stroke', defaultContourColor);
    });

  var contourEnter = contourPath.enter()
    .append('path')
    .style('fill', function (d, i) { return levelColours[d.level]; })
    .style('opacity', 1)
    .on('mouseover', function (d) {
      d3.select(this).style('stroke', 'black');
    })
    .on('mouseout', function (d) {
      d3.select(this).style('stroke', defaultContourColor);
    });

  contourPath.exit().remove();

  function resetView() {
    contourEnter.attr('d', function (d) {
      var pathStr = d.map(function (d1) {
        var point = theMap.latLngToLayerPoint(new L.LatLng(d1[1], d1[2]));
        return d1[0] + point.x + ',' + point.y;
      }).join('');
      return pathStr;
    });
  }

  theMap.on('zoom', resetView);

  resetView();
}

/**
 * Queries db to get the live sensors -- sensors that have data since yesterday beginning of day
 * @return {[type]} [description]
 */
function drawSensorOnMap() {

  $('#SLC-map').LoadingOverlay('show');

  getDataFromDB(liveSensorURL_all).then((data) => {
    // Standardize the PM2_5
    data.forEach((d) => d.PM2_5 = convertPM(d.PM2_5, d['SensorSource'], d['SensorModel']));
    
    // Update the number of sensors in the legend
    updateNumberOfSensors(data)

    data.forEach(createMarker);

    data.forEach(function (aSensor) {
      liveSensors.push({ 'id': aSensor.ID.split(' ').join('_'), 'sensorSource': aSensor['SensorSource'] });
    });

    $('#SLC-map').LoadingOverlay('hide');

  }).catch((err) => {
    console.error('Error: ', err)
  });
}

// layer with the marks where people clicked
function sensorLayerRandomMarker(response) {
  response.forEach(createRandomClickMarker);
}

function createMarker(markerData) {
  var dotIcon = {
    iconSize: [20, 20], // size of the icon
    iconAnchor: [10, 10], // point of the icon which will correspond to marker's location
    popupAnchor: [0, -5], // point from which the popup should open relative to the iconAnchor
    html: ''
  };

  let sensorSource = markerData['SensorSource'];

  if (markerData.Latitude !== null && markerData.Longitude !== null) {
    let classList = 'dot';
    let currentPM25 = markerData.PM2_5;

    let currentTime = new Date().getTime();
    let timeLastMeasurement = markerData.time;
    let minutesINBetween = (currentTime - timeLastMeasurement) / (1000 * 60);
    let theColor = displaySensor(sensorSource, minutesINBetween, currentPM25)
    classList = `${classList} ${theColor} `;

    // throw away the spaces in the sensor name string so we have a valid class name
    classList += sensorSource.replace(/ /g, '');
    dotIcon.className = classList;

    var mark = new L.marker(
      L.latLng(
        parseFloat(markerData.Latitude),
        parseFloat(markerData.Longitude)
      ),
      { icon: L.divIcon(dotIcon) }
    ).addTo(sensLayer);

    mark.id = markerData.ID;

    mark.bindPopup(
      L
        .popup({ closeButton: false, className: 'sensorInformationPopup' })
        .setContent(`<span class="popup">${sensorSource}: ${markerData.ID}</span>`)
    )

    mark.on('click', populateGraph)
    mark.on('mouseover', function (e) {
      this.openPopup();
    });
    mark.on('mouseout', function (e) {
      this.closePopup();
    });
  }
}

function createMarkerDebugging(markerData) {
  var dotIcon = {
    iconSize: [20, 20], // size of the icon
    iconAnchor: [10, 10], // point of the icon which will correspond to marker's location
    popupAnchor: [0, -5], // point from which the popup should open relative to the iconAnchor
    html: ''
  };

  if (markerData.Latitude !== null && markerData.Longitude !== null) {
    let classList = 'dot';
    let theColor = 'hazardousRed'

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
    iconSize: [20, 20], // size of the icon
    iconAnchor: [10, 10], // point of the icon which will correspond to marker's location
    popupAnchor: [0, -5], // point from which the popup should open relative to the iconAnchor
    html: ''
  };

  if (markerData.Latitude !== null && markerData.Longitude !== null) {
    let classList = 'dot';
    let theColor = 'dblclickOnMap';

    classList = `${classList} ${theColor} `;
    dotIcon.className = classList;

    var mark = new L.marker(
      L.latLng(
        parseFloat(markerData.Latitude),
        parseFloat(markerData.Longitude)
      ),
      { icon: L.divIcon(dotIcon) },
    ).addTo(sensLayer);

    mark.id = markerData.ID;

    mark.bindPopup(
      L
        .popup({ closeButton: false, className: 'sensorInformationPopup' })
        .setContent(`<span class="popup">${markerData.SensorSource}: ${markerData.ID}</span>`)
    );

    // set the border of created marker to selected
    d3.select(mark._icon).classed('sensor-selected', true);

    mark.on('mouseover', function (e) {
      this.openPopup();
    });

    mark.on('mouseout', function (e) {
      this.closePopup();
    });

  }
}

// get the data for the contours between start and end
function getContourData() {
  $('#SLC-map').LoadingOverlay('show');

  // get difference between the dates in displays
  var diffDays = Math.ceil((todaysDate - pastDate) / (1000 * 60 * 60 * 24));

  // if more than 1 day, load each day separately
  let endDate = todaysDate;
  var listOfPromises = [];

  for (let i = 1; i <= diffDays; i++) {
    // Get the startDate
    let startDate = new Date(todaysDate - i * 86400000);

    // Get the URL
    let contoursURL = generateURL('/contours', { 'start': formatDateTime(startDate), 'end': formatDateTime(endDate) });

    // Make a promise and push it to the list of promises
    listOfPromises.push(getDataFromDB(contoursURL));

    // Update endDate
    endDate = startDate
  }

  Promise.all(listOfPromises).then(result => {
    theContours = result.flat();
    theContours.sort((a, b) => (new Date(a.time) > new Date(b.time)) ? 1 : ((new Date(b.time) > new Date(a.time)) ? -1 : 0));
    $('#SLC-map').LoadingOverlay('hide');
  })
}

function updateDots() {
  getDataFromDB(liveSensorURL_all).then((data) => {
    // Standardize the PM2_5 and create markers
    data.forEach((d) => d.PM2_5 = convertPM(d.PM2_5, d.SensorSource, d.SensorModel));
    data.forEach(createMarker);

    // Update the number of sensors in the legend
    updateNumberOfSensors(data)

    sensLayer.eachLayer(layer => {
      // Find the updated value for a specific sensor id
      const updatedValue = data.find(latestValue => {
        return latestValue.ID === layer.id
      })

      if (updatedValue) {
        const currentTime = new Date().getTime()
        const timeLastMeasurement = new Date(updatedValue.time).getTime();
        const minutesINBetween = (currentTime - timeLastMeasurement) / (1000 * 60);

        const theColor = displaySensor(updatedValue.SensorSource, minutesINBetween, updatedValue.PM2_5)
        $(layer._icon).removeClass(epaColors.join(' '))
        $(layer._icon).addClass(theColor)
      }
    });
  }).catch((err) => {
    console.error('Error: ', err);
    console.warn(arguments);
  });
}

// Update the contours
function updateContour() {
  getDataFromDB(lastContourURL).then(data => {
    setContour(slcMap, data);
  }).catch(function (err) {
    console.error('Error when updating the contour: ', err)
  });
}


function displaySensor(aSensorSource, minutesPassedSinceLastDataValue, aCurrentValue) {
  let theColor = 'noColor';
  let calculatedColor = getColor(aCurrentValue);

  if ((aSensorSource === 'AirU' || aSensorSource === 'PurpleAir') && minutesPassedSinceLastDataValue <= 10.0) {
      theColor = calculatedColor;
  } else if (aSensorSource === 'DAQ' && minutesPassedSinceLastDataValue <= 180.0) {
      theColor = calculatedColor;
  } else {
    console.error('displaySensor: forgotten a case!!');
  }

  // TODO: Remove this line, currently hacks to last value regardless of timestamp
  theColor = calculatedColor;
  return theColor;
}

function getColor(currentValue) {
  let theColor;

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

function preprocessDBData(id, sensorData) {
  let sanitizedID = id.split(' ').join('_') // Make out of id 'Rose Park', 'Rose_Park'
  let tags = sensorData['tags'][0];
  let sensorSource = tags['SensorSource'];
  let sensorModel = tags['SensorModel'];

  let processedSensorData = sensorData['data'].map((d) => {
    return {
      id: sanitizedID,  
      time: new Date(d.time),
      PM2_5: convertPM(d.PM2_5, sensorSource, sensorModel)
    };
  });

  let present = false;
  for (var i = 0; i < lineArray.length; i++) {
    if (lineArray[i].id === sanitizedID) {
      present = true;
      break;
    }
  }

  if (!present) {
    var newLine = { id: sanitizedID, sensorSource: sensorSource, sensorData: processedSensorData };

    // pushes data for this specific line to an array so that there can be multiple lines updated dynamically on Click
    lineArray.push(newLine)

    drawChart();
  }
}

function drawChart() {

  var svg = d3.select('#timeline svg');
  var bounds = svg.node().getBoundingClientRect();
  var width = bounds.width;
  var height = bounds.height;

  var formatDate = d3.timeFormat('%a %m/%d/%Y');
  var formatTime = d3.timeFormat('%I:%M%p');
  var s = d3.formatSpecifier('f');
  s.precision = d3.precisionFixed(0.01);
  var pmFormat = d3.format(s);

  // Scale the range of the data
  var valueLine = d3.line().defined(function (d) { return d.PM2_5; })
    .x(function (d) {
      return x(d.time);
    })
    .y(function (d) {
      return y(d.PM2_5);
    });

  // Mike Bostock's code
  var voronoi = d3.voronoi()
    .x(function (d) { return x(d.time); })
    .y(function (d) { return y(d.PM2_5); })
    .extent([[-margin.left, -margin.top], [width + margin.right, height + margin.bottom]]);

  // adds the svg attributes to container
  var lines = svg.select('#lines').selectAll('path')
    .data(lineArray, function (d) {
      return d.id;
    }); //any path in svg is selected then assigns the data from the array

  lines.exit().remove(); //remove any paths that have been removed from the array that no longer associated data

  lines.enter().append('path') // looks at data not associated with path and then pairs it
    .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')')
    .attr('d', d => { return valueLine(d.sensorData); })
    .attr('class', d => 'line-style line' + d.id)
    .attr('id', function (d) { return 'line_' + d.id; });

  var focus = svg.select('.focus');
  var dateFocus = svg.select('.dateFocus');

  function mouseover(d) { //d is voronoi paths
    // iterate over the layers and get the right one
    sensLayer.eachLayer(function (layer) {
      if (layer.id === d.data.id) {
        layer.openPopup();
      }
    });

    let hoveredLine = svg.select('.line' + d.data.id);
    hoveredLine.classed('hover', true);
    // Sneaky hack to bump hoveredLine to be the last child of its parent;
    // in SVG land, this means that hoveredLine will jump to the foreground
    //.node() gets the dom element (line element), then when you append child to the parent that it already has, it bumps updated child to the front
    hoveredLine.node().parentNode.appendChild(hoveredLine.node());
    focus.attr('transform', 'translate(' + (x(d.data.time) + margin.left) + ',' + (y(d.data.PM2_5) + margin.top) + ')'); //x and y gets coordinates from values, which we can then change with margin
    focus.select('text').text(pmFormat(d.data.PM2_5) + ' µg/m\u00B3');

    // date focus
    dateFocus.attr('transform', 'translate(' + (x(d.data.time) + margin.left) + ',' + (y(2) + margin.top) + ')');
    dateFocus.select('rect').attr('x', -1);
    dateFocus.select('rect').attr('height', 9);
    dateFocus.select('rect').attr('width', 2);

    // the date
    dateFocus.select('#focusDate').text(formatDate(d.data.time));
    dateFocus.select('#focusDate').attr('text-anchor', 'middle');
    dateFocus.select('#focusDate').attr('y', '30');

    // the time
    dateFocus.select('#focusTime').text(formatTime(d.data.time));
    dateFocus.select('#focusTime').attr('text-anchor', 'middle');
    dateFocus.select('#focusTime').attr('y', '40');
  }

  function mouseout(d) {

    // close the popup
    sensLayer.eachLayer(function (layer) {
      if (layer.id === d.data.id) {
        layer.closePopup();
      }
    });

    let hoveredLine = svg.select('.line' + d.data.id);
    hoveredLine.classed('hover', false);
    focus.attr('transform', 'translate(-100,-100)');

    // clear the focus
    d3.select('#focusTime').text('')
    d3.select('#focusDate').text('')
    d3.select('.dateFocus rect').attr('x', null)
    d3.select('.dateFocus rect').attr('width', null)
    d3.select('.dateFocus rect').attr('height', null)
  }

  var listOfLists = lineArray.map((d) => d.sensorData);
  var listOfPoints = d3.merge(listOfLists);
  var voronoiPolygons = voronoi.polygons(listOfPoints);

  var voronoiGroup = svg.select('.voronoi')
    .attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');

  var voronoiPaths = voronoiGroup.selectAll('path')
    .data(voronoiPolygons);

  voronoiPaths.exit().remove();

  var voronoiPathsEnter = voronoiPaths.enter().append('path');

  voronoiPaths = voronoiPaths.merge(voronoiPathsEnter);

  voronoiPaths.attr('d', function (d) { return d ? 'M' + d.join('L') + 'Z' : null; })
    .on('mouseover', mouseover) //I need to add a name for this
    .on('mouseout', mouseout);

  // adds the svg attributes to container
  let labels = svg.select('#legend').selectAll('text').data(lineArray, d => d.id); //any path in svg is selected then assigns the data from the array
  labels.exit().remove(); //remove any paths that have been removed from the array that no longer associated data
}

function getGraphData(sensorID, sensorSource, aggregation) {
  let route = aggregation ? '/timeAggregatedDataFrom' : '/rawDataFrom';
  let parameters = aggregation ? 
    { 'id': sensorID, 'sensorSource': sensorSource, 'start': formatDateTime(pastDate), 'end': formatDateTime(todaysDate), 'function': 'mean', 'timeInterval': '5' }
    : { 'id': sensorID, 'sensorSource': sensorSource, 'start': formatDateTime(pastDate), 'end': formatDateTime(todaysDate)};

  var url = generateURL(route, parameters);

  getDataFromDB(url)
    .then((data) => preprocessDBData(sensorID, data))
    .catch((err) => console.error('Error: ', err));
}

function populateGraph() {
  // Un-click the sensor source legend and update the dot highlights (AirU, PurpleAir, etc.)
  if (currentlySelectedDataSource != 'none') {
    d3.select('.clickedLegendElement').classed('clickedLegendElement', false) // TODO: Fix bug with dot still showing in legend
    d3.select('#SLC-map').selectAll('.dot:not(noColor)').classed('notPartOfGroup', false);
    d3.select('#SLC-map').selectAll('.dot:not(noColor)').classed('partOfGroup-border', false);
  }

  if (d3.select(this._icon).classed('sensor-selected')) {
    // If the dot is already selected get the id and remove from the line array
    let clickedDotID = this.id.split(' ').join('_');
    lineArray = lineArray.filter(line => line.id != clickedDotID);

    // Re-render the line charts and set the dot to unselected
    drawChart();
    d3.select(this._icon).classed('sensor-selected', false);

  } else {
    // Else the dot is not already selected. Check it has a color (thus usable data) before continuing
    if (!d3.select(this._icon).classed('noColor')) {
      // Set the dot to selected
      d3.select(this._icon).classed('sensor-selected', true);

      // Check whether we need to aggregate the data
      let aggregation = getAggregation(whichTimeRangeToShow);

      // Get the sensorType and pass it through to getGraphData
      const dotClasses = d3.select(this._icon).attr('class').split(' ')
      const sensorType = dotClasses.includes('AirU') ? 'AirU' :
        dotClasses.includes('PurpleAir') ? 'PurpleAir' : 
        'DAQ';
      getGraphData(this.id, sensorType, aggregation);
    }
  }
}

function clearData(changingTimeRange) {
  d3.selectAll('#lines').html('');  // in theory, we should just call drawChart again
  d3.selectAll('.voronoi').html('');

  if (!changingTimeRange) {
    d3.selectAll('.dot').classed('sensor-selected', false);
  }

  // clear the focus
  d3.select('#focusTime').text('');
  d3.select('#focusDate').text('');
  d3.select('.dateFocus rect').attr('x', null);
  d3.select('.dateFocus rect').attr('width', null);
  d3.select('.dateFocus rect').attr('height', null);

  // reset the search box field
  document.getElementById('sensorDataSearch').value = '';
  document.getElementById('errorInformation').textContent = ''

  // remove the dblclick markers
  sensLayer.eachLayer(function (layer) {
    if (layer.id.split('_')[0] === 'personalMarker') {
      slcMap.removeLayer(layer);
      sensLayer.removeLayer(layer);
    }
  });
}

/* converts pm2.5 purpleAir to pm2.5 to federal reference method in microgram/m^3 so that the data is 'consistent'
only used when data is from purpleAir sensors. There are two different kinds of sensors, thus two different conversions
for sensors pms1003:
PM2.5,TEOM =−54.22405ln(0.98138−0.00772PM2.5,PMS1003)
for sensors pms5003:
PM2.5,TEOM =−64.48285ln(0.97176−0.01008PM2.5,PMS5003)
*/
function convertPM(pm, sensorSource, sensorModel) {
  // Bail out if pm is null
  if (pm === null) {
    return pm
  }

  let pmv = null;
      let model = null;
  if (sensorModel !== null) {
        model = sensorModel.split('+')[0];
      }

  correction_factors = {
    AirU: {
      b_one: 0.657,
      intercept: 3.075,
    },
    other: {
      PMS5003: {
        b_one: 0.617,
        intercept: 2.154,
      },
      PMS1003: {
        b_one: 0.641,
        intercept: 0.213,
      }
    }
  }

  if (sensorSource === 'AirU') {
    return (correction_factors.AirU.b_one * pm) + correction_factors.AirU.intercept
  } else if (model === 'PMS5003' || model === 'PMS1003') {
    return (correction_factors.other[model].b_one * pm) + correction_factors.other[model].intercept
  }

  if (pm != null) {
    if (sensorSource != 'AirU') {
      if (model === 'PMS5003') {
        // pmv = (-1) * 64.48285 * Math.log(0.97176 - (0.01008 * pm));
        // pmv = 0.7778*pm + 2.6536; // until October 10, 2018
        // pmv = (0.432805631 * pm) + 3.316987; // wildfire

      } else if (model === 'PMS1003') {
        // pmv = (-1) * 54.22405 * Math.log(0.98138 - (0.00772 * pm));
        // pmv = 0.5431*pm + 1.0607; // until October 10, 2018
        // pmv = (0.418860234 * pm) + 4.630728956; // wildfire
      } else {
        pmv = pm;
      }
    } else {
      // AirU calibration
      // pmv = 0.8582*pm + 1.1644; // until October 10, 2018
      // pmv = (0.448169438 * pm) + 5.885118729; // wildfire
    }
  }

  return pmv;
}

function createNewMarker(location) {
  // creating the ID for the marker
  let markerID = latestGeneratedID + 1;
  latestGeneratedID = markerID;
  markerID = 'personalMarker_' + markerID;

  // create Dot
  const randomClickMarker = [{ 'ID': markerID, 'SensorSource': 'sensorLayerRandomMarker', 'Latitude': String(location.latlng.lat), 'Longitude': String(location.latlng.lng) }]
  sensorLayerRandomMarker(randomClickMarker)

  const predictionsForLocationURL = generateURL('/getEstimatesForLocation', { 'location': { 'lat':  String(location.latlng.lat), 'lon': String(location.latlng.lng) }, 'start': formatDateTime(pastDate), 'end': formatDateTime(todaysDate), 'estimatesrate': 1 })

  getDataFromDB(predictionsForLocationURL).then(data => {
    // parse the incoming bilinear interpolated data
    var processedData = data.map((d) => {
      return {
        id: markerID,
        time: new Date(d.datetime),
        PM2_5: d.PM2_5,
        contour: d.contour          // DO I NEED THIS TODO
      };
    }).filter((d) => {
      return d.PM2_5 === 0 || !!d.PM2_5; // forces NaN, null, undefined to be false, all other values to be true
    });

    var newLine = { id: markerID, sensorSource: 'sensorLayerRandomMarker', sensorData: processedData };

    // pushes data for this specific line to an array so that there can be multiple lines updated dynamically on Click
    lineArray.push(newLine)

    drawChart();
  }).catch((err) => {
    console.error('Error: ', err)
  });
}


function flipMapDataVis() {
  if (showSensors) {

    // Only allow marker creation when showing dots
    slcMap.contextmenu.enable();

    sensLayer.eachLayer(function (aLayer) {
      theMap.removeLayer(aLayer);
      sensLayer.removeLayer(aLayer);
    });

  } else {
    slcMap.contextmenu.disable();
    clearMapSVG();
  }
  showSensors = !showSensors
  showMapDataVis();
}

function clearMapSVG() {
  var mapSVG = d3.select('#SLC-map').select('svg.leaflet-zoom-animated');
  mapSVG.select('g').selectAll('path').remove();
}

function hideSlider() {
  d3.select('#slider').classed('hide', true);
}

function showSlider() {
  d3.select('#slider').classed('hide', false);
}

function formatDateTime(dateTime) {
  return `${dateTime.toISOString().substr(0, 19)}Z`
}

function updateNumberOfSensors(data) {
  const numberOfPurpleAir = data.filter(sensor => sensor['SensorSource'] === 'PurpleAir').length;
  $('#numberOf_PurpleAir').html(numberOfPurpleAir);

  const numberOfAirU = data.filter(sensor => sensor['SensorSource'] === 'AirU').length;
  $('#numberOf_AirU').html(numberOfAirU);

  const numberOfDAQ = data.filter(sensor => sensor['SensorSource'] === 'DAQ').length;
  $('#numberOf_DAQ').html(numberOfDAQ);
}
