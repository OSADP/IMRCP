import './jquery/jquery.segmentSelector.js';
import './jquery/jquery.datetimepicker.full.js';
import './jquery/jquery.ui.labeledslider.min.js';
import './jquery/jquery.divStack.js';
import './jquery/jquery.notifications.dialog.js';

import {minutesToHHmm} from './common.js';
import {setupObstypeLegendDivs, buildPaneSourceInputs} from './map-legends.js'
import {FeatureDetailsWindow} from './map-feature-details-window.js';
import {saveSettings, loadSettings} from './map-settings.js';
import {ReportSelectionManager} from './map-report-window.js';
import {addSourceAndLayers, removeSourceAndLayers, pointToPaddedBounds, featureInSources, iconDivFromSprite} from './map-util.js';


const mapMouseMoveHandler = (sources, spriteDef) => {
  let lastHoverFeature;
  const markerDivHtml = iconDivFromSprite(spriteDef['detector-white']);
  const hoverablePointLayers = new Set(["Traffic Detectors", "Stormwatch Stations", "AHPS Stations"]);
  return ({target:map, point}) => {

    const features = map.queryRenderedFeatures(pointToPaddedBounds(point))
      .filter(featureInSources(sources));

    const currentSelectedFeature = features.length === 0 ? null : features[0];


    if (currentSelectedFeature)
      $(map.getCanvas()).addClass("clickable");
    else
      $(map.getCanvas()).removeClass("clickable");


    if (lastHoverFeature && (!currentSelectedFeature || lastHoverFeature.id !== currentSelectedFeature.id))
    {
      // if (!newHoverIds.has(f.id)) -- this doesn't work while we are only highlighting the first layer
      if (sources.get(lastHoverFeature.source).type === 'symbol')
      {
        if (lastHoverFeature.marker)
          lastHoverFeature.marker.remove();
      }
      else if (map.getSource(lastHoverFeature.source)) // don't change feature state if feature's source has been removed
        map.setFeatureState(lastHoverFeature, {hover: false});
    }

    if (currentSelectedFeature && (!lastHoverFeature || lastHoverFeature.id !== currentSelectedFeature.id))
    {
      const featureSource = sources.get(currentSelectedFeature.source);
      if (featureSource.type === 'symbol')
      {
        if (hoverablePointLayers.has(currentSelectedFeature.source))
        {
          currentSelectedFeature.marker =
            new mapboxgl.Marker($(markerDivHtml).get(0))
            .setLngLat(currentSelectedFeature.geometry.coordinates)
            .addTo(map);
        }
      }
      else if (featureSource.type === 'line')
        map.setFeatureState(currentSelectedFeature, {hover: true});

      lastHoverFeature = currentSelectedFeature;
    }
    else if (!currentSelectedFeature)
      lastHoverFeature = null;
  };
};

const buildSourceMap = (sourceData, spriteDef) => {
  const sources = new Map();
  Object.entries(sourceData).forEach(([sourceId, {group, source, layers, type}]) => {
    const legendElements = [];

    const fullLayerDefinitions = [];
    const defaultLayer = layers[0];
    for (let layer of layers)
    {
      const fullLayer = $.extend(true, {type, source: sourceId}, defaultLayer, layer);
      fullLayerDefinitions.push(fullLayer);

      const label = layer.metadata['imrcp-range'];
      switch (type)
      {
        case 'fill':
        case 'line':
          legendElements.push({label, style: layer.paint[ type + '-color']});
          break;
        case 'symbol':
          const img = layer.layout['icon-image'];
          if (img && spriteDef[img])
            legendElements.push({label, style: spriteDef[img]});
          break;
      }

      if (type === 'line')
	fullLayer.paint['line-width'] = ["let", "extra",
		["case", ["boolean", ["feature-state", "hover"], false], 2, ["boolean", ["feature-state", "clicked"], false], 2, 0],
		 ["interpolate", ["exponential", 2], ["zoom"], 10.0, ["+", ["var", "extra"], 1.0], 16.0, ["+", ["var", "extra"], 6.0]]];
//	["case",
//	["boolean", ["feature-state", "hover"], false], 4,
//	["boolean", ["feature-state", "clicked"], false], 4,
//	1];
    }

    const fullSource = {group, type, legendElements, layers: fullLayerDefinitions, id: sourceId, mapboxSource: Object.assign({}, source, {tiles: source.tiles})};

    sources.set(sourceId, fullSource);
  });
  return sources;
};


const refreshMap = (map, sources) => {
  sources.forEach(source => {
    if (map.getSource(source.id))
    {
      removeSourceAndLayers(map, source);
      addSourceAndLayers(map, source);
    }
  });
};

const scheduleEvenMinuteTimeout = callback => {
  //set the refresh method to run at the start of the next minute, and
  //then set it to run every minute after that.
  var now = moment();
  var currentMillis = now.valueOf();
  now.seconds(0).milliseconds(0).add(1, 'minute');
  setTimeout(function ()
  {
    callback();
    setInterval(callback, 60000);

  }, now.valueOf() - currentMillis);
};

const layerCheckHandler = (map, sources) => ({target}) => {
    const jqEl = $(target);
    const sourceId = jqEl.val();
    const source = sources.get(sourceId);
    if (jqEl.prop('checked'))
      addSourceAndLayers(map, source);
    else
      removeSourceAndLayers(map, source);
  };


const displayZoom = ({target}) => $('#txtLocationLookup').text(('' + target.getZoom()).substring(0, 3));
const  displaySelectedTime = moment => {
  $('#slider > .ui-slider-handle').text(moment.format("h:mm a"));
  $('#btnTime').val(moment.format("YYYY/MM/DD"));
};

const setRefCookie = moment => document.cookie = `rtime=${moment.valueOf()};path=/`;
const setTimeCookie = moment => document.cookie = `ttime=${moment.valueOf()};path=/`;

const sourceLayersPromise = $.getJSON('mapbox/sourcelayers.json').promise();
const spriteDefinitionPromise = $.getJSON('images/sprite.json').promise();
const obstypesPromise = $.getJSON('obstypes.json').promise();


async function initialize()
{
  const settings = await loadSettings;
  const {zoom, center: {lat, lng}} = settings.map;
  const map = new mapboxgl.Map({container: 'mapid', style: 'mapbox://styles/mapbox/light-v10', attributionControl: false,
    minZoom:4, maxZoom: 16, center: [lng, lat], zoom});

// disable map rotation using right click + drag
  map.dragRotate.disable();
// disable map rotation using touch rotation gesture
  map.touchZoomRotate.disableRotation();
  map.addControl(new mapboxgl.NavigationControl({showCompass: false}));

  const sources = buildSourceMap(await sourceLayersPromise, await spriteDefinitionPromise);

  map.on('load', async function ()
  {
    const legendDeckContainer = $('#legendDeck');

    buildPaneSourceInputs(map, sources, settings.layers, addSourceAndLayers);

    legendDeckContainer.find('.obstype-pane-list li input').change(layerCheckHandler(map, sources));

    var legendDivStack = legendDeckContainer.divStack({});
    setupObstypeLegendDivs(sources);
  });

  const refreshMyMap = () => refreshMap(map, sources);

  map.on('zoomend', displayZoom);
  map.on('zoom', displayZoom);
  displayZoom({target: map});

  map.on("mousemove", mapMouseMoveHandler(sources, await spriteDefinitionPromise));



  let currentMoment = moment().seconds(0).milliseconds(0);

  const loadNotifications = () => $.getJSON("notify/" + (currentMoment.valueOf()) + "/" + (currentMoment.valueOf() + mapTimeSlider.labeledSlider('value') * 60 * 1000))
      .done((data) => $('#divNotificationDialog').notificationDialog("processNotifications", data));

  var mapTimeSlider = $("#slider").labeledSlider({
    slide: () => displaySelectedTime(moment(currentMoment).add(mapTimeSlider.labeledSlider('value'), 'minutes')),
    labelPosition: 'on',
    min: -240,
    max: 480,
    value: settings.timeOffset ? settings.timeOffset : 0,
    ticks: true,
    labels: {0: currentMoment.format("h:mm a")},
    defaultLabelFn: minutesToHHmm,
    labelValues: [-180, -120, -60, 0, 60, 120, 180, 240, 300, 360, 420],
    tickValues: [-225, -210, -195, -165, -150, -135, -105, -90, -75, -45, -30, -15, 15, 30, 45, 75, 90, 105, 135, 150, 165, 195, 210, 225, 255, 270, 285, 315, 330, 345, 375, 390, 405, 435, 450, 465],
    stop: function (event, ui)
    {
      const selectedTime = moment(currentMoment).add(mapTimeSlider.labeledSlider('value'), 'minutes');
      setTimeCookie(selectedTime);
      displaySelectedTime(selectedTime);
      refreshMyMap();
      loadNotifications();
    }
  });


  const mapDialog = new FeatureDetailsWindow({mapSelector: '#mapid', sources,
    selectedTimeStartFunction: () => currentMoment.valueOf(), //+ selectedValue * 60 * 1000;
    selectedTimeEndFunction: () => currentMoment.valueOf() + mapTimeSlider.labeledSlider('value') * 60 * 1000
  });

  const mapClickHandler = ({target, point, lngLat}) => {
    const features = target.queryRenderedFeatures(pointToPaddedBounds(point))
      .filter(featureInSources(sources));

    if (features.length > 0)
      mapDialog.showFeatureDetails(features[0], lngLat);
  };

  map.on('click', mapClickHandler);




  function updateNowTime(refTime, minOffset)
  {
    setRefCookie(refTime);
    let trgtTime = moment(refTime).add(minOffset, 'minutes');
    setTimeCookie(trgtTime);
    currentMoment = refTime;
    mapTimeSlider.labeledSlider('option', 'labels', {0: refTime.format("h:mm a")});

    mapTimeSlider.labeledSlider('value', minOffset);
    displaySelectedTime(trgtTime);
    refreshMyMap();
    loadNotifications();
  }

  const  autoRefresh = () => {
    if ($('#chkAutoRefresh').prop('checked'))
      updateNowTime(currentMoment.add(1, 'minute'), mapTimeSlider.labeledSlider('value'));
  };

  scheduleEvenMinuteTimeout(autoRefresh);

  var minuteInterval = 60;
  var startDate = new Date();

  startDate.setSeconds(0);
  startDate.setMilliseconds(0);
  startDate.setMinutes(0);

//set max time to the next interval + 6 hours
  var maxTime = new Date(startDate.getTime() + (1000 * 60 * 60 * 6) + (1000 * 60 * minuteInterval));
  maxTime.setMinutes(maxTime.getMinutes() - maxTime.getMinutes() % minuteInterval);


  $('#txtTime').datetimepicker({
    step: minuteInterval,
    value: startDate,
    formatTime: 'h:i a',
    format: 'Y/m/d h:i a',
    yearStart: 2017,
    yearEnd: maxTime.getUTCFullYear(),
    maxTime: maxTime,
    maxDate: maxTime,
//    onClose: function ()
    onSelectTime: function()
    {
      var newTime = moment(this.getValue()).seconds(0).milliseconds(0);
      updateNowTime(newTime, 0);
      //If the newly selected time is within a minute of the current time, re-enable auto-refresh
      $('#chkAutoRefresh').prop('checked', Math.abs(newTime.valueOf() - new Date().getTime()) < 60000);
    }
  });


  var slider = $("#slider");
  slider.width(slider.width());


  const initialOffset = currentMoment.add(mapTimeSlider.labeledSlider('value'), 'minutes');
  setTimeCookie(initialOffset);
  setRefCookie(currentMoment);
  displaySelectedTime(initialOffset);


  const reportManager = new ReportSelectionManager({
    refTimeMax: maxTime,
    refTimeInterval: minuteInterval,
    refTimeInitial: startDate,
    obstypes: await obstypesPromise,
    map,
    sources,
    spriteDef: await spriteDefinitionPromise,

    beforeSelection: function ()
    {
      map.off('click', mapClickHandler);
      $('#legendDeck').hide();
      $('#legendDeck .obstype-pane-list li input, #chkAutoRefresh,#chkNotifications').each(function ()
      {
        const chk = $(this);
        if (chk.prop("checked"))
          chk.data("prevValue", true).prop("checked", false).change();
        chk.prop("disabled", true);
      });
    },
    afterSelection: function ()
    {
      map.on('click', mapClickHandler);
      $('#legendDeck').show();
      $('#legendDeck .obstype-pane-list li input, #chkAutoRefresh,#chkNotifications').each(function ()
      {
        const chk = $(this);
        chk.prop("disabled", false);
        if (chk.data("prevValue"))
          chk.prop("checked", true).change();
      });
    }
  });



  let autoCenteredDialogs = $("#divReportDialog, #dialog-form, #divNotificationDialog");
  $(window).resize(function ()
  {
    const position = {my: "center", at: "center", of: '#map-container'};
    autoCenteredDialogs.each(function (index)
    {
      if ($(this).hasClass("ui-dialog-content")) // Test if the dialog has been initialized
        $(this).dialog("option", "position", position);
    });
    $("#divNotificationDialog").notificationDialog("option", "position", position);
  });

  const  dialogDrag = () => {
    autoCenteredDialogs = autoCenteredDialogs.not('#dialog-form');
    $('#dialog-form').off('dialogdragstart', dialogDrag);
  };

  $('#dialog-form').on('dialogdragstart', dialogDrag);

  if (settings.roadTypes)
  {
    if (settings.roadTypes.arterials === false)
      $('#chkShowArterials').prop('checked', false);

    if (settings.roadTypes.highways === false)
      $('#chkShowHighways').prop('checked', false);
  }

  $('#chkNotifications').prop('checked', settings.notify);
  $('#chkAutoRefresh').prop('checked', settings.refresh);

  const notificationsDialog = $('#divNotificationDialog').notificationDialog(
    {
      selectNotification: (e, notification) => mymap.fitBounds([[notification.lat1, notification.lon1], [notification.lat2, notification.lon2]]),

      displayOnNewAlerts: () => $('#chkNotifications').prop('checked')
    });

  $('#chkNotifications').click(function (e)
  {
    if (this.checked)
      notificationsDialog.notificationDialog("open");
  });




  $("#save-user-view").click(saveSettings(map, mapTimeSlider));
  $('#btnTime').click(() => $('#txtTime').datetimepicker('show'));
}


$(document).ready(() => {
  initialize();
});
