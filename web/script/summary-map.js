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
import {addSourceAndLayers, removeSourceAndLayers, pointToPaddedBounds, featureInSources, iconDivFromSprite, g_oLayers} from './map-util.js';
import {getNetworksAjax, getProfileAjax, initCommonMap, getLastLayer} from './map-common.js';

window.g_oRequirements = {'groups': 'imrcp-user;imrcp-admin'};
let bRefreshMap = false;
let nLastSliderVal;
let nImrcpReqs = 0;
let sLastMapboxLayer;
const mapMouseMoveHandler = (sources, spriteDef) => 
{
	let lastHoverFeature;
	const hoverablePointLayers = new Set(["Field Sensors"]);
	return ({target:map, point}) => 
	{

		const features = map.queryRenderedFeatures(pointToPaddedBounds(point)).filter(featureInSources(sources));
		const currentSelectedFeature = features.length === 0 ? null : features[0];
		if (currentSelectedFeature)
			$(map.getCanvas()).addClass("clickable");
		else
			$(map.getCanvas()).removeClass("clickable");

		if (lastHoverFeature && (!currentSelectedFeature || lastHoverFeature.id !== currentSelectedFeature.id))
		{
		  // if (!newHoverIds.has(f.id)) -- this doesn't work while we are only highlighting the first layer
			let sType = sources.get(lastHoverFeature.source).type;
			if (sType === 'symbol')
			{
				if (lastHoverFeature.marker)
					lastHoverFeature.marker.remove();
				else
					map.setFeatureState(lastHoverFeature, {hover: false});
			}
			else if (map.getSource(lastHoverFeature.source) && sType === 'line') // don't change feature state if feature's source has been removed
				map.setFeatureState(lastHoverFeature, {hover: false});
		}

		if (currentSelectedFeature && (!lastHoverFeature || lastHoverFeature.id !== currentSelectedFeature.id))
		{
			const featureSource = sources.get(currentSelectedFeature.source);
			if (featureSource.type === 'symbol')
			{
				if (hoverablePointLayers.has(currentSelectedFeature.source))
				{
					let sImageName = currentSelectedFeature.layer.layout["icon-image"];
//					if (sImageName.indexOf("-sdf") >= 0)
//					{
						map.setFeatureState(currentSelectedFeature, {hover: true});
//					}
//					else
//					{
//						sImageName += "-hover"; // append hover suffix
//						currentSelectedFeature.marker = new mapboxgl.Marker($(iconDivFromSprite(spriteDef[sImageName])).get(0)).setLngLat(currentSelectedFeature.geometry.coordinates).addTo(map);
//					}
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

const buildSourceMap = (sourceData, spriteDef) =>
{
	const sources = new Map();
	Object.entries(sourceData).forEach(([sourceId, {group, source, layers, type}]) => 
	{
		if (sourceId[0] === '#')
			return;
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
					legendElements.push({'label': label, 'color': layer.paint[ type + '-color'], 'opacity': fullLayer.paint[type + '-opacity'] ? fullLayer.paint[type + '-opacity'] : 1.0});
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
				["interpolate", ["exponential", 2], ["zoom"], 10.0, ["+", ["var", "extra"], 2.0], 16.0, ["+", ["var", "extra"], 8.0], 22.0, ["+", ["var", "extra"], 12.0]]];
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


const refreshMap = (map, sources) => 
{
	sources.forEach(source => 
	{
		if (map.getSource(source.id))
		{
			removeSourceAndLayers(map, source);
			addSourceAndLayers(map, source, sLastMapboxLayer);
		}
	});
};

const scheduleEvenMinuteTimeout = (callback) => 
{
  //set the refresh method to run at the start of the next minute, and
  //then set it to run every minute after that.
	let now = moment();
	let currentMillis = now.valueOf();
	now.seconds(0).milliseconds(0).add(1, 'minute');
	setTimeout(function ()
	{
		callback();
		setInterval(callback, 60000);

	}, now.valueOf() - currentMillis);
};

const layerCheckHandler = (map, sources) => ({target}) => 
{
    const jqEl = $(target);
    const sourceId = jqEl.val();
    const source = sources.get(sourceId);
    if (jqEl.prop('checked'))
		addSourceAndLayers(map, source, sLastMapboxLayer);
    else
		removeSourceAndLayers(map, source);
};


const displayZoom = ({target}) => $('#txtLocationLookup').text(('' + target.getZoom()).substring(0, 3));
function updateDateSlider(oMoment)
{
	$('#dateslider > .ui-slider-handle').text(oMoment.format('MM/DD'));
}

function updateTimeSlider(oMoment)
{
	$('#timeslider > .ui-slider-handle').text(oMoment.format('HH:mm'));
}


function setTimeStep()
{
	let oSlider = $('#dateslider');
	let nVal = oSlider.labeledSlider('value');
	let nStep = 1;
	if (nVal > 2)
		nStep = 180;
	else if (nVal >= 1)
		nStep = 60;
	$('#timeslider').labeledSlider('option', 'step', nStep);
	return nStep;
}


const setRefCookie = moment => document.cookie = `rtime=${moment.valueOf()};path=/`;
const setTimeCookie = moment => document.cookie = `ttime=${moment.valueOf()};path=/`;

const sourceLayersPromise = $.getJSON('mapbox/sourcelayers.json').promise();
const spriteDefinitionPromise = $.getJSON('images/sprite.json').promise();
const obstypesPromise = $.getJSON('obstypes.json').promise();

async function initialize()
{
	$(document).prop('title', 'IMRCP Map - ' + sessionStorage.uname);
	let pNetworks = getNetworksAjax().promise();
	let pProfile = getProfileAjax().promise();
	
	const settings = await loadSettings;
	const map = initCommonMap('mapid', settings.map.center.lng, settings.map.center.lat, settings.map.zoom, 4, 22);
	
	
	const sources = buildSourceMap(await sourceLayersPromise, await spriteDefinitionPromise);

	map.on('load', async function ()
	{
		sLastMapboxLayer = getLastLayer(map);
//		$('#navbar').append('<li><div id="loadingTiles" style="display:inline-block" class="w3-navitem">Loading data tiles...</div></li>');
//		$('#loadingTiles').hide();
//		map.on('sourcedataloading', function(oEvent) 
//		{
//			if (oEvent.source.type !== 'vector' || oEvent.source.tiles === undefined || oEvent.source.tiles[0].indexOf('imrcp') < 0)
//				return;
//			let oEl = $('#loadingTiles');
//			if (nImrcpReqs++ === 0)
//				oEl.show();
//			oEl.html(nImrcpReqs);
//			
//		});
//		map.on('sourcedata', function(oEvent)
//		{
//			if (oEvent.source.type !== 'vector' || oEvent.source.tiles === undefined || oEvent.source.tiles[0].indexOf('imrcp') < 0)
//				return;
//			--nImrcpReqs;
//			let oEl = $('#loadingTiles');
//			oEl.html(nImrcpReqs);
//			if (nImrcpReqs === 0)
//				oEl.hide();
////			setTimeout(function() 
////			{
////				if (map.loaded())
////					$('#loadingTiles').hide();
////			}, 1000);
//		});
		const legendDeckContainer = $('#legendDeck');
		if (false) // imrcp-admin group
			$('#navbar').append('<li><div style="display:inline-block" class="w3-navitem"><input value="zip city county state" type="text" id="txtLocationLookup"/></div></li>');
		buildPaneSourceInputs(map, sources, settings.layers, addSourceAndLayers, sLastMapboxLayer);
		if (sessionStorage.uname === 'cherneya')
		{
			let oLatLon = $('<li><div id="mapLngLat" style="display:inline-block" class="w3-navitem">0,0</div></li>');
			oLatLon.click(() => map.showTileBoundaries = !map.showTileBoundaries);
			$('#navbar').append(oLatLon);
			map.on('click', function(e) {
				$('#mapLngLat').html(e.lngLat.lng.toFixed(7) + ' ' + e.lngLat.lat.toFixed(7) + ' ' + map.getZoom());
			});
		}
		legendDeckContainer.find('.obstype-pane-list li input').change(layerCheckHandler(map, sources));
		
		var legendDivStack = legendDeckContainer.divStack({});
		setupObstypeLegendDivs(sources);
		
		let oAllNetworks = await pNetworks;
		let oProfile = await pProfile;
		let oNetworks = {'type': 'geojson', 'maxzoom': 9, 'data': {'type': 'FeatureCollection', 'features': []}};
		
		for (let oNetwork of oAllNetworks.values())
		{
			for (let oProfileNetwork of oProfile.networks.values())
			{
				if (oProfileNetwork.id === oNetwork.properties.networkid)
				{
					let oFeature = {'type': 'Feature', 'properties': oNetwork.properties, 'geometry': {'type': 'LineString', 'coordinates': oNetwork.geometry.coordinates[0]}};
					oNetworks.data.features.push(oFeature);
				}
			}
		}
		
		map.addSource('network-outlines', oNetworks);
		map.addLayer(g_oLayers['network-outlines']);
	});

	const refreshMyMap = () => refreshMap(map, sources);

	map.on('zoomend', displayZoom);
	map.on('zoom', displayZoom);
	displayZoom({target: map});

	map.on("mousemove", mapMouseMoveHandler(sources, await spriteDefinitionPromise));


	let oQueryTime = moment().seconds(0).milliseconds(0);
	let oRefTime = moment().seconds(0).milliseconds(0);
	let oTimeForLabel = moment().hours(0).minutes(0).seconds(0).milliseconds(0);
	oRefTime.add(10 - (oRefTime.minutes() % 10), 'minutes');

	const loadNotifications = () => $.getJSON("api/notify/" + (oRefTime.valueOf()) + "/" + (oRefTime.valueOf() + mapTimeSlider.labeledSlider('value') * 60 * 1000))
		.done((data) => $('#divNotificationDialog').notificationDialog("processNotifications", data));
	
	let nMin = -5;
	let nMax = 5;
	let nLabelDaysBack = 4;
	let nLabelStep = 1;
	let nTickStep = nLabelStep;
	let aLabels = [];
	
	
	for (let nOffset = 0; nOffset > nMin; nOffset -= nLabelStep)
		aLabels.push(nOffset);
	aLabels = aLabels.reverse()
	
	for (let nOffset = nLabelStep; nOffset < nMax; nOffset += nLabelStep)
		aLabels.push(nOffset);
	
	let aTicks = [];
	for (let nOffset = 0; nOffset > nMin; nOffset -= nTickStep)
		aTicks.push(nOffset);
	aTicks = aTicks.reverse();
	
	for (let nOffset = nTickStep; nOffset < nMax; nOffset += nTickStep)
		aTicks.push(nOffset);
	
	let mapDateSlider = $('#dateslider').labeledSlider(
	{
		slide: function(event, ui) 
		{
			let oUpdateTime = moment(oRefTime).add(ui.value, 'days');
			updateDateSlider(oUpdateTime);
		},
		labelPosition: 'on',
		min: nMin,
		max: nMax,
		step: 1,
		value: 0,
		ticks: true,
		defaultLabelFn: function(nOffset)
		{
			return moment(oRefTime).add(nOffset, 'days').format("MM/DD");
		},
		labelValues: aLabels,
		tickValues: aTicks,
		stop: function (event, ui)
		{
			setTimeStep();
			let nStep = setTimeStep();
			let oTimeslider = $('#timeslider');
			oTimeslider.labeledSlider('option', 'step', nStep);
			let nTimeVal = $('#timeslider').labeledSlider('value');
			$('#timeslider').labeledSlider('value', Math.floor(nTimeVal / nStep) * nStep);
			let oSelectedTime = moment(oRefTime).add(ui.value, 'days').minutes(0).hours(0).add($('#timeslider').labeledSlider('value'), 'minutes');
			updateQueryTime(oSelectedTime);
			
			refreshMyMap();
//			loadNotifications();
		}
	});
	
	nMin = 0;
	nMax = 1440;
	nLabelStep = 60;
	nTickStep = nLabelStep / 2;
	aLabels = [];
	
	for (let nOffset = 0; nOffset > nMin; nOffset -= nLabelStep)
		aLabels.push(nOffset);
	aLabels = aLabels.reverse();
	
	for (let nOffset = nLabelStep; nOffset < nMax; nOffset += nLabelStep)
		aLabels.push(nOffset);
	
	aTicks = [];
	for (let nOffset = 0; nOffset > nMin; nOffset -= nTickStep)
		aTicks.push(nOffset);
	aTicks = aTicks.reverse();
	
	for (let nOffset = nTickStep; nOffset < nMax; nOffset += nTickStep)
		aTicks.push(nOffset);

	let mapTimeSlider = $("#timeslider").labeledSlider(
	{
		slide: function(event, ui) 
		{
			let oUpdateTime = moment(oRefTime).add(ui.value, 'minutes').subtract(oRefTime.hour(), 'hours').subtract(oRefTime.minute(), 'minutes');
			updateTimeSlider(oUpdateTime);
//			if (ui.value <= nTenMinThresh)
//			{
//				let nAdjust = nLastSliderVal > ui.value ? -10 : 10;
//				displaySelectedTime(moment(oRefTime).add((mapTimeSlider.labeledSlider('value') + nAdjust), 'minutes'));
//				nLastSliderVal = ui.value;
//			}
//			else
//			{
//				let nVal;
//				let nAdjust = 0;
//				
//				if (ui.value < nHourThresh)
//				{
//					nVal = Math.floor(ui.value / 60) * 60;
//					nAdjust = 60;
//				}
//				else
//				{
//					nVal = Math.floor(ui.value / 180) * 180;
//					nAdjust = 180;
//				}
//				if (ui.value < nLastSliderVal)
//					nAdjust = 0;
//				
//				
//				if (event.originalEvent.type === 'keydown')
//				{
//					displaySelectedTime(moment(oRefTime).add(nVal + nAdjust, 'minutes'));
//					nLastSliderVal = nVal + nAdjust;
//					mapTimeSlider.labeledSlider('value', nVal + nAdjust);
//					return false;
//				}
//				else
//				{
//					displaySelectedTime(moment(oRefTime).add(nVal, 'minutes'));
//					nLastSliderVal = nVal;
//				}
//			}
		},
		labelPosition: 'on',
		min: nMin,
		max: nMax,
		step: 1,
		value: oQueryTime.hours() * 60 + oQueryTime.minutes(),
		ticks: true,
		defaultLabelFn: function(nOffset)
		{
			return moment(oTimeForLabel).add(nOffset, 'minutes').format("HH:mm");
		},
		labelValues: aLabels,
		tickValues: aTicks,
		stop: function (event, ui)
		{
			let oSelectedTime = moment(oQueryTime).minutes(0).hours(0).add(ui.value, 'minutes');
			let nLast = ui.value;
			if (event.originalEvent.type === 'keyup' && ui.value === 0 && nLastSliderVal === 0 && mapDateSlider.labeledSlider('value') !== mapDateSlider.labeledSlider('option', 'min'))
			{
				mapDateSlider.labeledSlider('value', mapDateSlider.labeledSlider('value') - 1);
				setTimeStep();
				oSelectedTime.subtract(mapTimeSlider.labeledSlider('option', 'step'), 'minutes');
				mapTimeSlider.labeledSlider('value', mapTimeSlider.labeledSlider('option', 'max') - mapTimeSlider.labeledSlider('option', 'step'));
				updateDateSlider(oSelectedTime);
				
			}
			if (ui.value === nMax)
			{
				if (mapDateSlider.labeledSlider('value') === mapDateSlider.labeledSlider('option', 'max'))
				{
					oSelectedTime.subtract(mapTimeSlider.labeledSlider('option', 'step'), 'minutes');
					mapTimeSlider.labeledSlider('value', mapTimeSlider.labeledSlider('value') - mapTimeSlider.labeledSlider('option', 'step'));
				}
				else
				{
					mapTimeSlider.labeledSlider('value', 0);
					nLast = 0;
					mapDateSlider.labeledSlider('value', mapDateSlider.labeledSlider('value') + 1);
					setTimeStep();
					updateDateSlider(oSelectedTime);
				}
			}

			updateQueryTime(oSelectedTime);
			refreshMyMap();
			nLastSliderVal = nLast;
//			loadNotifications();
		}
	});
	nLastSliderVal = mapTimeSlider.labeledSlider('value');

	const mapDialog = new FeatureDetailsWindow(
	{
		mapSelector: '#mapid', sources,
		selectedTimeStartFunction: () => oRefTime.valueOf(), //+ selectedValue * 60 * 1000;
		selectedTimeEndFunction: () => oQueryTime.valueOf()
	});

	const mapClickHandler = ({target, point, lngLat}) => 
	{
		const features = target.queryRenderedFeatures(pointToPaddedBounds(point)).filter(featureInSources(sources));

		if (features.length > 0)
			mapDialog.showFeatureDetails(features[0], lngLat, map);
	};

	map.on('click', mapClickHandler);
	
	
	function updateRefTime(oNewTime)
	{
		setRefCookie(oNewTime);
		oRefTime = oNewTime;
		$('#dateslider .label-on').each(function(index, el) {
			$(el).html($('#dateslider').labeledSlider('option').defaultLabelFn(index - nLabelDaysBack));
		});
//		mapDateSlider.labeledSlider('value', 0);
//		updateDateSlider(oRefTime);
		$('#btnTime').val(oNewTime.format("YYYY/MM/DD HH:mm"));
		
		if (document.visibilityState === 'visible')
		{
			bRefreshMap = false;
			refreshMyMap();
//			loadNotifications();
		}
		else
		{
			bRefreshMap = true;
		}
	}
	
	
	function updateQueryTime(oNewTime)
	{
		setTimeCookie(oNewTime);
		oQueryTime = oNewTime;
		mapTimeSlider.labeledSlider('value', oQueryTime.hours() * 60 + oQueryTime.minutes());
		let nDayDiff = oQueryTime.dayOfYear() - oRefTime.dayOfYear();
		if (nDayDiff > 5) //
		{
			nDayDiff -= oQueryTime.isLeapYear() ? 366 : 365;
		}
		else if (nDayDiff < -5)
		{
			nDayDiff += oQueryTime.isLeapYear() ? 366 : 365;
		}
			
		mapDateSlider.labeledSlider('value', nDayDiff);
		updateTimeSlider(oQueryTime);
		updateDateSlider(oQueryTime);
		setTimeStep();

		if (document.visibilityState === 'visible')
		{
			bRefreshMap = false;
			refreshMyMap();
//			loadNotifications();
		}
		else
		{
			bRefreshMap = true;
		}
	}

	const autoRefresh = () => 
	{
		if ($('#chkAutoRefresh').prop('checked'))
		{
			let oNow = moment();
			let nStep = mapTimeSlider.labeledSlider('option', 'step');
			if (oNow.minutes() % nStep === 0)
			{
				let nHours = oQueryTime.hours();
				let oNewTime = moment(oQueryTime).hours(0).minutes(0).add(oQueryTime.hours() * 60 + oQueryTime.minutes() + nStep, 'minutes');
				updateQueryTime(oNewTime);
				if (oNewTime.hours() != nHours)
				{
					let nAdd = 1;
					if (nStep > 60)
						nAdd = Math.floor(nStep / 60);
					updateRefTime(moment(oRefTime).add(nAdd, 'hours'));
				}
			}
		}
//			updateNowTime(oRefTime.add(1, 'minute'), mapTimeSlider.labeledSlider('value'));
	};

	scheduleEvenMinuteTimeout(autoRefresh);
	document.addEventListener('visibilitychange', () => 
	{
		if (document.visibilityState === 'visible' && bRefreshMap)
		{
			bRefreshMap = false;
			refreshMyMap();
//			loadNotifications();
		}
	});
	var minuteInterval = 10;
	var startDate = new Date();

	startDate.setSeconds(0);
	startDate.setMilliseconds(0);
	startDate.setMinutes(0);

	//set max time to the next interval + 6 hours
	var maxTime = new Date(startDate.getTime() + (1000 * 60 * 60 * 6) + (1000 * 60 * minuteInterval));
	maxTime.setMinutes(maxTime.getMinutes() - maxTime.getMinutes() % minuteInterval);

	$('#txtTime').datetimepicker(
	{
		step: minuteInterval,
		value: startDate,
		formatTime: 'H:i',
		format: 'Y/m/d H:i',
		yearStart: 2017,
		yearEnd: maxTime.getUTCFullYear(),
		maxTime: maxTime,
		maxDate: maxTime,
		closeOnWithoutClick: false,
		onSelectTime: function()
		{
			let newTime = moment(this.getValue()).seconds(0).milliseconds(0);
			updateRefTime(newTime);
			updateQueryTime(newTime);
			//If the newly selected time is within a minute of the current time, re-enable auto-refresh
			$('#chkAutoRefresh').prop('checked', Math.abs(newTime.valueOf() - new Date().getTime()) < 60000);
			mapTimeSlider.labeledSlider('option', 'step', 1);
		},
	});

	$('.xdsoft_today_button').click(function() 
	{
		let oTime = moment().seconds(0).milliseconds(0);
		let oNextInterval = moment().seconds(0).milliseconds(0);
		oNextInterval.add(10 - (oNextInterval.minutes() % 10), 'minutes');
		
		$('#txtTime').datetimepicker({value: oNextInterval});
		$('#txtTime').datetimepicker('hide');
		updateRefTime(oNextInterval);
		updateQueryTime(oTime);
		mapTimeSlider.labeledSlider('option', 'step', 1);
	});

	updateQueryTime(oQueryTime);
	updateRefTime(oRefTime);

//	const reportManager = new ReportSelectionManager(
//	{
//		refTimeMax: maxTime,
//		refTimeInterval: minuteInterval,
//		refTimeInitial: startDate,
//		obstypes: await obstypesPromise,
//		map,
//		sources,
//		spriteDef: await spriteDefinitionPromise,
//
//		beforeSelection: function ()
//		{
//			map.off('click', mapClickHandler);
//			$('#legendDeck').hide();
//			$('#legendDeck .obstype-pane-list li input, #chkAutoRefresh,#chkNotifications').each(function ()
//			{
//				const chk = $(this);
//				if (chk.prop("checked"))
//					chk.data("prevValue", true).prop("checked", false).change();
//				chk.prop("disabled", true);
//			});
//		},
//		afterSelection: function ()
//		{
//			map.on('click', mapClickHandler);
//			$('#legendDeck').show();
//			$('#legendDeck .obstype-pane-list li input, #chkAutoRefresh,#chkNotifications').each(function ()
//			{
//				const chk = $(this);
//				chk.prop("disabled", false);
//				if (chk.data("prevValue"))
//					chk.prop("checked", true).change();
//			});
//		}
//	});

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

	const  dialogDrag = () => 
	{
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
		selectNotification: (e, notification) => map.fitBounds([[notification.lat1, notification.lon1], [notification.lat2, notification.lon2]]),

		displayOnNewAlerts: () => $('#chkNotifications').prop('checked')
	});

	$('#chkNotifications').click(function (e)
	{
		if (this.checked)
			notificationsDialog.notificationDialog("open");
	});

	$("#save-user-view").click(saveSettings(map));
	$('#btnTime').click(() =>
	{
		$('#txtTime').datetimepicker('toggle');
	});
}


$(document).on('initPage', initialize);
