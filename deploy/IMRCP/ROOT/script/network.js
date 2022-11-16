import {buildNetworkDialog, buildRoadLegendDialog, buildCancelDialog, buildNetworkLegendDialog, buildInstructionDialog, buildDetectorDialog,
		buildDetectorEditDialog, buildDetectorStatusDialog, buildUploadConfirmDialog, buildNetworkMetadataDialog, buildReprocessDialog} from './dialogs.js';
	
import {switchToDetector, leaveDetectors, checkDetectorFile, uploadFile} from './detectors.js';

import {g_oLayers, removeSource, getPolygonBoundingBox, startDrawPoly} from './map-util.js';

let g_oMap;
let g_nHoverId;
let g_nHoverPointId;
let g_sNetworkCoords;
let g_sCurNetwork = null;
let g_oCurBounds;
let g_oHashes = {};
let BUCKET_SPACING = 100000;
let g_sCurHash = null;
let g_oWays = {};
let g_bLoadingHash = false;
let g_oDialogs = {};
let g_oSpotlight = [];
let g_nPollInterval;
let g_oMerge = [];
let g_nSplitWay;
let g_nSplitNode;
let g_nEdits = 0;
let g_sCursor = '';
let g_nEditMode = -1;
let g_sCurPointHoverLayer;
let g_sCurPointHoverSource;
let g_oPopup;
let g_oReprocess;
let g_oInstructions = 
{
	'select' : ['Left-click a polygon to view/edit network. The label and number of segments are displayed in the popup.', 'Network Overview'],
	'delete' : ['Left-click a polygon to delete that network.<br>Esc: Exit Mode', 'Delete Network'],
	'reprocess' : ['Left-click a polygon to reprocess that network.<br>Esc: Exit Mode', 'Reprocess Network'],
	'edit' : ['Select an editing mode from the right side toolbar.', 'View/Edit Network'],
	'add' : ['Roads not included in network will be loaded on demand when the cursor is close to them<br>Left-click a road to add or remove<br>Esc: Exit mode', 'Add/Remove Road'],
	'merge' : ['Left-click two roads to merge<br>Enter: Commit merge<br>Esc: Cancel', 'Merge Roads'],
	'split' : ['Left-click road to split<br>Esc: Cancel', 'Split Road'],
	'upload' : ['Select "Upload File" to upload a detector file', 'View/Edit Traffic Detectors'],
	'editdet' : ['Left-click the point of detection for this detector on the map.<br>Select the next button or left-click a detector to edit a different detector.', 'Edit Detectors'],
	'det' : ['Left-click a detector or click "Edit Detectors" to open edit dialog.<br>Select "Upload File" to upload a different detector file.', 'View/Edit Traffic Detectors'],
	'detsave' : ['Save or revert changes', 'Edit Detectors']
};

window.g_oRequirements = {'groups': 'imrcp-admin'};

class MapControlIcons
{
	constructor(oHtml, sId)
	{
		this.oHtml = oHtml;
		this.sId = sId;
	}
	
	onAdd(map)
	{
		this._map = map;
		this._container = document.createElement('div');
		this._container.setAttribute('id', this.sId);
		this._container.className = 'mapboxgl-ctrl mapboxgl-ctrl-group six-button-control';

		
		this._container.innerHTML = this.oHtml
			.reduce((accum, opts) => accum +
				`<button class="mapboxgl-ctrl-icon" type="button" title="${opts.t}"${opts.id !== undefined ? ' id="' + opts.id + '"' :''}>
					<img src="images/${opts.i}.png" />
				</button>`, '');

		return this._container;
	}

	onRemove()
	{
		this._container.parentNode.removeChild(this._container);
		this._map = undefined;
	}
}

async function initialize()
{
	$(document).prop('title', 'IMRCP Network Creation - ' + sessionStorage.uname);
	const oMap = new mapboxgl.Map({container: 'mapid', style: 'mapbox/light-v9.json', attributionControl: false, minZoom:4, maxZoom: 24, center: [-98.585522, 39.8333333], zoom: 4, accessToken: 'pk.eyJ1Ijoia3J1ZWdlcmIiLCJhIjoiY2l6ZDl4dTlwMjJvaDJ3bW44bXFkd2NrOSJ9.KXqbeWgASgEUYQu0oi7Hbg'});
	g_oMap = oMap;
	oMap.dragRotate.disable();
	oMap.touchZoomRotate.disableRotation();
	let oMainControl = new MapControlIcons([{t:'Create Network', i:'drawpoly'}, {t:'Delete Network', i:'delete'}, {t:'Reprocess Network', i:'reload'}, {t:'Toggle Network Legend', i:'toggle'}], 'main-control');
	let oEditControl = new MapControlIcons([{t:'Add/Remove Road', i:'add'}, {t:'Merge Roads', i:'merge'}, {t:'Split Road', i:'split'}, {t:'Edit Traffic Detectors', i:'detector'}, {t:'Save Changes', i:'save'}, {t:'Export OSM Network', i:'download'}, {t:'Finalize Network', i:'download'}, {t:'Toggle Road Legend', i:'toggle'}, {t:'Go Back', i:'cancel'}], 'edit-control');
	let oTileControl = new MapControlIcons([{t:'Toggle Satellite Background', i:'satellite'}, {t:'Toggle Instructions', i:'instruction'}, {t:'Toggle Metadata', i:'metadata'}]);
	let oDetectorControl = new MapControlIcons([{t:'Upload File', i:'file'}, {t:'Edit Detectors', i:'edit'}, {t:'Toggle Detector Legend', i:'toggle'}, {t:'Back To Network Edit', i:'cancel'}], 'detector-control');

	oMap.addControl(new mapboxgl.NavigationControl({showCompass: false}));
	oMap.addControl(oTileControl, 'top-right');
	oMap.addControl(oMainControl, 'top-right');
	oMap.addControl(oEditControl, 'top-right');
	oMap.addControl(oDetectorControl, 'top-right');
	$('#detector-control').hide();
	$('#edit-control').hide();
	
	$("button[title|='Create Network']").click(startDrawPoly.bind(
	{
		'map': oMap,
		'startDraw': startCreateNetwork,
		'initPoly': initCreateNetwork,
		'movePoly': moveCreateNetwork,
		'addPoint': addPointCreateNetwork,
		'finishDraw': finishCreateNetwork,
		'cancelDraw': cancelCreateNetwork
	}));
	$("button[title|='Delete Network']").click(startDeleteNetwork);
	$("button[title|='Reprocess Network']").click(startReprocessNetwork);
	$("button[title|='Save Changes']").click(saveChanges);
	$("button[title|='Add/Remove Road']").click(startAddRemove);
	$("button[title|='Split Road']").click(startSplit);
	$("button[title|='Merge Roads']").click(startMerge);
	$("button[title|='Go Back']").click(confirmCancel);
	$("button[title|='Edit Traffic Detectors']").click(switchToDetector);
	$("button[title|='Back To Network Edit']").click(leaveDetectors);
	$("button[title|='Toggle Satellite Background']").click(toggleSatellite);
	$("button[title|='Toggle Metadata']").click({'dialog': 'networkmetadata'}, toggleDialog).hide();
	$("button[title|='Upload File']").click(checkDetectorFile);
	$("button[title|='Export OSM Network']").click(exportOsm);
	$("button[title|='Finalize Network']").click(finalizeNetwork);
	$('#detectorFile').on('change', uploadFile);
	
	oMap.on('load', async function() {
		buildNetworkDialog();
		buildRoadLegendDialog();
		buildCancelDialog();
		buildNetworkLegendDialog();
		buildInstructionDialog();
		buildDetectorDialog();
		buildDetectorEditDialog();
		buildDetectorStatusDialog();
		buildUploadConfirmDialog();
		buildNetworkMetadataDialog();
		buildReprocessDialog();
		instructions(g_oInstructions['select']);
		startSelectNetwork();
		g_oPopup = new mapboxgl.Popup({closeButton: false, closeOnClick: false, anchor: 'bottom', offset: [0, -25]});
	});
}


function toggleSatellite()
{
	if (g_oMap.getSource('mapbox://mapbox.satellite') === undefined)
	{
		let sBefore = 'road-number-shield';
		if (g_oMap.getLayer('geo-lines') !== undefined)
			sBefore = 'geo-lines';

		g_oMap.addSource('mapbox://mapbox.satellite', {'url': 'mapbox://mapbox.satellite', 'type': 'raster', 'tileSize': 256});
		g_oMap.addLayer(g_oLayers['satellite'], sBefore);
	}
	else
	{
		removeSource('mapbox://mapbox.satellite', g_oMap);
	}
}


function toggleDialog(oEvent)
{
	let oDialog = $(g_oDialogs[oEvent.data.dialog]);
	if (oDialog.dialog('isOpen'))
		oDialog.dialog('close');
	else
	{
		oDialog.dialog('open');
		document.activeElement.blur();
	}
}


function startSelectNetwork(bLoaded)
{
	clearInterval(g_nPollInterval);
	g_oMap.off('click', 'network-polygons', selectNetwork);
	g_oMap.off('mouseenter', 'network-polygons', mouseenterNetworkPolygons);
	g_oMap.off('mouseleave', 'network-polygons', mouseleaveNetworkPolygons);
	let oMetadata = $(g_oDialogs['networkmetadata']);
	if (oMetadata.dialog('isOpen'))
		oMetadata.dialog('close');
	$("button[title|='Toggle Metadata']").hide();
	if (bLoaded)
	{
		g_oMap.on('click', 'network-polygons', selectNetwork);
		g_oMap.on('mouseenter', 'network-polygons', mouseenterNetworkPolygons);
		g_oMap.on('mouseleave', 'network-polygons', mouseleaveNetworkPolygons);
		g_sCursor = '';
		g_oMap.getCanvas().style.cursor = g_sCursor;
	}
	else
	{
		$.ajax(
		{
			'url': 'api/generatenetwork/list',
			'dataType': 'json',
			'method': 'POST',
			'data': {'token': sessionStorage.token}
		}).done(networksSuccess).fail(function() 
		{
			$('#instructions-error').html('Failed to retrieve networks. Try again later.');
		});
	}
}


function networksSuccess(data, textStatus, jqXHR)
{
	let oGeoJson = {};
	oGeoJson.type = 'FeatureCollection';
	oGeoJson.features = [];
	let bLoading = false;
	for (let value of data.values())
	{
		oGeoJson.features.push(value);
		if (value.properties.loaded === 1)
			bLoading = true;
	}
	
	if (bLoading)
	{
		clearInterval(g_nPollInterval);
		g_nPollInterval = setInterval(startSelectNetwork, 30000);
	}
	
	if (oGeoJson.features.length === 0)
	{
		$('#instructions-error').html('No networks have been created. Create one by using the "Create Network" button');
		return;
	}
	
	removeSource('network-polygons', g_oMap);
	g_oMap.addSource('network-polygons', {'type': 'geojson', 'data': oGeoJson, 'generateId': true});
	g_oMap.addLayer(g_oLayers['network-polygons']);
	g_oMap.addLayer(g_oLayers['network-polygons-del']);
	
	g_oMap.on('click', 'network-polygons', selectNetwork);
	g_oMap.on('mouseenter', 'network-polygons', mouseenterNetworkPolygons);
	g_oMap.on('mouseleave', 'network-polygons', mouseleaveNetworkPolygons);
}


function selectNetwork(oEvent)
{
	let nHover = g_nHoverId;
	let oFeatures = oEvent.features
	if (oFeatures.length === 0)
		return;
	
	let oFeature = null;
	for (let nIndex = 0; nIndex < oFeatures.length; nIndex++)
	{
		if (oFeatures[nIndex].id == nHover)
			oFeature = oFeatures[nIndex];
	}
	
	if (oFeature == null)
		return;
	
	if (oFeature.properties.loaded === 1)
	{
		$('#instructions-error').html('Network generation still processing.');
		return;
	}
	else if (oFeature.properties.loaded === 2)
	{
		$('#instructions-error').html('Error occured while generating network.');
		return;
	}
	
	clearInterval(g_nPollInterval);
	g_nPollInterval = undefined;
	instructions(g_oInstructions['edit']);
	$.ajax(
	{
		'url': 'api/generatenetwork/geo',
		'dataType': 'json',
		'method': 'POST',
		'networkid': oFeature.properties.networkid,
		'networklabel': oFeature.properties.label,
		'data': {'token': sessionStorage.token, 'networkid': oFeature.properties.networkid}
	}).done(selectSuccess).fail(function() 
	{
		$('#instructions-error').html('Failed to retrieve network.');
		g_oMap.getCanvas().style.cursor = g_sCursor;
	});
	g_sCursor = 'wait';
	
	g_oMap.getCanvas().style.cursor = g_sCursor;
	g_oMap.off('click', 'network-polygons', selectNetwork);
	g_oMap.off('mouseenter', 'network-polygons', mouseenterNetworkPolygons);
	g_oMap.off('mouseleave', 'network-polygons', mouseleaveNetworkPolygons);
	
	let oSrc = g_oMap.getSource('network-polygons');
	let oData = oSrc._data;
	for (let nIndex = 0; nIndex < oData.features.length; nIndex++)
	{
		let oFeature = oData.features[nIndex];
		if (nIndex == nHover)
		{
			g_oCurBounds = getPolygonBoundingBox(oFeature);
			g_oMap.fitBounds(g_oCurBounds, {'padding': 50});
			oFeature.properties.hidden = false;
		}
		else
			oFeature.properties.hidden = true;	
	}
	
	oSrc.setData(oData);
	g_nHoverId = undefined;
}


function selectSuccess(data, textStatus, jqXHR)
{
	g_oWays = {};
	g_oHashes = {};
	g_sCurHash = null;
	
	removeSource('network-polygons', g_oMap);
	removeSource('geo-lines', g_oMap);

	let oLineData = {'type':'FeatureCollection', 'features': []};
	let oHighway = {};
	let nBridges = 0;
	let nRamps = 0;
	let nConnectors = 0;
	let nNotSet = 0;
	for (let oFeature of data.values())
	{
		g_oWays[oFeature.properties.imrcpid] = oFeature;
		if (oFeature.properties.bridge)
			++nBridges;
		if (oHighway[oFeature.properties.type])
			++oHighway[oFeature.properties.type];
		else
			oHighway[oFeature.properties.type] = 1;
		oFeature.properties.include = true;
		oFeature.properties.hidden = false;
		for (let [sKey, sVal] of Object.entries(oFeature.properties.tags))
			oFeature.properties[sKey] = sVal;
		oFeature.properties.tags = undefined;
		if (oFeature.properties.linktype)
		{
			if (oFeature.properties.linktype === 'ramp')
				++nRamps;
			else
				++nConnectors;
		}
		else if (!oFeature.properties.ingress)
			++nNotSet;
			
		oLineData.features.push(oFeature);
	}
	g_oMap.addSource('geo-lines', {'type': 'geojson', 'data': oLineData, 'generateId': true});
	g_oMap.addLayer(g_oLayers['geo-lines'], 'road-number-shield');
	g_oMap.on('mouseenter', 'geo-lines', hoverHighlight);
	g_oMap.on('mouseleave', 'geo-lines', unHighlight);
	let sOutput = `total segments: ${oLineData.features.length}`;
	let aTypes = [];
	for (let [sKey, sVal] of Object.entries(oHighway))
		aTypes.push([sKey, sVal]);
	
	aTypes.sort((a, b) => a[0].localeCompare(b[0]));
	for (let aType of aTypes.values())
		sOutput += `<br>${aType[0]}: ${aType[1]}`;
	sOutput += `<br>bridges: ${nBridges}`;
	sOutput += `<br>ramps: ${nRamps}`;
	sOutput += `<br>connectors: ${nConnectors}`;
	sOutput += `<br>notset: ${nNotSet}`;
	let oMetadata = $(g_oDialogs['networkmetadata']);
	oMetadata.dialog('option', 'title', this.networklabel);
	oMetadata.html(sOutput);
	oMetadata.dialog('open');
	$("button[title|='Toggle Metadata']").show();
	
	
	
	$(g_oDialogs['networklegend']).dialog('close');
	$(g_oDialogs['roadlegend']).dialog('open');
	document.activeElement.blur();
	g_sCurNetwork = this.networkid;
	
	clearInterval(g_nPollInterval);
	g_nPollInterval = undefined;
	
	g_nEdits = 0;
	g_nEditMode = 0;
	$('#main-control').hide();
	$('#edit-control button').prop('disabled', false);
	$('#edit-control').show();
	g_oMap.on('sourcedata', 'geo-lines', geoLinesLoaded);
}


function geoLinesLoaded(oEvent)
{
	if (oEvent.isSourceLoaded)
	{
		g_sCursor = '';
		g_oMap.getCanvas().style.cursor = g_sCursor;
		g_oMap.off('sourcedata', 'geo-lines', geoLinesLoaded);
		g_oPopup.remove();
	}
}

function mousemovePopupPos({target, lngLat, point})
{
	g_oPopup.setLngLat(lngLat);
}

function mouseenterNetworkPolygons(oEvent)
{
	let nHover = g_nHoverId;
	let nFeatureId = getFeatureId(oEvent);
	let oFeature = g_oMap.getSource('network-polygons')._data.features[nFeatureId];
	g_oPopup.setLngLat(oEvent.lngLat).setHTML(`<p>${oFeature.properties.label}<br>${oFeature.properties.segments}</p>`).addTo(g_oMap);
	g_oMap.on('mousemove', mousemovePopupPos);
	
	if (nHover >= 0)
		g_oMap.setFeatureState({source: 'network-polygons', id: nHover}, {hover: false});
	if (nFeatureId >= 0)
	{
		g_nHoverId = nFeatureId;
		g_oMap.setFeatureState({source: 'network-polygons', id: nFeatureId}, {hover: true});
	}
	g_oMap.on('mousemove', 'network-polygons', mousemoveNetworkPolygons);
}

function mousemoveNetworkPolygons(oEvent)
{
	let nHover = g_nHoverId;
	if (oEvent.features.length > 0)
		g_oMap.getCanvas().style.cursor = 'pointer';
	
	let nFeatureId = getFeatureId(oEvent);
	
	if (nFeatureId != nHover)
	{
		let oFeature = g_oMap.getSource('network-polygons')._data.features[nFeatureId];
		g_oPopup.setHTML(`<p>${oFeature.properties.label}<br>${oFeature.properties.segments}</p>`);
		if (nHover >= 0)
			g_oMap.setFeatureState({source: 'network-polygons', id: nHover}, {hover: false});
		if (nFeatureId >= 0)
		{
			g_nHoverId = nFeatureId;
			g_oMap.setFeatureState({source: 'network-polygons', id: nFeatureId}, {hover: true});
		}
	}
}


function mouseleaveNetworkPolygons(oEvent)
{
	let nHover = g_nHoverId;
	g_nHoverId = undefined;
	oEvent.target.getCanvas().style.cursor = g_sCursor;
	if (nHover >= 0)
		oEvent.target.setFeatureState({source: 'network-polygons', id: nHover}, {hover: false});
	
	oEvent.target.off('mousemove', 'network-polygons', mousemoveNetworkPolygons);
	oEvent.target.off('mousemove', mousemovePopupPos);
	g_oPopup.remove();
}


function startDeleteNetwork()
{
	g_oMap.off('click', 'network-polygons', selectNetwork);
	g_oMap.off('mouseenter', 'network-polygons', mouseenterNetworkPolygons);
	g_oMap.off('mouseleave', 'network-polygons', mouseleaveNetworkPolygons);
	g_oMap.on('click', 'network-polygons', deleteNetwork);
	g_oMap.on('mouseenter', 'network-polygons', mouseenterNetworkPolygonsDel);
	g_oMap.on('mouseleave', 'network-polygons', mouseleaveNetworkPolygonsDel);
	$(document).on('keyup', turnOffDelete);
	instructions(g_oInstructions['delete']);
}


function startReprocessNetwork()
{
	g_oMap.off('click', 'network-polygons', selectNetwork);
	g_oMap.on('click', 'network-polygons', reprocessNetwork);
	$(document).on('keyup', turnOffReprocess);
	instructions(g_oInstructions['reprocess']);
}


function turnOffReprocess(oEvent)
{
	if (oEvent.which == 27) // esc
	{
		cancelReprocess();
	}
}


function cancelReprocess()
{
	$(g_oDialogs['reprocess']).dialog('close');
	$(g_oDialogs['network']).dialog('close');
	g_oMap.off('click', 'network-polygons', reprocessNetwork);
	$(document).off('keyup', turnOffReprocess);
	instructions(g_oInstructions['select'], 'Exited reprocess mode');
	g_oReprocess = undefined;
	startSelectNetwork(true);
}


function confirmReprocess()
{
	let sData = '';
	for (let nIndex = 0; nIndex < g_oReprocess.geometry.coordinates[0].length - 1; nIndex++)
	{
		let aCoord = g_oReprocess.geometry.coordinates[0][nIndex];
		sData += aCoord[0].toFixed(7) + ',' + aCoord[1].toFixed(7) + ',';
	}
	
	$('#network-label').val(g_oReprocess.properties.label);
	let oDialog = $(g_oDialogs['network']);
	g_sNetworkCoords = sData.substring(0, sData.length - 1);
	$(g_oDialogs['reprocess']).dialog('close');
	g_oMap.off('click', 'network-polygons', reprocessNetwork);
	$(document).off('keyup', turnOffReprocess);
	oDialog.dialog('open');
}


function reprocessNetwork(oEvent)
{
	let nHover = g_nHoverId;
	let oFeatures = oEvent.features;
	if (oFeatures.length === 0)
		return;
	
	let oFeature = null;
	for (let nIndex = 0; nIndex < oFeatures.length; nIndex++)
	{
		if (oFeatures[nIndex].id == nHover)
			oFeature = oFeatures[nIndex];
	}
	
	if (oFeature == null)
		return;
	g_oReprocess = g_oMap.getSource('network-polygons')._data.features[nHover];
	$(g_oDialogs['reprocess']).dialog('open');
}


function turnOffDelete(oEvent)
{
	let nHover = g_nHoverId;
	if (oEvent.which == 27) // esc
	{
		g_oMap.off('click', 'network-polygons', deleteNetwork);
		g_oMap.off('mouseenter', 'network-polygons', mouseenterNetworkPolygonsDel);
		g_oMap.off('mousemove', 'network-polygons', mousemoveNetworkPolygonsDel);
		g_oMap.off('mouseleave', 'network-polygons', mouseleaveNetworkPolygonsDel);
		if (nHover >= 0)
		{
			g_oMap.setFeatureState({source: 'network-polygons', id: nHover}, {'delete': false});
		}
		$(document).off('keyup', turnOffDelete);
		instructions(g_oInstructions['select'], 'Exited delete mode');
		startSelectNetwork(true);
	}
}


function deleteNetwork(oEvent)
{
	let nHover = g_nHoverId;
	let oFeatures = oEvent.features
	if (oFeatures.length === 0)
		return;
	
	let oFeature = null;
	for (let nIndex = 0; nIndex < oFeatures.length; nIndex++)
	{
		if (oFeatures[nIndex].id == nHover)
			oFeature = oFeatures[nIndex];
	}
	
	if (oFeature == null)
		return;
	
	$.ajax(
	{
		'url': 'api/generatenetwork/delete',
		'method': 'POST',
		'networkid': oFeature.properties.networkid,
		'data': {'token': sessionStorage.token, 'networkid': oFeature.properties.networkid}
	}).done(function() 
	{
		g_sCursor = '';
		g_oMap.getCanvas().style.cursor = g_sCursor;
		instructions(g_oInstructions['select'], 'Delete Success');;
		startSelectNetwork();
	}).fail(function() 
	{
		$('#instructions-error').html('Failed to delete network.');
		g_oMap.getCanvas().style.cursor = g_sCursor;
	});
	instructions('', 'Deleting Network');
	g_sCursor = 'wait';
	g_oMap.getCanvas().style.cursor = g_sCursor;
	g_oMap.off('click', 'network-polygons', deleteNetwork);
	g_oMap.off('mouseenter', 'network-polygons', mouseenterNetworkPolygonsDel);
	g_oMap.off('mousemove', 'network-polygons', mousemoveNetworkPolygonsDel);
	g_oMap.off('mouseleave', 'network-polygons', mouseleaveNetworkPolygonsDel);
	$(document).off('keyup', turnOffDelete);
	g_nHoverId = undefined;
}


function mouseenterNetworkPolygonsDel(oEvent)
{
	let nHover = g_nHoverId;
	let nFeatureId = getFeatureId(oEvent);
		
	if (nHover >= 0)
		g_oMap.setFeatureState({source: 'network-polygons', id: nHover}, {'delete': false});
	if (nFeatureId >= 0)
	{
		g_nHoverId = nFeatureId;
		g_oMap.setFeatureState({source: 'network-polygons', id: nFeatureId}, {'delete': true});
	}
	g_oMap.on('mousemove', 'network-polygons', mousemoveNetworkPolygonsDel);
}

function mousemoveNetworkPolygonsDel(oEvent)
{
	let nHover = g_nHoverId;
	if (oEvent.features.length > 0)
		g_oMap.getCanvas().style.cursor = 'pointer';
	
	let nFeatureId = getFeatureId(oEvent);
	
	if (nFeatureId != nHover)
	{
		if (nHover >= 0)
			g_oMap.setFeatureState({source: 'network-polygons', id: nHover}, {'delete': false});
		if (nFeatureId >= 0)
		{
			g_nHoverId = nFeatureId;
			g_oMap.setFeatureState({source: 'network-polygons', id: nFeatureId}, {'delete': true});
		}
	}
}


function mouseleaveNetworkPolygonsDel(oEvent)
{
	let nHover = g_nHoverId;
	oEvent.target.getCanvas().style.cursor = g_sCursor;
	if (nHover >= 0)
		oEvent.target.setFeatureState({source: 'network-polygons', id: nHover}, {'delete': false});
	
	g_nHoverId = undefined;
	oEvent.target.off('mousemove', 'network-polygons', mousemoveNetworkPolygonsDel);
}


function startCreateNetwork()
{
	clearInterval(g_nPollInterval);
	g_nPollInterval = undefined;
	
	g_oMap.off('click', 'network-polygons', selectNetwork);
	g_oMap.off('mouseenter', 'network-polygons', mouseenterNetworkPolygons);
	g_oMap.off('mouseleave', 'network-polygons', mouseleaveNetworkPolygons);
	g_oMap.off('mousemove', 'network-polygons', mousemoveNetworkPolygons);
	instructions(['Left-click: Initial point<br>Esc: Cancel', 'Create Network']);
	g_sCursor = 'crosshair';
	g_oMap.getCanvas().style.cursor = g_sCursor;
}


function submitNetwork()
{
	let sOptions = '';
	let oDialog = $(g_oDialogs['network']);
	if (g_oReprocess === undefined)
	{
		$('#dlgNetwork :checked').each(function(){sOptions += this.name + ','});
		$.ajax(
		{
			'url': 'api/generatenetwork/create', 
			'method': 'POST',
			'data': {'token': sessionStorage.token, 'coords': g_sNetworkCoords, 'options': sOptions.substring(0, sOptions.length - 1), 'label': $('#network-label').val()}
		}).done(function() 
		{
			removeSource('poly-bounds', g_oMap);
			removeSource('poly-outline', g_oMap);
			g_sCursor = '';
			g_oMap.getCanvas().style.cursor = g_sCursor;
			instructions(g_oInstructions['select'], 'Successful network creation request.');
			startSelectNetwork();
		}).fail(function() 
		{
			$('#instructions-error').html('Request failed. Try again later');
		});

		g_sCursor = 'wait';
		g_oMap.getCanvas().style.cursor = g_sCursor;
		$('#network-label').val('');
		$('#dlgNetwork :checked').each(function(){$(this).prop('checked', false);});
		oDialog.dialog('close');
	}
	else
	{
		$('#dlgNetwork :checked').each(function(){sOptions += this.name + ',';});
		$.ajax(
		{
			'url': 'api/generatenetwork/reprocess', 
			'method': 'POST',
			'data': {'token': sessionStorage.token, 'coords': g_sNetworkCoords, 'networkid': g_oReprocess.properties.networkid, 'filter': sOptions.substring(0, sOptions.length - 1), 'label': $('#network-label').val()}
		}).done(function() 
		{
			g_sCursor = '';
			g_oMap.getCanvas().style.cursor = g_sCursor;
			instructions(g_oInstructions['select'], 'Successful network reprocess request.');
			startSelectNetwork();
		}).fail(function() 
		{
			$('#instructions-error').html('Request failed. Try again later');
		});

		g_oReprocess = undefined;
		g_sCursor = 'wait';
		g_oMap.getCanvas().style.cursor = g_sCursor;
		$('#network-label').val('');
		$('#dlgNetwork :checked').each(function(){$(this).prop('checked', false);});
		oDialog.dialog('close');
	}
}


function cancelNetwork()
{
	if (g_oReprocess === undefined)
	{
		$('#network-label').val('');
		$('#btnCancelNetwork').button('disable');
		$('#enable-cancel').prop('checked', false);
		$('#dlgNetwork :checked').each(function(){$(this).prop('checked', false);});
		let oDialog = $(g_oDialogs['network']);
		oDialog.dialog('close');
		instructions(g_oInstructions['select'], 'Network creation canceled');
		removeSource('poly-bounds', g_oMap);
		removeSource('poly-outline', g_oMap);
		startSelectNetwork(true);
	}
	else
	{
		g_oReprocess = undefined;
		$('#network-label').val('');
		$('#btnCancelNetwork').button('disable');
		$('#enable-cancel').prop('checked', false);
		$('#dlgNetwork :checked').each(function(){$(this).prop('checked', false);});
		let oDialog = $(g_oDialogs['network']);
		oDialog.dialog('close');
		instructions(g_oInstructions['select'], 'Network reprocess canceled');
		startSelectNetwork(true);
	}
}


function initCreateNetwork()
{
	instructions(['Left-click: Add point<br>Enter: Finish polygon<br>Esc: Cancel', 'Create Network']);
}


function moveCreateNetwork(oEvent)
{
//	console.log(oEvent.point);
}


function addPointCreateNetwork(oEvent)
{
//	console.log(oEvent.lngLat);
}


function finishCreateNetwork(oGeometry)
{
	let sData = '';
	for (let nIndex = 0; nIndex < oGeometry.coordinates[0].length - 1; nIndex++)
	{
		let aCoord = oGeometry.coordinates[0][nIndex];
		sData += aCoord[0].toFixed(7) + ',' + aCoord[1].toFixed(7) + ',';
	}

	let oDialog = $(g_oDialogs['network']);
	g_sNetworkCoords = sData.substring(0, sData.length - 1);
	oDialog.dialog("open");
}


function cancelCreateNetwork()
{
	instructions(g_oInstructions['select'], 'Network creation canceled');
	startSelectNetwork(true);
}

function switchToMain()
{
	$('#main-control').show();
	$('#edit-control').hide();
	let oCancel = $(g_oDialogs['cancel']);
	if (oCancel.dialog('isOpen'))
		oCancel.dialog('close');
	let oRoadLegend = $(g_oDialogs['roadlegend']);
	if (oRoadLegend.dialog('isOpen'))
		oRoadLegend.dialog('close');
	$(g_oDialogs['networklegend']).dialog('open');
	document.activeElement.blur();
	turnOffAddRemove();
	turnOffMerge();
	turnOffSplit();
	
	g_oMap.off('mouseenter', 'geo-lines', hoverHighlight);
	g_oMap.off('mousemove', 'geo-lines', updateHighlight);
	g_oMap.off('mouseleave', 'geo-lines', unHighlight);
	removeSource('geo-lines', g_oMap);
	g_oMap.flyTo({center: [-98.585522, 39.8333333], zoom: 4});
	instructions(g_oInstructions['select']);
	startSelectNetwork();
}


function confirmCancel()
{
	if (g_nEdits > 0)
	{
		$(g_oDialogs['cancel']).dialog('open');
		document.activeElement.blur();
	}
	else
		switchToMain();
}

function exportOsm()
{
	$('#pageoverlay').show();
	$.ajax(
	{
		'url': 'api/generatenetwork/osm',
		'method': 'POST',
		'data': {'networkid': g_sCurNetwork, 'token': sessionStorage.token, 'label': $(g_oDialogs['networkmetadata']).dialog('option', 'title')}
	}).done(doneExportOsm).fail(function(){alert('Failed to export OSM. Try again later.');}).always(function(){$('#pageoverlay').hide();});
}

function doneExportOsm(data)
{
	window.location = data;
}


function finalizeNetwork()
{
	$.ajax(
	{
		'url': 'api/generatenetwork/finalize',
		'method': 'POST',
		'data': {'networkid': g_sCurNetwork, 'token': sessionStorage.token}
	}).done(function() {alert('Finalize process queued');});
}


function keyupExitAddRemove(oEvent)
{
	if (oEvent.which == 27) // esc
	{
		instructions(g_oInstructions['edit'], 'Exited add/remove mode.');
		g_nEditMode = 0;
		turnOffAddRemove();
	}
}

function startAddRemove()
{
	if (g_nEditMode == 0)
		instructions(g_oInstructions['add']);
	else if (g_nEditMode == 1)
	{
		instructions(g_oInstructions['edit'], 'Exited add/remove mode');
		turnOffAddRemove();
		g_nEditMode = 0;
		return;
	}
	else if (g_nEditMode == 2)
	{
		instructions(g_oInstructions['add'], 'Switched from merge to add/remove');
		turnOffMerge();
	}
	else if (g_nEditMode == 3)
	{
		instructions(g_oInstructions['add'], 'Switched from split to add/remove');
		turnOffSplit();
	}
	g_nEditMode = 1;
	instructions(g_oInstructions['add']);
	$(document).on('keyup', keyupExitAddRemove);
	g_oMap.on('mousemove', getHash);
	g_oMap.on('click', 'geo-lines', toggleInclude);
	g_oMap.on('mousemove', spotlight);
}

function turnOffAddRemove()
{
	g_oMap.off('mousemove', getHash);
	g_oMap.off('mousemove', spotlight);
	g_oMap.off('click', 'geo-lines', toggleInclude);
	let oLineSrc = g_oMap.getSource('geo-lines');
	let oData = oLineSrc._data;
	while (g_oSpotlight.length > 0)
	{
		let nId = g_oSpotlight.pop();
		if (!oData.features[nId].properties.include)
			oData.features[nId].properties.hidden = true;
	}
	oLineSrc.setData(oData);
	$(document).off('keyup', keyupExitAddRemove);
}


function startSplit()
{
	if (g_nEditMode == 0)
		instructions(g_oInstructions['split']);
	else if (g_nEditMode == 1)
	{
		turnOffAddRemove();
		instructions(g_oInstructions['split'], 'Switched from add/remove to split');
	}
	else if (g_nEditMode == 2)
	{
		turnOffMerge();
		instructions(g_oInstructions['split'], 'Switched from merge to split');
	}
	else if (g_nEditMode == 3)
	{
		instructions(g_oInstructions['edit'], 'Exited split mode');
		turnOffSplit();
		g_nEditMode = 0;
		return;
	}
	g_nEditMode = 3;

	g_oMap.on('click', 'geo-lines', addToSplit);
	g_oMap.on('click', 'split-points', selectSplitPoint);
	g_oMap.on('mouseenter', 'split-points', hoverHighlightPoint);
	g_oMap.on('mouseleave', 'split-points', unHighlightPoint);
	$(document).on('keyup', null, {'map': g_oMap}, splitFinish);
	g_oMap.addSource('split-points', {'type': 'geojson', 'data': {'type': 'FeatureCollection', 'features': []}});
	g_oMap.addLayer(g_oLayers['selected-point']);
	g_oMap.addLayer(g_oLayers['split-points']);
}


function addToSplit(oEvent)
{
	if (oEvent.features.length > 0)
	{
		let oSrc = g_oMap.getSource('split-points');
		let oData = oSrc._data;
		if (g_nSplitWay >= 0)
		{
			g_oMap.setFeatureState({source: 'geo-lines', id: g_nSplitWay}, {hover: false});
			while (oData.features.length > 0)
				oData.features.pop();
		}
		g_nSplitWay = oEvent.features[0].id;
		g_oMap.setFeatureState({source: 'geo-lines', id: g_nSplitWay}, {hover: true});
		let oLineSrc = g_oMap.getSource('geo-lines');
		let oLineData = oLineSrc._data;
		let nCount = 0;
		for (let oCoord of oLineData.features[g_nSplitWay].geometry.coordinates.values())
		{
			oData.features.push({'type': 'Feature', 'id': nCount++, 'properties': {'imrcpid': oLineData.features[g_nSplitWay].properties.imrcpid}, 'geometry': {'type': 'Point', 'coordinates': oCoord}});
		}
		oSrc.setData(oData);
		instructions(['Left-click node to split road<br>Esc: Cancel', 'Split Road'], '', '');
	}
}


function selectSplitPoint(oEvent)
{
	if (oEvent.features.length > 0)
	{
		let oSrc = g_oMap.getSource('split-points');
		let oData = oSrc._data;
		if (g_nSplitNode >= 0)
		{
			oData.features[g_nSplitNode].properties.selected = false;
		}
		g_nSplitNode = oEvent.features[0].id;
		oData.features[g_nSplitNode].properties.selected = true;
		oSrc.setData(oData);
		instructions(['Left-click: Select a different road or node<br>Enter: Commit split<br>Esc: Cancel', 'Split Road']);
	}
}


function turnOffSplit()
{
	g_oMap.off('click', 'geo-lines', addToSplit);
	g_oMap.off('click', 'split-points', selectSplitPoint);
	g_oMap.off('mouseenter', 'split-points', hoverHighlightPoint);
	g_oMap.off('mouseleave', 'split-points', unHighlightPoint);
	g_oMap.off('mousemove', 'split-points', updateHighlightPoint);
	$(document).off('keyup', splitFinish);
	removeSource('split-points', g_oMap);
	if (g_nSplitWay >= 0)
		g_oMap.setFeatureState({source: 'geo-lines', id: g_nSplitWay}, {hover: false});
	g_nSplitWay = undefined;
	g_nSplitNode = undefined;
}

function splitFinish(oEvent)
{
	if (oEvent.which == 27) // esc
	{
		instructions(g_oInstructions['edit'], 'Canceled split');
		g_nEditMode = 0;
		turnOffSplit(oEvent);
	}
	if (oEvent.which == 13) // enter
	{
		if (g_nSplitWay === undefined || g_nSplitNode === undefined)
		{
			return;
		}
		
		let sWayImrcp = g_oMap.getSource('geo-lines')._data.features[g_nSplitWay].properties.imrcpid;
		let sNodeImrcp = g_oMap.getSource('split-points')._data.features[g_nSplitNode].properties.imrcpid;
		$.ajax(
		{
			'url': 'api/generatenetwork/split',
			'method': 'POST',
			'node': g_nSplitNode,
			'way': g_nSplitWay,
			'data': {'networkid': g_sCurNetwork, 'token': sessionStorage.token, 'way': sWayImrcp, 'node': g_nSplitNode.toString()}
		}).done(splitSuccess).fail(function()
		{
			instructions(g_oInstructions['edit'], '', 'Split failed');;
		});
		
		g_nEditMode = 0;
		turnOffSplit();
	}
}


function splitSuccess(data, status, jqXHR)
{
	g_nEdits++;
	let oSrc = g_oMap.getSource('geo-lines');
	let oData = oSrc._data;
	for (let oWay of data.values())
	{
		g_oWays[oWay.properties.imrcpid] = oWay;
		oWay.properties.include = true;
		oWay.properties.hidden = false;
		oData.features.push(oWay);
	}

	oData.features[this.way].geometry.coordinates = [];
	oSrc.setData(oData);
	instructions(g_oInstructions['edit'], 'Split succeeded');
}


function startMerge()
{
	if (g_nEditMode == 0)
		instructions(g_oInstructions['merge']);
	else if (g_nEditMode == 1)
	{
		turnOffAddRemove();
		instructions(g_oInstructions['merge'], 'Switched from add/remove to merge');
	}
	else if (g_nEditMode == 2)
	{
		instructions(g_oInstructions['edit'], 'Exited merge mode');
		turnOffMerge();
		g_nEditMode = 0;
		return;
	}
	else if (g_nEditMode == 3)
	{
		instructions(g_oInstructions['merge'], 'Switched from split to merge');
		turnOffSplit();
	}
	g_nEditMode = 2;
	g_oMerge = [];
	g_oMap.on('click', 'geo-lines', addToMerge);
	$(document).on('keyup', mergeFinish);
}


function addToMerge(oEvent)
{
	if (oEvent.features.length > 0)
	{
		if (oEvent.features[0].properties.hidden)
			return;
		let nNewId = oEvent.features[0].id;
		for (let nIndex = 0; nIndex < g_oMerge.length; nIndex++)
		{
			if (g_oMerge[nIndex] == nNewId)
			{
				g_oMap.setFeatureState({source: 'geo-lines', id: nNewId}, {hover: false});
				g_oMerge.splice(nIndex, 1);
				return;
			}
		}
		g_oMerge.push(nNewId);
		g_oMap.setFeatureState({source: 'geo-lines', id: nNewId}, {hover: true});
		while (g_oMerge.length > 2)
		{
			g_oMap.setFeatureState({source: 'geo-lines', id: g_oMerge[0]}, {hover: false});
			g_oMerge.splice(0, 1);
		}
	}
}


function turnOffMerge()
{
	g_oMap.off('click', 'geo-lines', addToMerge);
	while (g_oMerge.length > 0)
	{
		let nId = g_oMerge.pop();
		g_oMap.setFeatureState({source: 'geo-lines', id: nId}, {hover: false});
	}
	$(document).off('keyup', mergeFinish);
}

function mergeFinish(oEvent)
{
	if (oEvent.which == 27) // esc or enter
	{
		instructions(g_oInstructions['edit'], 'Canceled merge');
		g_nEditMode = 0;
		turnOffMerge();
	}
	if (oEvent.which == 13) // enter
	{
		if (g_oMerge.length != 2)
		{
			alert('Must select 2 roads to merge');
			return;
		}
		
		let oLineSrc = g_oMap.getSource('geo-lines');
		let oData = oLineSrc._data;
		let aImrcpIds = [];
		for (let nId of g_oMerge.values())
		{
			aImrcpIds.push(oData.features[nId].properties.imrcpid);
		}
		$.ajax(
		{
			'url': 'api/generatenetwork/merge',
			'method': 'POST',
			'ids': [g_oMerge[0], g_oMerge[1]],
			'data': {'networkid': g_sCurNetwork, 'token': sessionStorage.token, 'id1': aImrcpIds[0], 'id2': aImrcpIds[1]}
		}).done(mergeSuccess).fail(function()
		{
			instructions(g_oInstructions['edit'], '', 'Merge failed');
		});
		g_nEditMode = 0;
		turnOffMerge();
	}
}


function mergeSuccess(data, status, jqXHR)
{
	g_nEdits++;
	g_oWays[data.properties.imrcpid] = data;
	data.properties.include = true;
	data.properties.hidden = false;
	let oSrc = g_oMap.getSource('geo-lines');
	let oData = oSrc._data;
	oData.features.push(data);
	oData.features[this.ids[0]].geometry.coordinates = [];
	oData.features[this.ids[1]].geometry.coordinates = [];
	oSrc.setData(oData);
	instructions(g_oInstructions['edit'], 'Merge succeeded');
}


function toggleInclude(oEvent)
{
	if (oEvent.features.length > 0)
	{
		let oSrc = oEvent.target.getSource('geo-lines');
		let oData = oSrc._data;
		let oFeature = oData.features[oEvent.features[0].id];
		
		if (oFeature.properties.include === undefined)
		{
			$.ajax(
			{
				'method': 'POST',
				'url': 'api/generatenetwork/add',
				'data': {'networkid': g_sCurNetwork, 'id': oFeature.properties.imrcpid, 'token': sessionStorage.token}
			}).done(function() 
			{
				oFeature.properties.include = true;
				oSrc.setData(oData);
				g_nEdits++;
				instructions(g_oInstructions['add'], 'Add succeeded.');
			}).fail(function()
			{
				instructions(g_oInstructions['add'], '', 'Add failed.');
			});
		}
		else
		{
			let sMode = oFeature.properties.include ? 'remove' : 'add';
			$.ajax(
			{
				'method': 'POST',
				'url': 'api/generatenetwork/' + sMode,
				'data': {'networkid': g_sCurNetwork, 'id': oFeature.properties.imrcpid, 'token': sessionStorage.token}
			}).done(function() 
			{
				oFeature.properties.include = !oFeature.properties.include;
				oSrc.setData(oData);
				g_nEdits++;
				instructions(g_oInstructions['add'], sMode.charAt(0).toUpperCase() + sMode.slice(1) + ' succeeded.');
			}).fail(function()
			{
				instructions(g_oInstructions['add'], '', sMode.charAt(0).toUpperCase() + sMode.slice(1) + ' failed.');
			});
		}
	}
}


function saveChanges()
{
	if (g_nEdits > 0)
	{
		if (g_nEditMode == 1)
			turnOffAddRemove();
		else if (g_nEditMode == 2)
			turnOffMerge();
		else if (g_nEditMode == 3)
			turnOffSplit();
		g_nEditMode = 0;
		$('#edit-control button').prop('disabled', true);
		instructions(['', 'Save Changes'], 'Saving...');
		$.ajax(
		{
			'url': 'api/generatenetwork/save',
			'method': 'POST',
			'data': {'networkid': g_sCurNetwork, 'token': sessionStorage.token}
		}).done(function(data, textStatus, jqXHR)
		{
			instructions(g_oInstructions['edit'], 'Saved changes');
			$.ajax(
			{
				'url': 'api/generatenetwork/geo',
				'dataType': 'json',
				'method': 'POST',
				'networkid': g_sCurNetwork,
				'data': {'token': sessionStorage.token, 'networkid': g_sCurNetwork}
			}).done(selectSuccess).fail(function() 
			{
				instructions(g_oInstructions['select'], '', 'Failed to retrieve network')
				g_oMap.getCanvas().style.cursor = g_sCursor;
				$('#edit-control button').prop('disabled', false);
			});
		}).fail(function()
		{
			instructions(g_oInstructions['edit'], '', 'Failed to save changes');
		});
	}
}


function hoverHighlight(oEvent)
{
	let nHover = g_nHoverId;
	if (oEvent.features.length > 0)
	{
		if (oEvent.features[0].properties.hidden)
			return;
		g_oMap.getCanvas().style.cursor = 'pointer';
		g_oPopup.setLngLat(oEvent.lngLat);
		if (nHover >= 0)
		{
			let bMerge = false;
			for (let nId of g_oMerge.values())
			{
				if (nId == nHover)
				{
					bMerge = true;
					break;
				}
			}
			let bSplit = g_nSplitWay != null && g_nSplitWay == nHover;
			if (!bMerge && !bSplit)
				g_oMap.setFeatureState({source: 'geo-lines', id: nHover}, {hover: false});
		}
		g_nHoverId = oEvent.features[0].id;
		g_oMap.on('mousemove', 'geo-lines', updateHighlight);
		g_oMap.setFeatureState({source: 'geo-lines', id: oEvent.features[0].id}, {hover: true});
		if (g_nEditMode === 0)
		{
			let sPopup = '';
			if (oEvent.features[0].properties.linktype)
				sPopup += oEvent.features[0].properties.linktype;
			else if (oEvent.features[0].properties.ingress)
				sPopup += `ingress: ${oEvent.features[0].properties.ingress}<br>egress: ${oEvent.features[0].properties.egress}`;
			else
				sPopup += 'ramp not set';
			g_oPopup.setHTML(sPopup).addTo(g_oMap);
			g_oMap.on('mousemove', mousemovePopupPos);
		}
	}
}


function updateHighlight(oEvent)
{
	let nHover = g_nHoverId;
	if (oEvent.features.length > 0)
	{
		if (oEvent.features[0].properties.hidden)
			return;
		g_oMap.getCanvas().style.cursor = 'pointer';
		if (nHover >= 0)
		{
			let bMerge = false;
			for (let nId of g_oMerge.values())
			{
				if (nId == nHover)
				{
					bMerge = true;
					break;
				}
			}
			let bSplit = g_nSplitWay != null && g_nSplitWay == nHover;
			if (!bMerge && !bSplit)
				g_oMap.setFeatureState({source: 'geo-lines', id: nHover}, {hover: false});
		}
		g_nHoverId = oEvent.features[0].id;
		g_oMap.setFeatureState({source: 'geo-lines', id: oEvent.features[0].id}, {hover: true});
		if (g_nEditMode === 0)
		{
			let sPopup = '';
			if (oEvent.features[0].properties.linktype)
				sPopup += oEvent.features[0].properties.linktype;
			else if (oEvent.features[0].properties.ingress)
				sPopup += `ingress: ${oEvent.features[0].properties.ingress}<br>egress: ${oEvent.features[0].properties.egress}`;
			else
				sPopup += 'ramp not set';
			g_oPopup.setHTML(sPopup);
		}
	}
}


function unHighlight(oEvent)
{
	let nHover = g_nHoverId;
	g_nHoverId = undefined;
	oEvent.target.off('mousemove', 'geo-lines', updateHighlight);
	if (nHover >= 0)
	{
		let bMerge = false;
		for (let nId of g_oMerge.values())
		{
			if (nId == nHover)
			{
				bMerge = true;
				break;
			}
		}
		let bSplit = g_nSplitWay != null && g_nSplitWay == nHover;
		if (!bMerge && !bSplit)
			oEvent.target.setFeatureState({source: 'geo-lines', id: nHover}, {hover: false});
	}
	g_oPopup.remove();
	oEvent.target.off('mousemove', mousemovePopupPos);
	oEvent.target.getCanvas().style.cursor = g_sCursor;
}


function hoverHighlightPoint(oEvent)
{
	let nHover = g_nHoverPointId;
	if (oEvent.features.length > 0)
	{
		let sLayer = oEvent.features[0].layer.id;
		let sSrc = oEvent.features[0].layer.source;
		g_sCurPointHoverLayer = sLayer;
		g_sCurPointHoverSource = sSrc;
		g_nHoverPointId = oEvent.features[0].id;
		g_oMap.on('mousemove', sLayer, updateHighlightPoint);
		g_oMap.getCanvas().style.cursor = 'pointer';
		if (nHover >= 0)
		{
			g_oMap.setFeatureState({source: sSrc, id: nHover}, {hover: false});
		}
		g_oMap.setFeatureState({source: sSrc, id: oEvent.features[0].id}, {hover: true});
	}
}


function updateHighlightPoint(oEvent)
{
	let nHover = g_nHoverPointId;
	if (oEvent.features.length > 0)
	{
		g_nHoverPointId = oEvent.features[0].id;
		let sSrc = oEvent.features[0].layer.source;
		g_sCurPointHoverLayer = oEvent.features[0].layer.id;
		g_sCurPointHoverSource = sSrc;
		g_oMap.getCanvas().style.cursor = 'pointer';
		if (nHover >= 0)
		{
			g_oMap.setFeatureState({source: sSrc, id: nHover}, {hover: false});
		}
		g_oMap.setFeatureState({source: sSrc, id: oEvent.features[0].id}, {hover: true});
	}
}


function unHighlightPoint(oEvent)
{
	let nHover = g_nHoverPointId;
	g_nHoverPointId = undefined;
	oEvent.target.off('mousemove', g_sCurPointHoverLayer, updateHighlightPoint);
	if (nHover >= 0)
	{
		oEvent.target.setFeatureState({source: g_sCurPointHoverSource, id: nHover}, {hover: false});
	}
	
	oEvent.target.getCanvas().style.cursor = g_sCursor;
}


function getHash({target, lngLat, point})
{
	if (target.getZoom() < 12 || g_bLoadingHash)
		return;
	let sHash = hashLonLat(lngLat.lng, lngLat.lat);
	if (sHash !== g_sCurHash)
	{
		g_sCurHash = sHash;
		if (g_oHashes[sHash] === undefined)
		{
			g_bLoadingHash = true;
			g_oHashes[sHash] = {};
			g_sCursor = 'progress';
			target.getCanvas().style.cursor = g_sCursor;
			$.ajax('api/generatenetwork/hash',
			{
				'dataType': 'json',
				'method': 'POST',
				'map': target,
				'hash': sHash,
				'data': {'token': sessionStorage.token, 'networkid': g_sCurNetwork, 'lon': lngLat.lng.toString(), 'lat': lngLat.lat.toString(), 'hash': sHash}
			}).done(hashSuccess).fail(function() {g_bLoadingHash = false; g_sCursor = ''; target.getCanvas().style.cursor = g_sCursor; g_oHashes[sHash] = undefined;});
		}
	}
}


function hashSuccess(data, textStatus, jqXHR)
{
	g_oHashes[this.hash] = 'loaded';
	
	if (data.lines !== undefined)
	{
		let oLineSrc = g_oMap.getSource('geo-lines');
		let oData = oLineSrc._data;
		for (let oFeature of data.lines.values())
		{
			if (g_oWays[oFeature.properties.imrcpid] === undefined)
			{
				g_oWays[oFeature.properties.imrcpid] = oFeature;
				oFeature.properties.include = false;
				oFeature.properties.hidden = true;
				oData.features.push(oFeature);
			}
		}
		oLineSrc.setData(oData)
	}
	
	g_sCursor = '';
	g_oMap.getCanvas().style.cursor = g_sCursor;
	g_bLoadingHash = false;
}

function spotlight(oEvent)
{
	let oLineSrc = g_oMap.getSource('geo-lines');
	let oData = oLineSrc._data;
	while (g_oSpotlight.length > 0)
	{
		let nId = g_oSpotlight.pop();
		if (!oData.features[nId].properties.include)
			oData.features[nId].properties.hidden = true;
	}
	
	if (g_oMap.getZoom() < 12)
	{
		oLineSrc.setData(oData);
		return;
	}
	let oFeatures = g_oMap.queryRenderedFeatures([[oEvent.point.x - 50, oEvent.point.y + 50], [oEvent.point.x + 50, oEvent.point.y - 50]], {'layers': ['geo-lines']});
	
	for (let oFeature of oFeatures.values())
	{
		if (oFeature.properties.include)
			continue;
		
		oData.features[oFeature.id].properties.hidden = false;
		g_oSpotlight.push(oFeature.id);
	}
	
	oLineSrc.setData(oData);
}


function toIntDeg(dVal)
{
	return Math.round((dVal + 0.00000005) * 10000000.0);
}

function hashLonLat(dLon, dLat)
{
	return (getBucket(toIntDeg(dLon)) << 16) | (getBucket(toIntDeg(dLat)) & 0xffff); // 16-bit hashLonLat index by lat/lon
}

function getBucket(nVal)
{
	return Math.floor(Math.floor(nVal, BUCKET_SPACING) / BUCKET_SPACING)
}

function getFeatureId(oEvent)
{
	let nFeatureId = -1;
	if (oEvent.features.length > 0)
	{
		if (oEvent.features.length == 1)
		{
			nFeatureId = oEvent.features[0].id;
		}
		else
		{
			let nMinArea = Number.MAX_VALUE;
			let oData = g_oMap.getSource('network-polygons')._data;
			
			for (let nIndex = 0; nIndex < oEvent.features.length; nIndex++)
			{
				let aBbox = getPolygonBoundingBox(oData.features[oEvent.features[nIndex].id]);
				
				let nArea = (aBbox[1][0] - aBbox[0][0]) * (aBbox[1][1] - aBbox[0][1]);
				if (nArea < nMinArea)
				{
					nMinArea = nArea;
					nFeatureId = oEvent.features[nIndex].id;
				}
			}
			g_oMap.getSource('network-polygons').setData(oData);
		}
	}
	
	return nFeatureId;
}


function instructions(aContent, sStatus, sError)
{
	$('#instructions').html(aContent[0]);
	$(g_oDialogs['instructions']).dialog('option', 'title', aContent[1]);
	if (sStatus)
		$('#instructions-status').html(sStatus);
	else
		$('#instructions-status').html('');
	if (sError)
		$('#instructions-error').html(sError);
	else
		$('#instructions-error').html('');
}

function setCursor(sCursor)
{
	g_sCursor = sCursor;
}


$(document).on('initPage', initialize);

export {g_oMap, g_sCurNetwork, g_oCurBounds, g_oDialogs, g_sCursor, g_oInstructions, instructions, unHighlightPoint,
		hoverHighlightPoint, hoverHighlight, unHighlight, updateHighlight, updateHighlightPoint, submitNetwork, cancelNetwork, toggleDialog,
		turnOffAddRemove, turnOffMerge, turnOffSplit, startSelectNetwork, setCursor, cancelReprocess, confirmReprocess};