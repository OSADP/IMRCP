import {g_oLayers, removeSource, getPolygonBoundingBox, startDrawPoly, getLineStringBoundingBox, binarySearch, 
	isFeatureInsidePolygonFeature, pointToPaddedBounds, mapOffBoundFn, addStyleRule} from './map-util.js';
import {minutesToHHmm, minutesToHH} from './common.js';
import {loadSettings} from './map-settings.js';
import './jquery/jquery.datetimepicker.full.js';

window.g_oRequirements = {'groups': 'imrcp-user'};
String.format = function(sVal)
{
	let sArgs = Array.prototype.slice.call(arguments, 1);
	return sVal.replace(/{(\d+)}/g, function(match, number)
	{
		return typeof sArgs[number] != 'undefined' ? sArgs[number] : match;
	});
};

let g_oMap;
let g_oHovers = {};
let g_oSettings;
let g_oNetworksBoundingBox = [[Number.MAX_VALUE, Number.MAX_VALUE], [Number.MIN_SAFE_INTEGER, Number.MIN_SAFE_INTEGER]];
let g_sLoadedNetwork;
let g_bClearSelection = true;
let g_nNetworks;
let g_aGroups = [];
let g_oCurrentGroup;
let g_bClicked = false;
let g_aActions = [['plowed', 'checkbox"'], ['treated', 'checkbox"'], ['vsl', 'text" maxlength="2"', 'spdlimit'], ['lanes', 'text" maxlength="1"', 'lanecount']];
let g_nHours = 24;
let g_nSelectedScenario = -1;
let g_oScenarios;
let g_bSaveInstructions = false;
let g_bRunInstructions = false;
// let g_nColors = ['#1e7145', '#603cba', '#00aba9', '#2d89ef', '#ffc40d', '#ee1111', '#00a300', '#e3a21a'];
let g_nColors = ['#c00', '#0c0', '#00f', '#cc0', '#0cc', '#f0f', '#f90', '#9f0', '#90f'];
let g_oImrcpIds = {};
let ERROR = 0;
let ADDREMOVE = 1;
let SELECTEDIT = 2;
let VIEWGROUPS = 3;
let SELECTNETWORK = 4;
let STATE;




async function initialize()
{
	$('#pageoverlay').html(`<p class="centered-element">Initializing...</p>`).css({'opacity': 0.5, 'font-size': 'x-large'}).show();
	$(document).prop('title', 'IMRCP Scenario Creation - ' + sessionStorage.uname);
	let pNetworks = $.ajax(
	{
		'url': 'api/generatenetwork/list',
		'dataType': 'json',
		'method': 'POST',
		'data': {'token': sessionStorage.token}
	}).promise();
	let pProfile = $.ajax(
	{
		'url': 'api/dashboard/profile',
		'dataType': 'json',
		'method': 'POST',
		'data': {'token': sessionStorage.token}
	}).promise();
	let pScenarios = $.ajax(
	{
		'url': 'api/scenarios/list',
		'dataType': 'json',
		'method': 'POST',
		'data': {'token': sessionStorage.token}
	}).promise();
	let pObstypes = $.getJSON('obstypes.json').promise();
	g_oSettings = await loadSettings;
	let oMap = new mapboxgl.Map({container: 'mapid', style: 'mapbox/light-v9.json', attributionControl: false, minZoom:4, maxZoom: 24, center: g_oSettings.map.center, zoom: g_oSettings.map.zoom, accessToken: 'pk.eyJ1Ijoia3J1ZWdlcmIiLCJhIjoiY2l6ZDl4dTlwMjJvaDJ3bW44bXFkd2NrOSJ9.KXqbeWgASgEUYQu0oi7Hbg'});
	g_oMap = oMap;
	oMap.doubleClickZoom.disable();
	oMap.dragRotate.disable();
	oMap.touchZoomRotate.disableRotation();
	oMap.addControl(new mapboxgl.NavigationControl({showCompass: false}));
	
	oMap.on('load', async function() 
	{
		
		$('#dlgStatus').dialog({autoOpen: false, position: {my: "center", at: "center", of: "#map-container"}, modal: true, draggable: false, resizable: false, width: 400});
		buildInstructionDialog();	
		buildDtp();
		buildLoad();
//		buildSave();
		buildRun();
		buildConfirmRestart();
		buildConfirmDelete();
		buildConfirmDeleteTemplate();
		buildConfirmOverwrite();
		buildValues();
		buildGroupListDialog();
		
		g_oScenarios = await pScenarios;
		let oAllNetworks = await pNetworks;
		let oProfile = await pProfile;
		let oNetworks = {'type': 'geojson', 'maxzoom': 9, 'data': {'type': 'FeatureCollection', 'features': []}, 'generateId': true};
		for (let oNetwork of oAllNetworks.values())
		{
			for (let oProfileNetwork of oProfile.networks.values())
			{
				if (oProfileNetwork.id === oNetwork.properties.networkid)
				{
					oNetworks.data.features.push(oNetwork);
					for (let aCoord of oNetwork.geometry.coordinates[0].values())
					{
						if (aCoord[0] < g_oNetworksBoundingBox[0][0])
							g_oNetworksBoundingBox[0][0] = aCoord[0];
						
						if (aCoord[1] < g_oNetworksBoundingBox[0][1])
							g_oNetworksBoundingBox[0][1] = aCoord[1];
						
						if (aCoord[0] > g_oNetworksBoundingBox[1][0])
							g_oNetworksBoundingBox[1][0] = aCoord[0];
						
						if (aCoord[1] > g_oNetworksBoundingBox[1][1])
							g_oNetworksBoundingBox[1][1] = aCoord[1];
					}
				}
			}
		}
		
		g_nNetworks = oNetworks.data.features.length;
		g_oMap.addSource('network-polygons', oNetworks);
		g_oMap.addLayer(g_oLayers['network-polygons-report']);
		
		let oCan = document.createElement('canvas');
		oCan.width = 24;
		oCan.height = 24;
		
		let ctx = oCan.getContext('2d');
		ctx.fillStyle = '#179b54';
		ctx.font = '24px FontAwesome';
		ctx.textAlign = 'center';
		ctx.textBaseline = 'middle';
		ctx.fillText('\uf067', 12, 12);
		addStyleRule('.pluscursor', `cursor: url('${oCan.toDataURL('image/png')}') 12 12, auto`);
		ctx.clearRect(0, 0, oCan.width, oCan.height);
		ctx.fillStyle = '#b2000c';
		ctx.fillText('\uf068', 12, 12);
		addStyleRule('.minuscursor', `cursor: url('${oCan.toDataURL('image/png')}') 12 12, auto`);
		ctx.clearRect(0, 0, oCan.width, oCan.height);
		ctx.fillStyle = '#fff';
		ctx.arc(12, 12, 10, 0, 2 * Math.PI);
		ctx.fill();
		ctx.fillStyle = '#b2000c';
		ctx.fillText('\uf05e', 12, 12);
		addStyleRule('.bancursor', `cursor: url('${oCan.toDataURL('image/png')}') 12 12, auto`);
		$('#pageoverlay').hide();
		if (g_oScenarios.length === 0)
			$('#loadScenario').prop('disabled', true).addClass('ui-button-disabled ui-state-disabled');
		selectANetwork();
	});
}


function selectANetwork()
{
	switchState(SELECTNETWORK);
	if (g_oMap.getLayer('network-polygons-report') === undefined)
		g_oMap.addLayer(g_oLayers['network-polygons-report']);
	
	if (g_oMap.getLayer('geo-lines-scenario') !== undefined)
		g_oMap.removeLayer('geo-lines-scenario');
	g_oMap.fitBounds(g_oNetworksBoundingBox, {'padding': 50});
	if (g_nNetworks === 1)
	{
		loadANetwork(g_oMap.getSource('network-polygons')._data);
	}
	else
	{
		g_oMap.on('mouseenter', 'network-polygons-report', hoverHighlight);
		g_oMap.on('click', 'network-polygons-report', loadANetwork);
	}
}


function loadANetwork(oEvent)
{
	let oNetwork = oEvent.features[0];
	if (g_nNetworks === 1)
		g_oMap.fitBounds(getPolygonBoundingBox(oNetwork), {'padding': 50});
	else
	{
		if (g_sLoadedNetwork !== undefined && oEvent.features.length > 1)
		{
			for (let nIndex = 0; nIndex < oEvent.features.length; nIndex++)
			{
				let oTemp = oEvent.features[nIndex];
				if (!oTemp.hasOwnProperty('id'))
					oTemp.id = nIndex;
				if (oTemp.properties.networkid === g_sLoadedNetwork)
					oNetwork = oTemp;
			}
		}
		g_oMap.fitBounds(getPolygonBoundingBox(g_oMap.getSource('network-polygons')._data.features[oNetwork.id]), {'padding': 50});
	}
	g_oMap.off('mouseenter', 'network-polygons-report', hoverHighlight);
	mapOffBoundFn(g_oMap, 'mousemove', unHighlight.name);
	g_oMap.off('click', 'network-polygons-report', loadANetwork);
	if (g_oHovers['network-polygons-report'] !== undefined)
	{
		g_oMap.setFeatureState(g_oHovers['network-polygons-report'], {'hover': false});
		g_oHovers['network-polygons-report'] = undefined;
	}
	if (oNetwork.properties.networkid === g_sLoadedNetwork)
	{
		if (g_oMap.getLayer('network-polygons-report') !== undefined)
			g_oMap.removeLayer('network-polygons-report');
		if (g_oMap.getLayer('geo-lines-scenario') === undefined)
			g_oMap.addLayer(g_oLayers['geo-lines-scenario']);
		startScenario();
	}
	else
	{
		$('#pageoverlay').html(`<p class="centered-element">Loading network: ${oNetwork.properties.label}</p>`).css({'opacity': 0.5, 'font-size': 'x-large'}).show();
		$.ajax(
		{
			'url': 'api/generatenetwork/geo',
			'dataType': 'json',
			'method': 'POST',
			'networkid': oNetwork.properties.networkid,
			'data': {'token': sessionStorage.token, 'networkid': oNetwork.properties.networkid}
		}).done(geoSuccess).fail(function() 
		{
			$('#pageoverlay').html(`<p class="centered-element">Failed to retrieve network: "${oNetwork.properties.label}"<br>Try again later.</p>`);
			if (g_nNetworks > 1)
				selectANetwork();
		});
	}
	
}

function geoSuccess(oData, sStatus, oJqXHR)
{
	removeSource('geo-lines', g_oMap);
	g_oMap.addSource('geo-lines', {'type': 'geojson', 'data': {'type': 'FeatureCollection', 'features': []}, 'generateId': true});
	let oLineData = g_oMap.getSource('geo-lines')._data;
	oLineData.features = [];
	let nCount = 0;
	for (let oFeature of oData.values())
	{
		getLineStringBoundingBox(oFeature);
		oLineData.features.push(oFeature);
		oFeature.id = nCount++;
		g_oImrcpIds[oFeature.properties.imrcpid] = oFeature.id;
		oFeature.source = 'geo-lines';
	}
	
	g_oMap.getSource('geo-lines').setData(oLineData);
	g_oMap.on('sourcedata', finishedStyling);
	g_oMap.addLayer(g_oLayers['geo-lines-scenario'], 'road-number-shield');
	$(g_oMap.getCanvas()).removeClass('clickable');
	g_sLoadedNetwork = this.networkid;
	startScenario();
}

function startScenario()
{
	$('#dlgGroupList').dialog('open');
	switchState(VIEWGROUPS, VIEWGROUPS);
}

function hoverHighlight(oEvent)
{
	if (oEvent.features.length > 0)
	{
		let oFeature = oEvent.features[0];
		let bInclude = g_oMap.getFeatureState(oFeature).include;
		if (bInclude === undefined || STATE === SELECTEDIT)
			$(g_oMap.getCanvas()).addClass('clickable');
		else if (Object.keys(g_oCurrentGroup.segments).length > 0 && (g_oCurrentGroup.spdlimit !== oFeature.properties.spdlimit || g_oCurrentGroup.lanecount !== oFeature.properties.lanecount))
			$(g_oMap.getCanvas()).addClass('bancursor');
		else if (bInclude)
			$(g_oMap.getCanvas()).addClass('minuscursor');
		else
			$(g_oMap.getCanvas()).addClass('pluscursor');
		let oHover = g_oHovers[oFeature.layer.id];
		if (oHover !== undefined)
		{
			g_oMap.setFeatureState(oHover, {'hover': false});
		}
		g_oHovers[oFeature.layer.id] = oFeature;
		g_oMap.on('mousemove', oFeature.layer.id, updateHighlight);
		g_oMap.on('mousemove', unHighlight.bind({'layer': oFeature.layer.id}));
		g_oMap.setFeatureState(oFeature, {'hover': true});
	}
}


function updateHighlight(oEvent)
{
	if (oEvent.features.length > 0)
	{
		let oFeature = oEvent.features[0];
		let oHover = g_oHovers[oFeature.layer.id];
				
		if (oHover !== undefined)
		{
			g_oMap.setFeatureState(oHover, {'hover': false});
			$(g_oMap.getCanvas()).removeClass('clickable minuscursor pluscursor bancursor');
		}
		let bInclude = g_oMap.getFeatureState(oFeature).include;
		if (bInclude === undefined || STATE === SELECTEDIT)
			$(g_oMap.getCanvas()).addClass('clickable');
		else if (Object.keys(g_oCurrentGroup.segments).length > 0 && (g_oCurrentGroup.spdlimit !== oFeature.properties.spdlimit || g_oCurrentGroup.lanecount !== oFeature.properties.lanecount))
			$(g_oMap.getCanvas()).addClass('bancursor');
		else if (bInclude)
			$(g_oMap.getCanvas()).addClass('minuscursor');
		else
			$(g_oMap.getCanvas()).addClass('pluscursor');
		g_oHovers[oFeature.layer.id] = oFeature;
		g_oMap.setFeatureState(oFeature, {'hover': true});
	}
}


function unHighlight(oEvent)
{
	if (g_oMap.queryRenderedFeatures(pointToPaddedBounds(oEvent.point), {'layers': [this.layer]}).length > 0)
		return;
	let oHover = g_oHovers[this.layer];
	g_oHovers[this.layer] = undefined;
	if (oHover !== undefined)
	{
		g_oMap.setFeatureState(oHover, {'hover': false});
	}
	
	g_oMap.off('mousemove', this.layer, updateHighlight);
	mapOffBoundFn(g_oMap, 'mousemove', unHighlight.name);
	$(g_oMap.getCanvas()).removeClass('clickable minuscursor pluscursor bancursor');
}


function toggleInclude(oEvent)
{
	let oFeatures = g_oMap.queryRenderedFeatures(pointToPaddedBounds(oEvent.point), {'layers': ['geo-lines-scenario']});
	if (oFeatures.length > 0)
	{
		let oTemp;
		for (let oFeature of oFeatures.values())
		{
			if (oFeature.id === g_oHovers['geo-lines-scenario'].id)
			{
				oTemp = oFeature;
				break;
			}
		}
		if (oTemp === undefined || (Object.keys(g_oCurrentGroup.segments).length > 0 && (g_oCurrentGroup.spdlimit !== oTemp.properties.spdlimit || g_oCurrentGroup.lanecount !== oTemp.properties.lanecount)))
			return;

		let sColor = '#000';
		if (g_oMap.getFeatureState(oTemp).include)
		{
			$(g_oMap.getCanvas()).removeClass('minuscursor').addClass('pluscursor');
			delete g_oCurrentGroup.segments[oTemp.properties.imrcpid];
			if (Object.keys(g_oCurrentGroup.segments).length === 0)
			{
				g_oCurrentGroup.spdlimit = Number.MIN_SAFE_INTEGER;
				g_oCurrentGroup.lanecount = Number.MIN_SAFE_INTEGER;
			}
		}
		else
		{
			$(g_oMap.getCanvas()).removeClass('pluscursor').addClass('minuscursor');
			g_oCurrentGroup.segments[oTemp.properties.imrcpid] = oTemp;
			sColor = g_nColors[g_oCurrentGroup.index % g_nColors.length];
			if (Object.keys(g_oCurrentGroup.segments).length === 1)
			{
				g_oCurrentGroup.spdlimit = oTemp.properties.spdlimit;
				g_oCurrentGroup.lanecount = oTemp.properties.lanecount;
			}
		}
		g_oMap.setFeatureState(oTemp, {'include': !g_oMap.getFeatureState(oTemp).include, 'color': sColor});
		let oSaveBtn = $('#saveScenario');
		if (oSaveBtn.prop('disabled'))
		{
			oSaveBtn.prop('disabled', false).removeClass('ui-button-disabled ui-state-disabled');
			g_bSaveInstructions = true;
		}
		let oRunBtn = $('#runScenario');
		if (!oRunBtn.prop('disabled'))
		{
			oRunBtn.prop('disabled', true).addClass('ui-button-disabled ui-state-disabled');
			g_bRunInstructions = false;
		}
	}
}


function finishedStyling(oEvent)
{
	let oFeatures = g_oMap.getSource('geo-lines')._data.features;
	for (let nIndex = 0; nIndex < oFeatures.length; nIndex++) 
		g_oMap.setFeatureState({'id': nIndex, 'source': 'geo-lines'}, {'preview': false, 'include': false, 'color': '#000'});
	$('#pageoverlay').hide();

	if (g_oMap.getLayer('network-polygons-report') !== undefined)
		g_oMap.removeLayer('network-polygons-report');

	g_oMap.off('sourcedata', finishedStyling);
}


function buildInstructionDialog()
{
	let oDialog = $('#dlgInstructions');
	oDialog.dialog({autoOpen: false, position: {my: "left top", at: "left+8 top+8", of: "#map-container"}, draggable: false, resizable: false, width: 400,
		open:function() 
		{
			oDialog.dialog('option', 'position', {my: "left top", at: "left+8 top+8", of: "#map-container"});
		}});
	
	oDialog.dialog('option', 'title', 'Instructions');
	oDialog.siblings().children('.ui-dialog-titlebar-close').remove();
	let sHtml = '<p id="instructions"></p><p id="instructions-status" style="color: #108010"></p><p id="instructions-error" style="color: #d00010"></p>';
	
	$(window).resize(function()
	{
		oDialog.dialog('option', 'position', {my: "left top", at: "left+8 top+8", of: "#map-container"});
	});
	oDialog.html(sHtml);
	oDialog.dialog('open');
	document.activeElement.blur();
}


function buildGroupListDialog()
{
	let oDialog = $('#dlgGroupList');
	oDialog.dialog({autoOpen: false, position: {my: "right top", at: "right-45 top+8", of: "#map-container"}, draggable: true, resizable: false, width: 450, height: 'auto',
					buttons: [
					{text: 'Load', id: 'loadScenario', disabled: false, click: function() 
					{
						if (STATE === ADDREMOVE)
						{
							status('Finish Segment Selection', 'Finish adding or removing segments then click <i class="fa fa-road"></i>&nbsp;Done to be able to Load another scenario template.');
							return;
						}
						$('#dlgLoad').dialog('open');
					}},
//					{text: 'Save', id: 'saveScenario', disabled: true, click: function() 
//					{
//						if (STATE === ADDREMOVE)
//							return;
//						$('#dlgSave').dialog('open');
//					}},
					{text: 'Run', id: 'runScenario', disabled: true, click: function() 
					{
						if (STATE === ADDREMOVE)
						{
							status('Finish Segment Selection', 'Finish adding or removing segments then click <i class="fa fa-road"></i>&nbsp;Done to be able to Save and then Run the scenario.');
							return;
						}
						$('#dlgRun').dialog('open');
					}},
					{text: 'Restart', id: 'btnRestart', click: function() 
					{
						$('#dlgConfirmRestart').dialog('open');
						document.activeElement.blur();
					}}
		],
		open:function() 
		{
			oDialog.dialog('option', 'position', {my: "right top", at: "right-45 top+8", of: "#map-container"});
		}});
	
	oDialog.dialog('option', 'title', 'Scenario Settings');
	oDialog.siblings().children('.ui-dialog-titlebar-close').remove();
	let sHtml = `<div class="flexbox marginbottom12"><div class="flex3"><input id="scenarioName" type="text" placeholder="Enter name of scenario"/></div>
				<div class="flex1 flexbox"><button class="ui-button ui-corner-all flex1" id="saveScenario" style="width:120px; white-space:nowrap">Save</button></div></div>
				<div class="flexbox marginbottom12"><div class="flex3"><input id="groupname" type="text" placeholder="Enter name of new group"/></div>
				<div class="flex1 flexbox"><button class="ui-button ui-corner-all flex1" id="btnNewGroup" style="width:120px; white-space:nowrap">Add Group</button></div></div>
				<ul id="grouplist"></ul>`;
	
	oDialog.html(sHtml);
	$('#scenarioName').on('keypress', ignoreInput).on('paste', ignoreInput);
	$('#groupname').on('keypress', ignoreInput).on('paste', ignoreInput);
	$('#saveScenario').on('click', function()
	{
		if (STATE === ADDREMOVE)
		{
			status('Finish Segment Selection', 'Finish adding or removing segments then click <i class="fa fa-road"></i>&nbsp;Done to be able to save.');
			return;
		}
		saveScenario(false);
	});
	$('#saveScenario').prop('disabled', true).addClass('ui-button-disabled ui-state-disabled')
	$(window).resize(function()
	{
		oDialog.dialog('option', 'position', {my: "right top", at: "right-45 top+8", of: "#map-container"});
	});
	
	$('#btnNewGroup').click(addGroup);
	
	$('#btnStartOver').click(function()
	{
		$('#dlgConfirmRestart').dialog('open');
		document.activeElement.blur();
	});
}


function addGroup(oSaved, sGroup)
{
	if (STATE === ADDREMOVE)
	{
		status('Finish Segment Selection', 'Finish adding or removing segments then click <i class="fa fa-road"></i>&nbsp;Done to be able to add another group.');
		return;
	}
	let sName;
	let oSegments = {};
	let nIndex = g_aGroups.length;
	let nSpdLimit = Number.MIN_SAFE_INTEGER;
	let nLaneCount = Number.MIN_SAFE_INTEGER;
	if (oSaved.segments) // loading a previously saved group
	{
		sName = sGroup;
		let sColor = g_nColors[nIndex % g_nColors.length];
		let oSrc = g_oMap.getSource('geo-lines');
		let oFeatures = oSrc._data.features;

	
		
		for (let sId of oSaved.segments.values())
		{
			let nIndex = g_oImrcpIds[sId];
			let oFeature = oFeatures[nIndex];
			if (nSpdLimit === Number.MIN_SAFE_INTEGER)
			{
				nSpdLimit = oFeature.properties.spdlimit;
				nLaneCount = oFeature.properties.lanecount;
			}
			
			oSegments[sId] = oFeature;
			g_oMap.setFeatureState(oFeature, {'color': sColor});
		}
	}
	else
	{
		sName = $('#groupname').val();
	}


	if (sName === undefined || sName.length === 0)
	{
		status('Name Group', 'You must enter a group name before adding it')
		return;
	}
	
	if (!/^[a-zA-Z0-9\-_]+$/.exec(sName))
	{
		status('Name Group', 'Group names can only contain a-z, A-Z, 0-9, -, and _');
		return false;
	}
	
	for (let oGroup of g_aGroups.values())
	{
		if (!oGroup)
			continue;
		if (oGroup.label === sName)
		{
			status('Duplicate Group Name', 'Cannot have duplicate group names');
			$('#groupname').focus();
			return;
		}
	}
	$('#saveScenario').prop('disabled', false).removeClass('ui-button-disabled ui-state-disabled');
	g_bSaveInstructions = true;
	switchState(VIEWGROUPS, VIEWGROUPS);
	
	let oGroup = {'segments': oSegments, 'label': sName, 'index': nIndex, 'lanecount': nLaneCount, 'spdlimit': nSpdLimit};
	g_aGroups.push(oGroup);
	$('#groupname').val('');
	let oItem = $('<li id="group_' + sName + '" class="flexbox transparentborder"></li>');
	let sColor = g_nColors[nIndex % g_nColors.length];
	let oColor = $(`<div class="flex1 colorbox" style="background-color: ${sColor};"></div>`);
	let oLabel = $(`<div class="flex10"><label>${sName}</label></div>`);
	let oIcons = $(`<div id="icons" class="flex3 flexbox"></div>`);
	let oAddRemove = $('<div class="flex1 flexbox"><div class="fa fa-road clickable ttip" tip="Add/Remove Segment"></div></div>');
	let oSegCount = $('<div class="flex1 flexbox"><div id="segment" class="counter ttip" tip="Segment Count">0</div></div>');
	let oEdit = $('<div class="flex1 flexbox"><div class="fa fa-pencil-square-o clickable ttip" tip="Edit Values"></div></div>');
	let oEditCount = $('<div class="flex1 flexbox"><div id="edit" class="counter ttip" tip="Edit Count">0</div></div>');
	let oDeleteSpace = $('<div class="flex1 flexbox"><div class="counter"></div></div>');
	let oDelete = $('<div class="flex1 flexbox"><div class="fa fa-times clickable ttip" tip="Delete Group"></div></div>');
	let oDone = $(`<div id="done" class="flex3 flexbox"></div>`);
	let oBtn = $(`<button class="ui-button ui-corner-all flex1" id="btnDone" style="white-space:nowrap"><i class="fa fa-road"></i>&nbsp;Done</button>`);
	oBtn.css('padding', '0');
	let oEventData = {'group': oGroup};
	oBtn.on('click', oEventData, addRemove);
	oAddRemove.on('click', oEventData, addRemove);
	oEdit.on('click', oEventData, editMode);
	oDelete.on('click', oEventData, deleteGroup);
	oIcons.append(oSegCount).append(oAddRemove).append(oEditCount).append(oEdit).append(oDeleteSpace).append(oDelete);
	oDone.append(oBtn);
	oItem.append(oColor).append(oLabel).append(oIcons).append(oDone);
	oDone.hide();
	$('#grouplist').append(oItem);
	oItem.on('mouseenter', {'group': oGroup}, mouseEnterLabel);
	oItem.on('mouseleave', {'group': oGroup}, mouseLeaveLabel);
	oSegCount.children('#segment').html(Object.keys(oSegments).length);
	g_oCurrentGroup = oGroup;
	resetValues();
	saveValues(oSaved);
	g_oCurrentGroup = undefined;
}


function mouseLeaveLabel(oEvent)
{
	if (g_bClicked)
	{
		g_bClicked = false;
	}
	else
	{
		if (STATE === ADDREMOVE)
			return;
		let oThisGroup = oEvent.data.group;
		$(this).removeClass('highlightborder').addClass('transparentborder');;
		for (let nIndex = 0; nIndex < g_aGroups.length; nIndex++)
		{
			let oGroup = g_aGroups[nIndex];
			if (oThisGroup.label === oGroup.label)
				continue;

			let sColor = g_nColors[oGroup.index % g_nColors.length];
			for (let oFeature of Object.values(oGroup.segments))
				g_oMap.setFeatureState(oFeature, {'ignore': false, 'color': sColor});
		}
		
		for (let oFeature of Object.values(oThisGroup.segments))
			g_oMap.setFeatureState(oFeature, {'preview': false, 'include': false});

	}
}


function mouseEnterLabel(oEvent)
{
	if (STATE === ADDREMOVE)
		return;
	
	$(this).addClass('highlightborder').removeClass('transparentborder');;
	let oThisGroup = oEvent.data.group;
	for (let nIndex = 0; nIndex < g_aGroups.length; nIndex++)
	{
		let oGroup = g_aGroups[nIndex];
		if (oThisGroup.label === oGroup.label)
			continue;
		
		for (let oFeature of Object.values(g_aGroups[nIndex].segments))
			g_oMap.setFeatureState(oFeature, {'ignore': true, 'color': '#000'});
	}

	let sColor = g_nColors[oThisGroup.index % g_nColors.length];
	for (let oFeature of Object.values(oThisGroup.segments))
		g_oMap.setFeatureState(oFeature, {'preview': true, 'include': true, 'color': sColor, 'ignore': false});	
}


function deleteGroup(oEvent)
{
	if (STATE === ADDREMOVE)
		return;
	g_oCurrentGroup = oEvent.data.group;
	$('#dlgConfirmDelete').dialog('open');
}


function editMode(oEvent)
{
	if (STATE === ADDREMOVE)
		return;
	g_oCurrentGroup = oEvent.data.group;
	switchState(SELECTEDIT, VIEWGROUPS);
}


function addRemove(oEvent)
{
	if (STATE === ADDREMOVE)
	{
		switchState(VIEWGROUPS, VIEWGROUPS);
		return;
	}
	g_oCurrentGroup = oEvent.data.group;
	g_bClicked = true;
	let nMinLon = Number.MAX_SAFE_INTEGER;
	let nMinLat = Number.MAX_SAFE_INTEGER;
	let nMaxLon = Number.MIN_SAFE_INTEGER;
	let nMaxLat = Number.MIN_SAFE_INTEGER;
	let oFeatures = g_oMap.getSource('geo-lines')._data.features;
	let sColor = g_nColors[g_oCurrentGroup.index % g_nColors.length];
	for (let oFeature of Object.values(g_oCurrentGroup.segments))
	{
		g_oMap.setFeatureState(oFeature, {'ignore': false, 'preview': true, 'include': true, 'color': sColor});
		let aMins = oFeatures[oFeature.id].bbox[0];
		let aMaxes = oFeatures[oFeature.id].bbox[1];
		if (aMins[0] < nMinLon)
			nMinLon = aMins[0];
		if (aMins[1] < nMinLat)
			nMinLat = aMins[1];
		if (aMaxes[0] > nMaxLon)
			nMaxLon = aMaxes[0];
		if (aMaxes[1] > nMaxLat)
			nMaxLat = aMaxes[1];
	}
	if (Object.entries(g_oCurrentGroup.segments).length > 0)
		g_oMap.fitBounds([[nMinLon, nMinLat], [nMaxLon, nMaxLat]], {'padding': 50, 'linear': false, 'duration': 0});
	
	for (let nIndex = 0; nIndex < g_aGroups.length; nIndex++)
	{
		let oGroup = g_aGroups[nIndex];
		if (g_oCurrentGroup.label === oGroup.label)
			continue;

		let sColor = g_nColors[oGroup.index % g_nColors.length];
		for (let oFeature of Object.values(oGroup.segments))
			g_oMap.setFeatureState(oFeature, {'ignore': false, 'color': sColor});
	}
	
	setTimeout(function()
	{
		for (let oFeature of Object.values(oEvent.data.group.segments))
			g_oMap.setFeatureState(oFeature, {'ignore': false, 'preview': false, 'include': true});
	}, 1000);
	switchState(ADDREMOVE);
}


function switchState(nNewState, nFallback = ERROR)
{
	let bSame = nNewState === STATE;
	switch (STATE)
	{
		case ADDREMOVE:
		{
			g_oMap.off('click', toggleInclude);
			g_oMap.off('mouseenter', 'geo-lines-scenario', hoverHighlight);
			let sLabel = g_oCurrentGroup.label.replaceAll(' ', '\\ ');
			$('#group_' + sLabel + ' #icons').show();
			$('#group_' + sLabel + ' #done').hide();
			$('#group_' + sLabel + ' #segment').html(Object.keys(g_oCurrentGroup.segments).length);
			$('#icons > div > div').addClass('ttip');
			setValues();
			break;
		}
		case ERROR:
		default:
			break;
	}
	
	if (bSame)
		STATE = nFallback;
	else
		STATE = nNewState;

	
	switch (STATE)
	{
		case SELECTNETWORK:
		{
			$('#instructions').html('Left-click network to create a scenario');
			$('#dlgInstructions').dialog('option', 'title', 'Select Network');
			break;
		}
		case ADDREMOVE:
		{
			$('#instructions').html('Left-click to add or remove segment from the current group<br><i class="fa fa-plus"></i>&nbsp;Add Segment<br><i class="fa fa-minus"></i>&nbsp;Remove Segment<br><i class="fa fa-ban"></i>&nbsp;Segment does not have the same speed limit or number of lanes as group<br>');
			$('#dlgInstructions').dialog('option', 'title', 'Add/Remove Segment');
			g_oMap.on('click', toggleInclude);
			g_oMap.on('mouseenter', 'geo-lines-scenario', hoverHighlight);
			let sLabel = g_oCurrentGroup.label.replaceAll(' ', '\\ ');
			$('#group_' + sLabel + ' #icons').hide();
			$('#group_' + sLabel + ' #done').show();
			$('#icons > div > div').removeClass('ttip');
//			$('#saveScenario,#btnNewGroup,#loadScenario,#runScenario,#btnRestart').prop('disabled', true).addClass('ui-button-disabled ui-state-disabled');
			break;
		}
		case SELECTEDIT:
		{
			if (Object.keys(g_oCurrentGroup.segments).length === 0)
			{
				status('Select Segments First', 'You must have at least one segment in a group to set values');
				return;
			}

			$('#dlgValues').dialog('open');
			let sLabel = g_oCurrentGroup.label;
			if (sLabel)
				$('#dlgValues').dialog('option', 'title', `Edit ${sLabel} Values`);
			break;
		}
		case VIEWGROUPS:
		{
			let sHtml = 'A scenario consists of groups of road segments associated with actions<br><br>Enter a name and left-click "Add Group" to create a new group';
			sHtml += '<br><br>Valid characters for scenario and group names include a-z, A-Z, -, and _'
			if (g_aGroups.length > 0)
			{
				sHtml += '<br><br><i class="fa fa-road"></i> Add/remove segment mode';
				sHtml += '<br><i class="fa fa-pencil-square-o"></i> Edit action/values mode';
				sHtml += '<br><i class="fa fa-times"></i> Remove group';
				
				if (g_bSaveInstructions)
				{
					sHtml += '<br><br>Enter a scenario name and left-click "Save" to save progress as a template';
				}
				if (g_bRunInstructions)
				{
					sHtml += '<br><br>Left-click "Run" to submit the saved scenario template for processing.';
				}
			}
			if (g_oScenarios.length > 0)
				sHtml += '<br><br>Left-click "Load" to load an existing scenario template';
			sHtml += '<br><br>Left-click "Restart" to remove current scenario and start over';
			$('#instructions').html(sHtml);
			$('#dlgInstructions').dialog('option', 'title', 'Create and Edit Groups');
			g_oCurrentGroup = undefined;
			break;
		}
		case ERROR:
		default:
			break;
	}
}

function buildConfirmRestart()
{
	let oDialog = $('#dlgConfirmRestart');
	oDialog.dialog({autoOpen: false, position: {my: "center", at: "center", of: "#map-container"}, modal: true, draggable: false, resizable: false, width: 400,
		buttons: [
			{text: 'Restart', click: restart},
			{text: 'Cancel', click: function() {$(this).dialog('close');}}
		],
		open:function() 
		{
			oDialog.dialog('option', 'position', {my: "center", at: "center", of: "#map-container"});
		}});
	
	$(window).resize(function()
	{
		oDialog.dialog('option', 'position', {my: "center", at: "center", of: "#map-container"});
	});
	oDialog.dialog('option', 'title', 'Confirm Restart');
	oDialog.siblings().children('.ui-dialog-titlebar-close').remove();
	oDialog.html('Restarting will lose all progress (all groups and edited values will be lost). Would you like to restart or continue creating the scenario?');
}


function restart(bSkipNetwork)
{
	if (STATE === ADDREMOVE)
	{
		$('#group_' + g_oCurrentGroup.label.replaceAll(' ', '\\ ') + ' #btnDone').click();
	}
	$('#dlgConfirmRestart').dialog('close');
	$('#dlgGroupList').dialog('close');
	$('#grouplist > li').remove();
	g_aGroups = [];
	$('#scenarioName').val('');
	$('#saveScenario').prop('disabled', true).addClass('ui-button-disabled ui-state-disabled');
	$('#runScenario').prop('disabled', true).addClass('ui-button-disabled ui-state-disabled');
	g_bSaveInstructions = g_bRunInstructions = false;
	let oNow = moment().minutes(0).seconds(0).milliseconds(0);
	$('#btnTime').val(oNow.utc().valueOf()).html(oNow.local().format("YYYY/MM/DD HH:mm"));
	$('#dtpicker').datetimepicker('reset');
	$('#dtpicker').datetimepicker({'value': oNow});
	g_oCurrentGroup = undefined;
	g_oMap.off('click', toggleInclude);
	g_oMap.off('mouseenter', 'geo-lines-scenario', hoverHighlight);
	g_oMap.off('mousemove', 'geo-lines-scenario', updateHighlight);
	mapOffBoundFn(g_oMap, 'mousemove', unHighlight.name);
	if (bSkipNetwork && !bSkipNetwork.type)
	{
		loadANetwork(g_oMap.getSource('network-polygons')._data);
	}
	else if (g_nNetworks > 1)
	{
		selectANetwork();
	}
}


function buildConfirmDelete()
{
	let oDialog = $('#dlgConfirmDelete');
	oDialog.dialog({autoOpen: false, position: {my: "center", at: "center", of: "#map-container"}, modal: true, draggable: false, resizable: false, width: 400,
		buttons: [
			{text: 'Delete', click: function() 
				{
					oDialog.dialog('close');
					for (let oFeature of Object.values(g_oCurrentGroup.segments))
						g_oMap.setFeatureState(oFeature, {'preview': false, 'include': false, 'color': '#000'});
					let nDelete = g_oCurrentGroup.index;
					g_aGroups.splice(nDelete, 1);
					for (let nIndex = nDelete; nIndex < g_aGroups.length; nIndex++)
					{
						let oGroup = g_aGroups[nIndex];
						--oGroup.index;
						let sColor = g_nColors[oGroup.index % g_nColors.length];
						$('#group_' + oGroup.label.replaceAll(' ', '\\ ') + ' .colorbox').css('background-color', sColor);
						for (let oFeature of Object.values(oGroup.segments))
							g_oMap.setFeatureState(oFeature, {'color': sColor});
					}
					$('#group_' + g_oCurrentGroup.label.replaceAll(' ', '\\ ')).remove();
					g_oCurrentGroup = undefined;
					if (g_aGroups.length === 0)
					{
						$('#saveScenario').prop('disabled', true).addClass('ui-button-disabled ui-state-disabled');
						g_bSaveInstructions = false;
					}

					switchState(VIEWGROUPS, VIEWGROUPS);
				}},
			{text: 'Cancel', click: function() 
				{
					g_oCurrentGroup = undefined;
					switchState(VIEWGROUPS, VIEWGROUPS);
					$(this).dialog('close');
				}}
		],
		open:function() 
		{
			oDialog.dialog('option', 'position', {my: "center", at: "center", of: "#map-container"});
		}});
	$(window).resize(function()
	{
		oDialog.dialog('option', 'position', {my: "center", at: "center", of: "#map-container"});
	});
	oDialog.dialog('option', 'title', 'Confirm Group Delete');
	oDialog.siblings().children('.ui-dialog-titlebar-close').remove();
	oDialog.html('Deleting a group cannot be undone. Would you like to delete the group?');
}


function buildDtp()
{
	let oDialog = $('#dlgDtp');
	oDialog.dialog({autoOpen: false, position: {my: "center", at: "center", of: "#map-container"}, modal: true, draggable: false, resizable: false, width: 350, height:300,
		title: 'Set Start Time',
		close: function() 
		{
			$('#dtpicker').datetimepicker('hide');
		},
		open: function() 
		{
			$('#dtpicker').datetimepicker('show');
			oDialog.dialog('option', 'position', {my: "center", at: "center", of: "#map-container"});
		}});
	oDialog.html('<div id="dtpicker" style="width:0px; position:absolute; bottom:25px;"></div>');
	let oNow = moment().minutes(0).seconds(0).milliseconds(0);
	let oStart = new Date();

	
	$('#dtpicker').datetimepicker(
	{
		step: 60,
		value: oNow.toDate(),
		formatTime: 'H:i',
		format: 'Y/m/d H:i',
		yearStart: oNow.year(),
		yearEnd: oNow.year() + 1,
		closeOnWithoutClick: false,
		onSelectTime: function()
		{
			let oTime = moment(this.getValue()).seconds(0).milliseconds(0);
			$('#btnTime').val(oTime.utc().valueOf()).html(oTime.local().format("YYYY/MM/DD HH:mm"));
			oDialog.dialog('close');
			switchState(VIEWGROUPS, VIEWGROUPS);
		}
	});
	
	$(window).resize(function()
	{
		oDialog.dialog('option', 'position', {my: "center", at: "center", of: "#map-container"});
	});
	
}


function buildLoad()
{
	let oDialog = $('#dlgLoad');
	oDialog.dialog({autoOpen: false, position: {my: "center", at: "center", of: "#map-container"}, modal: true, draggable: false, resizable: false, width: 400,
		'title': "Select Scenario to Load",
		open: function() 
		{
			setLoadHtml();
			oDialog.dialog('option', 'position', {my: "center", at: "center", of: "#map-container"});
		},
		buttons: [
			{text: 'Cancel', click: function() 
				{
					oDialog.dialog('close');
				}},
			{text: 'Load Scenario', id: 'btnLoad', click: loadScenario}
		]});
	
	$(window).resize(function()
	{
		oDialog.dialog('option', 'position', {my: "center", at: "center", of: "#map-container"});
	});
}


function setLoadHtml()
{
	$('#selectScenarios').remove();
	g_nSelectedScenario = -1;
	g_oScenarios.sort((a, b) => a.name.localeCompare(b.name));
	let sHtml = `<ul id="selectScenarios">`;
	for (let nIndex = 0; nIndex < g_oScenarios.length; nIndex++)
	{
		let oScenario = g_oScenarios[nIndex];
		if (oScenario.network === g_sLoadedNetwork)
			sHtml += `<li id="scenario${nIndex}" class="clickable flexbox"><div class="flex10">${oScenario.name}</div><div id="delete${nIndex}" class="flex1 flexbox delete"><div class="fa fa-times clickable"></div></div></li>`;
	}
	sHtml += `</ul><div>Select a scenario template to load, then click 'Load'. Click <div class="fa fa-times"></div> to delete a template. Loading a scenario template will overwrite any unsaved settings.`;
	
	$('#dlgLoad').html(sHtml);
	
	$('#selectScenarios > li .delete').on('click', function()
	{
		let nNum = $(this).prop('id').substring('delete'.length);
		let oDialog = $('#dlgConfirmDeleteTemplate');
		oDialog.dialog('option', 'title', `Confirm Delete - ${g_oScenarios[nNum].name}`);
		$('#dlgConfirmDeleteTemplate').dialog('open');
	});
	$('#selectScenarios > li').on('click', function() 
	{
		if (g_nSelectedScenario >= 0)
			$('#scenario' + g_nSelectedScenario).removeClass('w3-fhwa-navy');
		$(this).removeClass('hoverScenario')
		let sId = $(this).prop('id');
		
		g_nSelectedScenario = sId.substring('scenario'.length);
	}).on('mouseenter', function()
	{
		let nNum = $(this).prop('id').substring('scenario'.length);
		if (nNum != g_nSelectedScenario)
			$(this).addClass('w3-fhwa-navy hoverScenario');
	}).on('mouseleave', function()
	{
		let nNum = $(this).prop('id').substring('scenario'.length);
		if (nNum == g_nSelectedScenario)
			$(this).removeClass('hoverScenario');
		else
			$(this).removeClass('w3-fhwa-navy hoverScenario');
	});
}


function loadScenario()
{
	if (g_nSelectedScenario < 0)
		return;
	
	$('#grouplist > li').remove();
	g_aGroups = [];
	
	$('#runScenario').prop('disabled', true).addClass('ui-button-disabled ui-state-disabled');
	g_bRunInstructions = false;
	let oScenario = g_oScenarios[g_nSelectedScenario];
	let oTime = moment(oScenario.starttime);
	$('#btnTime').val(oTime.utc().valueOf()).html(oTime.local().format("YYYY/MM/DD HH:mm"));
	$('#dlgGroupList').dialog('option', 'title', 'Scenario Settings');
	$('#scenarioName').val(oScenario.name);
	for (let oGroup of oScenario.groups.values())
	{
		addGroup(oGroup, oGroup.label);
	}
	
	if (g_aGroups.length > 0)
	{
		$('#runScenario').prop('disabled', false).removeClass('ui-button-disabled ui-state-disabled');
		g_bRunInstructions = true;
		let nMinLon = Number.MAX_SAFE_INTEGER;
		let nMinLat = Number.MAX_SAFE_INTEGER;
		let nMaxLon = Number.MIN_SAFE_INTEGER;
		let nMaxLat = Number.MIN_SAFE_INTEGER;
		for (let oGroup of g_aGroups.values())
		{
			for (let oFeature of Object.values(oGroup.segments))
			{
				let aMins = oFeature.bbox[0];
				let aMaxes = oFeature.bbox[1];
				if (aMins[0] < nMinLon)
					nMinLon = aMins[0];
				if (aMins[1] < nMinLat)
					nMinLat = aMins[1];
				if (aMaxes[0] > nMaxLon)
					nMaxLon = aMaxes[0];
				if (aMaxes[1] > nMaxLat)
					nMaxLat = aMaxes[1];
			}
		}
		
		if (nMinLon !== Number.MAX_SAFE_INTEGER)
			g_oMap.fitBounds([[nMinLon, nMinLat], [nMaxLon, nMaxLat]], {'padding': 50});
	}
	$('#saveScenario').prop('disabled', true).addClass('ui-button-disabled ui-state-disabled');
	g_bSaveInstructions = false;
	g_nSelectedScenario = -1;
	$('#dlgLoad').dialog('close');
	switchState(VIEWGROUPS, VIEWGROUPS);
}


function buildConfirmDeleteTemplate()
{
	let oDialog = $('#dlgConfirmDeleteTemplate');
	oDialog.dialog({autoOpen: false, position: {my: "center", at: "center", of: "#map-container"}, modal: true, draggable: false, resizable: false, width: 400,
		buttons: [
			{text: 'Go Back', click: function() 
				{
					oDialog.dialog('close');
				}},
			{text: 'Confirm Delete', id: 'btnDeleteTemplate', click: deleteTemplate}
		],
		open:function() 
		{
			oDialog.dialog('option', 'position', {my: "center", at: "center", of: "#map-container"});
		}});
	
	oDialog.html('Deleting a template is permanent. Confirm you would like to delete this template.');
	$(window).resize(function()
	{
		oDialog.dialog('option', 'position', {my: "center", at: "center", of: "#map-container"});
	});
}


function deleteTemplate()
{
	let oScenario = g_oScenarios[g_nSelectedScenario];
	$.ajax(
	{
		'url': 'api/scenarios/deleteTemplate',
		'data': {'token': sessionStorage.token, 'name': oScenario.name}
	}).done(function()
	{
		g_oScenarios.splice(g_nSelectedScenario, 1);
		setLoadHtml();
		restart(true);
	}).always(function(oRes)
	{
		$('#dlgConfirmDeleteTemplate').dialog('close');
		if (g_oScenarios.length === 0)
			$('#loadScenario').prop('disabled', true).addClass('ui-button-disabled ui-state-disabled');
		else
			$('#loadScenario').prop('disabled', false).removeClass('ui-button-disabled ui-state-disabled');
		status(oRes.status, oRes.msg);
	});
}

//function buildSave()
//{
//	let oDialog = $('#dlgSave');
//	oDialog.dialog({autoOpen: false, position: {my: "center", at: "center", of: "#map-container"}, modal: true, draggable: false, resizable: false, width: 400,
//		open: function() 
//		{
//			if ($('#scenarioName').val().length > 0)
//				$('#btnSaveName').removeClass('ui-button-disabled ui-state-disabled').prop('disabled', false);
//		},
//		buttons: [
//			{text: 'Cancel', click: function() 
//				{
//					switchState(VIEWGROUPS, VIEWGROUPS);
//					oDialog.dialog('close');
//				}},
//			{text: 'Save', id: 'btnSaveName', disabled: true, click: saveScenario}
//		]});
//	
//	oDialog.dialog('option', 'title', 'Save Scenario');
//	oDialog.siblings().children('.ui-dialog-titlebar-close').remove();
//	
//	let sHtml = `<div class="w3-container"><div class="flexbox"><label for="scenarioName" class="flex1">Name</label><div class="flex3"><input type="text" id="scenarioName" /></div></div></div>`;
//	oDialog.html(sHtml);
//	$('#scenarioName').on('input', function ()
//	{
//		if ($(this).val().length === 0)
//			$('#btnSaveName').addClass('ui-button-disabled ui-state-disabled').prop('disabled', true);
//		else
//			$('#btnSaveName').removeClass('ui-button-disabled ui-state-disabled').prop('disabled', false);
//	});
//}


function buildRun()
{
	let oDialog = $('#dlgRun');
	oDialog.dialog({autoOpen: false, position: {my: "center", at: "center", of: "#map-container"}, modal: true, draggable: false, resizable: false, width: 450,
		open: function()
			{
				let oNow = moment().minutes(0).seconds(0).milliseconds(0);
				$('#btnTime').val(oNow.utc().valueOf()).html(oNow.local().format("YYYY/MM/DD HH:mm"));
				$('#dtpicker').datetimepicker('reset');
				$('#dtpicker').datetimepicker({'value': moment().minute(0).second(0).millisecond(0).toDate()})
				oDialog.dialog('option', 'position', {my: "center", at: "center", of: "#map-container"});

			},
		buttons: [
			{text: 'Cancel', click: function() 
				{
					switchState(VIEWGROUPS, VIEWGROUPS);
					oDialog.dialog('close');
				}},
			{text: 'Run Scenario', id: 'btnRunScenario', disabled: true, click: runScenario}
		]});
	
	oDialog.dialog('option', 'title', 'Run Scenario');
	oDialog.siblings().children('.ui-dialog-titlebar-close').remove();
	$(window).resize(function()
	{
		oDialog.dialog('option', 'position', {my: "center", at: "center", of: "#map-container"});
	});
	let sHtml = `<div class="flexbox marginbottom12"><label class="flex2">Start Time</label><div class="flex2 flexbox"><button style="white-space:nowrap" class="ui-button ui-corner-all flex3" id="btnTime" value="0000/00/00">yyyy/MM/dd HH:mm</button></div></div>
<div class="flexbox marginbottom12" style="height:35px;"><label for="scenarioRun" class="flex1">Name</label><div class="flex3"><input style="width:100%;"type="text" id="scenarioRun" /></div></div>`;
	sHtml += `<br>Click "Run Scenario" button to submit the scenario to be processed. Scenarios can be viewed on the "View Scenarios" page by name.`;
	oDialog.html(sHtml);
	let oNow = moment().minutes(0).seconds(0).milliseconds(0);
	$('#btnTime').val(oNow.utc().valueOf()).html(oNow.local().format("YYYY/MM/DD HH:mm"));

	$('#btnTime').click(function()
	{
		if (STATE === ADDREMOVE)
		{
			return;
		}
		$('#dlgDtp').dialog('open');
	});
	$('#scenarioRun').on('input', function ()
	{
		if ($(this).val().length === 0)
			$('#btnRunScenario').addClass('ui-button-disabled ui-state-disabled').prop('disabled', true);
		else
			$('#btnRunScenario').removeClass('ui-button-disabled ui-state-disabled').prop('disabled', false);
	});
}


function runScenario()
{
	let oData = {'token': sessionStorage.token, 'starttime': $('#dtpicker').datetimepicker('getValue').valueOf(), 'name': $('#scenarioRun').val(), 'template': $('#scenarioName').val()};

	$.ajax(
	{
		'url': 'api/scenarios/run',
		'method': 'POST',
		'data': oData
	}).done(function()
	{
		restart();
	}).always(function(oRes)
	{
		$('#dlgRun').dialog('close');
		status(oRes.status, oRes.msg);
	});
}



function saveScenario(bOverwrite)
{
	if (validate())
	{
		let oScenario = {'name': $('#scenarioName').val(), 'run': false, 'groups': [], 'network': g_sLoadedNetwork};
		if (!bOverwrite)
		{
			let nIndex = binarySearch(g_oScenarios, oScenario, (a,b) => a.name.localeCompare(b.name));
			if (nIndex >= 0)
			{
				$('#dlgConfirmOverwrite').dialog('open');
				return;
			}
		}
		let oData = {'token': sessionStorage.token, 'scenario': oScenario};
		for (let oGroup of g_aGroups)
		{
			if (oGroup)
			{
				let aSegments = Object.keys(oGroup.segments);
				if (aSegments.length === 0)
					continue;
				let oGroupData = {'segments': aSegments};
				for (let [sKey, aValArr] of Object.entries(oGroup.groupvalues))
					oGroupData[sKey] = aValArr;
				oGroupData.label = oGroup.label;
				oData['scenario']['groups'].push(oGroupData);
			}
		}
		oData['scenario'] = JSON.stringify(oData['scenario']);
		$.ajax(
		{
			'url': 'api/scenarios/save',
			'method': 'POST',
			'data': oData
		}).done(function(oRes) 
		{
			$('#dlgGroupList').dialog('option', 'title', `Scenario Settings`);
			$('#saveScenario').prop('disabled', true).addClass('ui-button-disabled ui-state-disabled');
			$('#runScenario').prop('disabled', false).removeClass('ui-button-disabled ui-state-disabled');
			g_bRunInstructions = true;
			g_bSaveInstructions = false
			let nIndex = binarySearch(g_oScenarios, oScenario, (a,b) => a.name.localeCompare(b.name));
			if (nIndex < 0)
			{
				g_oScenarios.splice(~nIndex, 0, oRes.saved);
			}
			else
			{
				g_oScenarios[nIndex] = oRes.saved;
			}
			switchState(VIEWGROUPS, VIEWGROUPS);
		}).always(function(oRes)
		{
//			$('#dlgSave').dialog('close');
			if (g_oScenarios.length === 0)
				$('#loadScenario').prop('disabled', true).addClass('ui-button-disabled ui-state-disabled');
			else
				$('#loadScenario').prop('disabled', false).removeClass('ui-button-disabled ui-state-disabled');
			status(oRes.status, oRes.msg);
		});
	}
}


function buildConfirmOverwrite()
{
	let oDialog = $('#dlgConfirmOverwrite');
	oDialog.dialog({autoOpen: false, position: {my: "center", at: "center", of: "#map-container"}, modal: true, draggable: false, resizable: false, width: 400,
		title: 'Confirm Template Overwrite',
		open: function()
		{
			oDialog.dialog('option', 'position', {my: "center", at: "center", of: "#map-container"});
		},
		buttons: [
			{text: 'Go Back', click: function() 
				{
					oDialog.dialog('close');
				}},
			{text: 'Confirm Overwrite', id: 'btnDeleteTemplate', click: function() 
				{
					oDialog.dialog('close');
					saveScenario(true);
				}}
		]});
	
	oDialog.html('A Scenario Template already exists with this name, would you like to overwrite all of its settings?');
	$(window).resize(function()
	{
		oDialog.dialog('option', 'position', {my: "center", at: "center", of: "#map-container"});
	});
}


function buildValues()
{
	let oDialog = $('#dlgValues');
	oDialog.dialog({autoOpen: false, 'closeOnEscape': false, position: {my: "center", at: "center", of: "#map-container"}, modal: true, draggable: false, resizable: false, width: 1000,
		open: function()
		{
			oDialog.dialog('option', 'position', {my: "center", at: "center", of: "#map-container"});
			for (let aAction of g_aActions.values())
			{
				if (aAction[2])
					$('#' + aAction[0]).html(`${aAction[0]} (${g_oCurrentGroup[aAction[2]]}*)`);
			}
			setValues();
		},
		buttons: [
			{text: 'Cancel', click: function() 
				{
					switchState(VIEWGROUPS, VIEWGROUPS);
					oDialog.dialog('close');
				}},
			{text: 'Save Values', click: saveValues}
		]});
	oDialog.dialog('option', 'title', 'Edit Values');
	oDialog.siblings().children('.ui-dialog-titlebar-close').remove();

	
	let sHtml =	`<table class="valuetable"><colgroup><col id="colAction">`;
	for (let nOffset = 0; nOffset < g_nHours; nOffset++)
		sHtml += `<col class="colOffset" id="colOffset${nOffset}">`;
	sHtml += `</colgroup><tr><th>Action</th><th colspan="24" style="width: 100%">Future Offset Hours</th></tr>`;
	sHtml += `<tr><th/>`;
	for (let nOffset = 1; nOffset <= g_nHours; nOffset++)
		sHtml +=`<th>+${nOffset}</th>`;
	sHtml += '</tr>';
	for (let aArr of g_aActions.values())
	{
		let sAction = aArr[0];
		sHtml += `<tr><td id="${sAction}">${sAction}</td>`;
		for (let nOffset = 0; nOffset < g_nHours; nOffset++)
			sHtml += `<td><input type="${aArr[1]} id="${aArr[0].replace(/ /g, '') + nOffset}"></td>`;
		
		sHtml += `</tr>`;
	}
	sHtml += `</table><br><div>Set the actions applied to the segments in the selected group. Actions can be different for each hour in the 24 hour forecast. Segments are by default untreated and not plowed.<br>* normal operating values</div>`;
	oDialog.html(sHtml);
	$(window).resize(function()
	{
		oDialog.dialog('option', 'position', {my: "center", at: "center", of: "#map-container"});
	});
}


function saveValues(oSaved)
{
	let oGroup = g_oCurrentGroup;
	if (oSaved.segments)
	{
		for (let aAction of g_aActions.values())
		{
			let sName = aAction[0];
			let sId = sName.replace(/ /g, '');
			let aValues = oSaved[sId];
			if (aAction[1].indexOf('checkbox') >= 0)
			{
				for (let nOffset = 0; nOffset < g_nHours; nOffset++)
				{
					$('#' + sId + nOffset).prop('checked', aValues[nOffset]);
				}
			}
			else if (aAction[1].indexOf('text') >= 0)
			{
				for (let nOffset = 0; nOffset < g_nHours; nOffset++)
				{
					$('#' + sId + nOffset).val(aValues[nOffset]);
				}
			}
		}
	}
	oGroup['groupvalues'] = {};
	let nCount = 0;
	for (let aAction of g_aActions.values())
	{
		let sName = aAction[0];
		let sId = sName.replace(/ /g, '');
		let aValues = [];
		if (aAction[1].indexOf('checkbox') >= 0)
		{
			for (let nOffset = 0; nOffset < g_nHours; nOffset++)
			{
				let bChecked = $('#' + sId + nOffset).prop('checked');
				if (bChecked)
					++nCount;
				aValues.push(bChecked);
			}
		}
		else if (aAction[1].indexOf('text') >= 0)
		{
			let nDefault = g_oCurrentGroup[aAction[2]];
			for (let nOffset = 0; nOffset < g_nHours; nOffset++)
			{
				let sVal = $('#' + sId + nOffset).val();
				if (sVal === '')
					aValues.push(nDefault);
				else
				{
					if (sVal != nDefault)
						++nCount;
					aValues.push(Number.parseInt(sVal));
				}
			}
		}
		oGroup['groupvalues'][sId] = aValues;
	}
	$('#group_' + g_oCurrentGroup.label.replaceAll(' ', '\\ ') + ' #edit').html(nCount);
	$('#runScenario').prop('disabled', true).addClass('ui-button-disabled ui-state-disabled');
	$('#saveScenario').prop('disabled', false).removeClass('ui-button-disabled ui-state-disabled');
	g_bSaveInstructions = true;
	g_bRunInstructions = false;
	let oDialog = $('#dlgValues');
	oDialog.dialog('close');
	switchState(VIEWGROUPS, VIEWGROUPS);
}


function resetValues()
{
	for (let aAction of g_aActions.values())
	{
		let sId = aAction[0].replace(/ /g, '');
		if (aAction[1].indexOf('checkbox') >= 0)
		{
			for (let nOffset = 0; nOffset < g_nHours; nOffset++)
				$('#' + sId + nOffset).prop('checked', false);
		}
		else if (aAction[1].indexOf('text') >= 0)
		{
			let nDefault = g_oCurrentGroup[aAction[2]];
			for (let nOffset = 0; nOffset < g_nHours; nOffset++)
				$('#' + sId + nOffset).val(nDefault);
		}
	}
}


function setValues()
{
	let oGroup = g_oCurrentGroup;
	if (oGroup['groupvalues'] === undefined)
	{
		
		saveValues();
		switchState(SELECTEDIT, SELECTEDIT);
	}
	else
	{
		for (let aAction of g_aActions.values())
		{
			let sId = aAction[0].replace(/ /g, '');
			let aValues = oGroup['groupvalues'][sId];
			if (aAction[1].indexOf('checkbox') >= 0)
			{
				for (let nOffset = 0; nOffset < g_nHours; nOffset++)
					$('#' + sId + nOffset).prop('checked', aValues[nOffset]);
			}
			else if (aAction[1].indexOf('text') >= 0)
			{
				let nDefault = g_oCurrentGroup[aAction[2]];
				for (let nOffset = 0; nOffset < g_nHours; nOffset++)
				{
					if (aValues[nOffset] === Number.MIN_SAFE_INTEGER)
						aValues[nOffset] = nDefault;
					$('#' + sId + nOffset).val(aValues[nOffset]);
				}
			}
		}
	}
}


function status(sTitle, sHtml)
{
	let oStatus = $('#dlgStatus');
	oStatus.dialog('option', 'title', sTitle);
	oStatus.html(sHtml);
	oStatus.dialog('open');
}


function validate()
{
	let sText = $('#scenarioName').val();
	if (sText.length === 0)
	{
		status('Name Scenario', 'You must enter a scenario name before saving.');
		return false;
	}
	else if (!/^[a-zA-Z0-9\-_]+$/.exec(sText))
	{
		status('Name Scenario', 'Scenario names can only contain a-z, A-Z, 0-9, -, and _');
		return false;
	}
	
	return true;
}

function ignoreInput(oEvent)
{
	let sText = oEvent.originalEvent.key || oEvent.originalEvent.clipboardData.getData('text');
	if (!/^[a-zA-Z0-9\-_]+$/.exec(sText))
	{
		let oPopuptext = $('<span class="show">Invalid input</span>');
		if ($(this).parent()[0].getBoundingClientRect().top < 70)
			oPopuptext.addClass('popuptextbelow');
		else
			oPopuptext.addClass('popuptextabove');

		$(this).parent().addClass('popup').append(oPopuptext);
		
		setTimeout(function()
		{
			$(this).removeClass('popup');
			oPopuptext.remove();
		}, 1500);
		oEvent.preventDefault();
		return false;
	}
}

$(document).on('initPage', initialize);

