import {pointToPaddedBounds} from './map-util.js';

let g_sMapboxAccessToken = '<mapbox access token>';
let g_sMapboxStyle = 'mapbox://styles/mapbox/light-v9';
let g_sGeojson;
let g_oMap;
let g_oGeojsons = {};
let g_bRetrievedGeojson = false;
let g_oDefaults = {'opacity': 1, 'color': '#000', 'width': 3};
let g_oLayers = {};
let g_oPopup;
let g_aQueryLayers = [];
let g_oIcons = ["workzone","slow-traffic","very-slow-traffic","unusual-congestion","lengthy-queue","incident","low-visibility","dew-on-roadway","frost-on-roadway","ice-on-bridge","blowing-snow","flooded-road","detector-sdf","diamond-mgray","diamond-lblue","diamond-dblue","precip-light","precip-medium","precip-heavy","winter-precip-light","winter-precip-medium","winter-precip-heavy","light-precip","medium-precip","heavy-precip","light-winter-precip","medium-winter-precip","heavy-winter-precip","stage-action","stage-flood","nhc-sd","nhc-td","nhc-ss","nhc-ts","nhc-hu","nhc-mh","delete","chevron-sdf","hexagon-sdf"];

function getNetworksAjax()
{
	return $.ajax(
	{
		'url': 'api/map/networkList',
		'dataType': 'json',
		'method': 'POST',
		'data': {'token': sessionStorage.token}
	});
}

function getProfileAjax()
{
	return $.ajax(
	{
		'url': 'api/map/profile',
		'dataType': 'json',
		'method': 'POST',
		'data': {'token': sessionStorage.token}
	});
}


function initCommonMap(sContainer, dLng, dLat, nZoom, nMinZoom, nMaxZoom)
{
	mapboxgl.accessToken = g_sMapboxAccessToken;
	const oMap = new mapboxgl.Map({container: sContainer, style: g_sMapboxStyle, attributionControl: false,
		minZoom:nMinZoom, maxZoom: nMaxZoom, center: [dLng, dLat], zoom: nZoom, accessToken:g_sMapboxAccessToken});
	g_oMap = oMap;
			// disable map rotation using right click + drag
	oMap.dragRotate.disable();
	// disable map rotation using touch rotation gesture
	oMap.touchZoomRotate.disableRotation();
	oMap.doubleClickZoom.disable();
	oMap.addControl(new mapboxgl.NavigationControl({showCompass: false}));
	oMap.addControl(new TextControl('', 'imrcp-zoom-display'));
	addGeojsonToolbar();
	g_oPopup = new mapboxgl.Popup({closeButton: false, closeOnClick: false, anchor: 'bottom', offset: [0, -25], maxwidth: 'none'});
	
	oMap.on('zoom', () => {$('#imrcp-zoom-display').html(oMap.getZoom().toFixed(1));});
	oMap.on('zoomend', () => {$('#imrcp-zoom-display').html(oMap.getZoom().toFixed(1));});
	oMap.on('mousemove', updatePopupPos);
	oMap.on('mousemove', labelGeojson);
	addIcons();
	return oMap;
}


function getLastLayer(oMap)
{
	let oLayers = oMap.getStyle().layers;
	return oLayers[oLayers.length - 1].id;
}

function addIcons()
{
	for (let sIcon of g_oIcons.values())
		g_oMap.loadImage(`images/icons/${sIcon}.png`, (error, image) =>
		{
			g_oMap.addImage(sIcon, image, {'sdf': sIcon.indexOf("-sdf") >= 0});
		});
		
}

function addGeojsonToolbar()
{
	let oMainControl = new MapControlIcons([{t:'Upload GeoJSON', fa:'fa fa-upload fa-lg'}, {t:'View GeoJSON', fa:'fa fa-eye fa-lg', id:'btnViewGeojson'}], 'geojson-control');
	g_oMap.addControl(oMainControl, 'top-right');
	buildUploadGeojsonDialog();
	buildStatusDialog();
	buildViewGeojsonDialog();
	buildConfirmDeleteGeojson();
	
	let oFileInput = $('<input type="file" id="imrcp_geojsonfileinput" title="Upload GeoJSON">').css({'opacity': 0, 'filter':'alpha(opacity=0)', width: 29, height: 29, 'cursor': 'pointer', 'position': 'absolute', 'z-index': -1});
	$('#geojson-control').prepend(oFileInput);
	$("button[title|='Upload GeoJSON']").css('pointer-events', 'none');
	$('#imrcp_geojsonfileinput').on('input', selectGeojson);
	$('#btnViewGeojson').click(openGeojsonDialog);
	disableViewGeojson();
	$.ajax(
	{
		'url': 'api/map/listGeojson',
		'method': 'POST',
		'dataType': 'json',
		'data': {'token': sessionStorage.token}
	}).done(function(oData) 
	{
		if (oData.geojsons.length > 0)
			enableViewGeojson();
	});
}

function disableViewGeojson()
{
	let oButton = $('#btnViewGeojson').prop('disabled', true);
	oButton.children('i').addClass('grayout');
	oButton.attr('title', 'No GeoJSON files');
}

function enableViewGeojson()
{
	let oButton = $('#btnViewGeojson').prop('disabled', false);
	oButton.children('i').removeClass('grayout');
	oButton.attr('title', 'View GeoJSON');
}


function buildUploadGeojsonDialog()
{
	if (!document.getElementById('dlgUploadGeojson'))
	{
		$('body').append('<div id="dlgUploadGeojson"></div>');
	}
	let oDialog = $('#dlgUploadGeojson');
	oDialog.css('overflow', 'visible');
	oDialog.dialog({autoOpen: false, position: {my: "center", at: "center", of: "#" + g_oMap._container.id}, modal: true, draggable: false, resizable: false, width: 400,
		buttons: [
			{text: 'Upload', click: uploadGeojson, id: 'upload_geojson'},
			{text: 'Cancel', click: function() {$(this).dialog('close');}}
		],
		open:function() 
		{
			oDialog.dialog('option', 'position', {my: "center", at: "center", of: "#" + g_oMap._container.id});
		}});
	
	$(window).resize(function()
	{
		oDialog.dialog('option', 'position', {my: "center", at: "center", of: "#" + g_oMap._container.id});
	});
	oDialog.dialog('option', 'title', 'Upload Geojson');
	oDialog.siblings().children('.ui-dialog-titlebar-close').remove();
	let sHtml = '<div class="flexbox"><div class="flex3"><input id="geojson_label" type="text" placeholder="Enter label for geojson file"/></div>';
	oDialog.html(sHtml);
	$('#upload_geojson').prop('disabled', true).addClass('ui-button-disabled ui-state-disabled');
	$('#geojson_label').on('keypress', ignoreInput).on('paste', ignoreInput).on('keydown keyup', checkLabel).on('paste', checkLabel);
}


function buildStatusDialog()
{
	if (!document.getElementById('dlgStatus'))
	{
		$('body').append('<div id="dlgStatus"></div>');
	}
	let oDialog = $('#dlgStatus');
	oDialog.dialog({autoOpen: false, position: {my: "center", at: "center", of: "#" + g_oMap._container.id}, modal: true, draggable: false, resizable: false, width: 400,
		open:function() 
		{
			oDialog.dialog('option', 'position', {my: "center", at: "center", of: "#" + g_oMap._container.id});
		}});
	$(window).resize(function()
	{
		oDialog.dialog('option', 'position', {my: "center", at: "center", of: "#" + g_oMap._container.id});
	});
}


function buildViewGeojsonDialog()
{
	if (!document.getElementById('dlgViewGeojson'))
	{
		$('body').append('<div id="dlgViewGeojson"></div>');
	}
	let oDialog = $('#dlgViewGeojson');
	oDialog.dialog({autoOpen: false, position: {my: "center", at: "center", of: "#" + g_oMap._container.id}, modal: true, draggable: false, resizable: false, width: 'auto',
	open:function() 
	{
		setViewGeojsonHtml();
		oDialog.dialog('option', 'position', {my: "center", at: "center", of: "#" + g_oMap._container.id});
	}});
	$(window).resize(function()
	{
		oDialog.dialog('option', 'position', {my: "center", at: "center", of: "#" + g_oMap._container.id});
	});
	oDialog.dialog('option', 'title', 'Choose Geojson Files To View');
}


function buildConfirmDeleteGeojson()
{
	if (!document.getElementById('dlgConfirmDeleteGeojson'))
	{
		$('body').append('<div id="dlgConfirmDeleteGeojson"></div>');
	}
	let oDialog = $('#dlgConfirmDeleteGeojson');
	oDialog.dialog({autoOpen: false, position: {my: "center", at: "center", of: "#" + g_oMap._container.id}, modal: true, draggable: false, resizable: false, width: 400,
		buttons: [
			{text: 'Go Back', click: function() 
				{
					g_sGeojson = undefined;
					oDialog.dialog('close');
				}},
			{text: 'Confirm Delete', id: 'btnDeleteGeojson', click: deleteGeojson}
		],
		open:function() 
		{
			oDialog.dialog('option', 'position', {my: "center", at: "center", of: "#" + g_oMap._container.id});
		}});
	
	oDialog.html('Deleting a geojson file is permanent. Confirm you would like to delete this file.');
	$(window).resize(function()
	{
		oDialog.dialog('option', 'position', {my: "center", at: "center", of: "#" + g_oMap._container.id});
	});
}


function updateGeojsonSourceAndLayers(sLabel, oGeojson)
{
	let aFeatureList = [[], [], []]; // pointlist, linelist, polylist
	let aSourceLabels = [`imrcp_${sLabel}_points`, `imrcp_${sLabel}_lines`, `imrcp_${sLabel}_polys`];
	g_oLayers[sLabel] = [];
	for (let oFeature of oGeojson.features.values())
	{
		let sType = oFeature.geometry.type.toLowerCase();
		if (sType.indexOf('point') >= 0)
			aFeatureList[0].push(oFeature);
		else if (sType.indexOf('linestring') >= 0)
			aFeatureList[1].push(oFeature);
		else if (sType.indexOf('polygon') >= 0)
			aFeatureList[2].push(oFeature);
	}
	
	let nIndex = aFeatureList.length;
	while (nIndex-- > 0)
	{
		let sSourceLabel = aSourceLabels[nIndex];
		let oSrc = g_oMap.getSource(sSourceLabel);
		if (oSrc !== undefined)
		{
			let aLayers = g_oMap.getStyle().layers.filter(oLayer => oLayer.source === sSourceLabel);
			for (let oLayer of aLayers)
			{
				g_oMap.removeLayer(oLayer.id);
			}
			g_oMap.removeSource(sSourceLabel);
		}
		let aFeatures = aFeatureList[nIndex];
		if (aFeatures.length > 0)
		{
			g_oMap.addSource(sSourceLabel, {'type': 'geojson', 'data': {'type': 'FeatureCollection', 'features': aFeatures}, 'generateId': true});
			g_oLayers[sLabel].push(getLayerObject(sSourceLabel, nIndex));
		}
	}
}


function getLayerObject(sSrc, nType)
{
	if (nType === 0) // point
	{
		return {'id': sSrc, 'type': 'circle', 'source': sSrc, 'paint': {'circle-radius': ['case', ['has', 'width'], ['get', 'width'], ['get', 'width', ['literal', g_oDefaults]]], 'circle-color': ['case', ['has', 'color'], ['get', 'color'], ['get', 'color', ['literal', g_oDefaults]]], 'circle-opacity': ['case', ['has', 'opacity'], ['get', 'opacity'], ['get', 'opacity', ['literal', g_oDefaults]]]}};
	}
	else if (nType === 1) // line
	{
		return {'id': sSrc, 'type': 'line', 'source': sSrc, 'paint': {'line-width': ['case', ['has', 'width'], ['get', 'width'], ['get', 'width', ['literal', g_oDefaults]]], 'line-color': ['case', ['has', 'color'], ['get', 'color'], ['get', 'color', ['literal', g_oDefaults]]], 'line-opacity': ['case', ['has', 'opacity'], ['get', 'opacity'], ['get', 'opacity', ['literal', g_oDefaults]]]}, 'layout':{'line-cap':'round', 'line-join':'round'}};
	}
	else if (nType === 2) // poly
	{
		return {'id': sSrc, 'type': 'fill', 'source': sSrc, 'paint': {'fill-color': ['case', ['has', 'color'], ['get', 'color'], ['get', 'color', ['literal', g_oDefaults]]], 'fill-opacity': ['case', ['has', 'opacity'], ['get', 'opacity'], ['get', 'opacity', ['literal', g_oDefaults]]], 'fill-antialias': false}};
	}
}


function setViewGeojsonHtml()
{
	let aCheckThese = [];
	$('#selectGeojson input:checked').each(function(nIndex, oEl)
	{
		let oJqEl = $(oEl);
		oJqEl.click();
		aCheckThese.push(oJqEl.prop('id'));
	});
	$('#selectGeojson').remove();
	let aGeojsons = Object.keys(g_oGeojsons);
	aGeojsons.sort((a, b) => a.localeCompare(b));
	let sHtml;
	if (aGeojsons.length === 0)
	{
		sHtml = 'Upload a geojson file to view by clicking <i class="fa fa-upload fa-lg"></i>';
		$('#dlgViewGeojson').html(sHtml);
		return;
	}
	else
	{
		sHtml = `<ul id="selectGeojson">`;
		for (let nIndex = 0; nIndex < aGeojsons.length; nIndex++)
		{
			let sGeojson = aGeojsons[nIndex];
			sHtml += `<li id="select_${sGeojson}" class="flexbox"><div class="flex10">${sGeojson}</div><div class="flex1"><input type="checkbox" id="check_${sGeojson}"></div><div id="delete_${sGeojson}" class="flex1 flexbox delete"><div class="fa fa-times clickable"></div></div></li>`;
		}
		sHtml += `</ul><div>Left-click the checkboxes to toggle the display on the map.<br>Left-click <i class="fa fa-times"></i> to delete a file.</div>`;
	}
	
	$('#dlgViewGeojson').html(sHtml);
	$('#selectGeojson > li .delete').on('click', function()
	{
		g_sGeojson = $(this).prop('id').substring('delete_'.length);
		let oDialog = $('#dlgConfirmDeleteGeojson');
		oDialog.dialog('option', 'title', `Confirm Delete - ${g_sGeojson}`);
		$('#dlgConfirmDeleteGeojson').dialog('open');
	});

	$('#selectGeojson :checkbox').on('change', function() 
	{
		let sGeojson = $(this).prop('id').substring('check_'.length);
		if (this.checked) // now checked
		{
			turnOnGeojson(sGeojson);
		}
		else // now unchecked
		{
			turnOffGeojson(sGeojson);
		}
	});
	
	for (let sCheckThis of aCheckThese.values())
	{
		$('#' + sCheckThis).click();
	}
}

function turnOnGeojson(sGeojson)
{
	if (g_oLayers[sGeojson])
	{
		for (let oLayer of g_oLayers[sGeojson].values())
		{
			g_oMap.addLayer(oLayer);
			g_aQueryLayers.push(oLayer.id);
		}
	}
}

function turnOffGeojson(sGeojson)
{
	if (g_oLayers[sGeojson])
	{
		for (let oLayer of g_oLayers[sGeojson].values())
		{
			if (g_oMap.getLayer(oLayer.id))
				g_oMap.removeLayer(oLayer.id);
			
			let nIndex = g_aQueryLayers.indexOf(oLayer.id);
			if (nIndex >= 0)
				g_aQueryLayers.splice(nIndex, 1);
		}
	}
}

function selectGeojson()
{
	let sContents = '';
	let nRecv = 0;
	let oFile = $('#imrcp_geojsonfileinput')[0].files[0];
	if (oFile.size > Math.pow(2, 31))
	{
		alert('Files cannot be bigger than 2GB');
		return;
	}
	let oIn = oFile.stream().getReader();
	oIn.read().then(function processText({done, value})
	{
		if (done)
		{
			g_sGeojson = sContents;
			openUploadDialog();
			return;
		}
		
		nRecv += value.length;
		sContents += new TextDecoder("utf-8").decode(value);
		return oIn.read().then(processText);
	});
}

function openUploadDialog()
{
	$('#geojson_label').val('');
	$('#dlgUploadGeojson').dialog('open');
}


function openGeojsonDialog()
{
	if (!g_bRetrievedGeojson)
	{
		g_bRetrievedGeojson = true;
		showPageoverlay("Retrieving Geojson files...");
		$.ajax(
		{
			'url': 'api/map/getGeojson',
			'method': 'POST',
			'dataType': 'json',
			'data': {'token': sessionStorage.token}
		}).done(function(oData)
		{
			for (let [sLabel, oGeojson] of Object.entries(oData))
			{
				g_oGeojsons[sLabel] = oGeojson;
				updateGeojsonSourceAndLayers(sLabel, oGeojson);
			}
			$('#dlgViewGeojson').dialog('open');
		}).fail(function()
		{
			showPageoverlay("Failed to retrieve Geojson files. Try again later.");
		}).always(function()
		{
			timeoutPageoverlay();
		});
	}
	else
	{
		$('#dlgViewGeojson').dialog('open');
	}
}


function setFilePosition()
{
	
}

function uploadGeojson()
{
	showPageoverlay('Uploading geojson file');
	let sGeojson = g_sGeojson;
	g_sGeojson = undefined;
	let sLabel = $('#geojson_label').val();
	$.ajax(
	{
		'url': 'api/map/saveGeojson',
		'method': 'POST',
		'dataType': 'json',
		'data': {'token': sessionStorage.token, 'label': sLabel, 'geojson': sGeojson}
	}).done(function(oData)
	{
		$('#dlgUploadGeojson').dialog('close');
		showPageoverlay('Successfully uploaded geojson file');
		enableViewGeojson();
		g_oGeojsons[sLabel] = oData.geojson;
		updateGeojsonSourceAndLayers(sLabel, oData.geojson);
		setViewGeojsonHtml();
		let oCheckbox = $('#check_' + sLabel);
		if (oCheckbox[0].checked)
			oCheckbox.click(); // uncheck the box
			
		oCheckbox.click();
	}).fail(function(jqXHR)
	{
		$('#dlgUploadGeojson').dialog('close');
		showPageoverlay('Failed to upload geojson file');
		status(jqXHR.responseJSON.status, jqXHR.responseJSON.msg);
	}).always(function()
	{
		timeoutPageoverlay();
	});
}


function deleteGeojson()
{
	let sGeojson = g_sGeojson;
	g_sGeojson = undefined;
	showPageoverlay(`Deleting ${g_sGeojson}...`);
	$.ajax(
	{
		'url': 'api/map/deleteGeojson',
		'method': 'POST',
		'dataType': 'json',
		'data': {'token': sessionStorage.token, 'label': sGeojson}
	}).done(function()
	{
		showPageoverlay(`Successfully deleted ${sGeojson}`);
		turnOffGeojson(sGeojson);
		for (let oLayer of g_oLayers[sGeojson])
		{
			g_oMap.removeSource(oLayer.source);
		}
		delete g_oGeojsons[sGeojson];
		delete g_oLayers[sGeojson];
		let oViewDialog = $('#dlgViewGeojson');
		oViewDialog.dialog('close');
		if (Object.keys(g_oGeojsons).length === 0)
			disableViewGeojson();
		else
			oViewDialog.dialog('open');
	}).fail(function()
	{
		showPageoverlay(`Failed to delete ${sGeojson}`);
	}).always(function()
	{
		timeoutPageoverlay();
		$('#dlgConfirmDeleteGeojson').dialog('close');
	});
	
}


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

		
		this._container.innerHTML = this.oHtml.reduce(function(accum, opts)
		{
			let sHtml = accum;
			sHtml += `<button class="mapboxgl-ctrl-icon" type="button" title="${opts.t}"${opts.id !== undefined ? ' id="' + opts.id + '"' :''}>`;
			if (opts.i !== undefined)
				sHtml += `<img src="images/${opts.i}.png" />`;
			else
				sHtml += `<i class="${opts.fa}"></i>`;
			sHtml += `</button>`;
			return sHtml;
		}, '');
		

		return this._container;
	}

	onRemove()
	{
		this._container.parentNode.removeChild(this._container);
		this._map = undefined;
	}
}

class TextControl
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
		this._container.className = 'mapboxgl-ctrl mapboxgl-ctrl-group six-button-control';
		this._container.innerHTML = `<button class="mapboxgl-ctrl-icon" title="Current Zoom" disabled type="button" id="${this.sId}">${map.getZoom().toFixed(1)}</button>`;
		return this._container;
	}
	
	onRemove()
	{
		this._container.parentNode.removeChild(this._container);
		this._map = undefined;
	}
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


function checkLabel()
{
	if ($('#geojson_label').val().length === 0)
		$('#upload_geojson').prop('disabled', true).addClass('ui-button-disabled ui-state-disabled');
	else
		$('#upload_geojson').prop('disabled', false).removeClass('ui-button-disabled ui-state-disabled');
}


function status(sTitle, sHtml)
{
	let oStatus = $('#dlgStatus');
	oStatus.dialog('option', 'title', sTitle);
	oStatus.html(sHtml);
	oStatus.dialog('open');
}


function timeoutPageoverlay(nMillis = 1500)
{
	window.setTimeout(function()
	{
		$('#pageoverlay').hide();
	}, nMillis);
}


function showPageoverlay(sContents)
{
	$('#pageoverlay p').html(sContents);
	$('#pageoverlay').css({'opacity': 0.5, 'font-size': 'x-large'}).show();
}


function updatePopupPos(oEvent)
{
	g_oPopup.setLngLat(oEvent.lngLat);
}


function labelGeojson(oEvent)
{
	let oFeatures = g_oMap.queryRenderedFeatures(pointToPaddedBounds(oEvent.point), {'layers': g_aQueryLayers});
	if (oFeatures.length === 0)
	{
		g_oPopup.remove();
		return;
	}
	
	let sHtml = '';
	let oSources = {};
	for (let oFeature of oFeatures.values())
	{
		if (oFeature.properties.label !== undefined)
		{
			let oSet = oSources[oFeature.source];
			if (oSet === undefined)
			{
				oSet = new Set();
				oSources[oFeature.source] = oSet;
			}

			if (!oSet.has(oFeature.id))
			{
				oSet.add(oFeature.id);
				let sList = `<ul style="list-style:none; padding:0;">`;
				let bAdd = false;
				for (let [sKey, sValue] of Object.entries(oFeature.properties))
				{
					if (sKey === 'color' || sKey === 'opacity' || sKey === 'width')
						continue;
					
					bAdd = true;
					sList += `<li><strong>${sKey}</strong>: ${sValue}</li>`;
				}
				if (bAdd)
				{
					sList += '</ul><br>';
					sHtml += sList;
				}
			}			
		}
	}
	
	if (sHtml.length > 0)
	{
		sHtml = sHtml.substring(0, sHtml.length - 4); // remove last <br>
		g_oPopup.setHTML(`${sHtml}`);
		if (!g_oPopup.isOpen())
			g_oPopup.addTo(g_oMap);
	}
	
	
}


export {getNetworksAjax, getProfileAjax, MapControlIcons, addGeojsonToolbar, ignoreInput, initCommonMap, showPageoverlay, timeoutPageoverlay, getLastLayer}