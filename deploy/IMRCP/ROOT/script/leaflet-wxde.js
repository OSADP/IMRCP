/* 
 * Copyright 2017 Federal Highway Administration.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */	
function StaticLayerStyler(style)
{
  this.style = style;
}

StaticLayerStyler.prototype.styleLayer = function (layer)
{
  if (layer.setStyle)
    layer.setStyle(this.style);
};

function RoadStatusStyler(statusStyles, map, highlighter)
{
  this.statusStyles = statusStyles;
  this.highlighter = highlighter;
  this.map = map;
}

RoadStatusStyler.prototype.styleLayer = function (layer)
{

  if (this.highlighter)
  {
    var highlightLines = this.highlighter.getHighlightLines();

    var lineIndex = highlightLines.length;
    while (--lineIndex >= 0)
    {
      var line = highlightLines.pop();
      this.map.removeLayer(line);
    }
  }


  if (layer.setStyle)
  {
    if (layer.englishValue)
    {
      var breakpoints = this.statusStyles.breakpoints;
      var styles = this.statusStyles.styles;
      var value = 1 * layer.englishValue;

      for (var i = 0; i < breakpoints.length; ++i)
      {
        if (value < breakpoints[i])
        {
          layer.setStyle(styles[i]);
          return;
        }
      }
      //not less than any breakpoint, so use the max value style
      layer.setStyle(styles[styles.length - 1]);
    }
    else if (this.statusStyles && this.statusStyles.noData)
    {
      layer.setStyle(this.statusStyles.noData);
    }
  }
};

function RoadHighlighter(highlightStyle, map)
{
  this.style = highlightStyle;
  this.map = map;
  this.highlightLines = [];
}


RoadHighlighter.prototype.styleLayer = function (layer)
{
  layer.setStyle(this.style);
};

RoadHighlighter.prototype.getHighlightLines = function ()
{
  return this.highlightLines;
};


function IconMarkerParser(icon)
{
  this.icon = icon;
}

IconMarkerParser.prototype.parseLayers = function (groupData)
{
  var rowIndex = 0;
  var rowSize = 4;
  var platformFeatureGroup = [];
  while (rowIndex + rowSize <= groupData.length)
  {
    var id = groupData[rowIndex + 0];
    var code = groupData[rowIndex + 1];
    var lat = groupData[rowIndex + 2];
    var lng = groupData[rowIndex + 3];
    var marker = L.wxdeIconMarker(id, lat, lng, code, this.icon);

    platformFeatureGroup.push(marker);
    rowIndex += rowSize;
  }
  return platformFeatureGroup;
};


function ValueIconMarkerParser(codeIconMap)
{
  this.codeIconMap = codeIconMap;
}

ValueIconMarkerParser.prototype.parseLayers = function (groupData)
{
  var rowIndex = 0;
  var rowSize = 4;
  var platformFeatureGroup = [];
  while (rowIndex + rowSize <= groupData.length)
  {
    var id = groupData[rowIndex + 0];
    var code = groupData[rowIndex + 1];
    var lat = groupData[rowIndex + 2];
    var lng = groupData[rowIndex + 3];

    var icon = this.codeIconMap[code];
    if (icon)
    {
      var marker = L.wxdeIconMarker(id, lat, lng, code, icon);

      platformFeatureGroup.push(marker);
    }
    rowIndex += rowSize;
  }
  return platformFeatureGroup;
};


function ValueIconMarkerStyler(codeIconMap)
{
  this.codeIconMap = codeIconMap;
}

ValueIconMarkerStyler.prototype.styleLayer = function (layer)
{
  var icon = this.codeIconMap[layer.getStationCode()];
  if (icon && layer.setIcon)
  {
    layer.setIcon(icon);
  }
};



function PolylineParser()
{

}

PolylineParser.prototype.parseLayers = function (groupData)
{
  var rowIndex = 0;
  var rowSize = 8;
  var platformFeatureGroup = [];
  while (rowIndex + rowSize <= groupData.length)
  {

    var id = groupData[rowIndex + 0];
    var code = groupData[rowIndex + 1];
    var lat = groupData[rowIndex + 2];
    var lng = groupData[rowIndex + 3];
    var status = groupData[rowIndex + 4];
    var metricValue = groupData[rowIndex + 6];
    var englishValue = groupData[rowIndex + 5];
    var points = groupData[rowIndex + 7];

    var marker = L.wxdePolyline(id, points, new L.LatLng(lat, lng), code, {status: status});

    marker.englishValue = englishValue;
    marker.metricValue = metricValue;

    platformFeatureGroup.push(marker);
    rowIndex += rowSize;
  }
  return platformFeatureGroup;
};


function PolygonParser()
{

}

PolygonParser.hexLookup = {"0" : 0, "1" : 1, "2" : 2, "3" : 3, "4" : 4, "5" : 5,
						"6" : 6, "7" : 7, "8" : 8, "9" : 9, "a" : 10, "A" : 10,
						"b" : 11, "B" : 11, "c" : 12, "C" : 12, "d" : 13,
						"D" : 13, "e" : 14, "E" : 14, "f" : 15, "F" : 15};

PolygonParser.prototype.parseLayers = function (groupData)
{
  var rowIndex = 0;
  var rowSize = 5;
  var platformFeatureGroup = [];
  while (rowIndex + rowSize <= groupData.length)
  {

    var id = groupData[rowIndex + 0];
    var code = groupData[rowIndex + 1];
    // var status = groupData[rowIndex + 2];
    var metricValue = groupData[rowIndex + 3];
    var englishValue = groupData[rowIndex + 2];
    var points = groupData[rowIndex + 4];
	if (typeof(points) == "string")
		points = this.decodePoints(points);

    var marker = L.wxdePolygon(id, points, code);

    marker.englishValue = englishValue;
    marker.metricValue = metricValue;

    platformFeatureGroup.push(marker);
    rowIndex += rowSize;
  }
  return platformFeatureGroup;
};

PolygonParser.prototype.decodePoints = function(points)
{
	var nPos = 0;
	var nPoints = this.decodeSigned(points, nPos, 8, 2147483647);
	nPos += 8;
	var oOuterRing = [];

	var nX = this.decodeSigned(points, nPos, 8, 2147483647);
	nPos += 8;
	var nY = this.decodeSigned(points, nPos, 8, 2147483647);
	nPos += 8;
	this.pushIntPointToArray(oOuterRing, nX, nY);
	for (var nIndex = 1; nIndex < nPoints; nIndex++)
	{
		nX += this.decodeSigned(points, nPos, 8, 2147483647);
		nPos += 8;
		nY += this.decodeSigned(points, nPos, 8, 2147483647);
		nPos += 8;
		this.pushIntPointToArray(oOuterRing, nX, nY);
	}
	var oLatLngs = [oOuterRing];
	var nHoles = this.decodeSigned(points, nPos, 8, 2147483647);
	nPos += 8;
	for (var nHoleIndex = 0; nHoleIndex < nHoles; nHoleIndex++)
	{
		nPoints = this.decodeSigned(points, nPos, 8, 2147483647);
		nPos += 8;
		nX = this.decodeSigned(points, nPos, 8, 2147483647);
		nPos += 8;
		nY = this.decodeSigned(points, nPos, 8, 2147483647);
		nPos += 8;
		var oHole = [];
		this.pushIntPointToArray(oHole, nX, nY);
		for (nIndex = 1; nIndex < nPoints; nIndex++)
		{
			nX += this.decodeSigned(points, nPos, 8, 2147483647);
			nPos += 8;
			nY += this.decodeSigned(points, nPos, 8, 2147483647);
			nPos += 8;
			this.pushIntPointToArray(oHole, nX, nY);
		}
		oLatLngs.push(oHole);
	}

	return oLatLngs;
};

PolygonParser.prototype.decodeSigned = function(points, offset, length, maxpos)
{
	var nVal = 0;
	for (var nIndex = offset; nIndex < offset + length; nIndex++)
	{
		nVal *= 16;
		nVal += PolygonParser.hexLookup[points[nIndex]];
	}
	return nVal > maxpos ? nVal - ((maxpos + 1) * 2) : nVal;
};

PolygonParser.prototype.pushIntPointToArray = function(oArray, nX, nY)
{
	oArray.push([nY / 10000000.0, nX / 10000000.0]);
};

L.WxdeSummaryMap = L.Map.extend({
  options: {
    latDiv: null,
    lonDiv: null,
    stationCodeDiv: null,
    platformDetailsWindow:
            {
              dialog: null,
              platformDetailsDiv: null,
              platformObsTable: null,
              platformObsChart: null
            },
    selectedTimeStartFunction: function ()
    {
      return new Date().getTime();
    },
    selectedTimeEndFunction: function ()
    {
      return new Date().getTime();
    },
    selectedObsTypeFunction: function ()
    {
      return 0;
    },
    useMetricUnitsFunction: function ()
    {
      return true;
    }
  },
  initialize: function (id, options)
  {
    L.Map.prototype.initialize.call(this, id, options);
    this._wxdeLayers = [];
    this._minLayerZoom = 18;
    if (this.options.statesLayer)
    {
      this.setStatesLayer(this.options.statesLayer);

    }


    $("#road-legend-form").dialog({
      autoOpen: false,
      modal: true,
      draggable: false,
      resizable: false,
      width: "400",
      height: "auto",
      position: {my: "center", at: "center"}
    });


    $("#details-form").dialog({
      autoOpen: false,
      modal: true,
      draggable: false,
      resizable: false,
      width: "400",
      height: "auto",
      position: {my: "center", at: "center"}
    });

    $("#summary-legend-form").dialog({
      autoOpen: false,
      modal: true,
      draggable: false,
      resizable: false,
      width: "400",
      height: "auto",
      position: {my: "center", at: "center"}
    });


    if (this.options.lstObstypes)
    {
      var thisMap = this;
      var thisObstypeList = $(this.options.lstObstypes);
      $.ajax({
        type: "GET",
        url: "ObsType/list",
        complete: function (data, status)
        {
          var obsList = $.parseJSON(data.responseText);
          for (var rowIndex = 0; rowIndex < obsList.length; ++rowIndex)
          {
            //[{"id":"2001180","name":"canAirTemperature","englishUnits":"F","internalUnits":"C"}
            var obstype = obsList[rowIndex];
            $('<option value="' + obstype.id + '"></option>').appendTo(thisObstypeList).each(function ()
            {
              this.englishUnits = obstype.englishUnits;
              this.internalUnits = obstype.internalUnits;
              this.obstypeName = obstype.name;
            });
          }

          $('<option value="StationCode"></option>').appendTo(thisObstypeList).each(function ()
          {
            this.englishUnits = '';
            this.internalUnits = '';
            this.obstypeName = 'Station Code';
          });

          thisMap.updateObstypeLabels();
        },
        timeout: 3000
      });
    }
  },
  showDialog: function (alwaysShow)
  {
    var zoom = this.getZoom();

    var firstLayerZoom = this.getMinLayerZoom();
    var roadLayerZoom = 11;
    if (zoom >= roadLayerZoom)
    {
      if (!this.roadDialogShown || alwaysShow)
      {
        $("#details-form").dialog("close");
        $("#summary-legend-form").dialog("close");

        this.roadDialogShown = true;
        $("#road-legend-form").dialog("open");
      }

    }
    else if (zoom >= firstLayerZoom)
    {
      if (!this.detailDialogShown || alwaysShow)
      {
        $("#road-legend-form").dialog("close");
        $("#summary-legend-form").dialog("close");

        this.detailDialogShown = true;
        $("#details-form").dialog("open");
      }
    }
    else
    {
      if (!this.summaryDialogShown || alwaysShow)
      {
        $("#road-legend-form").dialog("close");
        $("#details-form").dialog("close");

        this.summaryDialogShown = true;

        $("#summary-legend-form").dialog("open");
      }
    }
  },
  updateObstypeLabels: function ()
  {
    if (this.options.lstObstypes)
    {
      var lstObstypes = this.options.lstObstypes;
      var obstypeCount = lstObstypes.length;
      var useMetricLabel = this.useMetricValue();
      for (var obstypeIndex = 1; obstypeIndex < obstypeCount; ++obstypeIndex)
      {
        var obstypeOption = lstObstypes[obstypeIndex];
        var obstypeName = obstypeOption.obstypeName;
        if (!obstypeName)
          continue;

        var unitLabel = useMetricLabel ? obstypeOption.internalUnits : obstypeOption.englishUnits;

        if (unitLabel)
          $(obstypeOption).text(obstypeName + ' (' + unitLabel + ')');
        else
          $(obstypeOption).text(obstypeName);

      }

    }
  },
  updateObsValueUnits: function ()
  {
    var layerCount = this._wxdeLayers.length;
    var thisMap = this;
    for (var layerIndex = 0; layerIndex < layerCount; ++layerIndex)
    {
      this._wxdeLayers[layerIndex].eachLayer(function (layer)
      {
        if (layer.obsMarker)
          layer.obsMarker.setText(thisMap.useMetricValue() ? layer.metricValue : layer.englishValue);
      }, this._wxdeLayers[layerIndex]);
    }
  },
  showStationCodeLabels: function ()
  {
    this.hasStationCodeLabels = true;
    this.eachWxdeLayer(function (wxdeLayer)
    {
      if (wxdeLayer.showObsLabels())
      {
        wxdeLayer.eachZoomLayer(function (zoomLayer)
        {
          zoomLayer.eachLayer(function (layer)
          {
            if (layer.obsMarker)
            {
              layer.obsMarker.setText(layer.getStationCode());
              if (!zoomLayer.hasLayer(layer.obsMarker))
                zoomLayer.addLayer(layer.obsMarker);
            }
            else
            {
              var obsMarker = L.wxdeObsMarker(layer.getLatLng(), layer.getStationCode());
              layer.obsMarker = obsMarker;
              zoomLayer.addLayer(obsMarker);
            }
          });
        });
      }
    });
  },
  hideLayerDivs: function ()
  {
    this.hasStationCodeLabels = false;
    this.eachWxdeLayer(function (wxdeLayer)
    {
      wxdeLayer.eachZoomLayer(function (zoomLayer)
      {
        zoomLayer.eachLayer(function (layer)
        {
          if (layer.obsMarker)
          {
            zoomLayer.hasLayer(layer.obsMarker);
            zoomLayer.removeLayer(layer.obsMarker);
          }
        });
      });
    });
  },
  eachWxdeLayer: function (method, context)
  {
    if (!this._wxdeLayers)
      return this;
    var layerCount = this._wxdeLayers.length;
    for (var layerIndex = 0; layerIndex < layerCount; ++layerIndex)
    {
      method.call(context, this._wxdeLayers[layerIndex]);
    }
    return this;
  }
  ,
  registerWxdeLayer: function (layer)
  {
    this._wxdeLayers.push(layer);
    return layer.setMap(this);
  }
  ,
  getsummary: function ()
  {
    return this._wxdeLayers;
  }
  ,
  refreshLayers: function ()
  {
    return Promise.all(this._wxdeLayers.map( layer => layer.refreshData()));
    
    //thisLayer.refreshData();
  }
  ,
  reorderLayerElements: function ()
  {
    var layerCount = this._wxdeLayers.length;
    for (var layerIndex = 0; layerIndex < layerCount; ++layerIndex)
    {
      if (this.hasLayer(this._wxdeLayers[layerIndex]))
      {
        this._wxdeLayers[layerIndex].bringToBack();
      }
    }
  }
  ,
  getMinLayerZoom: function ()
  {
    return this._minLayerZoom;
  }
  ,
  setStatesLayer: function (statesLayer)
  {
    var thisMap = this;
    this.statesLayer = statesLayer;
    this.addLayer(statesLayer);



    this.on('zoomend', function (event)
    {
      var zoom = thisMap.getZoom();
      var breakpointZoom = thisMap.getMinLayerZoom() - 1;
      var onMap = thisMap.hasLayer(statesLayer);

      thisMap.showDialog();

      if (zoom > breakpointZoom && onMap)
      {
        thisMap.removeLayer(statesLayer);
      }
      else if (zoom <= breakpointZoom && !onMap)
      {
        thisMap.addLayer(statesLayer);
      }

      var disabled = zoom > breakpointZoom ? false : true;

      $('.disableOnSummary').each(function (idx, el)
      {
        el.disabled = disabled;
        if (disabled)
          $(el).addClass('DisabledElement');
        else
          $(el).removeClass('DisabledElement');
      });


    });


    statesLayer.eachLayer(
            function (layer)
            {
              layer.on('click',
                      function (e)
                      {

                        thisMap.setView(e.target.getBounds().getCenter(), thisMap.getMinLayerZoom());
                        //thisMap.removeLayer(statesGroup);
                      });
            });
  }
  ,
  getStatesLayer: function ()
  {
    return this.statesLayer;
  }
  ,
  getSelectedTimeStart: function ()
  {
    return this.options.selectedTimeStartFunction();
  }
  ,
  getSelectedTimeEnd: function ()
  {
    return this.options.selectedTimeEndFunction();
  }
  ,
  useMetricValue: function ()
  {
    return this.options.useMetricUnitsFunction();
  }
  ,
  getSelectedObsType: function ()
  {
    return this.options.selectedObsTypeFunction();

  }
});
L.wxdeSummaryMap = function (id, options)
{
  return new L.WxdeSummaryMap(id, options);
};
L.WxdeLayer = L.LayerGroup.extend({
  options: {
    checkbox: null, // checkbox input used to enable/disable layer,
    requiresObstype: false,
    hasObs: true,
    hasDetailsPopup: true,
    ignoreObstype: false,
    showObsLabels: true,
    isForecastOnly: false,
    filterFunction: () => true,
    isUserSelectedFn: function ()
    {
      return(!this.checkbox || this.checkbox.checked);
    },
    layerObstypeFunction: function ()
    {
      return 0;
    },
    platformDetailsFunction: function (marker)
    {
      var details = [];
      details.sc = marker.getStationCode();
      //  details.id = marker.getPlatformId();

      var latLng = marker.getLatLng();
      details.lat = latLng.lat.toFixed(6);
      details.lng = latLng.lng.toFixed(6);
      return details;
    },
    obsRequestBoundsFunction: function (marker)
    {
      return marker.requestBounds;
    }
  },
  initialize: function (baseUrl, layerParser, layerStyler, options)
  {
    L.LayerGroup.prototype.initialize.call(this, null);
    L.setOptions(this, options);
    this._baseUrl = baseUrl;
    this._layerParser = layerParser;
    this.layerStyler = layerStyler;
    this._zoomLayers = [];
    this._zoomRequests = [];
    this._highlighter = this.options.highlighter;
    if (this.options.checkbox)
    {
      this._checkbox = this.options.checkbox;
      var thisLayer = this;
      $(this._checkbox).change(function ()
      {
        if (!this.checked && thisLayer._wxdeMap.hasLayer(thisLayer))
          thisLayer._wxdeMap.removeLayer(thisLayer);
        else if (this.checked)
        {
          thisLayer._wxdeMap.addLayer(thisLayer);
          if (thisLayer.isEnabled(thisLayer._wxdeMap.getZoom(), thisLayer._wxdeMap.getSelectedTimeStart()))
            thisLayer.refreshData(true);
        }
      });
    }

  },
  reprocessFilter: function()
  {
    this.eachZoomLayer((zoomLayer) => {
      var filteredLayers = zoomLayer.getFilteredOutLayers();

      var unFilteredLayers = [];
      var i = filteredLayers.length; 
      while(--i >= 0)
      {
        if(this.filter(filteredLayers[i]))
          unFilteredLayers.push(filteredLayers.splice(i,1)[0]);
      }

      zoomLayer.eachLayer( (layer) => {
        if(!this.filter(layer))
        {
          zoomLayer.removeLayer(layer);
          zoomLayer.filterOutLayer(layer);
        }
      });

      i = unFilteredLayers.length;
      while(--i >= 0)
      {
        zoomLayer.addLayer(unFilteredLayers[i]);
      }
    });
  },
  isForecastOnly: function ()
  {
    return this.options.isForecastOnly;
  },
  showObsLabels: function ()
  {
    return this.options.showObsLabels;
  },
  getSelectedObstype: function ()
  {
    return this.options.layerObstypeFunction();
  },
  bringToBack: function ()
  {
    this.eachLayer(function (layer)
    {
      if (layer.bringToBack)
        layer.bringToBack();
    }, this);
  },
  getPlatformDetails: function (marker)
  {
    return this.options.platformDetailsFunction(marker);
  },
  isEnabled: function (zoom, requestTime)
  {
    var enabled = zoom >= this._minZoom;
    if (enabled && requestTime && (this.options.enabledForTime))
    {
      return this.options.enabledForTime(requestTime);
    }

    return enabled;
  },
  filter: function(layer)
  {
    return this.options.filterFunction(layer);
  },
  isUserSelected: function ()
  {
    return this.options.isUserSelectedFn() && (!this._requiresObs() || this.getSelectedObstype() > 0);
  },
  _getZoomLayer: function (zoom)
  {
    var zoomLayer = this._zoomLayers[zoom];
    if (!zoomLayer)
    {
      zoomLayer = L.zoomLayer();
      this.addLayer(zoomLayer);
      this._zoomLayers[zoom] = zoomLayer;
    }
    return zoomLayer;
  },
  _getZoomRequest: function (zoom)
  {
    return this._zoomRequests[zoom];
  },
  _getMarkerObsRequestBounds: function (marker)
  {
    return this.options.obsRequestBoundsFunction(marker);
  },
  _ignoreObstype: function ()
  {
    return this.options.ignoreObstype;
  },
  _hasObs: function ()
  {
    return this.options.hasObs;
  },
  _requiresObs: function ()
  {
    return this.options.requiresObstype;
  },
  eachLayer: function (method, context)
  {
    if (!this._zoomLevels)
      return this;
    for (var zoomIndex = 0; zoomIndex < this._zoomLevels.length; ++zoomIndex)
    {
      this._getZoomLayer(this._zoomLevels[zoomIndex]).eachLayer(method, context);
    }
    return this;
  },
  eachZoomLayer: function (method, context)
  {
    if (!this._zoomLevels)
      return this;
    for (var zoomIndex = 0; zoomIndex < this._zoomLevels.length; ++zoomIndex)
    {
      method.call(context, this._getZoomLayer(this._zoomLevels[zoomIndex]));
    }
    return this;
  },
  setMap: function (map)
  {
    this._wxdeMap = map;
    var thisLayer = this;
    this._wxdeMap.on('dragend', function (event)
    {
      thisLayer.refreshData();
    });
    this._wxdeMap.on('zoomend', function (event)
    {
      thisLayer.refreshData();
    });
    if (map.options.stationCodeDiv || this._highlighter)
    {
      var stationDiv = map.options.stationCodeDiv;
      this._markerMouseOver = function (event)
      {
        if (thisLayer._highlighter)
          thisLayer._highlighter.styleLayer(this);
        if (stationDiv)
          stationDiv.innerHTML = this.getStationCode();
      };
      this._markerMouseOut = function (event)
      {
        if (thisLayer._highlighter)
          thisLayer.layerStyler.styleLayer(this);
        if (stationDiv)
          stationDiv.innerHTML = '';
      };
    }

    var obsTimeFormat = 'MM-DD hh:mm a';
    if (map.options.platformDetailsWindow && this.options.hasDetailsPopup)
    {
      var thisDetailsWindow = map.options.platformDetailsWindow;
      
    
    var chart;
    
    var platformObsChart = $(thisDetailsWindow.platformObsChart);
    var obsTable = $(thisDetailsWindow.platformObsTable);
    var chartContainer = platformObsChart.parent();
    
    const closeChart = () => 
      {
        chartContainer.hide();
        obsTable.show();
        
        if(chart)
          chart.destroy();
          
        chart = null;
      };
      
      chartContainer.find('.close-chart').click(closeChart);
    
      this._markerMouseClick = function (event)
      {
        //mae table visible or invisible based on _hasobs
        var colCount;
        var closeDetailsFn = function ()
        {
          thisDetailsWindow.dialog.dialog("close");
        };
        var platformDetails = thisLayer.getPlatformDetails(this);
        var detailsDiv = thisLayer._wxdeMap.options.platformDetailsWindow.platformDetailsDiv;
        closeChart();
        var buttonElement = '<button type="button" class="ui-button ui-widget ui-state-default ui-corner-all ui-button-icon-only no-title-form ui-dialog-titlebar-close" role="button" title="Close"><span class="ui-button-icon-primary ui-icon ui-icon-closethick"></span><span class="ui-button-text">Close</span></button>';
        var detailsContent = buttonElement;
        detailsContent += platformDetails.sc + '<br />';
        detailsContent += 'Lat, Lon: ' + platformDetails.lat;
        detailsContent += ', ' + platformDetails.lng + ' ';
        detailsDiv.html(detailsContent);
        detailsDiv.find('.ui-dialog-titlebar-close').click(closeDetailsFn);
        var bounds = thisLayer._getMarkerObsRequestBounds(this);
        obsTable.show();
        obsTable.find('tbody > tr').remove();
        obsTable.find('tbody:last-child').append('<tr><td colspan="' + colCount + '">Loading data...</td></tr>');
        if (this instanceof L.Polygon)
          this.setPlatformId(parseInt(platformDetails.sc.toString(), 36));
        
        const startTime = thisLayer._wxdeMap.getSelectedTimeStart() ;
        const chartUrlTemplate = thisLayer._baseUrl + "/chartObs/" + this.getPlatformId() 
                + "/obsstypeId"
                + "/" + startTime
                + "/" + (startTime - 1 * 60 * 60 * 1000)
                + "/" + (startTime + 1 * 60 * 60 * 1000)
                + "/" + bounds.getNorth()
                + "/" + bounds.getWest()
                + "/" + bounds.getSouth()
                + "/" + bounds.getEast()
                + "?src=srcId";

        $.ajax({
          type: "GET",
          url: thisLayer._baseUrl + "/platformObs/" + this.getPlatformId() + "/" + thisLayer._wxdeMap.getSelectedTimeStart() + "/" + thisLayer._wxdeMap.getSelectedTimeEnd() + "/" + bounds.getNorth() + "/" + bounds.getWest() + "/" + bounds.getSouth() + "/" + bounds.getEast(),
          complete: function (data, status)
          {
            if (data.responseText === '')
            {
              obsTable.find('tbody > tr').remove();
              obsTable.find('tbody:last-child').append('<tr><td colspan="' + colCount + '">Error loading data</td></tr>');
              return;
            }
            var additionalDetails = $.parseJSON(data.responseText);
            detailsContent = buttonElement;

            if (additionalDetails.tnm)
              detailsContent += additionalDetails.tnm + '<br />';
            if (additionalDetails.sdet)
              detailsContent += '<div style="max-width:500px; overflow-wrap:break-word;">' + additionalDetails.sdet + '</div>';
            else
              detailsContent += platformDetails.sc + '<br />';
            detailsContent += 'Lat, Lon: ';
            if (additionalDetails.lat && additionalDetails.lon)
              detailsContent += additionalDetails.lat + ', ' + additionalDetails.lon + ' ';
            else
            {
              detailsContent += platformDetails.lat;
              detailsContent += ', ' + platformDetails.lng + ' ';
            }
            if (additionalDetails.tel)
              detailsContent += ' Elevation: ' + additionalDetails.tel;
            detailsDiv.html(detailsContent);
            detailsDiv.find('.ui-dialog-titlebar-close').click(closeDetailsFn);
            obsTable.find('tbody > tr').remove();
            if (thisLayer._hasObs())
            {
              var obsList = additionalDetails.obs;
              if (!obsList || obsList.length === 0)
              {
                obsTable.find('tbody:last-child').append('<tr><td colspan="' + colCount + '">No data</td></tr>');
              }
              else
              {
                var newRows = '';
                for (rowIndex = 0; rowIndex < obsList.length; ++rowIndex)
                {
                  var iObs = obsList[rowIndex];
                  
                  var unit;
                  unit = iObs.eu;
                  
                  newRows += '<tr>';

                  const chartUrl = chartUrlTemplate.replace("obsstypeId", iObs.oi).replace("srcId", iObs.src);

                  newRows += "<td class=\"obsType\">" + iObs.od;
                  if(unit)
                  {
                    newRows += ' <i class="chart-link fa fa-line-chart" ' + 
                          'data-unit="' + unit + '" ' + 
                          'data-obstype="' + iObs.od + '" ' +
                          'data-url="' + chartUrl + '" ></i>';
                  } 
                  
                  newRows += '</td>\n';
                  
                  newRows += "<td class=\"obsType\">" + iObs.src + "</td>\n";
                  newRows += "<td class=\"timestamp\">" + moment(iObs.ts1).format(obsTimeFormat) + "</td>\n";
                  newRows += "<td class=\"timestamp\">" + moment(iObs.ts2).format(obsTimeFormat) + "</td>\n";
                  newRows += "<td class=\"td-value\">" + iObs.ev + "</td>\n";
                  newRows += "<td class=\"unit\">";
                  
                  if (unit)
                    newRows += unit;
                  newRows += "</td>\n";

                  newRows += '</tr>';
                }

                $(newRows).appendTo(obsTable.find('tbody:last-child'))
                        .find('i.chart-link').click(e => {
                          const chartObstype = $(e.target).data("obstype");
                          const chartObsUnit = $(e.target).data("unit");
                          
                          const icon = $(e.target);
                          icon.removeClass('fa-line-chart')
                                  .addClass('fa-spinner fa-spin');
    $.getJSON($(e.target).data("url"))
            .done( data => {
                            icon.addClass('fa-line-chart').removeClass('fa-spinner fa-spin fa-exclamation-circle');
                            let max = 0;
                            let min = 0;
                            for (let i = 0; i < data.length; ++i)
                            {
                              max = Math.max(max, data[i].y);
                              min = Math.min(min, data[i].y);
                            }

                            const step = 10;
                            if (max !== 0) // if max is not 0, then it is above 0
                              max += (step - max % step);

                            if (min !== 0) // if min is not 0, then it is less than 0
                              min -= (step + min % step);
                
              
              
                            var config = {
                              type: 'line',
                              data: {
                                datasets: [{
                                  label: chartObstype,
                                  fill: false,
                                  data: data,
                                  borderColor: 'black'
                                }]
                              },
                              options: {
                                title: {
                                  text: chartObstype 
                               },
                                scales: {
                                  xAxes: [{
                                    type: 'time',
                                    time: {
                                      // round: 'day'
                                      unit: 'hour',
                                      tooltipFormat: obsTimeFormat,
                                      displayFormats: {
                                      }
                                    }
                                  },{
                                    type: 'time',
                                    time: {
                                       unit: 'day',
                                      tooltipFormat: obsTimeFormat,
                                      displayFormats: {
                                      }
                                    }
                                  }],
                                  yAxes: [{
                                    scaleLabel: {
                                      display: true,
                                      labelString: chartObsUnit
                                    },
                                    ticks: {
                                      min: min,
                                      max: max,
                                      stepSize: step
                                    }
                                  }]
                                },
                                animation: {
                                  onComplete: () => 
                                  {
                                    obsTable.hide();
                                    thisDetailsWindow.dialog.dialog("option", "position", {my: "center", at: "center", of: '#map-container'}) ;
                                  }
                                }
                              }
                            };

                            chart = new Chart(platformObsChart.get(0),  config);
                            chartContainer.show();

                  })
                          .fail(() => {
                            icon.removeClass('fa-line-chart').addClass('fa-exclamation-circle');
                            setTimeout(() => icon.addClass('fa-line-chart').removeClass('fa-exclamation-circle'), "3000");
                            });
                        }).data("chart-obs", iObs);
              }
            }
            else
            {
              var sensorList = additionalDetails.sl;
              if (!sensorList || sensorList.length === 0)
              {
                obsTable.find('tbody:last-child').append('<tr><td colspan="' + colCount + '">No data</td></tr>');
              }
              else
              {
                var newRows = '';
                for (var rowIndex = 0; rowIndex < sensorList.length; ++rowIndex)
                {

                  var iSensor = sensorList[rowIndex];
                  newRows += '<tr>\n';
                  newRows += '<td class="obsType">' + iSensor.ot + '</td>\n';
                  newRows += '<td class="sensorIndex">' + iSensor.idx + '</td>\n';
                  newRows += '<td class="sensorMake">' + iSensor.mfr + '</td>\n';
                  newRows += '<td class="sensorModel">' + iSensor.model + '</td>\n';
                  newRows += '</tr>\n';
                }
                obsTable.find('tbody:last-child').append(newRows);
              }
            }


            //    thisDetailsWindow.dialog.dialog("open");

            thisDetailsWindow.dialog.resize();
            thisDetailsWindow.dialog.dialog("option", "position", "center");
          },
          error: function (XMLHttpRequest, textStatus, errorThrown)
          {
            obsTable.find('tbody > tr').remove();
            obsTable.find('tbody:last-child').append('<tr><td colspan="' + colCount + '">Error loading data</td></tr>');
          },
          timeout: 10000
        });
        //     if (!thisLayer._hasObs())
        {
          thisDetailsWindow.dialog.dialog("open");
        }

      };
    }

    if (map.options.latDiv && map.options.lngDiv)
    {
      var latDiv = map.options.latDiv;
      var lngDiv = map.options.lngDiv;
      map.on('mousemove', function (e)
      {
        latDiv.innerHTML = e.latlng.lat.toFixed(6);
        lngDiv.innerHTML = e.latlng.lng.toFixed(6);
      });
    }


    var thisLayer = this;
    return $.ajax({
      type: "GET",
      url: this._baseUrl + "/GetZoomLevels",
      complete: function (data, status)
      {
        var zoomLevels = data.responseJSON;
        thisLayer._zoomLevels = zoomLevels;
        zoomLevels.sort(function (a, b)
        {
          return a - b;
        });
        thisLayer._minZoom = zoomLevels[0];
        map._minLayerZoom = Math.min(map._minLayerZoom, thisLayer._minZoom);
        for (var zoomIndex = 0; zoomIndex < zoomLevels.length; ++zoomIndex)
        {
          var zoomLevel = zoomLevels[zoomIndex];
          var zoomRequest = L.platformRequest(0, 0, L.latLngBounds(L.latLng(0, 0), L.latLng(0, 0)));
          thisLayer._zoomRequests[zoomLevel] = zoomRequest;
        }
      },
      timeout: 3000
    }).promise();
  },
  refreshData: function (firstLoad)
  {
    if (this._wxdeMap)
    {
      var selectedTimeStart = this._wxdeMap.getSelectedTimeStart();
      var selectedTimeEnd = this._wxdeMap.getSelectedTimeEnd();
      var selectedObsType = this.getSelectedObstype();
      var bounds = this._wxdeMap.getBounds().pad(.5);
      var currentZoom = this._wxdeMap.getZoom();
      var currentTime = this._wxdeMap.getSelectedTimeEnd();
      var requestData = false;
      var highestValidZoomIndex = -1;
      //check if this layer is enabled/selected and make sure that the
      //layer is added to or removed from the map based on whether the
      //layer is enabled at the current zoom/time selection
      if (this.isEnabled(currentZoom, currentTime))
      {
        if (this._checkbox)
          this._checkbox.disabled = false;
        if (this.isUserSelected())
        {
          if (!this._wxdeMap.hasLayer(this))
            this._wxdeMap.addLayer(this);
        }
        else
        {
          if (this._wxdeMap.hasLayer(this))
            this._wxdeMap.removeLayer(this);
          return Promise.resolve();
        }
      }
      else
      {
        if (this._wxdeMap.hasLayer(this))
          this._wxdeMap.removeLayer(this);
        if (this._checkbox)
          this._checkbox.disabled = true;
        return Promise.resolve();
      }


      for (var zoomIndex = 0; zoomIndex < this._zoomLevels.length; ++zoomIndex)
      {
        var obstype = selectedObsType;
        var zoomLevel = this._zoomLevels[zoomIndex];
        var zoomLayer = this._getZoomLayer(zoomLevel);
        var zoomLevelRequest = this._getZoomRequest(zoomLevel);
        if (zoomLevel > currentZoom)
        {
          if (this.hasLayer(zoomLayer))
            this.removeLayer(zoomLayer);
          if (zoomIndex - highestValidZoomIndex > 1)
          {
            //if it's more than one zoom level above the current zoom level, drop all points
            zoomLayer.clear();
            zoomLevelRequest.clearValues();
          }
        }
        else
        {

          highestValidZoomIndex = zoomIndex;
          //make sure the layer is currently on the map
          if (!this.hasLayer(zoomLayer))
            this.addLayer(zoomLayer);
          if (this._ignoreObstype())
            obstype = 0;
          //if this layer doesn't have obs the time and obstype doesnt affect what layer elements are returned
          //if it does have obs changing the type or time will clear the cached elements
          if (this._hasObs() && (zoomLevelRequest.obsType !== obstype || zoomLevelRequest.timestamp !== currentTime))
          { 
            zoomLevelRequest.clearValues();
            zoomLayer.clear();
          }
          else if (zoomLevelRequest.latLngBounds.contains(bounds))
            continue;
          requestData = true;
          zoomLevelRequest.setBoundaryValues(bounds);
          zoomLevelRequest.timestamp = currentTime;
          zoomLevelRequest.obsType = obstype;
          var zoomLayer = this._getZoomLayer(zoomLevel);
          zoomLayer.eachLayer(function (layer)
          {
            if (!layer.intersects(bounds))
              zoomLayer.removeLayer(layer);
          });
        }

      }

      //drop cached data from high zoom levels if we have zoomed out
      //far enough

      //drop cached points from the highest zoom level
      //we have data for if we are scrolling around and the current boundary
      //isn't contained by the boundary that we have data for. If it
      // is contained, then we keep working with what we have cached. If it
      //just intersects or is entirely outside of what we have cached, then
      //we will get new points and drop whatever is outside if the current
      //boundary.

      //Check if there is a zoom level we don't already have the data for
      //before requesting new data


      if (requestData)
      {
        var thisLayer = this;
        return $.ajax({
          type: "GET",
          url: this._baseUrl + "/" + selectedTimeStart + "/" + selectedTimeEnd + "/" + currentZoom + "/" + bounds.getNorth() + "/" + bounds.getWest() + "/" + bounds.getSouth() + "/" + bounds.getEast() + "/" + obstype,
          complete: function (data, status)
          {
            var zoomLayers = $.parseJSON(data.responseText);
            var hasMarkerMouseOverEvents = (thisLayer._markerMouseOver && thisLayer._markerMouseOut) ? true : false;
            for (var zoomLevel in zoomLayers)
            {
              if (!zoomLayers.hasOwnProperty(zoomLevel))
                continue;
              var newLayers = thisLayer._layerParser.parseLayers(zoomLayers[zoomLevel], thisLayer._hasObs());
              var zoomLayer = thisLayer._getZoomLayer(zoomLevel);
              for (var layerIndex = 0; layerIndex < newLayers.length; ++layerIndex)
              {
                var layer = newLayers[layerIndex];
                thisLayer.layerStyler.styleLayer(layer);
                
                if(thisLayer.filter(layer))
                  zoomLayer.addLayer(layer);
                else
                  zoomLayer.filterOutLayer(layer);
                  
                layer.requestBounds = bounds;
                if (thisLayer.showObsLabels())
                {
                  var value;
                  if (thisLayer._wxdeMap.hasStationCodeLabels)
                    value = layer.getStationCode();
                  else if (thisLayer._wxdeMap.useMetricValue())
                    value = layer.metricValue;
                  else
                    value = layer.englishValue;
                  if (value)
                  {
                    var obsMarker = L.wxdeObsMarker(layer.getLatLng(), value);
                    layer.obsMarker = obsMarker;
                    zoomLayer.addLayer(obsMarker);
                  }
                }

                if (hasMarkerMouseOverEvents)
                {
                  layer.on('mouseover', thisLayer._markerMouseOver);
                  layer.on('mouseout', thisLayer._markerMouseOut);
                }
                if (thisLayer._markerMouseClick)
                  layer.on('click', thisLayer._markerMouseClick);
              }
            }

            // if (firstLoad)
            thisLayer._wxdeMap.reorderLayerElements();
          },
          timeout: 30000
        });
      } // this will be if a map layer is removed, but added back wiithout panning
      else if (firstLoad)
        this._wxdeMap.reorderLayerElements();
    }
    return Promise.resolve();
  }
});
L.wxdeLayer = function (baseUrl, minZoom, layerParser, layerStyler, options)
{
  return new L.WxdeLayer(baseUrl, minZoom, layerParser, layerStyler, options);
};
L.PlatformRequest = L.LayerGroup.extend({
  initialize: function (timestamp, obsType, latLngBounds)
  {
    this.timestamp = timestamp;
    this.obsType = obsType;
    this.latLngBounds = latLngBounds;
  },
  clearValues: function ()
  {
    this.latLngBounds.getSouthWest().lat = 0;
    this.latLngBounds.getSouthWest().lng = 0;
    this.latLngBounds.getNorthEast().lat = 0;
    this.latLngBounds.getNorthEast().lng = 0;
    this.timestamp = 0;
    this.obsType = 0;
  },
  setBoundaryValues: function (bounds)
  {
    this.latLngBounds.getSouthWest().lat = bounds.getSouthWest().lat;
    this.latLngBounds.getSouthWest().lng = bounds.getSouthWest().lng;
    this.latLngBounds.getNorthEast().lat = bounds.getNorthEast().lat;
    this.latLngBounds.getNorthEast().lng = bounds.getNorthEast().lng;
  }

});
L.platformRequest = function (zoom, timestamp, latLngBounds)
{
  return new L.PlatformRequest(zoom, timestamp, latLngBounds);
};
L.WxdeCircleMarker = L.CircleMarker.extend({
  options: {
    latlngDiv: null,
    stationCodeDiv: null
  },
  initialize: function (id, lat, lng, stationCode, options)
  {
    L.CircleMarker.prototype.initialize.call(this, new L.LatLng(lat, lng), options);
    this._stationCode = stationCode;
    this._platformId = id;
  },
  getPlatformId: function ()
  {
    return this._platformId;
  },
  getStationCode: function ()
  {
    return this._stationCode;
  },
  intersects: function (bounds)
  {
    return bounds.contains(this.getLatLng());
  }
});
L.wxdeCircleMarker = function (id, lat, lng, stationCode, options)
{
  return new L.WxdeCircleMarker(id, lat, lng, stationCode, options);
};
L.WxdeIconMarker = L.Marker.extend({
  options: {
    latlngDiv: null,
    stationCodeDiv: null
  },
  initialize: function (id, lat, lng, stationCode, options)
  {
    L.Marker.prototype.initialize.call(this, new L.LatLng(lat, lng), options);
    this._stationCode = stationCode;
    this._platformId = id;
  },
  getPlatformId: function ()
  {
    return this._platformId;
  },
  getStationCode: function ()
  {
    return this._stationCode;
  },
  intersects: function (bounds)
  {
    return bounds.contains(this.getLatLng());
  }
});
L.wxdeIconMarker = function (id, lat, lng, stationCode, icon)
{
  return new L.WxdeIconMarker(id, lat, lng, stationCode, {icon: icon});
};
L.WxdeObsMarker = L.Marker.extend({
  initialize: function (platformLatlng, value, options)
  {
    if (!options)
      options = {};
    options.icon = L.divIcon({html: value, iconSize: '', className: 'obs-marker-icon'});
    L.CircleMarker.prototype.initialize.call(this, platformLatlng, options);
  },
  intersects: function (bounds)
  {
    return bounds.contains(this.getLatLng());
  },
  setText: function (text)
  {
    $(this._icon).text(text);
  }

});
L.wxdeObsMarker = function (platformLatlng, value, options)
{
  return new L.WxdeObsMarker(platformLatlng, value, options);
};
L.WxdePolyline = L.Polyline.extend({
  options: {
    latlngDiv: null,
    stationCodeDiv: null,
    status: "0"
  },
  initialize: function (id, latlngs, midLatLng, stationCode, options)
  {

    this.midLatLng = midLatLng;
    L.Polyline.prototype.initialize.call(this, latlngs, options);
    this._stationCode = stationCode;
    this._platformId = id;
  },
  getPlatformId: function ()
  {
    return this._platformId;
  },
  getStationCode: function ()
  {
    return this._stationCode;
  },
  intersects: function (bounds)
  {
    return this.getBounds().intersects(bounds);
  },
  getLatLng: function ()
  {
    return this.midLatLng;
  }
});
L.wxdePolyline = function (id, latlngs, midLatLng, stationCode, options)
{
  return new L.WxdePolyline(id, latlngs, midLatLng, stationCode, options);
};
L.WxdePolygon = L.Polygon.extend({
  options: {
    latlngDiv: null,
    stationCodeDiv: null,
    status: "0"
  },
  initialize: function (id, latlngs, stationCode, options)
  {

    L.Polyline.prototype.initialize.call(this, latlngs, options);
    this._stationCode = stationCode;
    this._platformId = id;
  },
  getPlatformId: function ()
  {
    return this._platformId;
  },
  getStationCode: function ()
  {
    return this._stationCode;
  },
  intersects: function (bounds)
  {
    return this.getBounds().intersects(bounds);
  },
  getLatLng: function ()
  {
    return this.getBounds().getCenter();
  },
  setPlatformId: function (id)
  {
    this._platformId = id;
  }
});
L.wxdePolygon = function (id, latlngs, stationCode, options)
{
  return new L.WxdePolygon(id, latlngs, stationCode, options);
};


L.ZoomLayer = L.LayerGroup.extend({
  options: {
  },
  initialize: function (options)
  {
    L.LayerGroup.prototype.initialize.call(this, null);
    this._filteredLayers = [];
  },
  getFilteredOutLayers: function()
  {
    return this._filteredLayers;
  },
  filterOutLayer: function(layer)
  {
    this._filteredLayers.push(layer);
  },
  clear: function()
  {
    this.clearLayers();
    this._filteredLayers = [];
  }
});
L.zoomLayer = function (options)
{
  return new L.ZoomLayer( options);
};



