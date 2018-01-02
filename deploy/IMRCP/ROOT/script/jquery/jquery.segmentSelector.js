
function IconMarkerSelectionStyler(defaultIcon, selectedIcon)
{
  this.defaultIcon = defaultIcon;
  this.selectedIcon = selectedIcon;
}

IconMarkerSelectionStyler.prototype.styleLayer = function (layer)
{
  if (layer.setIcon)
    layer.setIcon(layer.selected ? this.selectedIcon : this.defaultIcon);
};

function StaticIconMarkerStyler(icon)
{
  this.icon = icon;
}

StaticIconMarkerStyler.prototype.styleLayer = function (layer)
{
  if (layer.setIcon)
    layer.setIcon(this.icon);
};

function RoadSelectionStyler(defaultStyle, selectedStyle)
{
  this.defaultStyle = defaultStyle;
  this.selectedStyle = selectedStyle;
}

RoadSelectionStyler.prototype.styleLayer = function (layer)
{
  if (layer.setStyle)
    layer.setStyle(layer.selected ? this.selectedStyle : this.defaultStyle);
};

function RoadSelectionHighlighter(style, selectedElements)
{
  this.style = style;
  this.selectedElements = selectedElements;
}

RoadSelectionHighlighter.prototype.styleLayer = function (layer)
{

  var allowed = false;
  if (this.selectedElements.length > 0)
  {
    var layerBounds = layer.getBounds();
    var padAmount = 0.0003;
    layerBounds = L.latLngBounds(L.latLng(layerBounds.getSouthWest().lat - padAmount, layerBounds.getSouthWest().lng - padAmount),
            L.latLng(layerBounds.getNorthEast().lat + padAmount, layerBounds.getNorthEast().lng + padAmount));

    $.each(this.selectedElements, function (i, segment)
    {
      if (segment.getBounds().intersects(layerBounds))
      {
        allowed = true;
        return false;
      }
    });
  }
  else
    allowed = true;

  layer.selectable = allowed;

  if (!allowed)
    return;

  if (layer.setStyle)
    layer.setStyle(this.style);
};

var SelectionModes = {AREA: 1, SEGMENTS: 2, POINTS: 3};


$.widget("sp.segmentSelector", {
  options: {
    map: null
  },
  beginAreaSelection: function ()
  {
    this._trigger("beforeSelection");
    this.isSelecting = true;
    this.selectionMode = SelectionModes.AREA;
    var mymap = this.options.map;
    var thisWidget = this;
    var mouseDown = function (e)
    {
      thisWidget.selectedRectangle = L.rectangle([e.latlng, e.latlng]);
      var selectedRectangle = thisWidget.selectedRectangle;
      selectedRectangle.addTo(mymap);

      var mouseMove = function (e2)
      {
        selectedRectangle.setBounds([e.latlng, e2.latlng]);
      };

      mymap.on('mousemove', mouseMove);
      var mouseOut;
      mouseOut = function ()
      {
        mymap.off('mousemove', mouseMove);
        mymap.off('mousedown', mouseDown);
        mymap.dragging.enable();

        thisWidget.nw = L.marker(selectedRectangle.getBounds().getNorthWest(), {draggable: true}).addTo(mymap);
        thisWidget.se = L.marker(selectedRectangle.getBounds().getSouthEast(), {draggable: true}).addTo(mymap);

        var nw = thisWidget.nw;
        var se = thisWidget.se;

        var dragEnd = function (e3)
        {
          selectedRectangle.setBounds([nw.getLatLng(), se.getLatLng()]);
        };
        document.getSelection().removeAllRanges();

        nw.on("dragend", dragEnd);
        se.on("dragend", dragEnd);

        mymap.off('mouseup', mouseOut);
        mymap.off('mouseout', mouseOut);
        thisWidget._trigger("initialAreaSelected");
      };

      mymap.on('mouseup', mouseOut);
      mymap.on('mouseout', mouseOut);

    };

    mymap.on('mousedown', mouseDown);
    mymap.dragging.disable();

  },
  beginSegmentSelection: function ()
  {
    this.selectionMode = SelectionModes.SEGMENTS;
    this._trigger("beforeSelection");
    this.isSelecting = true;
    this.options.map.refreshLayers();
  },
  beginPointSelection: function ()
  {
    this.selectionMode = SelectionModes.POINTS;
    this._trigger("beforeSelection");
    this.isSelecting = true;
    this.options.map.refreshLayers();
  },
  endSelection: function ()
  {
    if (!this.isSelecting)
      return;

    switch (this.selectionMode)
    {
      case SelectionModes.AREA:
        if (!this.selectedRectangle)
          return false;
        this.isSelecting = false;
        this._trigger("afterSelection");
        this.nw.remove();
        this.se.remove();
        this.selectedRectangle.remove();
        break;
      case SelectionModes.SEGMENTS:
        if (this.selectedElements.length === 0)
          return false;
        this.isSelecting = false;
        this._trigger("afterSelection");
        
        this._resetSelectedItems(this.roadStyler);
        this.options.map.refreshLayers();
        break;
      case SelectionModes.POINTS:
        if (this.selectedElements.length === 0)
          return false;
        this.isSelecting = false;
        this._trigger("afterSelection");
        
        this._resetSelectedItems(this.detectorStyler);
        this.options.map.refreshLayers();
        break;
    }
  },
  _resetSelectedItems(styler)
  {
    var thisSelector = this;
    $.each(this.selectedElements, function (i, element)
    {
      element.selected = false;
      styler.styleLayer(element);
    });
    this.selectedElements.length = 0;
  },
  hasValidSelection: function ()
  {
    switch (this.selectionMode)
    {
      case SelectionModes.AREA:
        return true && this.selectedRectangle.getBounds();
      case SelectionModes.SEGMENTS:
      case SelectionModes.POINTS:
        return this.selectedElements.length > 0;
    }
    return false;
  },
  mode: function()
  {
    return this.selectionMode;
  },
  bounds: function ()
  {
    if (!this.hasValidSelection())
      return null;

    switch (this.selectionMode)
    {
      case SelectionModes.AREA:
        return this.selectedRectangle.getBounds();
      case SelectionModes.POINTS:
          var selectedBounds = L.latLngBounds(this.selectedElements[0].getLatLng(),this.selectedElements[0].getLatLng());
          
        $.each(this.selectedElements, function (i, segment)
        {
          selectedBounds.extend(segment.getLatLng());
        });
        return selectedBounds;
      break;
      case SelectionModes.SEGMENTS:

        var selectedBounds=this.selectedElements[0].getBounds();
        $.each(this.selectedElements, function (i, segment)
        {
          selectedBounds.extend(segment.getBounds());
        });
        return selectedBounds;
    }
  },
  elements: function ()
  {
    return this.selectedElements;
  },
  _create: function ()
  {

    var thisWidget = this;

    var selectedElements = [];
    this.selectedElements = selectedElements;

    var roadOptions = {
      highlighter: new RoadSelectionHighlighter({weight: 7}, selectedElements),
      showObsLabels: false,
      hasDetailsPopup: false,
      isForecastOnly: true,
      //  requiresObstype: true,
      isUserSelectedFn: function ()
      {
        return thisWidget.isSelecting && thisWidget.selectionMode === SelectionModes.SEGMENTS;
      },
      layerObstypeFunction: function ()
      {
        return 0;
      },
      obsRequestBoundsFunction: function (layer)
      {
        return L.latLngBounds(layer.getLatLng(), layer.getLatLng());
      }};

    function setStandardPolylineStyleProperties(style)
    {
      style.fillColor = style.color;
      style.weight = 2;
      style.opacity = 1;
      style.fillOpacity = 0.8;
      return style;
    }


    var roadStyler = new RoadSelectionStyler(setStandardPolylineStyleProperties({color: "#000000"}), {weight: 7}, this.options.map, roadOptions.highlighter);

    this.roadStyler = roadStyler;

    var roadLayer = L.wxdeLayer('RoadLayer/segselect', new PolylineParser(), roadStyler, roadOptions);


    roadLayer._markerMouseClick = function (event)
    {
      var layer = event.target;

      if (!layer.selectable)
        return;

      layer.selected = !(layer.selected);
      if (layer.selected)
        selectedElements.push(layer);
      else
      {
        $.each(selectedElements, function (i, segment)
        {
          if (segment === layer)
            selectedElements.splice(i);
        });
      }
    };

    this.options.map.registerWxdeLayer(roadLayer);


    var alertIconSize = [20, 20];
    var iconAnchor = [10, 20];
    var alertClassName = "alert-marker-icon";
    var alertIconFolder = "images/icons/";

    var detectorBlack = L.icon({
      iconUrl: alertIconFolder + "detectorblack.png",
      iconSize: alertIconSize,
      className: alertClassName,
      iconAnchor: iconAnchor
    });

    var detectorWhite = L.icon({
      iconUrl: alertIconFolder + "detectorwhite.png",
      iconSize: alertIconSize,
      className: alertClassName,
      iconAnchor: iconAnchor
    });


    var alertLayerOptions = {
      hasDetailsPopup: false,
      highlighter: new StaticIconMarkerStyler(detectorWhite),
      layerObstypeFunction: function ()
      {
        return 4;
      },
      isUserSelectedFn: function ()
      {
        return thisWidget.isSelecting && thisWidget.selectionMode === SelectionModes.POINTS;
      }};


    this.detectorStyler = new IconMarkerSelectionStyler(detectorBlack, detectorWhite);

    var detectorLayer = L.wxdeLayer('alerts/segselect', new IconMarkerParser(detectorBlack), this.detectorStyler, alertLayerOptions);


    detectorLayer._markerMouseClick = function (event)
    {
      var layer = event.target;

      layer.selected = !(layer.selected);
      if (layer.selected)
        selectedElements.push(layer);
      else
      {
        $.each(selectedElements, function (i, segment)
        {
          if (segment === layer)
            selectedElements.splice(i);
        });
      }
    };

    this.options.map.registerWxdeLayer(detectorLayer);
  },
  _destroy: function ()
  {
  },
  _setOption: function (key, value)
  {
    this._super(key, value);
  }
});