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
 {
  var mymap;


  var reportStepDivs;
  function setReportStep(step)
  {
    reportStepDivs.hide();
    $("#divReportStep" + step).show();
  }

  function resetSubscriptionFields()
  {
    $('#txtName').val('');
    $('#txtDescription').val('');
    $('#txtMax').val('');
    $('#txtMin').val('');
    $("#lstReportObstypes option:selected").prop("selected", false);
    $("#lstReportObstypes").change();
    $("#lstOffset").prop('selectedIndex', '0');
    $("#lstDuration").prop('selectedIndex', '0');

    $("#divSubOffsetSlider").labeledSlider('values', 0, 0);
    $("#divSubOffsetSlider").labeledSlider('values', 1, 15);

    $('#divSubResult,#divSubError').hide();
    $("#btnSubmitReport, #divSubInput").show();
    $("#btnClose").val('Cancel');
    $("#divReportDialog").dialog("option", "position", {my: "center", at: "center", of: '#map-container'});
  }



  function setStandardStyleProperties(style)
  {
    style.radius = 4;
    style.fillColor = style.color;
    style.color = 'black';
    style.weight = 1;
    style.opacity = 1;
    style.fillOpacity = 1;
    return style;
  }



  $(document).ready(function ()
  {


    var notificationsDialog = $('#divNotificationDialog').notificationDialog(
            {
              selectNotification: function (e, notification)
              {
                mymap.fitBounds([[notification.lat1, notification.lon1], [notification.lat2, notification.lon2]]);
              },
              displayOnNewAlerts: function ()
              {
                return $('#chkAutoRefresh').attr('checked');
              }
            });

    function loadNotifications()
    {

      $.getJSON("notify/" + (currentMoment.valueOf()) + "/" + (currentMoment.valueOf() + mapTimeSlider.labeledSlider('value') * 60 * 1000), function (data)
      {
        $('#divNotificationDialog').notificationDialog("processNotifications", data);
      });
    }

    $('#chkNotifications').change(function (e)
    {
      if (this.checked)
        $("#divNotificationDialog").notificationDialog("open");
    });


    $.ajax({
      type: "GET",
      url: "ResetSession"
    });

    reportStepDivs = $("#divReportStep1, #divReportStep2, #divReportStep3");


    function setStandardPolylineStyleProperties(style)
    {
      style.fillColor = style.color;
      style.weight = 5;
      style.opacity = 1;
      style.fillOpacity = 0.8;
      return style;
    }

    function setStandardPolygonStyleProperties(style)
    {
      style.stroke = false;
      style.fillColor = style.color;
      style.weight = 2;
      style.opacity = 1;
      style.fillOpacity = 0.4;
      return style;
    }


    var obstypeStyles = {};

    obstypeStyles[0] = {};
    obstypeStyles[0].noData = setStandardPolylineStyleProperties({color: "#000000"});

    function handleColorList(colorList, colorsToStyleFn)
    {
      for (var j = 0; j < colorList.length; ++j)
      {
        var styleColor = colorList[j];
        colorList[j] = colorsToStyleFn({color: styleColor});
      }
      return colorList;
    }

    var noDataStyles = setStandardPolylineStyleProperties({color: "#ffffff"});
    noDataStyles.opacity = 0;

    var obstypeMap = {};
    obstypeMap[5] = {};
    obstypeMap[5].noData = setStandardPolylineStyleProperties({color: "#000000"});

    function addObstypeSelectOptions(selectId, obstypeList, colorsToStyleFn)
    {
      var count = obstypeList.length;
      var obstypeOptions = '';
      for (var index = 0; index < count; ++index)
      {
        var obstype = obstypeMap[obstypeList[index]];
        if (!obstype.rangeColors)
          continue;
        obstypeOptions += '<option value ="' + obstype.id + '">' + obstype.label + '</option>';

        var styleSet = {};
        styleSet.styles = handleColorList(obstype.rangeColors, colorsToStyleFn);
        styleSet.breakpoints = obstype.rangeBreakPoints;
        styleSet.styleLabels = obstype.rangeLabels;
        styleSet.noDataLabel = obstype.noDataLabel;
        styleSet.noData = noDataStyles;

        obstypeStyles[obstype.id] = styleSet;
      }
      $(obstypeOptions).appendTo($('#' + selectId));
    }

    $.ajax({
      url: 'obstypes.jsp',
      type: 'get',
      dataType: 'json',
      success: function (obstypes)
      {
        var index = obstypes.length;
        while (--index >= 0)
        {
          var obstype = obstypes[index];
          obstypeMap[obstype.name] = obstype;
          obstypeMap[obstype.id] = obstype;
        }

        var roadLayerObstypes = ["STPVT", "TPVT", "DPHSN", "DPHLNK", "TRFLNK", "SPDLNK", "TDNLNK", "VOLLNK", "TIMERT"];

        var areaLayerObstypes = ["TAIR", "VIS", "GSTWND", "SPDWND", "RDR0", "PCCAT", "EVT"];

        addObstypeSelectOptions('lstRoadObstypes', roadLayerObstypes, setStandardPolylineStyleProperties);

        addObstypeSelectOptions('lstAreaObstypes', areaLayerObstypes, setStandardPolygonStyleProperties);

        var obstypeOptions = '';
        for (var index = 0; index < obstypes.length; ++index)
        {
          var obstype = obstypes[index];
          obstypeOptions += '<option value ="' + obstype.id + '">' + obstype.name + '(' + obstype.desc + ')</option>';
        }
        $(obstypeOptions).appendTo('#lstReportObstypes');

        updateRoadWeight(); //override the default weight
      },
      error: function ()
      {
        $('#lstReportObstypes,#lstRoadObstypes,#lstAreaObstypes').empty().append('<option value="0">ERROR</option>').prop('disabled', 'disabled');
      }
    });

    var tileLayer = L.tileLayer('<background map layer url>', {
      maxZoom: 17,
      subdomains: ['1', '2', '3', '4'],
      attribution: '',
      id: 'mapbox.light'
    });


    var dialog = $("#dialog-form").dialog({
      autoOpen: false,
      resizable: true,
      draggable: true,
      width: 'auto',
      modal: true,
      //  dialogClass: "no-title-form",
      position: {my: "center", at: "center", of: '#map-container'},
      minHeight: 380

    });

    mymap = L.wxdeSummaryMap('mapid', {
      attributionControl: false,
      layers: [tileLayer],
      minZoom: 10,
      maxZoom: 17,
//  stationCodeDiv: document.getElementById('stationCodeValue'),
//  latDiv: document.getElementById('latValue'),
//  lngDiv: document.getElementById('lngValue'),
//  lstObstypes: document.getElementById('obstypes'),
      platformDetailsWindow:
              {
                dialog: dialog,
                platformDetailsDiv: $('#dialog-form').parent().find('.ui-dialog-title'),
                platformObsTable: $('#obs-data')
              },
      selectedTimeStartFunction: function ()
      {
//        var selectedValue = mapTimeSlider.labeledSlider('value');
        return currentMoment.valueOf(); //+ selectedValue * 60 * 1000;
      },
      selectedTimeEndFunction: function ()
      {
        return currentMoment.valueOf() + mapTimeSlider.labeledSlider('value') * 60 * 1000;
      },
      useMetricUnitsFunction: function ()
      {
        return false;
//    return $("#metricUnits").is(':checked');
      }
    });


    mymap.createPane('labels');
    mymap.getPane('labels').style.zIndex = 650;
    mymap.getPane('labels').style.pointerEvents = 'none';
    L.tileLayer('<road label layer url>', {
      maxZoom: 17,
      attribution: '',
      pane: 'labels'
    }).addTo(mymap);


    mymap.fitBounds([[38.957, -94.519], [38.856, -94.724264]]);


    $("#divReportDialog").dialog({
      autoOpen: false,
      resizable: false,
      draggable: false,
      width: 'auto',
      modal: true,
      dialogClass: "no-title-form",
      position: {my: "center", at: "center", of: '#map-container'},
      close: function (e, ui)
      {
        resetSubscriptionFields();
      }
    });


    var roadOptions = {checkbox: document.getElementById("chkRoadLayer"),
      highlighter: new RoadHighlighter({weight: 7}, mymap),
      showObsLabels: false,
      isForecastOnly: true,
      //  requiresObstype: true,
      isUserSelectedFn: function ()
      {
        return $("#lstRoadObstypes").prop('selectedIndex') > 0;
      },
      enabledForTime: function (time)
      {
        return true;
      },
      layerObstypeFunction: function ()
      {
        return $("#lstRoadObstypes").val();
      },
      obsRequestBoundsFunction: function (layer)
      {
        return L.latLngBounds(layer.getLatLng(), layer.getLatLng());
      }};


    var roadStyler = new RoadStatusStyler({styles: [], breakpoints: []}, mymap, roadOptions.highlighter);

    var roadLayer = L.wxdeLayer('RoadLayer', new PolylineParser(), roadStyler, roadOptions);
    mymap.registerWxdeLayer(roadLayer);


    var jCheckboxes = $("#chkAutoRefresh,#chkNotifications");
    var jObSelects = $("#lstRoadObstypes, #lstAreaObstypes, #lstAlertObstypes");

    var segmentSelector = $('#mapid').segmentSelector({map: mymap, roadHighlighter: RoadHighlighter, roadStyler: roadStyler,
      beforeSelection: function ()
      {
        jCheckboxes.each(function ()
        {
          var jqThis = $(this);
          jqThis.data("prevValue", jqThis.attr("checked")).attr("checked", false).prop("disabled", true);
        });

        jObSelects.each(function ()
        {
          var jqThis = $(this);
          jqThis.data("prevValue", this.selectedIndex).prop("disabled", true);
          this.selectedIndex = 0;
          jqThis.change();
        });
      },
      afterSelection: function ()
      {
        jCheckboxes.each(function ()
        {
          var jqThis = $(this);
          jqThis.attr("checked", jqThis.data("prevValue")).prop("disabled", false);
        });

        jObSelects.each(function ()
        {
          var jqThis = $(this);
          this.selectedIndex = jqThis.data("prevValue");
          jqThis.prop("disabled", false).change();
        });
      }
    });

// set road weight based on zoom and re-style existing layers if it changes
    function updateRoadWeight()
    {
      var newWeight = Math.round((mymap.getZoom() - 11.0) / 2.0);
      if (newWeight < 1)
        newWeight = 1;

      var currentWeight = obstypeStyles[0].noData.weight;

      if (currentWeight === newWeight)
        return;

      $.each(obstypeStyles, function (obstype, settings)
      {
        if (settings.styles)
        {
          $.each(settings.styles, function (index, style)
          {
            style.weight = newWeight;
          });
        }

        if (settings.noData)
          settings.noData.weight = newWeight;
      });

      roadLayer.eachLayer(function (layer)
      {
        roadStyler.styleLayer(layer);
      });
    }

    mymap.on('zoomend', updateRoadWeight);


    var areaLayerOptions = {checkbox: document.getElementById("chkRoadLayer"),
      //highlighter: new RoadHighlighter(highlightRoadStyle, mymap),
      showObsLabels: false,
      isForecastOnly: true,
      requiresObstype: true,
      highlighter: new StaticLayerStyler({fillOpacity: 0.7}),
      enabledForTime: function (time)
      {
        return true;
      },
      layerObstypeFunction: function ()
      {
        return $("#lstAreaObstypes").val();
      },
      obsRequestBoundsFunction: function (layer)
      {
        var oPoint = layer._events.click[0].fn.arguments[0].latlng;
        return L.latLngBounds(oPoint, oPoint);
      }};


    var areaStyler = new RoadStatusStyler({styles: [], breakpoints: []});
    mymap.registerWxdeLayer(L.wxdeLayer('areas', new PolygonParser(), areaStyler, areaLayerOptions));


    var rwisStyle = setStandardStyleProperties({color: "#d819d8"});

    var alertIconSize = [32, 37];
    var alertIconAnchor = [16, 37];
    var alertClassName = "alert-marker-icon";
    var alertIconFolder = "images/icons/";

    var alertIcons = {
        4: "detectorblack.png",
      203: "blowing_snow.png",
      201: "dew_on_roadway.png",
      305: "flooded_road.png",
      202: "frost_on_roadway.png",
      301: "incident.png",
      306: "lengthy_queue.png",
      107: "low_visibility.png",
      106: "Heavy_Precip.png",
      105: "Medium_Precip.png",
      104: "Light_Precip.png",
      303: "slow_traffic.png",
      304: "very_slow_traffic.png",
      101: "Light_Winter_Precip.png",
      102: "Medium_Winter_Precip.png",
      103: "Heavy_Winter_Precip.png",
      302: "workzone.png",
      204: "ice_on_bridge.png",
      307: "unusual_congestion.png"
    };

    //non-standard icon sizes
    var alertIconSizes = {4: {iconSize: [20, 20], iconAnchor: [10, 20]}};

    $.each(alertIcons, function (alertValue, iconName)
    {
      var iconSize = alertIconSize;
      var iconAnchor = alertIconAnchor;

      if(alertIconSizes[alertValue])
      {
        iconSize = alertIconSizes[alertValue].iconSize;
        iconAnchor = alertIconSizes[alertValue].iconAnchor;
      }

      alertIcons[alertValue] = L.icon({
        iconUrl: alertIconFolder + iconName,
        iconSize: iconSize,
        className: alertClassName,
        iconAnchor: iconAnchor
      });
    });



    var alertLayerOptions = {checkbox: document.getElementById("chkRoadLayer"), hasDetailsPopup: true,

      layerObstypeFunction: function ()
      {
        return $("#lstAlertObstypes").val();
      },
      isUserSelectedFn: function ()
      {
        return $("#lstAlertObstypes").prop('selectedIndex') > 0;
      },
      obsRequestBoundsFunction: function (layer)
      {
        return L.latLngBounds(layer.getLatLng(), layer.getLatLng());
      }};
    mymap.registerWxdeLayer(L.wxdeLayer('alerts', new ValueIconMarkerParser(alertIcons), new StaticLayerStyler(rwisStyle), alertLayerOptions));



    var autoCenteredDialogs = $("#divReportDialog, #dialog-form, #divNotificationDialog");
    $(window).resize(function ()
    {
      var position = {my: "center", at: "center", of: '#map-container'};
      autoCenteredDialogs.each(function (index)
      {
        if ($(this).hasClass("ui-dialog-content")) // Test if the dialog has been initialized
          $(this).dialog("option", "position", position);
      });
      $("#divNotificationDialog").notificationDialog("option", "position", position);
    });

    var dialogDrag;
    dialogDrag = function ()
    {
      autoCenteredDialogs = autoCenteredDialogs.not('#dialog-form');
      $('#dialog-form').off('dialogdragstart', dialogDrag);
    };
    $('#dialog-form').on('dialogdragstart', dialogDrag);



    var minuteInterval = 60;
    var startDate = new Date();

    var currentMoment = moment().seconds(0).milliseconds(0);

    startDate.setSeconds(0);
    startDate.setMilliseconds(0);
    startDate.setMinutes(0);

//set max time to the next interval + 6 hours
    var maxTime = new Date(startDate.getTime() + (1000 * 60 * 60 * 6) + (1000 * 60 * minuteInterval));
    maxTime.setMinutes(maxTime.getMinutes() - maxTime.getMinutes() % minuteInterval);

    $('#txtReportRefTime').datetimepicker({
      step: minuteInterval,
      value: startDate,
      formatTime: 'h:i a',
      format: 'Y/m/d h:i a',
      yearStart: 2017,
      yearEnd: maxTime.getUTCFullYear(),
      maxTime: maxTime,
      maxDate: maxTime
    });


    //start/end dates have the same initial value, but passing the same
    //date object results in them returning the same date value even when they
    //are set differently by the user
//    var endDate = new Date();
//    endDate.setTime(startDate.getTime());
//    $('#txtReportEnd').datetimepicker({
//      step: minuteInterval,
//      value: endDate,
//      formatTime: 'h:i a',
//      format: 'Y/m/d h:i a',
//      yearStart: 2017,
//      yearEnd: maxTime.getUTCFullYear(),
//      maxTime: maxTime,
//      maxDate: maxTime
//    });

    var handle;
    var btnTime = $('#btnTime');

    function displaySelectedTime(moment)
    {

      handle.text(moment.format("h:mm a"));
      btnTime.val(moment.format("YYYY/MM/DD"));
    }



    $('#txtTime').datetimepicker({
      step: minuteInterval,
      value: startDate,
      formatTime: 'h:i a',
      format: 'Y/m/d h:i a',
      yearStart: 2017,
      yearEnd: maxTime.getUTCFullYear(),
      maxTime: maxTime,
      maxDate: maxTime,
      onClose: function ()
      {
        var newTime = moment(this.getValue()).seconds(0).milliseconds(0);
        updateNowTime(newTime);
        //If the newly selected time is within a minute of the current time, re-enable auto-refresh
        $('#chkAutoRefresh').prop('checked', Math.abs(newTime.valueOf() - new Date().getTime()) < 60000);
      }
    });

    btnTime.click(function ()
    {
      $('#txtTime').datetimepicker('show');
    });


//currentDate.getTime() + mapTimeSlider.labeledSlider('value') * 60 * 1000
//moment(currentDate).format("YYYY/MM/DD")
    var mapTimeSlider = $("#slider").labeledSlider({
      slide: function (event, ui)
      {
        displaySelectedTime(moment(currentMoment).add(mapTimeSlider.labeledSlider('value'), 'minutes'));
      },
      labelPosition: 'on',
      min: -240,
      max: 480,
      //    step: 1,
      value: 0,
//      allowedValues: values,
      ticks: true,
//      labelValues: [0],
      labels: {0: currentMoment.format("h:mm a")},
      defaultLabelFn: minutesToHHmm,
      labelValues: [-180, -120, -60, 0, 60, 120, 180, 240, 300, 360, 420],
      tickValues: [-225, -210, -195, -165, -150, -135, -105, -90, -75, -45, -30, -15, 15, 30, 45, 75, 90, 105, 135, 150, 165, 195, 210, 225, 255, 270, 285, 315, 330, 345, 375, 390, 405, 435, 450, 465],
      stop: function (event, ui)
      {
        displaySelectedTime(moment(currentMoment).add(mapTimeSlider.labeledSlider('value'), 'minutes'));
        mymap.refreshLayers();
        loadNotifications();
      }
    });
    handle = $('#slider > .ui-slider-handle');

    var jSlider = $("#slider");
    jSlider.width(jSlider.width());

    displaySelectedTime(currentMoment);

    var updateRangeFn = function (event, ui)
    {
      var offset = ui.values[0];
      var duration = ui.values[1] - offset;
      $('#spnOffset').text(minutesToHHmm(offset));
      $('#spnDuration').text(minutesToHHmm(duration, true));
    };

    var subWindowSlider = $("#divSubOffsetSlider").labeledSlider({
      min: -1440,
      range: true,
      max: 480,
      step: 30,
      values: [0, 30],
      ticks: true,
      labelValues: [-1440, -1200, -960, -720, -480, -240, 0, 240, 480],
      defaultLabelFn: minutesToHH,
      slide: updateRangeFn,
      change: updateRangeFn
    });


    $("#lstReportOptions").change(function ()
    {
      if ($('#optSegmentReport').is(':selected'))
        segmentSelector.segmentSelector("beginSegmentSelection");
      else if ($('#optAreaReport').is(':selected'))
        segmentSelector.segmentSelector("beginAreaSelection");
      else if ($('#optDetectorReport').is(':selected'))
        segmentSelector.segmentSelector("beginPointSelection");

      setReportStep(2);

      $('#optInitialReport').prop('selected', true);
    });

    $("#btnSetReportArea").click(function ()
    {
      if(!segmentSelector.segmentSelector("hasValidSelection"))
        return;
      $("#divReportDialog").dialog("open");
      $('#txtReportRefTime').datetimepicker('setOptions', {value: new Date(currentMoment.valueOf())});
//      $('#txtReportEnd').datetimepicker('setOptions', {value: new Date(currentMoment.valueOf())});


      var selectedBounds = segmentSelector.segmentSelector("bounds");

      mymap.fitBounds(selectedBounds);
      var selectedNw = selectedBounds.getNorthWest();
      var selectedSe = selectedBounds.getSouthEast();
      $("#txtLat1").val(selectedNw.lat.toFixed(6));
      $("#txtLng1").val(selectedNw.lng.toFixed(6));
      $("#txtLat2").val(selectedSe.lat.toFixed(6));
      $("#txtLng2").val(selectedSe.lng.toFixed(6));
      setReportStep(0);
    });


    $("#radTypeReport").click(function ()
    {
      $(".SubscriptionOnly").hide();
      var subWindowSlider = $("#divSubOffsetSlider");
      subWindowSlider.labeledSlider('option', 'values', [0, 30]);
      subWindowSlider.labeledSlider('option', 'min', -1440);
      subWindowSlider.labeledSlider('option', 'labelValues', [-1440, -1200, -960, -720, -480, -240, 0, 240, 480]);
      $(".ReportOnly").show();
      $('#spnType').text("Report");
    });
    $("#radTypeSubscription").click(function ()
    {
      $(".ReportOnly").hide();
      var subWindowSlider = $("#divSubOffsetSlider");
      subWindowSlider.labeledSlider('option', 'values', [0, 30]);
      subWindowSlider.labeledSlider('option', 'min', -240);
      subWindowSlider.labeledSlider('option', 'labelValues', [-240, -120, 0, 120, 240, 360, 480]);
      $(".SubscriptionOnly").show();
      $('#spnType').text("Subscription");
    });

    $("#radTypeReport").click();

    $("#lstReportObstypes").change(function ()
    {
      var val = $("#lstReportObstypes").val();
      if (val.length === 1)
        $('#trMinMax').show();
      else
        $('#trMinMax').hide();
    });

    $("#lstReportObstypes").change();

    $('#divReportDialog').on('dialogclose', function (event)
    {
      setReportStep(1);
      segmentSelector.segmentSelector("endSelection");
    });

    $("#btnSubmitReport").click(function ()
    {
      $("#btnSubmitReport").hide();
      $("#btnClose").val('Close');
      var obstypeList = $('#lstReportObstypes').val();

      var offset = subWindowSlider.labeledSlider('values', 0);
      var duration = subWindowSlider.labeledSlider('values', 1) - offset;

      var elementIds = [];

      $.each(segmentSelector.segmentSelector("elements"), function (index, segment)
      {
        elementIds.push(segment.getPlatformId());
      });

      var requestData = {
        lat1: $('#txtLat1').val(),
        lon1: $('#txtLng1').val(),
        lat2: $('#txtLat2').val(),
        lon2: $('#txtLng2').val(),
        format: $('#lstFormat').val(),
        offset: offset,
        duration: duration,
        elementIds: elementIds,
        elementType: elementIds.length === 0 ? null : segmentSelector.segmentSelector("mode"),
        name: $('#txtName').val(),
        description: $('#txtDescription').val()
      };
      if (obstypeList.length > 0)
      {
        requestData.obsTypeId = obstypeList;
        if (obstypeList.length === 1)
        {
          requestData.maxValue = $('#txtMax').val();
          requestData.minValue = $('#txtMin').val();
        }
      }

      if ($('#radTypeReport').prop("checked"))
      {
        requestData.reftime = $('#txtReportRefTime').datetimepicker('getValue').getTime();
        requestData.starttime = requestData.reftime + offset * 60000;
        requestData.endtime = requestData.starttime + duration * 60000;
      }
      else
      {
        requestData.cycle = $('input[name="interval"]:checked').val();
      }


      $.ajax({
        url: 'reports',
        type: 'post',
        dataType: 'json',
        data: requestData,
        success: function (data)
        {
          $('#spnName').text(data.name);
          $('#spnDesc').text(data.desc);
          $('#spnUuid').text(data.uuid);

          var url = window.location.href;
          url = url.substring(0, url.lastIndexOf('/')) + '/reports/' + data.uuid + '/files/[filename]';
          $('#spnUrl').text(url);

          $('#divSubInput').hide();
          $('#divSubResult').show();
          $("#divReportDialog").dialog("option", "position", {my: "center", at: "center", of: '#map-container'});
        },
        error: function ()
        {
          $('#divSubInput').hide();
          $('#divSubError').show();
        }
      });
    });

    $("#btnClose").click(function ()
    {
      $("#divReportDialog").dialog("close");

    });

    $("#lstAlertObstypes").change(function ()
    {
      mymap.refreshLayers();
    });


    $("#lstRoadObstypes").data("legendId", "divRoadLegend");
    $("#lstRoadObstypes").data("layerStyler", roadStyler);

    $("#lstAreaObstypes").data("legendId", "divAreaLegend");
    $("#lstAreaObstypes").data("layerStyler", areaStyler);


    var obstypeSelects = $('#lstRoadObstypes, #lstAreaObstypes');
    obstypeSelects.change(function ()
    {
      var lstObstypes = $(this);
      var selectedObsType = lstObstypes.val() * 1;
      var selectedIndex = lstObstypes.prop('selectedIndex');

      var legendId = lstObstypes.data('legendId');
      if (legendId)
      {
        var legend = $('#' + legendId);
        if (selectedIndex > 0)
        {
          legend.offset({left: lstObstypes.offset().left});

          var obstype = obstypeStyles[selectedObsType];

          var layerStyler = lstObstypes.data('layerStyler');
          if (layerStyler)
            layerStyler.statusStyles = obstype;

          var selectedObsType = obstypeMap[lstObstypes.val()];
          if (obstype.styleLabels && selectedObsType.rangeBreakPoints.length > 0)
          {
            var newLegendContent = selectedObsType.label;
            if (selectedObsType.englishUnits.length > 0)
              newLegendContent += ' (' + selectedObsType.englishUnits + ')';
            newLegendContent += '<br />';
            if (obstype.noData)
            {
              var noDataLabel = obstype.noDataLabel ? obstype.noDataLabel : 'no data';
              newLegendContent += '<i class="fa fa-square-o" style="color: #000"></i> ' + noDataLabel + '<br/>';
            }

            var rangeCount = obstype.styleLabels.length;
            for (var rangeIndex = 0; rangeIndex < rangeCount; ++rangeIndex)
            {
              var label = obstype.styleLabels[rangeIndex];
              var color = obstype.styles[rangeIndex].color;

              newLegendContent += '';

              if (color === "#ffffff")// if the color is white, then use a black background with a white rounded square inside it
                newLegendContent += '<i class="fa fa-square-o" style="color: #000"></i> ';
              else
                newLegendContent += '<i class="fa fa-stop " style="color: ' + color + '"></i> ';

              newLegendContent += label + '<br/>';
            }
            legend.html(newLegendContent);
            legend.show();
          }
          else
            legend.hide();

        }
        else
          legend.hide();
      }
      mymap.refreshLayers();
    });


    obstypeSelects.change();


    var options = '';

    for (var i = 0; i <= 180; i += 30)
      options += '<option value="' + i + '">' + i + '</option>';

    $(options).appendTo('#lstDuration');
    $(options).appendTo('#lstOffset');

    resetSubscriptionFields();


    function updateNowTime(nowTime)
    {

      currentMoment = nowTime;
      mapTimeSlider.labeledSlider('option', 'labels', {0: nowTime.format("h:mm a")});

      displaySelectedTime(moment(nowTime).add(mapTimeSlider.labeledSlider('value'), 'minutes'));
      mymap.refreshLayers();
      loadNotifications();
    }

    function autoRefresh()
    {
      if (!$('#chkAutoRefresh').prop('checked'))
        return;

      updateNowTime(currentMoment.add(1, 'minute'));
    }

    //set the refresh method to run at the start of the next minute, and
    //then set it to run every minute after that.
    var now = moment();
    var currentMillis = now.valueOf();
    now.seconds(0).milliseconds(0).add(1, 'minute');
    setTimeout(function ()
    {
      autoRefresh();
      setInterval(autoRefresh, 60000);

    }, now.valueOf() - currentMillis);


  });


}
