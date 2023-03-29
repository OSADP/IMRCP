import {polylineMidpoint} from './map-util.js';

const POINTTOL = 0.0000001;
const CLOSE_BUTTON_HTML = '<button type="button" class="ui-button ui-widget ui-state-default ui-corner-all ui-button-icon-only no-title-form ui-dialog-titlebar-close" role="button" title="Close"><span class="ui-button-icon-primary ui-icon ui-icon-closethick"></span><span class="ui-button-text">Close</span></button>';
const obsTimeFormat = 'MM-DD HH:mm';
const MAXLONG = Math.pow(2, 63) - 1;

const getBoundsForPoint = (lngLat, m, tol) => 
{
	let oP = m.project(lngLat);
	
	return new mapboxgl.LngLatBounds(m.unproject({x: oP.x - tol, y: oP.y + tol}), m.unproject({x: oP.x + tol, y: oP.y - tol}));
};

const getBoundsForClick = lngLat => new mapboxgl.LngLatBounds(lngLat, lngLat);

const featureTypeHandlers = new Map([
  ['symbol', {baseUrl: 'api/points',
      getFeatureBounds: (f, c, m) => getBoundsForPoint(f.geometry.coordinates, m, 2)
    }
  ],
  ['line', {baseUrl: 'api/road',
      getFeatureBounds: (f, c, m) => getBoundsForPoint(polylineMidpoint(f.geometry.coordinates), m, 0)
    }
  ],
  ['fill', {baseUrl: 'api/area',
      getFeatureBounds: (f, clickLatLng, m) => getBoundsForClick(clickLatLng, m, 0)
    }
  ]
]);

class FeatureDetailsWindow
{
  constructor(
  {mapSelector, sources, selectedTimeStartFunction, selectedTimeEndFunction})
  {
    this.selectedTimeStartFunction = selectedTimeStartFunction;
    this.selectedTimeEndFunction = selectedTimeEndFunction;
    this.sources = sources;
    const dialogDivHtml = `
<div id="dialog-form" style="display:none;">
      <div id="chart-container" style="display: none;">
        <i class="close-chart fa fa-list" aria-hidden="true"></i>
        <canvas id="obs-chart"></canvas>
      </div>
      <table id="obs-data" class = "pure-table pure-table-bordered">
      <thead class="obs-table">   
        <tr class="w3-sand">     
          <td>ObsType</td>
          <td>Source</td>
          <td>Start Time</td>
          <td>End Time</td>
          <td>Value</td> 
          <td>Units</td> 
          </tr>
        </thead>

    <tbody class="obs-table pure-table-striped">
    </tbody></table>
      
    </div>`;

    const dialogDiv = this.dialogDiv = $(dialogDivHtml).prependTo('body');


    const dialog = this.dialog = dialogDiv.dialog({
      autoOpen: false,
      resizable: true,
      draggable: true,
      width: 'auto',
      modal: true,
      //  dialogClass: "no-title-form",
      position: {my: "center", at: "center", of: mapSelector},
      minHeight: 380,
      maxHeight: 600
    });

    this.dialogTitleDiv = dialogDiv.parent().find('.ui-dialog-title');
    this.platformObsChart = dialogDiv.find('#obs-chart');
    this.platformObsTable = dialogDiv.find('#obs-data');
  }

  showFeatureDetails(feature, clickLatLng, mbMap)
  {
    let detailsContent = `${CLOSE_BUTTON_HTML} Loading...`;

    const {dialogTitleDiv, dialog, platformObsTable: obsTable, platformObsChart, sources} = this;
    dialogTitleDiv.html(detailsContent);
    const closeDialog = () => dialog.dialog("close");
    dialogTitleDiv.find('.ui-dialog-titlebar-close').click(closeDialog);

    obsTable.show();
    obsTable.find('tbody > tr').remove();
    obsTable.find('tbody:last-child').append('<tr><td>Loading data...</td></tr>');


//    var platformObsChart = $(platformDetailsWindow.platformObsChart);
//    var obsTable = $(platformDetailsWindow.platformObsTable);
    const chartContainer = platformObsChart.parent();

    const featureType = sources.get(feature.source).type;


    const startTime = this.selectedTimeStart();
    const featureHandler = featureTypeHandlers.get(featureType);
    const bounds = featureHandler.getFeatureBounds(feature, clickLatLng, mbMap);
	
	
    const urlBoundarySubPath =
      [bounds.getNorth() + POINTTOL, bounds.getWest() - POINTTOL, bounds.getSouth() - POINTTOL, bounds.getEast() + POINTTOL]
      .map(d => d.toFixed(7))
      .join('/');

    let chart;

    const closeChart = () =>
    {
      chartContainer.hide();
      obsTable.show();

      if (chart)
        chart.destroy();

      chart = null;
    };
    closeChart();

    chartContainer.find('.close-chart').click(closeChart);

	$('#pageoverlay').show();
    const load = $.ajax({
      dataType: "json",
      url: `${featureHandler.baseUrl}/platformObs`
        + `/${startTime}/${this.selectedTimeEnd()}/${urlBoundarySubPath}`,
      timeout: 25000
    });

    const chartUrlTemplate = `${featureHandler.baseUrl}/chartObs`
      + `/obsstypeId/${startTime}/${startTime - 1 * 60 * 60 * 1000}/${startTime + 1 * 60 * 60 * 1000}`
      + `/${urlBoundarySubPath}?src=srcId`;

    load.fail((jqXHR, textStatus) => 
	{
		obsTable.find('tbody > tr').remove();
		if (textStatus === 'timeout')
		{
			obsTable.find('tbody:last-child').append('<tr><td>Timeout loading data. Try again.</td></tr>');
			dialogTitleDiv.html(`${CLOSE_BUTTON_HTML} Timeout loading data`);  
		}
		else
		{
			obsTable.find('tbody:last-child').append('<tr><td>Error loading data</td></tr>');
			dialogTitleDiv.html(`${CLOSE_BUTTON_HTML} Error loading data`);
		}
		$('#pageoverlay').hide();
    });

    load.done(additionalDetails => 
	{
		detailsContent = CLOSE_BUTTON_HTML;


		if (additionalDetails.tnm)
			detailsContent += additionalDetails.tnm + '<br />';
		if (additionalDetails.sdet)
			detailsContent += '<div style="max-width:500px; overflow-wrap:break-word;">' + additionalDetails.sdet.replace('   ', '&nbsp;') + '</div>';

		detailsContent += 'Lat, Lon: ';
		if (additionalDetails.lat && additionalDetails.lon)
			detailsContent += additionalDetails.lat + ', ' + additionalDetails.lon + ' ';
		else
		{
			const center = bounds.getCenter();
			detailsContent += `${center.lat.toFixed(6)}, ${center.lng.toFixed(6)} `;
		}

		if (additionalDetails.tel)
			detailsContent += ' Elevation: ' + additionalDetails.tel;
		dialogTitleDiv.html(detailsContent);
		dialogTitleDiv.find('.ui-dialog-titlebar-close').click(closeDialog);
		obsTable.find('tbody > tr').remove();

		if (true)
		{
			var obsList = additionalDetails.obs;
			if (!obsList || obsList.length === 0)
			{
				obsTable.find('tbody:last-child').append('<tr><td>No data</td></tr>');
			}
			else
			{
				var newRows = '';
				for (let iObs of obsList)
				{
					const unit = iObs.eu;
					newRows += '<tr>';

					const chartUrl = chartUrlTemplate.replace("obsstypeId", iObs.oi).replace("srcId", iObs.src);

					newRows += "<td class=\"obsType\">" + iObs.od;
					if (unit)
					{
						newRows += `<i class="chart-link fa fa-line-chart clickable" 
						  data-unit="${unit}" 
						  data-obstype="${iObs.od}" 
						  data-url="${chartUrl}" ></i>`;
					}

					newRows += `</td>
						<td class="obsType">${iObs.src}</td>
						<td class="timestamp">${moment(iObs.ts1).format(obsTimeFormat)}</td>
						<td class="timestamp">${iObs.ts2 !== MAXLONG ? moment(iObs.ts2).format(obsTimeFormat) : ''}</td>`;
					
					if (iObs.url)
					{
						newRows += `<td class="td-value-link"><a href="${iObs.url}" style="color: blue;">${iObs.ev}</a></td>`;
					}
					else
					{
						newRows+= `<td class="td-value">${iObs.ev}</td>`;
					}
						
					newRows += `<td class="unit">`;

					if (unit)
						newRows += unit;
					newRows += '</td></tr>';
				}
				if (window.location.toString().indexOf("testimrcp") >= 0)
					newRows += `<tr><td class="obsType">${additionalDetails.imrcpid}</td></tr>`;

				$(newRows).appendTo(obsTable.find('tbody:last-child')).find('i.chart-link').click(e => 
				{
					const chartObstype = $(e.target).data("obstype");
					const chartObsUnit = $(e.target).data("unit");

					const icon = $(e.target);
					icon.removeClass('fa-line-chart').addClass('fa-spinner fa-spin');
					$.getJSON($(e.target).data("url"))
					.always(() => icon.removeClass('fa-spinner fa-spin '))
					.done(data => 
					{
						icon.addClass('fa-line-chart');
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

						let config = {
							type: 'line', 
							data: {datasets:[{label: chartObstype, fill: false, data: data, borderColor: 'black'}]},
							options: {title: {text: chartObstype}, scales: {
							  xAxes: [{type: 'time', time: {unit: 'hour', tooltipFormat: obsTimeFormat,  displayFormats: {}}}, 
									  {type: 'time',time: {unit: 'day', tooltipFormat: obsTimeFormat, displayFormats: {}}}],
							  yAxes: [{scaleLabel: {display: true,labelString: chartObsUnit},ticks: {min: min, max: max, stepSize: step}}]},
							animation: {onComplete: () =>
							{
								obsTable.hide();
								dialog.dialog("option", "position", {my: "center", at: "center", of: '#map-container'});
							}}}};

						chart = new Chart(platformObsChart.get(0), config);
						chartContainer.show();

					}).fail(() => 
					{
						icon.addClass('fa-exclamation-circle');
						setTimeout(() => icon.addClass('fa-line-chart').removeClass('fa-exclamation-circle'), "3000");
					});
				});

			}
		}
		else
		{
			var sensorList = additionalDetails.sl;
			if (!sensorList || sensorList.length === 0)
			{
				obsTable.find('tbody:last-child').append('<tr><td>No data</td></tr>');
			}
			else
			{
				let newRows = '';
				for (let rowIndex = 0; rowIndex < sensorList.length; ++rowIndex)
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

		dialog.resize();
		dialog.dialog("option", "position", "center");
		$('#pageoverlay').hide();
    });


    dialog.dialog("open");
  }

  selectedTimeStart()
  {
    return this.selectedTimeStartFunction();
  }

  selectedTimeEnd()
  {
    return this.selectedTimeEndFunction();
  }
}

export {FeatureDetailsWindow};