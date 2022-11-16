

const generateSymbolLegendContent = legendEntries =>
{
  let legendEntryHtml = '';
  for (let legendEntry of legendEntries)
  {
	const {width, height, x, y} = legendEntry.style;
	legendEntryHtml +=
	  `<li>
		  <div width="1" height="1" 
			style="width: ${width}px; height: ${height}px;  
			background: url(images/sprite.png) -${x}px -${y}px; display:inline-block;vertical-align:middle">
		  </div>
		  <span style="padding-left:5px;">${legendEntry.label}</span>
		</li>`;
  }
  return legendEntryHtml;
};

const generateFillLegendContent = legendEntries =>
  {
    let legendEntryHtml = '';
    for (let legendEntry of legendEntries)
    {
      if (legendEntry.color === "#ffffff")// if the color is white, then use a black background with a white rounded square inside it
        legendEntryHtml += '<li><i class="fa fa-square-o" style="color: #000"></i> ';
      else
        legendEntryHtml += `<li><i class="fa fa-stop " style="color: ${legendEntry.color}; opacity: ${legendEntry.opacity}"></i>`;

      legendEntryHtml += legendEntry.label + '</li>';
    }
    return legendEntryHtml;
  };


const fillLegendDiv = (sources, legendContainer) => (e) => {
    const legend = legendContainer.find('.legend-content');
    const legendHeader = legendContainer.find('.legend-heading .heading-title');
    const dialogIcon = $(e.target);
    const selectedObstypeSourceId = dialogIcon.data('source-id');
	
	if (legendHeader.text() === selectedObstypeSourceId && legend.parent().is(":visible"))
	{
		legend.parent().hide();
		$('.legend-header').show();
		return;
	}
	$('.legend-header').hide();
    legendHeader.text(dialogIcon.data('obstype-label'));

    let newLegendContent = '<ul class="layer-legend w3-ul ul-no-border">';
    const source = sources.get(selectedObstypeSourceId);
    switch (source.type)
    {
      case 'fill' :
      case 'line' :
        newLegendContent += generateFillLegendContent(source.legendElements);
        break;
      case 'symbol':
        newLegendContent += generateSymbolLegendContent(source.legendElements);
        break;
    }

    newLegendContent += '</ul>';
	legend.html(newLegendContent);
    legend.parent().show();
	if (source.legendElements.length > 15)
	{
		let dHeight = legendContainer.outerHeight();
		let dLegendHeight = legendContainer.siblings('.divStackLayer').filter((n, e)=>$(e).css('display') === 'block').outerHeight();
		if (dHeight > dLegendHeight)
		{
			legendContainer.css({'position': 'relative', 'bottom': dHeight - dLegendHeight});
		}
	}
	else
	{
		legendContainer.css({'bottom': 0});
	}
    
  };


const setupObstypeLegendDivs = sources =>
{
  $('#legendDeck .divStackDeck .divStackLayer').each((idx, div) => {

    const legendContainer = $('<div style="display: none; float:left;" class="ui-widget-content"></div>')
      .insertAfter(div);

    const legendHeader = $('<span class="w3-sand w3-round legend-heading clickable"><span class="heading-title"></span></span>')
      .appendTo(legendContainer);
    const legendIcon = $('<i style="padding-right:3px; padding-left:6px; padding-top:3px" class="fa fa-times clickable fa-lg w3-right"></i>')
      .appendTo(legendHeader)
      .click(e => {legendContainer.hide(); $('.legend-header').show();});

    const legendContent = $('<div class="legend-content" style="margin-top:5px;margin-bottom:5px"></div>').appendTo(legendContainer);

    $(div).find('i')
      .each((idx, icon) => {
        const obstypeListItem = $(icon).parents(('li'));
        const sourceId = obstypeListItem.find('input').val();
        const source = sources.get(sourceId);

        if (source && source.legendElements.length > 0)
        {
          $(icon).data('source-id', sourceId);
          $(icon).data('obstype-label', obstypeListItem.text());
        }
        else
          $(icon.remove());
      });

    $(div).find('i')
      .click(fillLegendDiv(sources, legendContainer));

  });
};


const buildPaneSourceInputs = (map, sources, initialLayers,addSourceAndLayersFn) => {
  const initialSources = new Set(initialLayers);
  const groupDivs = new Map();
  const settingsDiv = $('#settingsPane');
  groupDivs.set('Settings', settingsDiv);

  sources.forEach(source => {

    const {group, mapboxSource, id} = source;

    if (!groupDivs.has(group))
      groupDivs.set(group, $('<div title="' + group + '"><ul class="w3-ul ul-no-border obstype-pane-list"></ul></div>').insertBefore(settingsDiv));

    const groupDiv = groupDivs.get(group);
    const groupUl = groupDiv.find('ul.obstype-pane-list');
    const sourceLi = $(`<li><label><input id="${source.layers[0].metadata.obstype}" type="checkbox" value="${id}"/>${id}</label><i class="w3-right	fa fa-window-restore clickable" aria-hidden="true"></i></li>`).appendTo(groupUl);

    if (initialSources.has(id))
    {
      sourceLi.find('input').prop('checked', true);
      addSourceAndLayersFn(map, source);
    }
  });
};

export {setupObstypeLegendDivs, buildPaneSourceInputs};