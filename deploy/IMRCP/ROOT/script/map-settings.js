 const DEFAULT_MAP_VIEW = {
      center: {lng: -98.585522, lat: 39.8333333}, 
      zoom: 4
    };

 const saveSettings =  (oMap) => () => {
      
    const settingsIcon = $('<i class="fa fa-spinner fa-spin"></i>');
    const initialSaveText = $("#save-user-view").text();
      $("#save-user-view")
              .prop('disabled', true)
              .empty()
              .append(settingsIcon);
            
      const {lat,lng} = oMap.getCenter();
      const requestData = {
        map: {
          zoom: oMap.getZoom(),
          center: {lat,lng}
        },
        layers: $('.obstype-pane-list input[type="checkbox"]:checked').map((idx,el) => $(el).val()).get(),
        roadTypes: {
          arterials: $('#chkShowArterials').prop('checked'),
          highways: $('#chkShowHighways').prop('checked')
        },
//        notify: $('#chkNotifications').prop('checked'),
		notify: false,
        refresh: $('#chkAutoRefresh').prop('checked')
      };
      
      var saveRequest = $.ajax('api/settings/saveMapSettings', {
        method: 'POST',
        dataType: 'text',
//        contentType: "application/json; charset=utf-8",
        data : {settings: JSON.stringify(requestData), token: sessionStorage.token}
      });
        
        //saving can happen so fast it's confusing to see the spinner just flicker. 
        //Show the in-progress state for at least a second
        setTimeout(() => {
      saveRequest.done(() =>   {
        settingsIcon
                .removeClass("fa-spin fa-spinner")
                .addClass("fa-check-circle");
      });
      
      saveRequest.fail((resp) =>   {
        settingsIcon
                .removeClass("fa-spin fa-spinner")
                .addClass("fa-exclamation-circle");
      });
      
      saveRequest.always(() => {window.setTimeout(() => $("#save-user-view").empty().text(initialSaveText).prop('disabled', false), "1500");});
    }, "1000");
    };
    
    
    
const loadSettings = $.Deferred(defer => {
  const defaultSettings = {
    layers: [],
    map: DEFAULT_MAP_VIEW,
    notify: false,
    refresh: true
  };

  $.getJSON('api/settings/MapSettings')
    .done(settings => {
      const userHasProfile = settings.map !== undefined; // check for non-empty response
      if(userHasProfile && Array.isArray(settings.map.center))
      {
        const [lat,lng] = settings.map.center;
        settings.map.center = {lat,lng};
      }
      return defer.resolveWith(null, [Object.assign({userHasProfile}, defaultSettings, settings)]);
    })
    .fail(() => defer.resolveWith(null, [defaultSettings]));

}).promise();


    export {saveSettings, loadSettings,DEFAULT_MAP_VIEW};