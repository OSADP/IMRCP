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

  $(document).ready(function ()
  {

    var dateFormat = "MMM D HH:mm [UTC]";
    var ulFiles = $('#ulFiles');

    $.ajax({
      url: 'reports',
      type: 'get',
      dataType: 'json',
      success: function (subs, status, xhr)
      {
        var userSubs = $('#divUserSubs');
        var userReports = $('#divUserReports');
        userSubs.text('');
        userReports.text('');
        for (var i = 0; i < subs.length; ++i)
        {
          var sub = subs[i];
          var newSub =
                  ' <br /><div class="w3-card w3-white">' +
                  '  <header class="w3-container w3-sand"><h4 class="w3-left">';


          var subName = sub.name;
          if (!subName || subName.length === 0)
            subName = "(no name provided)";
          if (sub.fulfilled)
          {
            newSub += '<a href="';
            if (sub.isReport)
              newSub += 'reports/' + sub.uuid + '/files/latest';
            else
              newSub += '#';
            newSub += '">' + subName + '</a>';
          }
          else
            newSub += subName + ' (pending fulfillment)';

          newSub += '</h4>';

          if (sub.lastAccessed)
            newSub += '		<h4 class="w3-right">Downloaded: ' + moment(sub.lastAccessed).utc().format(dateFormat) + '</h4> ';

          newSub +=
                  '	</header>' +
                  '	<div class="w3-container">' +
                  '    <p class="w3-left sub-date"><span class="sub-date">Created: ' + moment(sub.created).utc().format(dateFormat) + '</span>';
          if (sub.isSubscription)
          {
            newSub += '<br/>Interval: ' + sub.cycle + ' minutes' +
                    '<br />Offset: ' + minutesToHHmm(sub.offset, false) +
                    '<br/>Duration: ' + minutesToHHmm(sub.duration, true) + '</p>';
          }
          else
          {
              newSub += '<br/>Start: ' + moment(sub.startMillis).utc().format(dateFormat) +
                    '<br />End: ' + moment(sub.endMillis).utc().format(dateFormat) + '</p>';
            //include report start/end?
          }
          newSub += '</p><p class="w3-right">';

          if(sub.elementType)
          {
            newSub +='Elements: ' +  sub.elementCount + (sub.elementType === 2 ? ' segment' : ' detector') ;
            if(sub.elementCount > 1)
              newSub += 's';
          }
          else
            newSub += 'Area: ' + sub.lat1 + ', ' + sub.lon1 + ', ' + sub.lat2 + ', ' + sub.lon2;

          if (sub.obstypes)
          {
            if (sub.obstypes.length === 1)
            {
              var obstype = sub.obstypes[0];
              newSub += '<br/>Obs: <span class="sub-obs">' + obstype.name + '</span>';
              if (obstype.min)
                newSub += '<br />Min: ' + obstype.min;
              if (obstype.max)
                newSub += '<br/>Max: ' + obstype.max + '</p>';
            }
            else
            {
              var obsListCount = sub.obstypes.length === 5 ? 5 : Math.min(4, sub.obstypes.length);
              newSub += '<br/>Obs: ';
              for (var index = 0; index < obsListCount; ++index)
              {
                if (index > 0)
                  newSub += ', ';
                newSub += '<span class="sub-obs">' + sub.obstypes[index].name + '</span>';
              }

              if (obsListCount < sub.obstypes.length)
                newSub += '....(' + (sub.obstypes.length - obsListCount) + ' more)';
            }
          }

          newSub +=
                  '  </div>' +
                  '</div>';

          newSub = $(newSub);

          if (sub.isSubscription)
          {
            var link = newSub.find('a');
            link.data('uuid', sub.uuid);

            link.click(function ()
            {
              $('#subFilesHeader').text($(this).text());
              $('#divSubFilesError').hide();

              var uuid = $(this).data('uuid');
              var fileUrlBase = 'reports/' + uuid + '/files/';
              $.ajax({
                url: fileUrlBase,
                type: 'get',
                dataType: 'json',
                success: function (files, status, xhr)
                {
                  var items = '';

                  for (var i = 0; i < files.length; ++i)
                  {
                    var file = files[i];
                    items += '<li class="w3-' + (i % 2 === 0 ? 'white' : 'sand') + '"><p class="name"><a href="' + fileUrlBase + file + '">' + file + '</a></p></li>';
                  }
                  //could re-use existing elements and update text instead of always dumping and recreating
                  ulFiles.empty();
                  $(items).appendTo(ulFiles);
                  ulFiles.show();
                  //figure out some paginating widget
//                  var monkeyList = new List('divFiles', {
//                    valueNames: ['name'],
//                    page: 3,
//                    pagination: true
//                  });
//reIndex()

//                  $('#ulFiles').easyPaginate({
//                    paginateElement: 'li',
//                    elementsPerPage: 3,
//                    prevButton: false,
//                    nextButton: false,
//                    effect: 'climb'
//                  });

                },
                error: function (xhr, status, error)
                {
                  $('#divSubFilesError').show();
                }
              });
            });
          }

          newSub.appendTo(sub.isReport ? userReports : userSubs);
        }

      },
      error: function (xhr, status, error)
      {
        $('#divUserSubs').text('Error loading subscriptions.');
        $('#divUserReports').text('Error loading reports.');
      }
    });
  });
}