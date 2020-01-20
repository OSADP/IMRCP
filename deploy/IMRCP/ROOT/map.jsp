<%
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
%>
<%@page import="imrcp.system.ObsType"%>
<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8" trimDirectiveWhitespaces="true" %>
<%
	String sUser = "unknown";
	if (request.getUserPrincipal() == null)
		response.sendRedirect("/");
	else
		sUser = request.getUserPrincipal().getName();
%>
<!DOCTYPE html>
<html>
  <head>
    <title>IMRCP Map - <%= sUser %></title>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link rel="stylesheet" href="css/w3.css"/> 
    <link rel="stylesheet" href="css/pure-min.css"/>
    <link href="style/jquery/themes/overcast/jquery-ui.css" rel="stylesheet" type="text/css"/>
    <link href="style/jquery/jquery.ui.labeledslider.min.css" rel="stylesheet" type="text/css"/>
    <link rel="stylesheet" href="css/font-awesome.min.css"/>
    <link rel="stylesheet" href="https://api.mapbox.com/mapbox-gl-js/v1.6.1/mapbox-gl.css"/>
    <link href="style/jquery/jquery.datetimepicker.css" rel="stylesheet">
    <link href="style/jquery/jquery.notifications.dialog.css" rel="stylesheet" />
    <link href="style/jquery/jquery.divStack.css" rel="stylesheet" />
    <link href="style/imrcp.css" rel="stylesheet">
    <script type="text/javascript" src="script/jquery/jquery.js"></script>
    <script type="text/javascript" src="https://api.mapbox.com/mapbox-gl-js/v1.6.1/mapbox-gl.js"></script> 
    <script type="text/javascript" src="script/jquery/jquery-ui.js"></script>
    <script type="text/javascript" src="script/moment.min.js"></script>
    <script type="text/javascript" src="script/Chart.min.js"></script>
    <script src="script/summary-map.js" type="module"></script>
  </head>
  <body>
    <div id="divNotificationDialog">
    </div>
     
<div>
	<ul class="w3-navbar w3-teal">
		<li><a class="w3-light-gray" href="#"><i class="fa fa-globe"></i>&nbsp;&nbsp;Map</a></li>
		<li><a href="reports.jsp"><i class="fa fa-newspaper-o"></i>&nbsp;&nbsp;Reports</a></li>
		<li><a href="IMRCP-help.pdf"><i class="fa fa-life-ring"></i>&nbsp;&nbsp;Help</a></li>

		<li><a href="./"><i class="fa fa-sign-out"></i>&nbsp;&nbsp;Logoff</a></li>
<% if (request.isUserInRole("imrcp-admin")) 
{
%>
		<li><div style="display:inline-block" class="w3-navitem"><input value="zip city county state" type="text" id="txtLocationLookup"/></div></li>
<%
}
%>
	</ul>
</div>
          
    <div id="map-container" >
    <div id="mapid" style="height: 100%"></div>
    </div>

          
<div id="footer" class="w3-bottom w3-teal">
  <div style="display:flex; flex-direction: row">
              <div id="legendDeck">
                <div id="settingsPane" title="Settings">
                  
                  <div>
                   <!-- <ul class="w3-ul ul-no-border">
                      <li>Show Road Types</li>
                      <li><label><input id="chkShowHighways" type="checkbox" checked="checked"/> Highways</label></li>
                      <li><label><input id="chkShowArterials" type="checkbox"  checked="checked" /> Arterials</label></li>
                    </ul> -->
                  </div>
                  <div>
                    <ul class="w3-ul ul-no-border">
                      <li>Map Behaviors</li>
                      <li><label><input type="checkbox" checked="checked" id="chkAutoRefresh" /> 1 min refresh</label></li>
                      <li><label><input type="checkbox"  checked="checked" id="chkNotifications" /> Notify</label></li>
                    </ul>
                  </div>
                  <div>
                    <ul class="w3-ul ul-no-border">
                      <li>Selected layers, current map position, zoom, and time slider are also saved</li>
                      <li><button type="button" id="save-user-view">Save as Default</button></li>
                    </ul>
                  </div>
                </div>
              </div>
    <i id="btnReport" class="tooltip footer-icon fa fa-newspaper-o" tip="create report/subscription"></i>
      <div id="divCurrentTime">
        <div><input type="button" id="btnTime" value="0000/00/00" /></div>
        <div id="divInputContainer"><input type="text" id="txtTime" /></div>
      </div> 
        <div id="zoom" class="w3-left"></div>
  <div id="slider-container" class="w3-teal" style="flex-grow: 1; flex-shrink: 1">
<div id="slider">
  <div class="ui-slider-handle"></div>
</div>
      </div>
    
  </div>
	<div class="w3-container" >
      <div id="divFooterControls">
        <div id="divFooterControlsLeft">
          
        </div>
     
      <div id="divFooterControlsRight">
       
      </div>
    </div>
      </div>
</div>
  </body>
</html>
