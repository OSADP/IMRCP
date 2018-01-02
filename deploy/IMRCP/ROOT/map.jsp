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
    <link rel="stylesheet" href="style/leaflet.css" />
    <link href="style/jquery/jquery.datetimepicker.css" rel="stylesheet">
    <link href="style/jquery/jquery.notifications.dialog.css" rel="stylesheet" />
    <link href="style/imrcp.css" rel="stylesheet">
    <script type="text/javascript" src="script/jquery/jquery.js"></script>
    <script type="text/javascript" src="script/leaflet.js"></script>
    <script type="text/javascript" src="script/common.js"></script>
    <script type="text/javascript" src="script/leaflet-wxde.js"></script> 
    <script type="text/javascript" src="script/jquery/jquery-ui.js"></script>
    <script type="text/javascript" src="script/jquery/jquery.ui.labeledslider.min.js"></script>
    <script type="text/javascript" src="script/jquery/jquery.datetimepicker.full.js"></script>
    <script type="text/javascript" src="script/jquery/jquery.notifications.dialog.js"></script>
    <script type="text/javascript" src="script/jquery/jquery.segmentSelector.js"></script>
<script type="text/javascript" src="script/moment.min.js"></script>
    <script type="text/javascript" src="script/summary-map.js"></script>
  </head>
  <body>
    <div id="divNotificationDialog">
    </div>
    <div id="dialog-form" style="display:none;">
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
      
    </div>
    
    
    <div id="divReportDialog" style="display:none">
      <div id="divSubInput">
        <table>
          <tr><td class="LatLngLabel"><label for="txtLat1">Lat 1</label></td><td><input size="10" maxlength="9" id="txtLat1" type="text" /></td><td class="LatLngLabel"><label for="txtLng1">Lon 1</label></td><td><input size="10" maxlength="10" id="txtLng1" type="text" /></td></tr>

          <tr><td class="LatLngLabel"><label for="txtLat2">Lat 2</label></td><td><input size="10" maxlength="9" id="txtLat2" type="text" /></td><td class="LatLngLabel"><label for="txtLng2">Lon 2</label></td><td><input size="10" maxlength="10" id="txtLng2" type="text" /></td></tr>
          <tr><td>Name</td><td colspan="3"><input type="text" id="txtName" /></td></tr>
          <tr><td><label for="lstReportObstypes">Obstype</label></td><td colspan="3"><select multiple="multiple" style="width:100%; height:90px;" id="lstReportObstypes"></select></td></tr>
          <tr id="trMinMax"><td><label for="txtMin">Min</label></td><td><input size="10" maxlength="9" id="txtMin" type="text" /></td><td><label for="txtMax">Max</label></td><td><input size="10" maxlength="9" id="txtMax" type="text" /></td></tr>
          <tr><td><label for="lstFormat" >Format</label></td><td colspan="3"><select id="lstFormat" name="format" style="width: 100%;">
          <option value="CSV" selected="selected">CSV</option>
        </select></td></tr>
          <tr><td colspan="4"><span style="float:left;"><input id="radTypeReport" type="radio" name="TYPE" checked="checked" /><label for="radTypeReport">Run Report</label></span><span style="float:right"><input id="radTypeSubscription" type="radio" name="TYPE" /><label for="radTypeSubscription">Create Subscription</label></span></td></tr>
          <tr class="ReportOnly"><td colspan="4">Ref Time&nbsp;&nbsp;<input style="width:200px" type="text" value="" id="txtReportRefTime"/></td></tr>
          <tr class="SubscriptionOnly"><td><label for="radInterval15">Interval</label></td><td colspan="3">
              <input value="15" id="radInterval15"  checked="checked" type="radio" name="interval" /><label for="radInterval15">15 min</label>
              <input value="30" id="radInterval30" type="radio" name="interval" /><label for="radInterval30">30 min</label>
              <input value ="60" id="radInterval60" type="radio" name="interval" /><label for="radInterval60">1 hour</label>
            </td></tr>
          <tr><td colspan="4"><div id="divSubOffsetSlider"></div></td></tr>
          <tr><td><label>Offset</label></td><td><span id="spnOffset"></span></td><td><label>Duration</label></td><td><span id="spnDuration"></span></td></tr>
          
        </table>
      </div>
      <div id="divSubResult" style="display:none;">
    	<div>
			<h3><span id="spnType">Report</span> Info</h3>
			<div> 
        <b>Name: </b><span id="spnName"></span><br/>
        <b>Description: </b><span id="spnDesc"></span><br/>
        <b>Subscription Identifier: </b><span id="spnUuid"></span><br/>
        <b>Direct URL: </b><span id="spnUrl"></span>
			</div>
		</div>
        <br />
			You can view your current reports and subscriptions and their available files
      on the <a href="reports.jsp">Reports</a> page.
	    </div>
      <div id="divSubError" style="display:none;">An error occurred creating your report or subscription. Please try again later.</div>
      <input type="button" id="btnSubmitReport" value="Submit"/><input type="button" id="btnClose" value="Cancel" />
    </div>
     
<div>
	<ul class="w3-navbar w3-teal">
		<li><a class="w3-light-gray" href="#"><i class="fa fa-globe"></i>&nbsp;&nbsp;Map</a></li>
		<li><a href="reports.jsp"><i class="fa fa-newspaper-o"></i>&nbsp;&nbsp;Reports</a></li>
		<li><a href="IMRCP-help.pdf"><i class="fa fa-life-ring"></i>&nbsp;&nbsp;Help</a></li>
		<li><a href="./"><i class="fa fa-sign-out"></i>&nbsp;&nbsp;Logoff</a></li>
		<li class="w3-right"><a href="https://www.fhwa.dot.gov"><img alt="FHWA" src="images/fhwa-small.png"/>&nbsp;&nbsp;Federal Highway Administration</a></li>
	</ul>
</div>
          
    <div id="map-container" >
    <div id="mapid" style="height: 100%"></div>
    </div>

          
<div id="footer" class="w3-bottom w3-teal">
    
	<div id="divRoadLegend" class="legend" style="display: none;">
	</div> 
	<div id="divAreaLegend" class="legend" style="display: none;">
	</div> 
  <div id="slider-container" class="w3-light-gray">
<div id="slider">
  <div class="ui-slider-handle"></div>
</div>
      </div>
	<div class="w3-container" >
      <div id="divFooterControls">
        <div id="divFooterControlsLeft">
           <select id="lstRoadObstypes">
            <option value = "0">Display Road Data...</option>
            <option value = "0">Road Network Model</option>
          </select>
          <select id="lstAreaObstypes">
            <option value = "0">Display Area Data...</option>
          </select>
          <select id="lstAlertObstypes">
            <option value = "0">Display Point Data...</option>
            <option value = "4">Traffic Detectors</option>
            <option value = "0">All Alerts</option>
            <option value = "1">Traffic Alerts</option>
            <option value = "2">Road Condition Alerts</option>
            <option value = "3">Weather Alerts</option>
          </select>
          <label><input type="checkbox" checked="checked" id="chkAutoRefresh" />1 min refresh</label>
          <label><input type="checkbox" checked="checked" id="chkNotifications" />notify</label>
        </div>
     
     <div id="divFooterControlsRight">
       <input checked="checked" type="checkbox" id="chkRoadLayer" />
     <div id="divReportTabs">
       <div id="divReportStep1"><select id="lstReportOptions"><option id="optInitialReport">Report/Subscribe</option><option id="optDetectorReport">Detectors</option><option id="optSegmentReport">Segments</option><option id="optAreaReport">Area</option></select> <!--<input id="btnInitReport" type="button" value="Report/Subscribe" /> --></div>
     <div id="divReportStep2"><input id="btnSetReportArea" type="button" value="Set Selection" /></div>
     </div>
      <div id="divCurrentTime">
        <div><input type="button" id="btnTime" /></div>
        <div id="divInputContainer"><input type="text" id="txtTime" /></div>
      </div> 
     </div>
    </div>
      </div>
</div>
  </body>
</html>
