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
<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8" trimDirectiveWhitespaces="true" %>
<%
	String sUser = "unknown";
	if (request.getUserPrincipal() == null)
		response.sendRedirect("/");
	else
		sUser = request.getUserPrincipal().getName();
%>
<!DOCTYPE html>
<html lang="en">
<head>
<title>IMRCP Reports - <%= sUser %></title>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<link rel="stylesheet" href="css/w3.css"/>
<link rel="stylesheet" href="css/font-awesome.min.css"/>
<link rel="stylesheet" href="css/imrcp.css"/>
<script type="text/javascript" src="script/jquery/jquery.js"></script>
    <script type="text/javascript" src="script/common.js"></script>
<script type="text/javascript" src="script/reports.js"></script>
<script type="text/javascript" src="script/moment.min.js"></script>
<!--
<script type="text/javascript" src="script/list.min.js"></script>
<script type="text/javascript" src="script/jquery/jquery.easyPaginate.js"></script>
-->
</head>
<body>

<div>
	<ul class="w3-navbar w3-teal">
		<li><a href="map.jsp"><i class="fa fa-globe"></i>&nbsp;&nbsp;Map</a></li>
		<li><a class="w3-light-gray" href="#"><i class="fa fa-newspaper-o"></i>&nbsp;&nbsp;Reports</a></li>
		<li><a href="IMRCP-help.pdf"><i class="fa fa-life-ring"></i>&nbsp;&nbsp;Help</a></li>
		<li><a href="./"><i class="fa fa-sign-out"></i>&nbsp;&nbsp;Logoff</a></li>
		<li class="w3-right"><a href="https://www.fhwa.dot.gov"><img alt="FHWA" src="images/fhwa-small.png"/>&nbsp;&nbsp;Federal Highway Administration</a></li>
	</ul>
</div>

<div id="main-content" class="w3-row w3-light-gray">
	<div class="w3-third w3-container">
		<h3>Reports</h3>
		<p>
		Reports requested through the map interface are listed below with their identifying attributes. 
		They are run in the order submitted and are available upon completion (which may take a few 
		minutes after submission).
		</p>
		<p>
		Reports can be retrieved as many times as needed, but will be removed from the system if they 
		have not been accessed for two weeks.
		</p>
    <div id="divUserReports">Loading reports...</div>
	</div>
	<div class="w3-third w3-container">
		<h3>Subscriptions</h3>
		<p>
		Subscriptions defined through the map interface are listed below. Similar to reports, 
		subscriptions are retained for up to two weeks after the most recent download, after which 
		time they will be removed from the system.
		</p>
		<p>
		Each subscription is listed below with its attributes. When a subscription is selected, the 
		subscription files are listed in the column to the right. The download URL can be used by 
		external scripts to retrieve the output automatically.
		</p>
    <div id="divUserSubs">Loading subscriptions...</div>
	</div>
	<div id="divFiles" class="w3-third w3-container">
		<h3>Subscription Files</h3>
		<p>
		The selected subscription’s files are listed below with the most recent files listed at the top.
		</p>
    <h3 id="subFilesHeader">&nbsp;</h3>
		<ul class="w3-ul w3-card list" id="ulFiles" style="display: none;">
		</ul>
    <ul class="pagination"></ul>
    <div id="divSubFilesError" style="display: none;">Unable to load subscription file list.</div>
	</div>
</div>

<div class="w3-bottom">
	<div class="w3-container w3-teal" style="height: 39px;"></div>
</div>

</body>
</html>
