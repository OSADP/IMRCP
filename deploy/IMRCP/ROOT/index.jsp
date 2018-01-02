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
	session.invalidate();
%>
<!DOCTYPE html>
<html>
<title>IMRCP Login</title>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<link rel="stylesheet" href="css/w3.css"/>
<link rel="stylesheet" href="css/font-awesome.min.css"/>
<link rel="stylesheet" href="css/imrcp.css"/>
<script type="text/javascript" src="script/jquery/jquery.js"></script>
<script type="text/javascript">
$(document).ready(function()
{
	$('#identity').focus(); // set default focus to username field

	$('#btnLogin').click(function()
	{
		$.post("validate.jsp", 
		{
			"identity": $('#identity').val(), 
			"codeword": $('#codeword').val()
		}, 
		function(data, status)
		{
			var oJson = JSON.parse(data);
			if (oJson.login)
				document.location="map.jsp";
			else
			{
				$('#statusmsg').show();
				$('#codeword').val("").focus(); // reset password and focus there
			}
		});
	});

	$('#codeword').on('keydown', function(oEvent)
	{
		if (oEvent.which === 13) // keycode for enter key
		{
			 $('#btnLogin').click(); // implicitly click login button
			 return false;
		}
		return true;
	}); // end of function
});
</script>
<body>

<div>
	<ul class="w3-navbar w3-teal">
		<li class="w3-right"><a href="https://www.fhwa.dot.gov"><img alt="FHWA" src="images/fhwa-small.png"/>&nbsp;&nbsp;Federal Highway Administration</a></li>
	</ul>
</div>

<div id="main-content" class="w3-row w3-light-gray">
	<div class="w3-third w3-container">
		<br/>
		<div class="w3-card-4 w3-white">
			<br/>
			<div class="w3-container">
	                        <div id="statusmsg" class="w3-container w3-red" style="display: none;">
				<p>Username or password error. Verify password and try again.</p>
                	        </div>
 				<br/>
			<h4>Please login to access the system.</h4>
			<p>
			<input class="w3-input w3-border w3-sand" id="identity" name="identity" type="text"/>
			<label class="w3-label w3-text-black"><b>Username</b></label>
			</p>
			<p>
			<input class="w3-input w3-border w3-sand" id="codeword" name="codeword" type="password"/>
			<label class="w3-label w3-text-black"><b>Password</b></label>
			</p>
			<p><button id="btnLogin" class="w3-btn w3-ripple w3-teal">Login</button></p>
			</div>
		</div>
	</div>
	<div class="w3-twothird w3-container">
		<h3><img alt="IMRCP logo" src="images/imrcp-logo.png"/> Welcome to the Integrated Modeling for Road Condition Prediction System</h3>
		<p>
		The Integrated Modeling for Road Condition Prediction (IMRCP) project intends to demonstrate 
		integrating weather and traffic data sources and predictive methods to effectively predict road 
		and travel conditions in support of tactical and strategic decisions by travelers, transportation 
		operators and maintenance providers. The system collects and integrates environmental observations 
		and transportation operations data; collects forecast environmental and operations data when 
		available; initiates road weather and traffic forecasts based on the collected data; generates 
		travel and operational alerts from the collected real-time and forecast data; and provides the 
		road condition data, forecasts and alerts to users and other systems.
		</p>
		<br/>
		<p>This website provides two primary user interfaces: interactive map and reports.</p>
		<p>
		<i class="fa fa-arrow-right"></i> Map &#8212; The map presents the modeled road network in its 
		geographic setting with familiar map viewing tools. Information layers may be selected to display 
		road weather, atmospheric weather, traffic conditions, and alerts on the map. Selecting a road 
		segment, area polygon, or alert icon will display more detailed observations about the selected 
		object. The IMRCP map also includes a time selector that enables users to view observations up to 
		4 hours in the past and forecasts up to 8 hours in the future. The date/time function enables users 
		to select a previous date and time for which they would like to view the data layers. A reporting 
		and subscription tool enables users to specify criteria for creating a report or subscription.
		</p>
		<p>
		<i class="fa fa-arrow-right"></i> Reports &#8212; The reports interface lists system-defined 
		reports along with reports and subscriptions defined by the user through the map interface. The 
		system reports and subscriptions provide a way to download user-specified data sets from the 
		system. Reports and subscriptions are identified with the name, creation time, last access time, 
		data filtering criteria and download URL. Report and subscription output can be downloaded using 
		this web interface or with a unique URL and will remain available for up to two weeks after the 
		last access time.
		</p>
		<br/>
		<p>
		Many thanks to the Federal Highway Administration for sponsoring this project and for
		the patience and support of Kansas City Scout, the Mid-America Regional Council and Operation 
		Green Light, and their member cities, without which this valuable research would not have been 
		possible.
		</p>
	</div>
</div>

<div class="w3-bottom">
	<div class="w3-container w3-teal" style="height: 39px;"></div>
</div>

</body>
</html>
