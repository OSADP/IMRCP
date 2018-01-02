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
<%@page contentType="application/json" pageEncoding="UTF-8"%><%@page import="imrcp.system.ObsType"%><%@page import="java.util.HashMap"%><%

  HashMap<Integer, String[]> oObstypeFields = new HashMap<>(); 
  String redRoad = "#ff0000";
  String orangeRoad = "#ff751a";
  String yellowRoad = "#ffc700";
  String greenRoad = "#00af20";
  
  String roadCatBreakpoints = "1.5, 2.5, 3.5";
   
  String redToGreenColors = "\"" + redRoad + "\",\"" + orangeRoad + "\",\"" + yellowRoad + "\",\"" + greenRoad + "\"";
  String greenToRedColors = "\"" + greenRoad + "\",\"" + yellowRoad + "\",\"" + orangeRoad + "\",\"" + redRoad + "\"";
  String darkRedToGreenColors = "\"#8c1515\"," + redToGreenColors;
  String greenToDarkRedColors = greenToRedColors + ",\"#8c1515\"";
    
  oObstypeFields.put(ObsType.VOLLNK, new String[]{"Traffic Flow", greenToRedColors, "40, 80, 120", "\"Below 40\", \"40 - 80\", \"80 - 120\", \"Above 120\""});
  oObstypeFields.put(ObsType.DPHLNK, new String[]{"Pavement Flood Depth", "\"#1a1aff\", \"#000066\"", "12", "\"0 - 12\",\"Above 12\""});
  oObstypeFields.put(ObsType.DPHSN, new String[]{"Pavement Snow Depth", "\"#ffffff00\", \"#80ffff\", \"#1a1aff\", \"#000066\"", "0.01, 1, 3", "\"0\", \"0.01 - 1\", \"1 - 3\", \"Above 3\""});
  oObstypeFields.put(ObsType.TPVT, new String[]{"Pavement Temperature", "\"#aa00ff\", \"#00007f\", \"#0000ff\", \"#00af20\", \"#ff0000\"", "20, 30, 35, 120", "\"Below 20\", \"20 - 29\", \"30 - 35\", \"35 - 120\", \"Above 120\""});
  oObstypeFields.put(ObsType.VIS, new String[]{"Surface Visibility", "\"#000000\", \"#9b9b9b\", \"#ffffff\"", "0.2, 0.6", "\"Below 0.2\", \"0.2 - 0.6\", \"Above 0.6\""});
  oObstypeFields.put(ObsType.SPDLNK, new String[]{"Traffic Speed", darkRedToGreenColors, "15,30,45,60", "\"0 - 15\",\"15 - 30\",\"30 - 45\",\"45 - 60\",\"60 - 75\""});
  oObstypeFields.put(ObsType.TRFLNK, new String[]{"Traffic", darkRedToGreenColors, "20,40,60,80", "\"Slow\",\"\",\"\",\"\",\"Fast\""});
  oObstypeFields.put(ObsType.DNTLNK, new String[]{"Average Link Density", greenToDarkRedColors, "15,30,45,60", "\"0 - 15\",\"15 - 30\",\"30 - 45\",\"45 - 60\",\"Above 60\""});
  oObstypeFields.put(ObsType.FLWCAT, new String[]{"Flow", redToGreenColors, roadCatBreakpoints, "\"Below 600 v/l/hr\",\"600 - 1200 v/l/hr\",\"1200 - 1800 v/l/hr\",\"Above 1800 v/l/hr\""});
  oObstypeFields.put(ObsType.SPDCAT, new String[]{"Speed", darkRedToGreenColors, "1.5, 2.5, 3.5, 4.5", "\"Below 13 mph\",\"13 - 26 mph\",\"26 - 39 mph\",\"39 - 52 mph\",\"Above 52 mph\""});
  oObstypeFields.put(ObsType.OCCCAT, new String[]{"Occupancy", greenToRedColors, roadCatBreakpoints, "\"Below 25%\",\"25 - 50%\",\"50 - 75%\",\"Above 75%\""});
  oObstypeFields.put(ObsType.TDNLNK, new String[]{"Traffic Density", greenToRedColors, "25,50,75", "\"Below 25%\",\"25 - 50%\",\"50 - 75%\",\"Above 75%\""});
  oObstypeFields.put(ObsType.STPVT, new String[]{"Pavement State", "\"#996600\",\"#00af20\",\"#99ffa1\",\"#fdafff\",\"#00faff\",\"#af1cff\"" , "1.5,2.5,3.5,4.5,5.5", "\"Dry\",\"Wet\",\"Dew\",\"Frost\",\"Snow/Ice\",\"Water/Snow\""});
  oObstypeFields.put(ObsType.TAIR, new String[]{"Air Temperature", "\"#aa00ff\", \"#00007f\", \"#0000ff\", \"#00af20\", \"#ffc700\", \"#ff751a\", \"#ff0000\"", "20,33,40,60,80,95", "\"Below 20\", \"20 - 32\", \"33 - 39\", \"40 - 59\", \"60 - 79\", \"80 - 95\", \"Above 95\""});
  oObstypeFields.put(ObsType.GSTWND, new String[]{"Wind Gust Speed", "\"#aacfeb\", \"#567593\", \"#003050\"", "5, 15", "\"Below 5\", \"5 - 15\", \"Above 15\""});
  oObstypeFields.put(ObsType.SPDWND, new String[]{"Wind Speed", "\"#aacfeb\", \"#567593\", \"#003050\"", "5, 15", "\"Below 5\", \"5 - 15\", \"Above 15\""});
  oObstypeFields.put(ObsType.EVT, new String[]{"NWS Alerts", 
     "\"#ff0000\", \"#ff751a\", \"#ffc700\", \"#a6a6a6\", \"#996600\"," +
     "\"#dbc3a3\", \"#80ffff\", \"#ff4bf6\", \"#8b4af\", \"#1a1aff\", \"#00007f\", \"#00af20\", \"#8c1515\", \"#99ffa1\"", 
     "1003.5, 1006.5, 1013.5, 1022.5, 1024.5, 1028.5, 1042.5, 1051.5, 1056.5, 1065.5, 1095.5, 1109.5, 1111.5",
     "\"Fire\", \"Heat\", \"Storm/Tornado\", \"Wind/Fog/Smoke\", \"Air Quality\"," + 
     "\"Earthquake/Volcano\", \"Winter Storm\", \"Freeze\", \"Cold\", \"Flood\", \"Lake/Marine/Coastal\", \"Tropical Storm\", \"Special Weather\", \"Other\""});
  oObstypeFields.put(ObsType.PCCAT, new String[]{"Precipitation Rate and Type", 
    "\"#ffffff\",\"#80ff80\",\"#00af20\",\"#00561e\"," +
    "\"#d9b3ff\",\"#8b4baf\",\"#4a0070\"," +
    "\"#80ffff\",\"#1a1aff\",\"#000066\"," +
    "\"#ffc2f1\",\"#ff4bf6\",\"#830d56\"", 
    "0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5, 11.5",
    "\"No Precipitation\", \"Rain - Light\", \"Rain - Medium\", \"Rain - Heavy\"," + 
    "\"Freezing Rain - Light\", \"Freezing Rain - Medium\", \"Freezing Rain - Heavy\","+ 
    "\"Snow - Light\", \"Snow - Medium\", \"Snow - Heavy\"," +
    "\"Ice Pellets - Light\", \"Ice Pellets - Medium\", \"Ice Pellets - Heavy\""});
 oObstypeFields.put(ObsType.TIMERT, new String[]{"Routes", "\"#000000\"", "", ""});
 oObstypeFields.put(ObsType.RDR0, new String[]{"Radar", "\"#e3e3e3\",\"#00efe7\",\"#009cf7\",\"#0000f7\",\"#00ff00\"," +
    "\"#00c600\",\"#008c00\",\"#ffff00\"," +
    "\"#e7bd00\",\"#fe9300\",\"#ff0000\"," +
    "\"#d60000\",\"#bd0000\",\"#fe00fe\",\"#9c52c6\",\"#fefefe\"", 
    "5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75",
    "\"Below 5\", \"5 - 10\", \"10 - 15\", \"15 - 20\"," + 
    "\"20 - 25\", \"25 - 30\", \"30 - 35\","+ 
    "\"35 - 40\", \"40 - 45\", \"45 - 50\"," +
    "\"50 - 55\", \"55 - 60\", \"60 - 65\", \"65 - 70\", \"70 - 75\", \"Above 75\""});
 //oObstypeFields.put(ObsType.RTEPC, new String[]{"Precipitation Rate", "\"#ffffff\",\"#80ff80\",\"#00af20\",\"#00561e\"", "0.0000000000000001, 0.1, 0.3", "\"No Precipitation\", \"Below 0.1\", \"0.1 - 0.3\", \"Above 0.3\""});
 

//  oObstypeFields.put(ObsType.DPHSN, new String[]{"", "", "", ""});

  boolean bFirst = true;
  %>[<%
  for(int nObstypeId : ObsType.ALL_OBSTYPES)
  {
    if(bFirst)
    {
      bFirst = false;
    }
    else
    {
    %>, <%
    }

  %>{"name": "<%= ObsType.getName(nObstypeId) %>",<%
  %>"desc": "<%= ObsType.getDescription(nObstypeId) %>",<%
  %>"metricUnits": "<%= ObsType.getUnits(nObstypeId, true) %>",<%
  %>"englishUnits": "<%= ObsType.getUnits(nObstypeId, false) %>",<%
  %>"id": <%= nObstypeId %><%
    
    if(oObstypeFields.containsKey(nObstypeId))
    {
      String[] sFields = oObstypeFields.get(nObstypeId);
      
      %>,"label": "<%= sFields[0] %>"<%
      %>,"rangeColors": [<%= sFields[1] %>]<%
      %>,"rangeLabels": [<%= sFields[3] %>]<%
      %>,"rangeBreakPoints": [<%= sFields[2] %>]<%
      
    }


 %>}<%
  }
  %>]