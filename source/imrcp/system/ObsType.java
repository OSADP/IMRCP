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
package imrcp.system;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * {@code ObsType} is a static class that manages the IMRCP system's list of
 * valid observation types, their labels, descriptions and units. The labels are
 * 6 character or less strings that are used to describe the medium and type of
 * observation. The string is converted into an integer that is used as the obs
 * type id using base 36
 */
public class ObsType
{

	/**
	 * List of database sources.
	 */
	private static final String OBS_TYPES[][] = new String[][]
	{
		{"obstype", "english", "metric", "description"}, // reserved for unknown observation type
		{"TAIR", "F", "C", "air temperature"},
		{"TDEW", "F", "C", "dew point temperature"},
		{"TPVT", "F", "C", "pavement temperature"},
		{"PRBAR", "psi", "mbar", "barometric pressure"},
		{"PRSUR", "psi", "mbar", "surface pressure"},
		{"RH", "%", "%", "relative humidity"},
		{"VIS", "mi", "km", "surface visibility"},
		{"CONPVT", "V", "V", "pavement conductivity"},
		{"STPVT", "", "", "pavement state"},
		{"DIRWND", "º", "º", "wind direction from which blowing height above ground"},
		{"GSTWND", "mph", "m/s", "wind speed gust height above ground"},
		{"SPDWND", "mph", "m/s", "wind speed height above ground"},
		{"COVCLD", "%", "%", "total cloud cover entire atmosphere single layer"},
		{"RTEPC", "in/hr", "mm/hr", "precipitation rate surface"},
		{"TYPPC", "", "", "precipitation type"},
		{"STG", "ft", "m", "flood stage"},
		{"EVT", "", "", "event"},
		{"VOLLNK", "veh/min", "veh/min", "link volume"},
		{"GENLNK", "veh", "veh", "vehicles generated on each generation link"},
		{"VEHLNK", "veh", "veh", "number of vehicles on each link"},
		{"QUELNK", "veh", "veh", "number of queued vehicles on each link"},
		{"SPDLNK", "mph", "kph", "average speed of vehicles on each link"},
		{"DNTLNK", "%", "%", "average density of vehicles on each link"},
		{"SPFLNK", "mph", "kph", "average speed of moving vehicles on each link"},
		{"DNFLNK", "%", "%", "average density of moving vehicles on each link"},
		{"CTLEFT", "veh", "veh", "number of left-turning vehicles on each link"},
		{"DURGRN", "s", "s", "average green time for each approach"},
		{"CTTHRU", "veh", "veh", "number of vehicles that pass through the link"},
		{"CTMID", "veh", "veh", "cumulative number of vehicles that pass the mid point of links"},
		{"TSSRF", "F", "C", "subsurface temperature"},
		{"DPHLIQ", "in", "mm", "liquid inundation depth"},
		{"DPHSN", "in", "cm", "snow inundation depth"},
		{"FLWCAT", "", "", "predicted flow category"},
		{"SPDCAT", "", "", "predicted speed category"},
		{"OCCCAT", "", "", "predicted occupancy category"},
		{"QPRLNK", "%", "%", "queue percentage on link"},
		{"DPHLNK", "in", "mm", "link depth"},
		{"PCCAT", "", "", "precipitation category"},
		{"TRFLNK", "%", "%", "traffic"},
		{"TDNLNK", "", "", "traffic density"},
		{"TIMERT", "min", "min", "route time"},
		{"RDR0", "dBZ", "dBZ", "merged base reflectivity"},
		{"NOTIFY", "", "", "Notification"}
	};

	/**
	 * Air Temperature
	 */
	public static final int TAIR = Integer.valueOf(OBS_TYPES[1][0], 36);

	/**
	 * Dew Point Temperature
	 */
	public static final int TDEW = Integer.valueOf(OBS_TYPES[2][0], 36);

	/**
	 * Pavement Temperature
	 */
	public static final int TPVT = Integer.valueOf(OBS_TYPES[3][0], 36);

	/**
	 * Barometric Pressure
	 */
	public static final int PRBAR = Integer.valueOf(OBS_TYPES[4][0], 36);

	/**
	 * Surface Pressure
	 */
	public static final int PRSUR = Integer.valueOf(OBS_TYPES[5][0], 36);

	/**
	 * Relative Humidity
	 */
	public static final int RH = Integer.valueOf(OBS_TYPES[6][0], 36);

	/**
	 * Visibility
	 */
	public static final int VIS = Integer.valueOf(OBS_TYPES[7][0], 36);

	/**
	 * Pavement Conductivity
	 */
	public static final int CONPVT = Integer.valueOf(OBS_TYPES[8][0], 36);

	/**
	 * Pavement State
	 */
	public static final int STPVT = Integer.valueOf(OBS_TYPES[9][0], 36);

	/**
	 * Wind Direction
	 */
	public static final int DIRWND = Integer.valueOf(OBS_TYPES[10][0], 36);

	/**
	 * Wind Gust Speed
	 */
	public static final int GSTWND = Integer.valueOf(OBS_TYPES[11][0], 36);

	/**
	 * Wind Speed
	 */
	public static final int SPDWND = Integer.valueOf(OBS_TYPES[12][0], 36);

	/**
	 * Cloud Cover
	 */
	public static final int COVCLD = Integer.valueOf(OBS_TYPES[13][0], 36);

	/**
	 * Precipitation Rate
	 */
	public static final int RTEPC = Integer.valueOf(OBS_TYPES[14][0], 36);

	/**
	 * Precipitation Type
	 */
	public static final int TYPPC = Integer.valueOf(OBS_TYPES[15][0], 36);

	/**
	 * Flood Stage
	 */
	public static final int STG = Integer.valueOf(OBS_TYPES[16][0], 36);

	/**
	 * Event
	 */
	public static final int EVT = Integer.valueOf(OBS_TYPES[17][0], 36);

	/**
	 * Link Volume
	 */
	public static final int VOLLNK = Integer.valueOf(OBS_TYPES[18][0], 36);

	/**
	 * Vehicles Generated on Link
	 */
	public static final int GENLNK = Integer.valueOf(OBS_TYPES[19][0], 36);

	/**
	 * Number of Vehicles on Link
	 */
	public static final int VEHLNK = Integer.valueOf(OBS_TYPES[20][0], 36);

	/**
	 * Number of Queued Vehicles on Link
	 */
	public static final int QUELNK = Integer.valueOf(OBS_TYPES[21][0], 36);

	/**
	 * Average Speed of Vehicles on Link
	 */
	public static final int SPDLNK = Integer.valueOf(OBS_TYPES[22][0], 36);

	/**
	 * Average Density of Vehicles on Link
	 */
	public static final int DNTLNK = Integer.valueOf(OBS_TYPES[23][0], 36);

	/**
	 * Average Speed of Moving Vehicles on Link
	 */
	public static final int SPFLNK = Integer.valueOf(OBS_TYPES[24][0], 36);

	/**
	 * Average Density of Vehicles on Link
	 */
	public static final int DNFLNK = Integer.valueOf(OBS_TYPES[25][0], 36);

	/**
	 * Number of Left-Turning Vehicles on Link
	 */
	public static final int CTLEFT = Integer.valueOf(OBS_TYPES[26][0], 36);

	/**
	 * Average Green Time
	 */
	public static final int DURGRN = Integer.valueOf(OBS_TYPES[27][0], 36);

	/**
	 * Number of Vehicles that Pass Through the Link
	 */
	public static final int CTTHRU = Integer.valueOf(OBS_TYPES[28][0], 36);

	/**
	 * Cumulative Number of Vehicles that Pass the Mid Point of the Link
	 */
	public static final int CTMID = Integer.valueOf(OBS_TYPES[29][0], 36);

	/**
	 * Subsurface Temperature
	 */
	public static final int TSSRF = Integer.valueOf(OBS_TYPES[30][0], 36);

	/**
	 * Liquid Inundation Depth
	 */
	public static final int DPHLIQ = Integer.valueOf(OBS_TYPES[31][0], 36);

	/**
	 * Snow/Ice Inundation Depth
	 */
	public static final int DPHSN = Integer.valueOf(OBS_TYPES[32][0], 36);

	/**
	 * Flow Category
	 */
	public static final int FLWCAT = Integer.valueOf(OBS_TYPES[33][0], 36);

	/**
	 * Speed Category
	 */
	public static final int SPDCAT = Integer.valueOf(OBS_TYPES[34][0], 36);

	/**
	 * Occupancy Category
	 */
	public static final int OCCCAT = Integer.valueOf(OBS_TYPES[35][0], 36);

	/**
	 * Queue Percentage on Link
	 */
	public static final int QPRLNK = Integer.valueOf(OBS_TYPES[36][0], 36);

	/**
	 * Depth of Water on Link
	 */
	public static final int DPHLNK = Integer.valueOf(OBS_TYPES[37][0], 36);

	/**
	 * Precipitation Category
	 */
	public static final int PCCAT = Integer.valueOf(OBS_TYPES[38][0], 36);

	/**
	 * Traffic
	 */
	public static final int TRFLNK = Integer.valueOf(OBS_TYPES[39][0], 36);

	/**
	 * Traffic Density
	 */
	public static final int TDNLNK = Integer.valueOf(OBS_TYPES[40][0], 36);

	/**
	 * Travel Time of Route
	 */
	public static final int TIMERT = Integer.valueOf(OBS_TYPES[41][0], 36);

	/**
	 * Merged Base Reflectivity (Radar)
	 */
	public static final int RDR0 = Integer.valueOf(OBS_TYPES[42][0], 36);

	/**
	 * Notification
	 */
	public static final int NOTIFY = Integer.valueOf(OBS_TYPES[43][0], 36);

	/**
	 * Contains all the Obs Type Ids
	 */
	private static final int[] TYPE_MAP
	   =
	   {
		   0, // reserved for unknown observation type
		   TAIR, TDEW, TPVT, PRBAR, PRSUR, RH, VIS, CONPVT,
		   STPVT, DIRWND, GSTWND, SPDWND, COVCLD, RTEPC, TYPPC, STG,
		   EVT, VOLLNK, GENLNK, VEHLNK, QUELNK, SPDLNK, DNTLNK, SPFLNK,
		   DNFLNK, CTLEFT, DURGRN, CTTHRU, CTMID, TSSRF, DPHLIQ, DPHSN,
		   FLWCAT, SPDCAT, OCCCAT, QPRLNK, DPHLNK, PCCAT, TRFLNK, TDNLNK,
		   TIMERT, RDR0, NOTIFY
	   };

	/**
	 * Contains all the Obs Type Ids
	 */
	public static final int[] ALL_OBSTYPES = Arrays.copyOfRange(TYPE_MAP, 1, TYPE_MAP.length);

	private static final TreeMap<String, TreeMap<Integer, String>> LOOKUP
	   = new TreeMap();


	/**
	 * Creates the Lookup Maps that contain mappings from integer ids to Strings
	 * for the different Obs Types
	 */
	static
	{
		TreeMap<Integer, String> oTemp = new TreeMap(); // NTCIP 1204 surface condition
		oTemp.put(1, "other");
		oTemp.put(2, "error");
		oTemp.put(3, "dry");
		oTemp.put(4, "trace-moisture");
		oTemp.put(5, "wet");
		oTemp.put(6, "chemically-wet");
		oTemp.put(7, "ice-warning");
		oTemp.put(8, "ice-watch");
		oTemp.put(9, "snow-warning");
		oTemp.put(10, "snow-watch");
		oTemp.put(11, "absorption");
		oTemp.put(12, "dew");
		oTemp.put(13, "frost");
		oTemp.put(14, "absorption-at-dewpoint");
		oTemp.put(20, "ice/snow"); // METRo Road conditions
		oTemp.put(21, "slush");
		oTemp.put(22, "melting-snow");
		oTemp.put(23, "icing-rain");
		LOOKUP.put("STPVT", oTemp);

		oTemp = new TreeMap();
		oTemp.put(0, "no-precipitation");
		oTemp.put(1, "light-rain");
		oTemp.put(2, "medium-rain");
		oTemp.put(3, "heavy-rain");
		oTemp.put(4, "light-freezing-rain");
		oTemp.put(5, "medium-freezing-rain");
		oTemp.put(6, "heavy-freezing-rain");
		oTemp.put(7, "light-snow");
		oTemp.put(8, "medium-snow");
		oTemp.put(9, "heavy-snow");
		oTemp.put(10, "light-ice");
		oTemp.put(11, "medium-ice");
		oTemp.put(12, "heavy-ice");
		LOOKUP.put("PCCAT", oTemp);

		oTemp = new TreeMap(); // RAP categorical precipitation types
		oTemp.put(0, "none");
		oTemp.put(1, "rain");
		oTemp.put(2, "snow");
		oTemp.put(3, "ice-pellets");
		oTemp.put(4, "freezing-rain");
		LOOKUP.put("TYPPC", oTemp);

		oTemp = new TreeMap();
		oTemp.put(1, "very-low");
		oTemp.put(2, "low");
		oTemp.put(3, "high");
		oTemp.put(4, "very-high");
		LOOKUP.put("FLWCAT", oTemp);

		oTemp = new TreeMap();
		oTemp.put(1, "low");
		oTemp.put(2, "medium-low");
		oTemp.put(3, "medium");
		oTemp.put(4, "medium-high");
		oTemp.put(5, "high");
		LOOKUP.put("SPDCAT", oTemp);

		oTemp = new TreeMap();
		oTemp.put(1, "very-low");
		oTemp.put(2, "low");
		oTemp.put(3, "high");
		oTemp.put(4, "very-high");
		LOOKUP.put("OCCCAT", oTemp);

		oTemp = new TreeMap(); // notifications (match corresponding alerts)
		oTemp.put(101, "light-winter-precip"); // imrcp alert types 100s - areal weather, 200s - road weather, 300s - traffic
		oTemp.put(102, "medium-winter-precip");
		oTemp.put(103, "heavy-winter-precip");
		oTemp.put(104, "light-precip");
		oTemp.put(105, "medium-precip");
		oTemp.put(106, "heavy-precip");
		oTemp.put(107, "low-visibility");
		oTemp.put(201, "dew-on-roadway");
		oTemp.put(202, "frost-on-roadway");
		oTemp.put(203, "blowing-snow");
		oTemp.put(204, "icy-bridge");
		oTemp.put(301, "incident");
		oTemp.put(302, "workzone");
		oTemp.put(303, "slow-traffic");
		oTemp.put(304, "very-slow-traffic");
		oTemp.put(305, "flooded-road");
		oTemp.put(306, "lengthy-queue");
		oTemp.put(307, "unusual-congestion");
		oTemp.put(399, "test");
		LOOKUP.put("NOTIFY", oTemp);

		oTemp = new TreeMap(); // TMDD PavementConditions
		oTemp.put(5888, "impassable");
		oTemp.put(5889, "almost-impassable");
		oTemp.put(5890, "passable-with-care");
		oTemp.put(5891, "passable");
		oTemp.put(5892, "surface-water-hazard");
		oTemp.put(5893, "danger-of-hydroplaning");
		oTemp.put(5894, "wet-pavement");
		oTemp.put(5895, "treated-pavement");
		oTemp.put(5896, "slippery");
		oTemp.put(5897, "low-ground-clearance");
		oTemp.put(5898, "at-grade-level-crossing");
		oTemp.put(5899, "mud-on-roadway");
		oTemp.put(5900, "leaves-on-roadway");
		oTemp.put(5901, "loose-sand-on-roadway");
		oTemp.put(5902, "loose-gravel");
		oTemp.put(5903, "fuel-on-roadway");
		oTemp.put(5904, "oil-on-roadway");
		oTemp.put(5905, "road-surface-in-poor-condition");
		oTemp.put(5906, "melting-tar");
		oTemp.put(5907, "uneven-lanes");
		oTemp.put(5908, "rough-road");
		oTemp.put(5909, "rough-crossing");
		oTemp.put(5910, "ice");
		oTemp.put(5911, "icy-patches");
		oTemp.put(5912, "black-ice");
		oTemp.put(5913, "ice-pellets-on-roadway");
		oTemp.put(5914, "ice-build-up");
		oTemp.put(5915, "freezing-rain");
		oTemp.put(5916, "wet-and-icy-roads");
		oTemp.put(5917, "melting-snow");
		oTemp.put(5918, "slush");
		oTemp.put(5919, "frozen-slush");
		oTemp.put(5920, "snow-on-roadway");
		oTemp.put(5921, "packed-snow");
		oTemp.put(5922, "packed-snow-patches");
		oTemp.put(5923, "plowed-snow");
		oTemp.put(5924, "wet-snow");
		oTemp.put(5925, "fresh-snow");
		oTemp.put(5926, "powder-snow");
		oTemp.put(5927, "granular-snow");
		oTemp.put(5928, "froazen-snow");
		oTemp.put(5929, "crusted-snow");
		oTemp.put(5930, "deep-snow");
		oTemp.put(5931, "snow-drifts");
		oTemp.put(5932, "drifting-snow");
		oTemp.put(5933, "expected-snow-accumulation");
		oTemp.put(5934, "current-snow-accumulation");
		oTemp.put(5935, "sand");
		oTemp.put(5936, "gravel");
		oTemp.put(5937, "paved");
		oTemp.put(5938, "dry-pavement");
		oTemp.put(5939, "snow-cleared");
		oTemp.put(5940, "pavement-conditions-improved");
		oTemp.put(5941, "skid-hazard-reduced");
		oTemp.put(5942, "pavement-conditions-cleared");

		oTemp.put(512, "accident"); // TMDD AccidentsAndIncidents
		oTemp.put(513, "serious-accident");
		oTemp.put(514, "injury-accident");
		oTemp.put(515, "minor-accident");
		oTemp.put(516, "multi-vehicle-accident");
		oTemp.put(517, "numerous-accidents");
		oTemp.put(518, "accident-involving-a-bicycle");
		oTemp.put(519, "accident-involving-a-bus");
		oTemp.put(520, "accident-involving-a-motorcycle");
		oTemp.put(521, "accident-involving-a-pedestrian");
		oTemp.put(522, "accident-involving-a-train");
		oTemp.put(523, "accident-involving-a-truck");
		oTemp.put(524, "accident-involving-a-semi-trailer");
		oTemp.put(525, "accident-involving-a-hazardous-materials");
		oTemp.put(526, "earlier-accident");
		oTemp.put(527, "medical-emergency");
		oTemp.put(528, "secondary-accident");
		oTemp.put(529, "rescue-and-recovery-work-removed");
		oTemp.put(530, "accident-investigation-work");
		oTemp.put(531, "incident");
		oTemp.put(532, "stalled-vehicle");
		oTemp.put(533, "abandoned-vehicle");
		oTemp.put(534, "disabled-vehicle");
		oTemp.put(535, "disabled-truck");
		oTemp.put(536, "disabled-semi-trailer");
		oTemp.put(537, "disabled-bus");
		oTemp.put(538, "disabled-train");
		oTemp.put(539, "vehicle-spun-out");
		oTemp.put(540, "vehicle-on-fire");
		oTemp.put(541, "vehicle-in-water");
		oTemp.put(542, "vehicles-slowing-to-look-at-accident");
		oTemp.put(543, "jackknifed-semi-trailer");
		oTemp.put(544, "jackknifed-trailer-home");
		oTemp.put(545, "jackknifed-trailer");
		oTemp.put(546, "spillage-occurring-from-moving-vehicle");
		oTemp.put(547, "acid-spill");
		oTemp.put(548, "chemical-spill");
		oTemp.put(549, "fuel-spill");
		oTemp.put(550, "hazardous-materials-spill");
		oTemp.put(551, "oil-spill");
		oTemp.put(552, "spilled-load");
		oTemp.put(553, "toxic-spill");
		oTemp.put(554, "overturned-vehicle");
		oTemp.put(555, "overturned-truck");
		oTemp.put(556, "overturned-semi-trailer");
		oTemp.put(557, "overturned-bus");
		oTemp.put(558, "derailed-train");
		oTemp.put(559, "stuck-vehicle");
		oTemp.put(560, "truck-stuck-under-bridge");
		oTemp.put(561, "bus-stuck-under-bridge");
		oTemp.put(562, "accident-cleared");
		oTemp.put(563, "incident-cleared");

		oTemp.put(101, "light-winter-precip"); // imrcp alert types 100s - areal weather, 200s - road weather, 300s - traffic
		oTemp.put(102, "medium-winter-precip");
		oTemp.put(103, "heavy-winter-precip");
		oTemp.put(104, "light-precip");
		oTemp.put(105, "medium-precip");
		oTemp.put(106, "heavy-precip");
		oTemp.put(107, "low-visibility");
		oTemp.put(201, "dew-on-roadway");
		oTemp.put(202, "frost-on-roadway");
		oTemp.put(203, "blowing-snow");
		oTemp.put(204, "icy-bridge");
		oTemp.put(301, "incident");
		oTemp.put(302, "workzone");
		oTemp.put(303, "slow-traffic");
		oTemp.put(304, "very-slow-traffic");
		oTemp.put(305, "flooded-road");
		oTemp.put(306, "lengthy-queue");
		oTemp.put(307, "unusual-congestion");
		oTemp.put(399, "test");
		// NWS Cap alert types 1000s
		oTemp.put(1000, "Extreme Fire Danger"); // fire
		oTemp.put(1001, "Fire Warning");
		oTemp.put(1002, "Fire Weather Watch");
		oTemp.put(1003, "Red Flag Warning");
		oTemp.put(1004, "Heat Advisory"); // heat
		oTemp.put(1005, "Excessive Heat Warning");
		oTemp.put(1006, "Excessive Heat Watch");
		oTemp.put(1007, "Severe Thunderstorm Warning"); //storm/tornado
		oTemp.put(1008, "Severe Thunderstorm Watch");
		oTemp.put(1009, "Storm Warning");
		oTemp.put(1010, "Storm Watch");
		oTemp.put(1011, "Tornado Warning");
		oTemp.put(1012, "Tornado Watch");
		oTemp.put(1013, "Severe Weather Statement");
		oTemp.put(1014, "High Wind Warning"); // wind/fog/smoke
		oTemp.put(1015, "High Wind Watch");
		oTemp.put(1016, "Wind Advisory");
		oTemp.put(1017, "Extreme Wind Warning");
		oTemp.put(1018, "Brisk Wind Advisory");
		oTemp.put(1019, "Blowing Dust Advisory");
		oTemp.put(1020, "Dust Storm Warning");
		oTemp.put(1021, "Dense Fog Advisory");
		oTemp.put(1022, "Dense Smoke Advisory");
		oTemp.put(1023, "Air Quality Alert"); // air quality
		oTemp.put(1024, "Air Stagnation Advisory");
		oTemp.put(1025, "Ashfall Advisory"); // earthquake/volcano
		oTemp.put(1026, "Ashfall Warning");
		oTemp.put(1027, "Earthquake Warning");
		oTemp.put(1028, "Volcano Warning");
		oTemp.put(1029, "Winter Storm Warning"); //winter storm
		oTemp.put(1030, "Winter Storm Watch");
		oTemp.put(1031, "Winter Weather Advisory");
		oTemp.put(1032, "Ice Storm Warning");
		oTemp.put(1033, "Blizzard Warning");
		oTemp.put(1034, "Blizzard Watch");
		oTemp.put(1035, "Avalanche Warning");
		oTemp.put(1036, "Avalanche Watch");
		oTemp.put(1037, "Blowing Snow Advisory");
		oTemp.put(1038, "Snow and Blowing Snow Advisory");
		oTemp.put(1039, "Heavy Snow Warning");
		oTemp.put(1040, "Sleet Advisory");
		oTemp.put(1041, "Sleet Warning");
		oTemp.put(1042, "Snow Advisory");
		oTemp.put(1043, "Freeze Warning"); // freeze
		oTemp.put(1044, "Freeze Watch");
		oTemp.put(1045, "Freezing Drizzle Advisory");
		oTemp.put(1046, "Freezing Fog Advisory");
		oTemp.put(1047, "Freezing Rain Advisory");
		oTemp.put(1048, "Freezing Spray Advisory");
		oTemp.put(1049, "Frost Advisory");
		oTemp.put(1050, "Hard Freeze Warning");
		oTemp.put(1051, "Hard Freeze Watch");
		oTemp.put(1052, "Wind Chill Advisory"); // cold
		oTemp.put(1053, "Wind Chill Warning");
		oTemp.put(1054, "Wind Chill Watch");
		oTemp.put(1055, "Extreme Cold Warning");
		oTemp.put(1056, "Extreme Cold Watch");
		oTemp.put(1057, "Flash Flood Statement"); // flood
		oTemp.put(1058, "Flash Flood Warning");
		oTemp.put(1059, "Flash Flood Watch");
		oTemp.put(1060, "Flood Advisory");
		oTemp.put(1061, "Flood Statement");
		oTemp.put(1062, "Flood Warning");
		oTemp.put(1063, "Flood Watch");
		oTemp.put(1064, "Hydrologic Advisory");
		oTemp.put(1065, "Hydrologic Outlook");
		oTemp.put(1066, "Beach Hazards Statement"); // lake/marine/coastal
		oTemp.put(1067, "Coastal Flood Advisory");
		oTemp.put(1068, "Coastal Flood Statement");
		oTemp.put(1069, "Coastal Flood Warning");
		oTemp.put(1070, "Coastal Flood Watch");
		oTemp.put(1071, "Gale Warning");
		oTemp.put(1072, "Gale Watch");
		oTemp.put(1073, "Hazardous Seas Warning");
		oTemp.put(1074, "Hazardous Seas Watch");
		oTemp.put(1075, "Heavy Freezing Spray Warning");
		oTemp.put(1076, "Heavy Freezing Spray Watch");
		oTemp.put(1077, "High Surf Advisory");
		oTemp.put(1078, "High Surf Warning");
		oTemp.put(1079, "Lake Effect Snow Advisory");
		oTemp.put(1080, "Lake Effect Snow and Blowing Snow Advisory");
		oTemp.put(1081, "Lake Effect Snow Warning");
		oTemp.put(1082, "Lake Effect Snow Watch");
		oTemp.put(1083, "Lakeshore Flood Advisory");
		oTemp.put(1084, "Lakeshore Flood Statement");
		oTemp.put(1085, "Lakeshore Flood Warning");
		oTemp.put(1086, "Lakeshore Flood Watch");
		oTemp.put(1087, "Lake Wind Advisory");
		oTemp.put(1088, "Low Water Advisory");
		oTemp.put(1089, "Marine Weather Statement");
		oTemp.put(1090, "Rip Current Statement");
		oTemp.put(1091, "Small Craft Advisory");
		oTemp.put(1092, "Special Marine Warning");
		oTemp.put(1093, "Tsunami Advisory");
		oTemp.put(1094, "Tsunami Warning");
		oTemp.put(1095, "Tsunami Watch");
		oTemp.put(1096, "Hurricane Force Wind Warning"); // tropical storm
		oTemp.put(1097, "Hurricane Force Wind Watch");
		oTemp.put(1098, "Hurricane Statement");
		oTemp.put(1099, "Hurricane Warning");
		oTemp.put(1100, "Hurricane Watch");
		oTemp.put(1101, "Hurricane Wind Warning");
		oTemp.put(1102, "Hurricane Wind Watch");
		oTemp.put(1103, "Tropical Storm Warning");
		oTemp.put(1104, "Tropical Storm Watch");
		oTemp.put(1105, "Tropical Storm Wind Warning");
		oTemp.put(1106, "Tropical Storm Wind Watch");
		oTemp.put(1107, "Typhoon Statement");
		oTemp.put(1108, "Typhoon Warning");
		oTemp.put(1109, "Typhoon Watch");
		oTemp.put(1110, "Hazardous Weather Outlook"); // special weather
		oTemp.put(1111, "Special Weather Statement");
		oTemp.put(1112, "911 Telephone Outage"); // other
		oTemp.put(1113, "Administrative Message");
		oTemp.put(1114, "Child Abduction Emergency");
		oTemp.put(1115, "Civil Danger Warning");
		oTemp.put(1116, "Civil Emergency Message");
		oTemp.put(1117, "Evacuation Immediate");
		oTemp.put(1118, "Hazardous Materials Warning");
		oTemp.put(1119, "Law Enforcement Warning");
		oTemp.put(1120, "Local Area Emergency");
		oTemp.put(1121, "Nuclear Power Plant Warning");
		oTemp.put(1122, "Radiological Hazard Warning");
		oTemp.put(1123, "Shelter In Place Warning");
		oTemp.put(1124, "Test");

		LOOKUP.put("EVT", oTemp);
	}


	/**
	 * Private Default Constructor
	 */
	private ObsType()
	{
	}


	/**
	 * Returns the index of given Obs Type Id in the TYPE_MAP array
	 *
	 * @param nObsTypeId the Obs Type Id you need the index of
	 * @return index of the given Obs Type Id in the TYPE_MAP array. Returns 0
	 * if the Id could not be found
	 */
	private static int getIndex(int nObsTypeId)
	{
		int nIndex = TYPE_MAP.length;
		while (nIndex-- > 0)
		{
			if (TYPE_MAP[nIndex] == nObsTypeId)
				return nIndex;
		}
		return 0;
	}


	/**
	 * Prints the data items contained in {@code iObsSet} in the form:
	 * <p>
	 * <blockquote>
	 * {object type}, {Sensor ID}, {Timestamp}, {Latitude}, {Longitude},
	 * {Elevation}, {Value}, {Run}, {Flags}, {Confidence Level}
	 * </blockquote>
	 * </p>
	 *
	 * @param nObsTypeId
	 * @param
	 * @return
	 */
	public static String getName(int nObsTypeId)
	{
		return OBS_TYPES[getIndex(nObsTypeId)][0];
	}


	/**
	 * Returns the English units for the given Obs Type Id
	 *
	 * @param nObsTypeId the Obs Type Id you need the units of
	 * @return English units for the given Obs Type Id
	 */
	public static String getUnits(int nObsTypeId)
	{
		return getUnits(nObsTypeId, false);
	}


	/**
	 * Returns the units for the given Obs Type Id. Whether English or Metric
	 * units are return is based off of the given boolean
	 *
	 * @param nObsTypeId the Obs Type Id you need the units of
	 * @param bMetric true if Metric units are needed, false for English units
	 * @return English or Metric units for the given Obs Type Id. Metric units
	 * if bMetric is true, otherwise English units
	 */
	public static String getUnits(int nObsTypeId, boolean bMetric)
	{
		if (bMetric)
			return OBS_TYPES[getIndex(nObsTypeId)][2];
		else
			return OBS_TYPES[getIndex(nObsTypeId)][1];
	}


	/**
	 * Returns the description of the given Obs Type Id
	 *
	 * @param nObsTypeId the Obs Type Id you need the description of
	 * @return description of the given Obs Type Id
	 */
	public static String getDescription(int nObsTypeId)
	{
		return OBS_TYPES[getIndex(nObsTypeId)][3];
	}


	/**
	 *
	 * @param nObstypeId
	 * @return
	 */
	public static boolean hasLookup(int nObstypeId)
	{
		return LOOKUP.containsKey(getName(nObstypeId));
	}


	/**
	 *
	 * @param nObsTypeId
	 * @param nValue
	 * @return
	 */
	public static String lookup(int nObsTypeId, int nValue)
	{
		TreeMap<Integer, String> oMap = LOOKUP.get(getName(nObsTypeId));
		if (oMap != null)
			return oMap.get(nValue);

		return "";
	}


	/**
	 *
	 * @param nObsTypeId
	 * @param sValue
	 * @return
	 */
	public static int lookup(int nObsTypeId, String sValue)
	{
		TreeMap<Integer, String> oMap = LOOKUP.get(getName(nObsTypeId));
		if (oMap != null)
		{
			Iterator<Map.Entry<Integer, String>> oIt = oMap.entrySet().iterator();
			while (oIt.hasNext())
			{
				Map.Entry<Integer, String> oEntry = oIt.next();
				if (oEntry.getValue().compareTo(sValue) == 0)
					return oEntry.getKey();
			}
		}
		return Integer.MIN_VALUE;
	}
}
