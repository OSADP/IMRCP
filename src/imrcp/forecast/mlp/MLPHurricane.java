/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.forecast.mlp;

import imrcp.system.BaseBlock;
import imrcp.store.FileCache;
import imrcp.system.FilenameFormatter;
import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.Network;
import imrcp.geosrv.WayNetworks;
import imrcp.geosrv.osm.OsmWay;
import imrcp.store.HurricaneCenter;
import imrcp.store.NHCStore;
import imrcp.store.NHCWrapper;
import imrcp.store.Obs;
import imrcp.store.ObsList;
import imrcp.store.ObsView;
import imrcp.system.Arrays;
import imrcp.system.CsvReader;
import imrcp.system.Directory;
import imrcp.system.FileUtil;
import imrcp.system.Id;
import imrcp.system.Introsort;
import imrcp.system.MathUtil;
import imrcp.system.ObsType;
import imrcp.system.Scheduling;
import imrcp.system.Units;
import imrcp.system.Util;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.json.JSONObject;
import org.rosuda.REngine.Rserve.RConnection;

/**
 *
 * @author aaron.cherney
 */
public class MLPHurricane extends BaseBlock
{
	/**
	 * Object used to create time dependent file names to save on disk
	 */
	private FilenameFormatter m_oDest;

	
	/**
	 * Object used to create time dependent file names for available hurricane
	 * forecast files downloaded from the National Hurricane Center
	 */
	private FilenameFormatter m_oNhcFf;

	
	/**
	 * Format String used to generate file names for input files to the SVM model
	 */
	private String m_sSvmInputFf;

	
	/**
	 * Path to the Rdata file that contains the markov chains for the SVM model
	 */
	private String m_sSvmMarkov;

	
	/**
	 * Path to the R script that contains the methods needed for the hurricane
	 * model
	 */
	private String m_sRScript;

	
	/**
	 * Network Id of the Louisiana Network
	 */
	private String m_sNetworkId;

	
	/**
	 * Number of threads to use to process a single run of the model
	 */
	private int m_nThreads;

	
	/**
	 * Path to the base directory of the MLP hurricane files
	 */
	private String m_sWorkingDir;

	
	/**
	 * Queue that contains the NHC forecast files to be processed
	 */
	private final ArrayDeque<String> m_oFileQueue = new ArrayDeque();

	
	/**
	 * Period of execution in seconds
	 */
	private int m_nPeriod;

	
	/**
	 * Schedule offset from midnight in seconds
	 */
	private int m_nOffset;

	
	/**
	 * Number of hours to use as the horizon in the online prediction. This is 
	 * the number of forecast hours that will be generated
	 */
	private int m_nHorizon;
	
	
	/**
	 * Format string used to generate files to contain data from the oneshot model
	 */
	private String m_sOneshotFf;

	
	/**
	 * File that is currently being processed. {@code null} if no file is being
	 * processed
	 */
	private String m_sCurrentFile;

	
	/**
	 * Format string used to generate files for the .info file for each storm
	 */
	private String m_sStormInfoFf;
	
	
	/**
	 * A rough estimate of the coast line used to determine when landfall 
	 * happens
	 */
	private final double[] m_dCoastLine = new double[]{-97.1836293,27.5947176,-96.9851421,27.8959487,-96.5378880,28.2554942,
		-95.4662076,28.8118550,-94.7655095,29.2723850,-94.7124878,29.3353212,-94.6860702,29.4059219,-94.0173520,29.6624647,-93.8447573,29.6639951,
		-93.7619823,29.7206023,-93.2353925,29.7649478,-92.7915777,29.6349141,-92.6524743,29.5908297,-92.3215409,29.5246281,-92.2522758,29.5357888,
		-92.1368340,29.5833935,-92.0524405,29.5779252,-92.0096842,29.5526366,-91.8437900,29.4744995,-91.7659736,29.4804549,-91.6931480,29.5554194,
		-91.5403823,29.5280930,-91.3405020,29.4485561,-91.3419297,29.2930307,-91.2848210,29.2443715,-91.0592418,29.1745860,-90.8408011,29.1446633,
		-90.6480594,29.2020074,-90.5852398,29.2929440,-90.4681671,29.2854728,-90.2782807,29.2493543,-90.2640036,29.0798011,-90.1926177,29.0810488,
		-89.8942249,29.2792464,-89.4958920,29.2530913,-89.4130844,28.9262144,-89.1318242,28.9849289,-88.9807647,29.1773387,-89.0550059,29.2558420,
		-89.2163379,29.3753502,-89.4262123,29.4151552,-89.4247846,29.6536571,-89.2203503,29.8253632,-89.1603862,30.0641284,-89.1703802,30.3220395,
		-88.9084479,30.3826008,-88.7057121,30.3333227,-88.4958378,30.3111394,-88.1574689,30.3037438,-88.0189804,30.2087848,-87.5506893,30.2408592,
		-86.8994420,30.3627083,-86.4882596,30.3737948,-86.2716506,30.3488598,-85.7528762,30.1237185,-85.3981792,29.8917873,-86.1799602,29.5691135};
	
	
	/**
	 * Name of the major highways in Louisiana
	 */
	private final static String[] m_sHighways = new String[]{"I 10", "I 12", "I 49", "I 20", "I 55", "US 61", "US 165", "US 171"};
	
	
	/**
	 * Stores the lon/lat of the major cities identified in the MLP Hurricane 
	 * model
	 */
	private final static ArrayList<double[]> m_dCities;
	
	
	/**
	 * Ensures all of the highways are upper and adds the lon/lats of the cities
	 * necessary for the model
	 */
	static
	{
		for (int nIndex = 0; nIndex < m_sHighways.length; nIndex++)
			m_sHighways[nIndex] = m_sHighways[nIndex].toUpperCase();
		m_dCities = new ArrayList();
		m_dCities.add(new double[]{-93.198068, 30.213061}); // lake_charles 
		m_dCities.add(new double[]{-92.011838, 30.251668}); // lafayette
		m_dCities.add(new double[]{-91.158722, 30.445230}); // baton_rouge
		m_dCities.add(new double[]{-90.132138, 29.957029}); // new_orleans
		m_dCities.add(new double[]{-92.434701, 31.330712}); // alexandria
		m_dCities.add(new double[]{-93.721275, 32.523608}); // shreveport
		m_dCities.add(new double[]{-92.131521, 32.496555}); // monroe
	}
	
	
	@Override
	public void reset(JSONObject oBlockConfig)
	{
		m_sNetworkId = oBlockConfig.optString("network", "");
		m_oDest = new FilenameFormatter(oBlockConfig.optString("dest", ""));
		m_oNhcFf = new FilenameFormatter(oBlockConfig.optString("nhcfile", ""));
		m_sSvmInputFf = oBlockConfig.optString("svmff", "");
		m_sSvmMarkov = oBlockConfig.optString("svmmarkov", "");
		m_sRScript = oBlockConfig.optString("script", "");
		m_sWorkingDir = oBlockConfig.optString("dir", "");
		m_nThreads = oBlockConfig.optInt("threads", 5);
		m_nPeriod = oBlockConfig.optInt("period", 300);
		m_nOffset = oBlockConfig.optInt("offset", 60);
		m_nHorizon = oBlockConfig.optInt("horizon", 6);
		m_sOneshotFf = oBlockConfig.optString("oneshot", "");
		m_sStormInfoFf = oBlockConfig.optString("storminfo", "/dev/shm/imrcp-test/mlp/hurricane/%s.info");
	}
	
	
	/**
	 * Sets a schedule to execute on a fixed interval.
	 * @return true if no Exceptions are thrown
	 * @throws Exception
	 */
	@Override
	public boolean start()
		throws Exception
	{
//		m_oFileQueue.addLast("/opt/imrcp-data-prod/nhc/2021/nhc_AL092021_001adv_202108261500_202109010000_202108261445.zip");
		m_oFileQueue.addLast("/opt/imrcp-data-prod/nhc/2021/nhc_AL092021_005Aadv_202108271800_202109020000_202108271743.zip");
		Scheduling.getInstance().scheduleOnce(this, 10000);
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}
	
	
	/**
	 * Called when a message from {@link imrcp.system.Directory} is received. If
	 * the message is "file download" adds the files to the queue and if the queue
	 * is then not empty calls {@link #execute()}
	 * @param sMessage
	 */
	@Override
	public void process(String[] sMessage)
	{
		if (sMessage[MESSAGE].compareTo("file download") == 0)
		{
			synchronized (m_oFileQueue)
			{
				for (int i = 2; i < sMessage.length; i++)
					m_oFileQueue.addLast(sMessage[i]);
				
				if (!m_oFileQueue.isEmpty())
					execute();
			}
		}
	}
	
	
	/**
	 * Determines the first time a hurricane warning was issued for the NHC storm
	 * the given file is associated with.
	 * @param sFile NHC hurricane forecast file
	 * @return Timestamp in milliseconds since Epoch when the first hurricane
	 * warning was issued for the associated storm. If no hurricane warning has
	 * been issued return {@code Long.MIN_VALUE}
	 * @throws Exception
	 */
	public long getHurWarningTime(String sFile)
		throws Exception
	{
		String sStormId = getStormId(sFile);
		Path oDir = Paths.get(sFile.substring(0, sFile.lastIndexOf("/")));
		List<Path> oFiles = Files.walk(oDir).filter(Files::isRegularFile).filter(oP -> oP.toString().contains(sStormId)).collect(Collectors.toList());
		Introsort.usort(oFiles, (Path o1, Path o2) -> FileCache.REFTIMECOMP.compare(o2.toString(), o1.toString())); // sort files for newest to oldest
		int nHu = ObsType.lookup(ObsType.TRSCAT, "Hurricane");
		int nMh = ObsType.lookup(ObsType.TRSCAT, "Major Hurricane");
		for (Path oPath : oFiles)
		{
			NHCWrapper oFile = new NHCWrapper();
			long[] lTimes = new long[3];
			m_oNhcFf.parse(oPath.toString(), lTimes);
			oFile.load(lTimes[FileCache.START], lTimes[FileCache.END], lTimes[FileCache.VALID], oPath.toString(), Integer.valueOf("nhc", 36));
			for (Obs oObs : oFile.m_oObs)
			{
				if (oObs.m_nObsTypeId == ObsType.TRSCAT && (oObs.m_dValue == nHu || oObs.m_dValue == nMh))
				{
					return oFile.m_lStartTime;
				}
			}
		}
		
		return Long.MIN_VALUE;
	}
	
	
	/**
	 * Determines the landfall of the storm associated with the given file by 
	 * parsing the files associated with the storm from newest to oldest until
	 * a landfall can be calculated.
	 * @param oFile NHC hurricane forecast file
	 * @return [lon of landfall, lat of landfall, sshws hurricane category]
	 * @throws Exception
	 */
	private double[] getLandfall(NHCWrapper oFile)
		throws Exception
	{
		double[] dLandfall = new double[]{Double.NaN, Double.NaN, Double.NaN, Double.NaN};
		getLandfall(oFile, dLandfall); // first attempt to calculate the landfall from the given file
		
		if (!Double.isFinite(dLandfall[0])) // if it cannot be calculated from the given file, try to calculate it from other files associated with the same storm
		{
			String sFile = oFile.m_sFilename;
			String sStormId = getStormId(sFile);
			Path oDir = Paths.get(sFile.substring(0, sFile.lastIndexOf("/")));
			List<Path> oFiles = Files.walk(oDir).filter(Files::isRegularFile).filter(oP -> oP.toString().contains(sStormId)).collect(Collectors.toList());
			Introsort.usort(oFiles, (Path o1, Path o2) -> FileCache.REFTIMECOMP.compare(o2.toString(), o1.toString())); // sort files from newest to oldest
			int nIndex = oFiles.size();
			long[] lTimes = new long[3];
			while (nIndex-- > 0 && !Double.isFinite(dLandfall[0])) // check files until a landfall has been calculated
			{
				Path oPath = oFiles.get(nIndex);
				NHCWrapper oNhcFile = new NHCWrapper();
				String sFilename = oPath.toString();
				m_oNhcFf.parse(sFilename, lTimes);
				oNhcFile.load(lTimes[FileCache.START], lTimes[FileCache.END], lTimes[FileCache.VALID], sFilename, Integer.valueOf("nhc", 36));
				if (oFile.m_lValidTime <= oNhcFile.m_lValidTime) // ignore files that are not valid for the timestamp of the original file
					continue;
				
				getLandfall(oNhcFile, dLandfall);
			}
		}
		
		return dLandfall;
	}
	
	
	/**
	 * Determine the landfall of the storm from the given NHC forecast file by
	 * finding the intersection of the hurricane track with the coastline.
	 * @param oFile NHC hurricane forecast file
	 * @param dLandfall array used to store the landfall values:
	 * [lon of landfall, lat of landfall, sshws hurricane category]
	 */
	private void getLandfall(NHCWrapper oFile, double[] dLandfall)
	{
		for (int nIndex = 0; nIndex < oFile.m_oCenters.size() - 1; nIndex++) // for each line segment of the hurricane track
		{
			HurricaneCenter o1 = oFile.m_oCenters.get(nIndex);
			HurricaneCenter o2 = oFile.m_oCenters.get(nIndex + 1);

			double dX1 = GeoUtil.fromIntDeg(o1.m_nLon);
			double dY1 = GeoUtil.fromIntDeg(o1.m_nLat);
			double dX2 = GeoUtil.fromIntDeg(o2.m_nLon);
			double dY2 = GeoUtil.fromIntDeg(o2.m_nLat);

			for (int nCoastIndex = 0; nCoastIndex < m_dCoastLine.length - 2; nCoastIndex += 2) // see if the hurricane track segment intersects with any of the line segments of the coastline
			{
				double dCx1 = m_dCoastLine[nCoastIndex];
				double dCy1 = m_dCoastLine[nCoastIndex + 1];
				double dCx2 = m_dCoastLine[nCoastIndex + 2];
				double dCy2 = m_dCoastLine[nCoastIndex + 3];

				if (GeoUtil.boundingBoxesIntersect(Math.min(dX1, dX2), Math.min(dY1, dY2), Math.max(dX1, dX2), Math.max(dY1, dY2), 
					Math.min(dCx1, dCx2), Math.min(dCy1, dCy2), Math.max(dCx1, dCx2), Math.max(dCy1, dCy2)))
					GeoUtil.getIntersection(dX1, dY1, dX2, dY2, dCx1, dCy1, dCx2, dCy2, dLandfall); // getIntersection fills in the first two position of dLandfall with the lon and lat if the two line segments insect, otherwise they get seet to NaN

				
				if (Double.isFinite(dLandfall[0])) // intersection was found
				{
					dLandfall[3] = o1.m_lTimestamp;
					break;
				}
			}
			if (Double.isFinite(dLandfall[0])) // if an intersection was found
			{
				dLandfall[2] = NHCWrapper.getSSHWS(o1.m_nMaxSpeed); // set the hurricane category
				break;
			}
		}
	}
	
	
	/**
	 * Get the storm id by parsing the given filename
	 * @param sFile NHC hurricane forecast file
	 * @return Storm Id in the format BB##yyyy where BB is the 2 char basin 
	 * abbreviation, ## is the 2 digit storm number for that basin, yyyy is the 
	 * 4 digit year
	 */
	private String getStormId(String sFile)
	{
		int nStart = sFile.lastIndexOf("/");
		nStart = sFile.indexOf("_", nStart) + 1;
		return sFile.substring(nStart, sFile.indexOf("_", nStart));
	}
	
	
	/**
	 * If the file is not empty and a file is not currently being processed, gets
	 * the first file out of the queue and calls {@link #buildData(java.lang.String)}
	 * with that file as input. After {@link #buildData(java.lang.String)} returns,
	 * Checks the working dir for any stale data files that can be deleted.
	 */
	@Override
	public void execute()
	{
		long lCutoff = System.currentTimeMillis() - 86400000 * 2; // remove data files that are 2 days old
		String sFile = null;
		synchronized (m_oFileQueue)
		{
			if (m_sCurrentFile != null) // already processing a file
				return;

			sFile = m_oFileQueue.pollFirst();
			m_sCurrentFile = sFile;
		}
		
		if (!buildData(sFile)) // buildData early outted so it isn't processing the forecast
			m_sCurrentFile = null;
		
		try
		{
			Path oBaseDir = Paths.get(m_sWorkingDir);
			Pattern oFilePattern = Pattern.compile("((AL|EP)[0-9]{6}_[0-9]{12}_[0-9]{1,3}\\.csv(\\.histdata)?)|(.+\\.info)|([0-9abcdef]{32}\\.csv)");
			List<Path> oDirs = Files.walk(oBaseDir, 1).filter(Files::isDirectory).filter(p -> p.toString().compareTo(oBaseDir.toString()) != 0).collect(Collectors.toList());
			for (Path oDir : oDirs)
			{
				List<Path> oFiles = Files.walk(oDir, 1).filter(Files::isRegularFile)
					.filter(p -> {
						return oFilePattern.matcher(p.getFileName().toString()).matches();
						}).collect(Collectors.toList());
				for (Path oFile : oFiles)
				{
					if (Files.getLastModifiedTime(oFile).toMillis() < lCutoff)
						Files.deleteIfExists(oFile);
	}
	
				try (DirectoryStream oStream = Files.newDirectoryStream(oDir))
				{
					if (!oStream.iterator().hasNext())
						Files.delete(oDir);
				}
			}
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
	
	
	/**
	 * Sets up the necessary data structures and queries ObsView for speed 
	 * observations that will be used by all of the threads used to process
	 * the MLP Hurricane model for the given file.Then executes the
	 * {@link imrcp.forecast.mlp.MLPHurricane.Delegate} that handles the multi-
     * threaded execution of the model.
	 * @param sNhcFile NHC hurricane forecast file to process
	 * @return false if the situations occur that allow the file to not be processed,
	 * true if a {@link imrcp.forecast.mlp.MLPHurricane.Delegate} is created and
	 * executed.
	 */
	public boolean buildData(String sNhcFile)
	{
		try
		{
			if (sNhcFile == null)
				return false;
			long lHurWarn = getHurWarningTime(sNhcFile);
			if (lHurWarn == Long.MIN_VALUE) // no hurricane warning so the file doesn't need to be processed
				return false;
			
			String sStormId = getStormId(sNhcFile);
			m_oLogger.debug(sNhcFile);
			SimpleDateFormat oMyFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			oMyFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
			NHCWrapper oFile = new NHCWrapper();
			long[] lTimes = new long[3];
			m_oNhcFf.parse(sNhcFile, lTimes);
			oFile.load(lTimes[FileCache.START], lTimes[FileCache.END], lTimes[FileCache.VALID], sNhcFile, Integer.valueOf("nhc", 36));
			ArrayList<Obs> oHurCats = new ArrayList();
			for (Obs oObs : oFile.m_oObs) // accumulate the hurricane category observations
			{
				if (oObs.m_nObsTypeId == ObsType.TRSCAT)
					oHurCats.add(oObs);
			}
			Introsort.usort(oHurCats, Obs.g_oCompObsByTime);
			WayNetworks oWayNetworks = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
			Network oLaNetwork = oWayNetworks.getNetwork(m_sNetworkId);
			int[] nNetBb = oLaNetwork.getBoundingBox();
			if (!GeoUtil.boundingBoxesIntersect(nNetBb[0], nNetBb[1], nNetBb[2], nNetBb[3], oFile.m_nBB[0], oFile.m_nBB[1], oFile.m_nBB[2], oFile.m_nBB[3])) // the current model only is valid for Louisiana, so ignore if the forecast does not intersect the Louisiana network
			{
				m_oLogger.info(String.format("%s does not intersect the LA network in file %s", oFile.m_sStormName, oFile.m_sFilename));
				return false;
			}
			
			double[] dLandfall = getLandfall(oFile);
			if (Double.isNaN(dLandfall[0])) // if landfall cannot be calculated, the file cannot be processed
			{
				m_oLogger.info(String.format("Could not determine landfall for %s for file %s", oFile.m_sStormName, oFile.m_sFilename));
				return false;
			}
			int nRun = 0;
			Path oInfoFile = Paths.get(String.format(m_sStormInfoFf, sStormId, sNhcFile.substring(sNhcFile.lastIndexOf("/") + 1)));
			Files.createDirectories(oInfoFile.getParent(), FileUtil.DIRPERS);
			if (Files.exists(oInfoFile))
			{
				try (DataInputStream oIn = new DataInputStream(Files.newInputStream(oInfoFile)))
				{
					nRun = oIn.readByte();
				}
			}
			
			try (DataOutputStream oOut = new DataOutputStream(Files.newOutputStream(oInfoFile, FileUtil.WRITEOPTS)))
			{
				oOut.writeByte(nRun + 1);
			}
			
			long lStart = lHurWarn - 604800000;
			long lEnd = oFile.m_lStartTime + (nRun + 1) * 3600000;
			long lRef = oFile.m_lStartTime + nRun * 3600000;
			if (nRun > 0)
			{
				lStart = oFile.m_lStartTime;
				NHCStore oStore = (NHCStore)Directory.getInstance().lookup("NHCStore");
				ArrayList<NHCWrapper> oPossibleFiles = oStore.getFiles(lRef, lRef);
				Introsort.usort(oPossibleFiles, (NHCWrapper o1, NHCWrapper o2) -> Long.compare(o1.m_lValidTime, o2.m_lValidTime));
				
				for (NHCWrapper oPossibleFile : oPossibleFiles)
				{
					if (sStormId.compareTo(getStormId(oPossibleFile.m_sFilename)) == 0 && oFile.m_sFilename.compareTo(oPossibleFile.m_sFilename) != 0 && oPossibleFile.m_lValidTime < lRef)
					{
						synchronized (m_oFileQueue)
						{
							m_oFileQueue.addLast(oPossibleFile.m_sFilename);
							Scheduling.getInstance().scheduleOnce(this, 10000);
						}
						return false;
					}
				}
			}
			m_oLogger.info(String.format("Run %d for %s", nRun, sNhcFile));
			
			ObsList oSpeedData = ((ObsView)Directory.getInstance().lookup("ObsView")).getData(ObsType.SPDLNK, lStart, lEnd, nNetBb[1], nNetBb[3], nNetBb[0], nNetBb[2], lRef);
			ObsList oTemp = new ObsList();
			oTemp.ensureCapacity(oSpeedData.size());
			int nIndex = oSpeedData.size();
			int nMlpAContrib = Integer.valueOf("MLPA", 36);
			int nMlpBContrib = Integer.valueOf("MLPB", 36);
			int nMlpHContrib = Integer.valueOf("MLPH", 36);
			int nMlpOContrib = Integer.valueOf("MLPO", 36);
			while (nIndex-- > 0) // ignore any predicted speeds from the different mlp models
			{
				Obs oObs = oSpeedData.get(nIndex);
				if (oObs.m_nContribId != nMlpAContrib && oObs.m_nContribId != nMlpBContrib && oObs.m_nContribId != nMlpHContrib && oObs.m_nContribId != nMlpOContrib)
					oTemp.add(oObs);
			}
			oSpeedData = oTemp;
			Introsort.usort(oSpeedData, Obs.g_oCompObsByTime);
			ArrayList<OsmWay> oTempWays = new ArrayList();
			oWayNetworks.getWays(oTempWays, 0, nNetBb[0], nNetBb[1], nNetBb[2], nNetBb[3]);
//			oTempWays.add(oWayNetworks.getWayById(new Id("04b8507f1153d0a153a0f0bb83616341")));
//			oTempWays.add(oWayNetworks.getWayById(new Id("046b442edc790cd5b202e142f1d7aa7f")));

			
			HurWork[] oWorks = new HurWork[m_nThreads];
			int nStart = oFile.m_sFilename.indexOf("nhc_") + "nhc_".length();
			SimpleDateFormat oSdf = new SimpleDateFormat("yyyyMMddHHmm");
			oSdf.setTimeZone(Directory.m_oUTC);
			String sFilename;
			if (nRun == 0)
				sFilename = m_oDest.format(oFile.m_lValidTime, oFile.m_lStartTime, Math.max(lHurWarn + 168 * 3600000, oFile.m_lStartTime + m_nHorizon * 3600000));
			else
				sFilename = m_oDest.format(lRef, lRef, lRef + m_nHorizon * 3600000);
			Delegate oDelegate = new Delegate(oWorks, sFilename, oSpeedData, nRun); // forecast includes the 7-day oneshot values
			for (int nWorkIndex = 0; nWorkIndex < m_nThreads; nWorkIndex++)
				oWorks[nWorkIndex] = new HurWork(oFile, oHurCats, new ArrayList(), dLandfall, nWorkIndex, oDelegate);
			
			int nCount = 0;
			for (OsmWay oWay : oTempWays) // evenly distribute the ways to different threads to process
			{
				oWorks[nCount++].m_oWays.add(oWay);
				if (nCount >= oWorks.length)
					nCount = 0;
			}

			Scheduling.getInstance().execute(oDelegate); 
			return true;
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
			return false;
		}
	}
	
	
	/**
	 * Gzips and writes the bytes that represent the characters of the given StringBuilder 
	 * in UTF-8 to the given file path
	 * @param sFile File path to write output to
	 * @param sOutput Output to write
	 */
	private void write(String sFile, StringBuilder sOutput)
	{
		try
		{
			if (!sFile.endsWith(".gz"))
				sFile += ".gz";
			Path oOutput = Paths.get(sFile);
			Files.createDirectories(oOutput.getParent(), FileUtil.DIRPERS);
			try (BufferedOutputStream oOut = new BufferedOutputStream(Util.getGZIPOutputStream(Files.newOutputStream(oOutput, FileUtil.WRITEOPTS))))
			{
				oOut.write(sOutput.toString().getBytes(StandardCharsets.UTF_8));
			}

			notify("file download", oOutput.toString());
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
	
	
	/**
	 * Contains data that gets mapped to an associated roadway segment used in
	 * the MLP Hurricane models
	 */
	public class HurData
	{
		/**
		 * Predicted hurricane category when the projected center of the storm
		 * is closest to the associated roadway segment
		 */
		public int m_nLandfallCat;

		
		/**
		 * Time in milliseconds since Epoch the projected center of the storm is
		 * closest to the associated roadway segment.
		 */
		public long m_lLandfallTime;

		
		/**
		 * Distance in meters from the midpoint of the associated roadway segment 
		 * to each of the cities in the MLP Hurricane model.
		 * 
		 * [distance to Lake Charles, distance to Lafayette, distance to Baton Rouge, distance to New Orleans, distance to Alexandria, distance to Shreveport, distance to Monroe]
		 */
		public double[] m_dDistances;
	}
	
	
	/**
	 * Delegate object that handles the multi-threaded execution of the MLP 
	 * Hurricane model for all the roadway segments in the Louisiana network.
	 */
	public class Delegate implements Runnable
	{
		/**
		 * Keeps track of the number of threads that have finished processing
		 * all of its work
		 */
		int m_nCount = 0;

		
		/**
		 * The HurWork objects to process
		 */
		HurWork[] m_oWorks;

		
		/**
		 * Path of the file where the output of the model's run is written
		 */
		String m_sOutput;

		
		/**
		 * StringBuilder to contain the CSV output from the MLP Hurricane model
		 */
		final StringBuilder m_sOutputBuffer = new StringBuilder();

		
		/**
		 * ResultSet containing speed observations
		 */
		ObsList m_oSpeedData;

		
		/**
		 * Number of times this file has been processed. After the first time
		 * only the online prediction is ran
		 */
		int m_nRun;
		
		
		/**
		 * Constructs a Delegate with the given parameters
		 * @param oWorks each of these objects will be processed by a different 
		 * thread
		 * @param sOutput path for the output file
		 * @param oSpeeds ResultSet containing speed observations
		 * @param nRun run number
		 */
		Delegate(HurWork[] oWorks, String sOutput, ObsList oSpeeds, int nRun)
		{
			m_oWorks = oWorks;
			m_sOutput = sOutput;
			m_sOutputBuffer.setLength(0);
			m_sOutputBuffer.append("3600000"); // predictions are an hour apart
			m_oSpeedData = oSpeeds;
			m_nRun = nRun;
		}
		
		
		/**
		 * Calls {@link imrcp.system.Scheduling#getInstance()#execute(java.lang.Runnable)}
		 * on each {@link HurWork} in this Delegate
		 */
		@Override
		public void run()
		{
			for (HurWork oWork : m_oWorks)
				Scheduling.getInstance().execute(oWork);
		}
		
		
		/**
		 * Called after a {@link HurWork} is done processing all of its work.
		 * Checks if all other threads have finished processing. If they have 
		 * applies the predictions to downstream roadway segments that do not have
		 * a prediction, writes the outputs to disk, and schedules the MLPHurricane
		 * instance to run again in one second and an hour after the current file's
		 * start time so the online prediction can be reran for that file with 
		 * new traffic data.
		 */
		public void check()
		{
			synchronized (this)
			{
				if (++m_nCount == m_nThreads)
				{
					ArrayList<Prediction> oAllPreds = new ArrayList();
					ArrayList<Id> oHasPred = new ArrayList();
					for (HurWork oWork : m_oWorks)
					{
						for (Prediction oPred : oWork.m_oPreds)
						{
							oAllPreds.add(oPred);
							oHasPred.add(oPred.m_oId);
						}
					}
					Introsort.usort(oAllPreds);
					Introsort.usort(oHasPred);
					Prediction oPredSearch = new Prediction();
					for (HurWork oWork : m_oWorks)
					{
						for (OsmWay oWay : oWork.m_oWays)
						{
							OsmWay oCur = oWay;
							oPredSearch.m_oId = oWay.m_oId;
							int nSearch = Collections.binarySearch(oAllPreds, oPredSearch);
							if (nSearch < 0) // only do B for segments with a prediction
								continue;
							double[] dPred = oAllPreds.get(nSearch).m_dVals;
							double[] dOneshot = oAllPreds.get(nSearch).m_dOneshot;
							double dTotal = 0.0;
							double dLastDistance = dTotal;
							boolean bStop = false;
							while (dTotal < 480000 && !bStop)
							{
								dLastDistance = dTotal;
								for (OsmWay oUp : oCur.m_oNodes.get(0).m_oRefs)
								{
									String sHighway = oUp.get("highway");

									if (sHighway == null || sHighway.contains("link"))
										continue;
									int nFollowSearch = Collections.binarySearch(oHasPred, oUp.m_oId);
									if (nFollowSearch >= 0)
										continue;

									oPredSearch.m_oId = oUp.m_oId;
									nSearch = Collections.binarySearch(oAllPreds, oPredSearch);
									if (nSearch >= 0)
									{
										bStop = true;
										break;
									}
									oHasPred.add(~nFollowSearch, oUp.m_oId);
									oCur = oUp;
									dTotal += oUp.m_dLength;

									m_sOutputBuffer.append(String.format("\n%s:H", oUp.m_oId.toString()));
									for (int i = 0; i < dPred.length; i++)
										m_sOutputBuffer.append(String.format(",%4.2f",dPred[i]));
									if (dOneshot != null)
									{
										m_sOutputBuffer.append(String.format("\n%s:O", oUp.m_oId.toString()));
										for (int i = 0; i < dOneshot.length; i++)
											m_sOutputBuffer.append(String.format(",%4.2f",dOneshot[i]));
									}

									break;
								}

								if (dTotal == dLastDistance)
									bStop = true;
							}
						}
					}

					write(m_sOutput, m_sOutputBuffer);
					m_sCurrentFile = null;
					Scheduling.getInstance().scheduleOnce(MLPHurricane.this, 1000);
					Scheduling.getInstance().scheduleOnce(() -> 
					{
						if (m_nRun < 6)
						{
							synchronized (m_oFileQueue)
							{
								m_oFileQueue.addLast(m_oWorks[0].m_oFile.m_sFilename);
							}
							Scheduling.getInstance().scheduleOnce(MLPHurricane.this, 1000);
						}
					}, new Date(m_oWorks[0].m_oFile.m_lStartTime + m_nRun * 3600000));
				}
			}
		}
	}
	
	
	/**
	 * Does the actual work of running the MLP Hurricane models by using the
	 * RConnection and Rserve interfaces.
	 */
	public class HurWork implements Runnable
	{
		/**
		 * NHC hurricane forecast file being processed
		 */
		NHCWrapper m_oFile;

		
		/**
		 * Hurricane category observations for the hurricane forecast
		 */
		ArrayList<Obs> m_oHurCats;

		
		/**
		 * Roadway segments to process
		 */
		ArrayList<OsmWay> m_oWays;

		
		/**
		 * [lon of landfall, lat of landfall, sshws hurricane category]
		 */
		double[] m_dLandfall;

		
		/**
		 * Sequential Thread id
		 */
		int m_nThread;

		
		/**
		 * Stores the Predictions made by the MLP Hurricane model
		 */
		ArrayList<Prediction> m_oPreds = new ArrayList();

		
		/**
		 * The Delegate in charge of executing this HurWork
		 */
		Delegate m_oDelegate;
		
		
		/**
		 * Constructs a HurWork with the given parameters.
		 * @param oNHC NHC hurricane forecast file to process
		 * @param oHurCats Hurricane category observations for the hurricane forecast
		 * @param oWays List of OsmWays to process
		 * @param dLandfall [lon of landfall, lat of landfall, sshws hurricane category]
		 * @param nThread thread number
		 * @param oDelegate Delegate executing this HurWork
		 */
		public HurWork(NHCWrapper oNHC, ArrayList<Obs> oHurCats, ArrayList<OsmWay> oWays, double[] dLandfall, int nThread, Delegate oDelegate)
		{
			m_oFile = oNHC;
			m_oHurCats = oHurCats;
			m_oWays = oWays;
			m_dLandfall = dLandfall;
			m_nThread = nThread;
			m_oDelegate = oDelegate;
		}
		
		
		/**
		 * Creates the necessary input files and runs the 3 models (SVM, oneshot, 
		 * and online prediction), if it is first run of this file, or just the 
		 * online prediction if it isn't the first run.
		 */
		@Override
		public void run()
		{
			try
			{
				int nIndex = m_oWays.size();
				long lHurWarningTime = getHurWarningTime(m_oFile.m_sFilename);
				String sStormId = getStormId(m_oFile.m_sFilename);
				SimpleDateFormat oSdf = new SimpleDateFormat("yyyyMMddHHmm");
				oSdf.setTimeZone(TimeZone.getTimeZone("UTC"));
				int nStart = m_oFile.m_sFilename.indexOf("nhc_") + "nhc_".length();
				Path oSvmInput = Paths.get(String.format(m_sSvmInputFf, sStormId, m_oFile.m_sFilename.substring(nStart, m_oFile.m_sFilename.indexOf("_", nStart)), oSdf.format(m_oFile.m_lValidTime), m_nThread));
				Files.createDirectories(oSvmInput.getParent(), FileUtil.DIRPERS);
				SimpleDateFormat oWarnTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				oWarnTime.setTimeZone(TimeZone.getTimeZone("UTC"));
				NHCStore oStore = (NHCStore)Directory.getInstance().lookup("NHCStore");
				RConnection oConn = new RConnection();
				try
				{
					Path oMessageLog = oSvmInput.getParent().resolve("message_" + m_nThread + ".Rlog");
					Path oStdLog = oSvmInput.getParent().resolve("std_" + m_nThread + ".Rlog");
					oConn.eval(String.format("logfile=file('%s')", oMessageLog.toString()));
					oConn.eval("open(logfile, 'w')");
					oConn.eval("sink(logfile, type = 'message', append = TRUE)");
					oConn.eval(String.format("stdfile=file('%s')", oStdLog.toString()));
					oConn.eval("open(stdfile, 'w')");
					oConn.eval("sink(stdfile, type = 'output', append = TRUE)");
					oConn.eval("library('forecast')");
					oConn.eval("library('e1071')");
					oConn.eval(String.format("test_warntime='%s'", oWarnTime.format(lHurWarningTime)));
					if (m_oDelegate.m_nRun == 0) // only need to create input for SVM model once
					{
						TreeMap<OsmWay, HurData> oLandfalls = new TreeMap(OsmWay.WAYBYTEID);
						while (nIndex-- > 0)
						{
							OsmWay oWay = m_oWays.get(nIndex);
							HurData oData = new HurData();
							oData.m_dDistances = new double[m_dCities.size()];
							for (int nCity = 0; nCity < m_dCities.size(); nCity++) // calculate distance in meters from midpoint of way to each city
								oData.m_dDistances[nCity] = GeoUtil.distanceFromLatLon(GeoUtil.fromIntDeg(oWay.m_nMidLat), GeoUtil.fromIntDeg(oWay.m_nMidLon), m_dCities.get(nCity)[1], m_dCities.get(nCity)[0]) * 1000.0;
							for (HurricaneCenter oCenter : m_oFile.m_oCenters)
							{
								double dDist = GeoUtil.distanceFromLatLon(GeoUtil.fromIntDeg(oCenter.m_nLat), GeoUtil.fromIntDeg(oCenter.m_nLat), GeoUtil.fromIntDeg(oWay.m_nMidLat), GeoUtil.fromIntDeg(oWay.m_nMidLon));
								if (dDist <= oCenter.m_dDistance) // set the "landfall" parameters where the segment is first within a forecasted hurricane area
								{
									oData.m_nLandfallCat = NHCWrapper.getSSHWS(oCenter.m_nMaxSpeed);
									oData.m_lLandfallTime = oCenter.m_lTimestamp;
									break;
								}
							}
							oLandfalls.put(oWay, oData);
						}

						long lTime = lHurWarningTime;
						int nLoc;
						if (m_dLandfall[0] > -90.3)
							nLoc = 3;
						else if (m_dLandfall[0] > -92.7)
							nLoc = 2;
						else
							nLoc = 1;
						ArrayList<SVMRecord> oSVMRecords = new ArrayList();


						while (lTime < m_oFile.m_lStartTime + 604800000) // always have at least 7 days of data
						{
							NHCWrapper oValidFile = m_oFile;
							ArrayList<Obs> oHurCats = m_oHurCats;
							if (lTime < m_oFile.m_lStartTime) // for times before the current files start time, look for previous hurricane forecasts to use
							{
								ArrayList<NHCWrapper> oPossibleFiles = oStore.getFiles(lTime, m_oFile.m_lStartTime);
								for (NHCWrapper oPossibleFile : oPossibleFiles)
								{
									if (sStormId.compareTo(getStormId(oPossibleFile.m_sFilename)) == 0)
									{
										oValidFile = oPossibleFile;
										break;
									}
								}
								oHurCats = new ArrayList();
								for (Obs oObs : oValidFile.m_oObs)
								{
									if (oObs.m_nObsTypeId == ObsType.TRSCAT)
										oHurCats.add(oObs);
								}
								Introsort.usort(oHurCats, Obs.g_oCompObsByTime);
							}
							Obs oObs = null;
							for (int nCatIndex = 0; nCatIndex < oHurCats.size() - 1; nCatIndex++)
							{
								Obs oCat1 = oHurCats.get(nCatIndex);
								Obs oCat2 = oHurCats.get(nCatIndex + 1);
								if (lTime >= oCat1.m_lObsTime1 && lTime < oCat2.m_lObsTime2)
								{
									oObs = oCat1;
									break;
								}
							}
							if (oObs == null)
							{
								Obs oLast = oHurCats.get(oHurCats.size() - 1);
								if (lTime >= oLast.m_lObsTime1 && lTime < oLast.m_lObsTime2)
									oObs = oLast;
							}

							int nStatus = 0;
							int nLatHur = 0;
							int nLonHur = 0;
							int nMaxWindSpeed = 0;
							for (int nCenIndex = 0; nCenIndex < oValidFile.m_oCenters.size() - 1; nCenIndex++)
							{
								HurricaneCenter oC1 = oValidFile.m_oCenters.get(nCenIndex);
								HurricaneCenter oC2 = oValidFile.m_oCenters.get(nCenIndex + 1);
								if (lTime >= oC1.m_lTimestamp && lTime < oC2.m_lTimestamp)
								{
									nMaxWindSpeed = oC1.m_nMaxSpeed;
									nLatHur = oC1.m_nLat;
									nLonHur = oC2.m_nLon;
								}
							}
							if (oObs != null)
							{
								if (oObs.m_dValue == ObsType.lookup(ObsType.TRSCAT, "Major Hurricane") || oObs.m_dValue == ObsType.lookup(ObsType.TRSCAT, "Hurricane"))
									nStatus = 3;
								else if (oObs.m_dValue == ObsType.lookup(ObsType.TRSCAT, "Tropical Storm"))
									nStatus = 2;
								else
									nStatus = 1;

								if (nLatHur == 0)
									nLatHur = oObs.m_oGeo.get(0)[2];
								if (nLonHur == 0)
									nLonHur = oObs.m_oGeo.get(0)[1];
								if (nMaxWindSpeed == 0)
									nMaxWindSpeed = oValidFile.m_oCenters.get(oValidFile.m_oCenters.size() - 1).m_nMaxSpeed;
							}
							for (OsmWay oWay : m_oWays)
							{
								SVMRecord oRec = new SVMRecord();
								oRec.m_oWay = oWay;
								oRec.m_lStartDate = lHurWarningTime;
								oRec.m_lForecastDate = lTime;
								oRec.m_nMaxSpeed = nMaxWindSpeed;
								if (nLatHur == 0 && nLonHur == 0 && nMaxWindSpeed == 0)
									oRec.m_nMinPressure = 0;
								else
									oRec.m_nMinPressure = oValidFile.m_nMinPressure;
								oRec.m_sHurricane = oValidFile.m_sStormName;
								oRec.m_nLatHur = nLatHur;
								oRec.m_nLonHur = nLonHur;
								oRec.m_nStatusHur = nStatus;
								HurData oData = oLandfalls.get(oWay);
								oRec.m_nCategory = (int)m_dLandfall[2];
								oRec.m_dDistances = oData.m_dDistances;
								oRec.m_nLocation = nLoc;
								oRec.m_dDistToHur = GeoUtil.distanceFromLatLon(GeoUtil.fromIntDeg(oWay.m_nMidLat), GeoUtil.fromIntDeg(oWay.m_nMidLon), GeoUtil.fromIntDeg(nLatHur), GeoUtil.fromIntDeg(nLonHur)) * 1000.0;
								oSVMRecords.add(oRec);
							}
							lTime += 86400000;
						}

						Introsort.usort(oSVMRecords);

						try (BufferedWriter oOut = new BufferedWriter(Channels.newWriter(Files.newByteChannel(oSvmInput, FileUtil.WRITE), StandardCharsets.UTF_8)))
						{
							oOut.append(SVMRecord.HEADER).append('\n');
							for (SVMRecord oRec : oSVMRecords)
								oRec.write(oOut);
						}
					}

					oConn.eval(String.format("load('%s')", m_sSvmMarkov));
					oConn.eval(String.format("data_link_hur=read.csv('%s')", oSvmInput.toString()));
					oConn.eval("freeway=c('I 10', 'I 12', 'I 49', 'I 20', 'I 55', 'US 61', 'US 165', 'US 171')");
					oConn.eval("final_predict=c()");
					for (String sHighway : m_sHighways)
					{
						oConn.eval(String.format("link=data_link_hur[grepl('%s',data_link_hur$ref),]", sHighway));
						oConn.eval("predict_t=link");
						oConn.eval(String.format("predict_t$pred[predict_t$DayAftWarn < 4] = predict(models_during[['%s']],link[link$DayAftWarn<4,])", sHighway));
						oConn.eval(String.format("predict_t$pred[predict_t$DayAftWarn >= 4] = predict(models_after[['%s']],link[link$DayAftWarn>=4,])", sHighway));
						oConn.eval("final_predict = rbind(final_predict, predict_t)");
					}

					SimpleDateFormat oDay = new SimpleDateFormat("MM/dd/yy");
					oDay.setTimeZone(TimeZone.getTimeZone("UTC"));
					oConn.eval(String.format("landfall_time=as.Date('%s', format='%%m/%%d/%%Y')", oDay.format((long)m_dLandfall[3])));
					int nLength = oConn.eval("length(final_predict[,1])").asInteger();
					oConn.eval("final_predict$DaytoLF=c(1)");
					for (int i = 1; i <= nLength; i++)
						oConn.eval(String.format("final_predict[%d,]$DaytoLF=as.numeric(landfall_time-as.Date(final_predict[%d,]$onlydate,  format='%%m/%%d/%%Y'))", i, i));

					oConn.eval("svm_pred_test = final_predict");
					System.out.println(oDay.format((long)m_dLandfall[3]));
					Path oHistDataPath = Paths.get(oSvmInput.toString() + ".histdata");
					ArrayList<HurHistDataRecord> oHistData = new ArrayList();
					ArrayList<OsmWay> oWaysWithData = new ArrayList();		
					WayNetworks oWayNetworks = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
					long lTime = lHurWarningTime - 604800000;
					if (m_oDelegate.m_nRun > 0) // histdat files are already written so read the records and set the time to the star time of the file so we don't reprocess the data in the file
					{
						lTime = m_oFile.m_lStartTime;
						if (Files.exists(oHistDataPath))
						{
							try (CsvReader oIn = new CsvReader(Files.newInputStream(oHistDataPath)))
							{
								oIn.readLine(); // skip header
								while (oIn.readLine() > 0)
								{
									HurHistDataRecord oRec = new HurHistDataRecord(oIn, oWayNetworks);
									if (oRec.m_lTimestamp < m_oFile.m_lStartTime)
									{
										oHistData.add(oRec);
										int nSearch = Collections.binarySearch(oWaysWithData, oRec.m_oWay, OsmWay.WAYBYTEID);
										if (nSearch < 0)
											oWaysWithData.add(~nSearch, oRec.m_oWay);
									}
								}
							}
						}
					}

					Obs oSearch = new Obs();
					oSearch.m_lObsTime1 = -1;

					double[] dSpeeds = Arrays.newDoubleArray(12);
					double[] dMean = new double[]{Double.NaN};


					while (lTime <= m_oFile.m_lStartTime)
					{
						NHCWrapper oValidFile = m_oFile;
						ArrayList<Obs> oHurCats = m_oFile.m_oObs;
						if (lTime < m_oFile.m_lStartTime)
						{
							oValidFile = (NHCWrapper)oStore.getFile(lTime, m_oFile.m_lStartTime);
							oHurCats = new ArrayList();
							for (Obs oObs : oValidFile.m_oObs)
							{
								if (oObs.m_nObsTypeId == ObsType.TRSCAT)
									oHurCats.add(oObs);
							}
							Introsort.usort(oHurCats, Obs.g_oCompObsByTime);
						}

						ObsList oSpeedData = new ObsList();
						oSpeedData.ensureCapacity(m_oDelegate.m_oSpeedData.size() / 7);
						oSearch.m_lObsTime1 = lTime - 240001; // speed obs from LADOTD come in 4 minutes before the hour
						int nStartTimeSearch = Collections.binarySearch(m_oDelegate.m_oSpeedData, oSearch, Obs.g_oCompObsByTime);
						nStartTimeSearch = ~nStartTimeSearch;
						oSearch.m_lObsTime1 = lTime + 86400000 - 240001;
						int nEndTimeSearch = Collections.binarySearch(m_oDelegate.m_oSpeedData, oSearch, Obs.g_oCompObsByTime);
						nEndTimeSearch = ~nEndTimeSearch;
						oSpeedData.addAll(m_oDelegate.m_oSpeedData.subList(nStartTimeSearch, nEndTimeSearch));
						Introsort.usort(oSpeedData, Obs.g_oCompObsByIdTime);

						Units oUnits = Units.getInstance();
						int[] nStatus = new int[24]; // these arrays have a position for each hour of a day
						int[] nLatHur = new int[24];
						int[] nLonHur = new int[24];
						int[] nMaxWindSpeed = new int[24];
						for (int nHour = 0; nHour < 24; nHour++)
						{
							long lThisHour = lTime + nHour * 3600000;
							Obs oObs = null;
							for (int nCatIndex = 0; nCatIndex < oHurCats.size() - 1; nCatIndex++)
							{
								Obs oCat1 = oHurCats.get(nCatIndex);
								Obs oCat2 = oHurCats.get(nCatIndex + 1);
								if (lThisHour >= oCat1.m_lObsTime1 && lThisHour < oCat2.m_lObsTime2)
								{
									oObs = oCat1;
									break;
								}
							}
							if (oObs == null)
							{
								Obs oLast = oHurCats.get(oHurCats.size() - 1);
								if (lThisHour >= oLast.m_lObsTime1 && lThisHour > oLast.m_lObsTime2)
									oObs = oLast;
							}

							for (int nCenIndex = 0; nCenIndex < oValidFile.m_oCenters.size() - 1; nCenIndex++)
							{
								HurricaneCenter oC1 = oValidFile.m_oCenters.get(nCenIndex);
								HurricaneCenter oC2 = oValidFile.m_oCenters.get(nCenIndex + 1);
								if (lThisHour >= oC1.m_lTimestamp && lThisHour < oC2.m_lTimestamp)
									nMaxWindSpeed[nHour] = oC1.m_nMaxSpeed;
							}
							if (oObs != null)
							{
								if (oObs.m_dValue == ObsType.lookup(ObsType.TRSCAT, "Major Hurricane") || oObs.m_dValue == ObsType.lookup(ObsType.TRSCAT, "Hurricane"))
									nStatus[nHour] = 3;
								else if (oObs.m_dValue == ObsType.lookup(ObsType.TRSCAT, "Tropical Storm"))
									nStatus[nHour] = 2;
								else
									nStatus[nHour] = 1;
								nLatHur[nHour] = oObs.m_oGeo.get(0)[2];;
								nLonHur[nHour] = oObs.m_oGeo.get(0)[1];;
							}
						}
						for (OsmWay oWay : m_oWays)
						{
							oSearch.m_oObjId = oWay.m_oId;
							oSearch.m_lObsTime1 = lTime - 240001;
							int nSearch = Collections.binarySearch(oSpeedData, oSearch, Obs.g_oCompObsByIdTime); // the list was sorted by id and then timestamp, so should never be in the list with timestamp -1
							nSearch = ~nSearch; // but the 2's compliment should be the first instance of the correct id
							if (nSearch >= oSpeedData.size() || Id.COMPARATOR.compare(oSpeedData.get(nSearch).m_oObjId, oWay.m_oId) != 0) // that is if it is in the list at all
								continue;

							int nWaySearch = Collections.binarySearch(oWaysWithData, oWay, OsmWay.WAYBYTEID);
							if (nWaySearch < 0)
								oWaysWithData.add(~nWaySearch, oWay);

							for (int nHour = 0; nHour < 24; nHour++)
							{
								dMean[0] = Double.NaN;
								dSpeeds[0] = 1;
								long lThisHour = lTime + nHour * 3600000;
								long lNextHour = lThisHour + 3600000;

								if (nSearch < oSpeedData.size())
								{
									Obs oCur = oSpeedData.get(nSearch);
									while (oCur.m_lObsTime2 >= lThisHour && oCur.m_lObsTime1 < lNextHour && Id.COMPARATOR.compare(oCur.m_oObjId, oWay.m_oId) == 0)
									{
										dSpeeds = Arrays.add(dSpeeds, oUnits.convert(oUnits.getSourceUnits(ObsType.SPDLNK, oCur.m_nContribId), "mph", oCur.m_dValue));
										++nSearch;
										if (nSearch == oSpeedData.size())
											break;
										oCur = oSpeedData.get(nSearch);
									}
								}
								HurHistDataRecord oRec = new HurHistDataRecord();
								oRec.m_dSpeedStd = MathUtil.standardDeviation(dSpeeds, dMean);
								oRec.m_dSpeed = dMean[0];
								oRec.m_lTimestamp = lThisHour;
								oRec.m_oWay = oWay;
								oRec.m_nLatHur = nLatHur[nHour];
								oRec.m_nLonHur = nLonHur[nHour];
								oRec.m_nMaxSpeedHur = nMaxWindSpeed[nHour];
								oRec.m_nStatusHur = nStatus[nHour];
								if (nLatHur[nHour] == 0 && nLonHur[nHour] == 0 && nMaxWindSpeed[nHour] == 0)
									oRec.m_nMinPressureHur = 0;
								else
									oRec.m_nMinPressureHur = oValidFile.m_nMinPressure; 
								oHistData.add(oRec);
							}
						}

						lTime += 86400000;
					}

					Introsort.usort(oHistData);
					try (BufferedWriter oOut = new BufferedWriter(Channels.newWriter(Files.newByteChannel(oHistDataPath, FileUtil.WRITE), StandardCharsets.UTF_8)))
					{
						oOut.append(HurHistDataRecord.HEADER);
						for (HurHistDataRecord oRec : oHistData)
							oRec.write(oOut);
					}


					oConn.eval(String.format("test_histdata=read.csv('%s')", oHistDataPath.toString()));
					oConn.eval(String.format("source('%s')", m_sRScript));
					oConn.eval("pred_oneshot = list()");
					if (m_oDelegate.m_nRun == 0)
					{
						oConn.eval("svm_pred_test = final_predict");
						oConn.eval("pred_oneshot = seven_day_oneshot()");
					}
					else
					{
						for (OsmWay oWay : m_oWays)
						{
							String sId = oWay.m_oId.toString();
							Path oOneshot = Paths.get(String.format(m_sOneshotFf, sStormId, sId));
							if (Files.exists(oOneshot))
							{
								oConn.eval(String.format("pred_oneshot[['%s']]$Timestamp = timestamplist", sId));
								oConn.eval(String.format("pred_oneshot[['%s']]$Oneshot = as.numeric(read.csv('%s', header=FALSE))", sId, oOneshot.toString()));
							}
						}
					}

					oConn.eval(String.format("startt = '%s'", oWarnTime.format(m_oFile.m_lStartTime + m_oDelegate.m_nRun * 3600000)));
					oConn.eval(String.format("horizon = %d", m_nHorizon));
					StringBuilder sBuffer = new StringBuilder();
					int nOneshotIndex = Integer.MIN_VALUE;
					StringBuilder sBuf = new StringBuilder();
					for (OsmWay oWay : oWaysWithData)
					{
						try
						{
							oConn.eval(String.format("testlink='%s'", oWay.m_oId.toString()));
							oConn.eval("histdatafm = test_histdata[which(test_histdata$linkid==testlink),]");
							oConn.eval("long_ts_predfm = pred_oneshot[[testlink]]");
							if (m_oDelegate.m_nRun == 0 && nOneshotIndex == Integer.MIN_VALUE && oConn.eval("length(long_ts_predfm$Timestamp)").asInteger() == 168)
							{
								String[] sTimes = oConn.eval("as.vector(long_ts_predfm$Timestamp)").asStrings();
								SimpleDateFormat oDf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
								oDf.setTimeZone(Directory.m_oUTC);
								for (int nTsIndex = 0; nTsIndex < sTimes.length; nTsIndex++)
								{
									String sTime = sTimes[nTsIndex];
									long lTsTime = oDf.parse(sTime).getTime();
									if (lTsTime >= m_oFile.m_lStartTime)
									{
										nOneshotIndex = nTsIndex;
										break;
									}
								}
							}
							double[] dOneshot = null;
							if (m_oDelegate.m_nRun == 0 && oConn.eval("length(long_ts_predfm$Oneshot)").asInteger() == 168 && nOneshotIndex != Integer.MIN_VALUE)
							{
								dOneshot = oConn.eval("as.vector(long_ts_predfm$Oneshot)").asDoubles();
								sBuffer.append(String.format("\n%s:O", oWay.m_oId.toString()));
								for (int i = nOneshotIndex; i < dOneshot.length; i++)
									sBuffer.append(String.format(",%4.2f",dOneshot[i]));
								Path oOneshot = Paths.get(String.format(m_sOneshotFf, sStormId, oWay.m_oId.toString()));
								Files.createDirectories(oOneshot.getParent(), FileUtil.DIRPERS);
								try (BufferedWriter oOut = new BufferedWriter(Channels.newWriter(Files.newByteChannel(oOneshot, FileUtil.WRITEOPTS), StandardCharsets.UTF_8)))
								{
									sBuf.setLength(0);
									for (int i = 0; i < dOneshot.length; i++)
										sBuf.append(String.format("%4.2f,", dOneshot[i]));
									sBuf.setCharAt(sBuf.length() - 1, '\n');
									oOut.append(sBuf);
								}
							}

							if (oConn.eval("length(histdatafm[,1])").asInteger() < 10)
							{
								m_oLogger.info(String.format("Skipped %s", oWay.m_oId.toString()));
								continue;
							}
							oConn.eval("predsp<-pred(horizon,startt,histdatafm,long_ts_predfm)");
							if (oConn.eval("predsp").isNull())
								continue;
							double[] dPred = MLPBlock.evalToGetError(oConn, "predsp").asDoubles();
							sBuffer.append(String.format("\n%s:H", oWay.m_oId.toString()));
							for (int i = 0; i < dPred.length; i++)
								sBuffer.append(String.format(",%4.2f",dPred[i]));


							m_oPreds.add(new Prediction(oWay.m_oId, dPred, dOneshot));
						}
						catch(Exception oEx)
						{
							m_oLogger.error(oEx, oEx);
						}
					}

					synchronized(m_oDelegate)
					{
						m_oDelegate.m_sOutputBuffer.append(sBuffer);
						m_oDelegate.check();
					}
				}
				finally
				{
					oConn.eval("sink(NULL, type='message')");
					oConn.eval("sink(NULL)");
					oConn.eval("close(logfile)");
					oConn.eval("close(stdfile)");
				}
			}
			catch (Exception oEx)
			{
				m_oLogger.error(oEx, oEx);
			}
		}
	}
}
