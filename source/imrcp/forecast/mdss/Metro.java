package imrcp.forecast.mdss;

import imrcp.system.BaseBlock;
import imrcp.system.FilenameFormatter;
import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.Mercator;
import imrcp.geosrv.osm.OsmWay;
import imrcp.geosrv.osm.TileBucket;
import imrcp.geosrv.WayNetworks;
import imrcp.store.ProjProfile;
import imrcp.store.ProjProfiles;
import imrcp.system.CsvReader;
import imrcp.system.Directory;
import imrcp.system.FileUtil;
import imrcp.system.Introsort;
import imrcp.system.ObsType;
import imrcp.system.Scheduling;
import imrcp.system.Util;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages running the METRo model in real-time or can be configured to process 
 * specific dates on demand for all the roadway segments in the system. A multi
 * threaded approach is used along with categorizing locations that share the 
 * same characteristics and weather forecasts to optimize performance
 * @author Federal Highway Administration
 */
public class Metro extends BaseBlock
{
	/**
	 * Local thread pool 
	 */
	private ExecutorService m_oThreadPool;

	
	/**
	 * Number of hours of forecast data used as input for the METRo model.
	 */
	private int m_nForecastHours;

	
	/**
	 * Number of hours of observed data used as input for the METRo model
	 */
	private int m_nObsHours;

	
	/**
	 * Object used to create time dependent file names to save on disk
	 */
	private FilenameFormatter m_oFileFormat;

	
	/**
	 * Schedule offset from midnight in seconds
	 */
	private int m_nOffset;

	
	/**
	 * Period of execution in seconds
	 */
	private int m_nPeriod;
	
	
	/**
	 * Number of runs that can be processed per execution
	 */
	private int m_nRunsPerPeriod;
	
	
	/**
	 * Maximum number of times that can be queued at once
	 */
	private int m_nMaxQueue;
	
	
	/**
	 * Queue of times to process
	 */
	private final ArrayDeque<Long> m_oRunTimes = new ArrayDeque();
	
	
	/**
	 * File used to store timestamps that are queued
	 */
	private String m_sQueueFile;

	
	/**
	 * Flag indicating if this instance is processing real time data. If it is
	 * false times have to be queue to run on demand
	 */
	private boolean m_bRealTime;
	
	
	/**
	 * Contains the map tiles that contain the roadway segments used as input
	 * for the METRo model
	 */
	private final ArrayList<MetroTile> m_oTiles = new ArrayList();
	
	
	/**
	 * Map zoom level used for {@link MetroTile}s. Must be the same zoom used
	 * by {@link imrcp.geosrv.WayNetworks#m_oBucketCache}
	 */
	private int m_nZoom;
	
	
	/**
	 * Schedule id used for the process to update the tiles
	 */
	private int m_nTileUpdateSched = Integer.MIN_VALUE;
	
	
	/**
	 * Object that updates {@link Metro#m_oTiles} on a fixed schedule.
	 */
	private TileUpdate m_oTileUpdate;
	
	
	/**
	 * Counts the total number of segments processed each period of execution
	 */
	private AtomicInteger m_nTotal = new AtomicInteger();

	
	/**
	 * Counts the number of times the METRo model is ran each period of execution
	 */
	private AtomicInteger m_nRuns = new AtomicInteger();

	
	/**
	 * Counts the number of times the METRo model fails to execute each period 
	 * of execution
	 */
	private AtomicInteger m_nFailed = new AtomicInteger();

	
	/**
	 * Contains the data files created each period of execution
	 */
	private final ArrayList<String> m_oFileNames = new ArrayList();

	
	/**
	 * Compares int[]s by comparing values in corresponding positions. Used for 
	 * comparing {@link MetroLocation#m_nCategories}
	 */
	private Comparator<int[]> CATCOMP = (int[] o1, int[] o2) ->
	{
		for (int nIndex = 0; nIndex < o1.length; nIndex++)
		{
			int nComp = o1[nIndex] - o2[nIndex];
			if (nComp != 0)
				return nComp;
		}
		
		return 0;
	};

	
	/**
	 * Compares {@link MetroLocation}s by {@link MetroLocation#m_nCategories}. 
	 * Wrapper for {@link Metro#CATCOMP}
	 */
	private Comparator<MetroLocation> LOCBYARRAY = (MetroLocation o1, MetroLocation o2) -> CATCOMP.compare(o1.m_nCategories, o2.m_nCategories);

	
	/**
	 * Compares {@link MetroLocation}s by {@link MetroLocation#m_nCategory}.
	 */
	private Comparator<MetroLocation> LOCBYCAT = (MetroLocation o1, MetroLocation o2) -> o1.m_nCategory - o2.m_nCategory;

	
	/**
	 * Contains the contributor ids of the forecast files used by Metro.
	 */
	private int[] m_nContribs;

	
	/**
	 * Instance name of the {@link imrcp.geosrv.WayNetworks} block that serves
	 * the {@link imrcp.geosrv.osm.OsmWay}s and {@link imrcp.geosrv.osm.TileBucket}
	 * to process
	 */
	private String m_sWaysBlock;

	
	/**
	 * Format string used to generate file names for log files. Only used to debug
	 */
	private String m_sLogFf;

	
	/**
	 * Name of the file that if it exists during a period of execution triggers 
	 * log files to be created
	 */
	private String m_sLogTrigger;

	
	@Override
	public void reset()
	{
		m_oThreadPool = Executors.newFixedThreadPool(m_oConfig.getInt("threads", 5));
		m_nForecastHours = m_oConfig.getInt("fcsthrs", 6);
		m_nObsHours = m_oConfig.getInt("obshrs", 6);
		m_oFileFormat = new FilenameFormatter(m_oConfig.getString("format", ""));
		m_nOffset = m_oConfig.getInt("offset", 3300);
		m_nPeriod = m_oConfig.getInt("period", 3600);
		m_nRunsPerPeriod = m_oConfig.getInt("runs", 4);
		m_nMaxQueue = m_oConfig.getInt("maxqueue", 504);
		m_sQueueFile = m_oConfig.getString("queuefile", "/dev/shm/imrcp-prod/metroqueue.txt");
		m_bRealTime = Boolean.parseBoolean(m_oConfig.getString("realtime", "True"));
		m_nZoom = m_oConfig.getInt("zoom", 12);
		String[] sContribs = m_oConfig.getStringArray("contribs", "");
		m_nContribs = new int[sContribs.length];
		for (int nIndex = 0; nIndex < sContribs.length; nIndex++)
			m_nContribs[nIndex] = Integer.valueOf(sContribs[nIndex], 36);
		m_sWaysBlock = m_oConfig.getString("way", "WayNetworks");
		MetroFileset.setStores(m_oConfig.getString("metrostore", "MetroStore"),
			m_oConfig.getString("tpvtstore", "TPVTDAStore"), 
			m_oConfig.getString("tssrfstore", "TSSRFDAStore"), 
			m_oConfig.getString("rtmastore", "RTMAStore"), 
			m_oConfig.getString("rapstore", "RAPStore"),
			m_oConfig.getString("mrmsstore", "RadarPrecipStore"),
			m_oConfig.getString("ndfdtempstore", "NDFDTempStore"), 
			m_oConfig.getString("ndfdtdstore", "NDFDTdStore"), 
			m_oConfig.getString("ndfdwspdstore", "NDFDWspdStore"), 
			m_oConfig.getString("ndfdskystore", "NDFDSkyStore"));
		m_sLogFf = m_oConfig.getString("logff", "");
		m_sLogTrigger = m_oConfig.getString("logtrigger", "");
	}

	
	/**
	 * Queues any times found in the queue file. Then updates the tiles that will
	 * be processed each period of execution. Then sets a schedule to execute 
	 * this instance and {@link Metro#m_oTileUpdate} on a fixed interval.
	 * @return true if no exceptions are thrown
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		if (m_nForecastHours < 2)
		{
			m_oLogger.error("Must have at least 2 forecast hours");
			return false;
		}

		File oQueue = new File(m_sQueueFile);
		if (!oQueue.exists())
			oQueue.createNewFile();
		try (CsvReader oIn = new CsvReader(new FileInputStream(m_sQueueFile)))
		{
			while (oIn.readLine() > 0)
				m_oRunTimes.addLast(oIn.parseLong(0));
		}
		m_oTileUpdate = new TileUpdate();
		m_oTileUpdate.run();
		long lNextDay = System.currentTimeMillis();
		lNextDay = (lNextDay / 86400000 * 86400000) + 86400000; // floor to the current day and add one day

		m_nTileUpdateSched = Scheduling.getInstance().createSched(m_oTileUpdate, new Date(lNextDay), 86400000);
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}

	
	/**
	 * Wrapper for {@link java.util.concurrent.ExecutorService#shutdown()} and
	 * cancels the schedule for {@link Metro#m_oTileUpdate}
	 * @return true
	 */
	@Override
	public boolean stop()
	{
		m_oThreadPool.shutdownNow();
		Scheduling.getInstance().cancelSched(m_oTileUpdate, m_nTileUpdateSched);
		return true;
	}

	
	/**
	 * Called when a message from {@link imrcp.system.Directory} is received. If
	 * the message is "new ways", the tiles that will be processed each period 
	 * of execution is updated
	 * @param sNotification
	 */
	@Override
	public void process(String[] sNotification)
	{
		if (sNotification[MESSAGE].compareTo("new ways") == 0)
		{
			m_oTileUpdate.run();
		}
	}
	
	
	/**
	 * If this instance is configured to reprocess files, it parses the given
	 * start and end times and queues times within that range to be reprocessed.
	 * The given StringBuilder gets filled with basic html that contains the
	 * files that were queued.
	 * @param sStart start timestamp in the format yyyy-MM-ddTHH:mm 
	 * @param sEnd end timestamp in the format yyyy-MM-ddTHH:mm
	 * @param sBuffer Buffer that gets fills with html containing the queued files
	 */
	public void queue(String sStart, String sEnd, StringBuilder sBuffer)
	{
		try
		{
			SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
			oSdf.setTimeZone(Directory.m_oUTC);
			int nPeriodInMillis = 600 * 1000; // METRo is usually ran every 10 minutes
			long lStartTime = oSdf.parse(sStart).getTime();
			lStartTime = (lStartTime / nPeriodInMillis) * nPeriodInMillis;
			long lEndTime = oSdf.parse(sEnd).getTime();
			lEndTime = (lEndTime / nPeriodInMillis) * nPeriodInMillis;
			int nCount = 0;
			while (lStartTime <= lEndTime && nCount++ < m_nMaxQueue)
			{
				synchronized (m_oRunTimes)
				{
					m_oRunTimes.addLast(lStartTime);
				}
				lStartTime += nPeriodInMillis;
			}
			synchronized (m_oRunTimes)
			{
				try (BufferedWriter oOut = new BufferedWriter(new FileWriter(m_sQueueFile)))
				{
					Iterator<Long> oIt = m_oRunTimes.iterator();
					while (oIt.hasNext())
					{
						Long lTime = oIt.next();
						oOut.write(lTime.toString());
						oOut.write("\n");
						sBuffer.append(lTime.toString()).append("<br></br>");
					}
				}
			}
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
	
	
	/**
	 * Adds the current times in the queue to the StringBuilder in basic html
	 * @param sBuffer StringBuilder to fill with current files queued
	 */
	public void queueStatus(StringBuilder sBuffer)
	{
		synchronized (m_oRunTimes)
		{
			sBuffer.append(m_oRunTimes.size()).append(" times in queue");
			Iterator<Long> oIt = m_oRunTimes.iterator();
			while (oIt.hasNext())
			{
				Long lTime = oIt.next();
				sBuffer.append("<br></br>").append(lTime.toString());
			}
		}
	}
	
	
	/**
	 * If {@link Metro#m_bRealTime} is true, adds the current time to the front
	 * of the queue. Then processes up to {@link Metro#m_nRunsPerPeriod} times
	 * in the queue.
	 */
	@Override
	public void execute()
	{
		long lNow = System.currentTimeMillis();
		lNow = (lNow / 600000) * 600000;
		int nTimesToRun;
		synchronized (m_oRunTimes)
		{
			if (m_bRealTime)
				m_oRunTimes.addFirst(lNow);
			nTimesToRun = Math.min(m_nRunsPerPeriod, m_oRunTimes.size());
		}
		
		for (int i = 0; i < nTimesToRun; i++)
		{
			runMetro(m_oRunTimes.removeFirst());
		}
		
		synchronized (m_oRunTimes)
		{
			try (BufferedWriter oOut = new BufferedWriter(new FileWriter(m_sQueueFile))) // write the times since in the queue to disk
			{
				Iterator<Long> oIt = m_oRunTimes.iterator();
				while (oIt.hasNext())
				{
					oOut.write(oIt.next().toString());
					oOut.write("\n");
				}
			}
			catch (Exception oEx)
			{
				m_oLogger.error(oEx, oEx);
			}
			if (!m_oRunTimes.isEmpty()) // if there are still times in the queue, execute this instance again
				Scheduling.getInstance().scheduleOnce(this, 10);
		}
	}

	
	/**
	 * Runs the METRo model for the each tile in {@link Metro#m_oTiles} by 
	 * aggregating all of the input files (except previous METRo runs,
	 * which is done later by each thread) needed and then creating a 
	 * {@link java.util.concurrent.Callable} object for each {@link MetroTile} 
	 * and calling {@link java.util.concurrent.ExecutorService#invokeAll(java.util.Collection)}
	 * on {@link Metro#m_oThreadPool}.
	 * @param lRunTime Time in milliseconds since Epoch of the METRo run. The first
	 * forecast generated by the run is this time.
	 */
	public void runMetro(long lRunTime)
	{
		try
		{
			if (!DoMetroWrapper.g_bLibraryLoaded) // don't run if the shared library isn't loaded
				return;
			SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			synchronized (m_oTiles)
			{
				if (!m_oTiles.isEmpty())
				{
					m_nTotal.set(0);
					m_nRuns.set(0);
					m_nFailed.set(0);
					m_oFileNames.clear();
					ArrayList<MetroTile> oTilesToRun = new ArrayList();
					for (MetroTile oTile : m_oTiles)
						oTilesToRun.add(oTile);

					m_oLogger.info("Running METRo for " + oSdf.format(lRunTime) + " for " + oTilesToRun.size() + " tiles");
					if (!oTilesToRun.isEmpty())
					{
						MetroFileset oInit = new MetroFileset(m_oTiles.get(0).m_nX, m_oTiles.get(0).m_nY, m_nObsHours, m_nForecastHours); // load files needed into memory
						oInit.fillFiles(lRunTime, m_nObsHours, m_nForecastHours);
						String sCheck = oInit.checkFiles();
						if (sCheck == null) // if there are no errors
						{
							ArrayList<Callable<Object>> oWork = new ArrayList();
							for (MetroTile oTile : oTilesToRun) // create the callables
							{	
								MetroFileset oFiles = new MetroFileset(oTile.m_nX, oTile.m_nY, oInit);
								oTile.m_oFiles = oFiles;
								oTile.m_oResult.clear();
								oTile.m_lRunTime = lRunTime;
								oWork.add(Executors.callable(oTile));
							}
							m_oThreadPool.invokeAll(oWork); // and run them all
						}
						else
						{
							m_oLogger.error(sCheck);
						}
					}
				}
			}
			if (m_bRealTime) // only send notifications if this instance processing real time data
			{
				String[] sFiles = new String[m_oFileNames.size()];
				for (int nIndex = 0; nIndex < sFiles.length; nIndex++)
					sFiles[nIndex] = m_oFileNames.get(nIndex);
				notify("file download", sFiles);
			}
			m_oLogger.info(String.format("Finished METRo for %s. %d runs out of %d total. %d failed.", oSdf.format(lRunTime), m_nRuns.get(), m_nTotal.get(), m_nFailed.get()));
			
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}

	
	/**
	 * Writes the given {@link imrcp.forecast.mdss.RoadcastData}s to the given
	 * file name.
	 * @param sFilename file name to write to
	 * @param oRoadcasts List of RoadcastData to write
	 */
	private void writeFile(String sFilename, ArrayList<RoadcastData> oRoadcasts)
	{
		new File(sFilename.substring(0, sFilename.lastIndexOf("/") + 1)).mkdirs();
		if (oRoadcasts.isEmpty()) // if there no no roadcasts, do not create the file
		{
			m_oLogger.info("No roadcast data from Metro to write to file.");
			return;
		}
		Collections.sort(oRoadcasts);

		try
		{
			ByteArrayOutputStream oBytes = new ByteArrayOutputStream();
			try (DataOutputStream oOut = new DataOutputStream(new BufferedOutputStream(Util.getGZIPOutputStream(oBytes))))
			{
				oOut.writeByte(2); // version
				int nObsPerType = (m_nForecastHours - 3) * 3 + 30; // first hour has a forecast every 2 minutes, after that each hour has a forecast every 20 minutes
				oOut.writeInt(nObsPerType);
				long[] lStarts = oRoadcasts.get(0).m_lStartTimes;
				long[] lEnds = oRoadcasts.get(0).m_lEndTimes;
				for (int nIndex = 0; nIndex < nObsPerType; nIndex++) // all the timestamps are the same so write them once
				{
					oOut.writeLong(lStarts[nIndex]);
					oOut.writeLong(lEnds[nIndex]);
				}
				oOut.writeInt(oRoadcasts.size()); // write number of roadcasts
				for (RoadcastData oRd : oRoadcasts)
				{
					oOut.writeInt(oRd.m_nLon);
					oOut.writeInt(oRd.m_nLat);
					oOut.writeFloat(oRd.m_fRainRes);
					oOut.writeFloat(oRd.m_fSnowRes);
					
					oOut.writeInt(ObsType.TPVT);
					for (int nIndex = 0; nIndex < nObsPerType; nIndex++)
						oOut.writeShort(Util.toHpfp(oRd.m_fTpvt[nIndex]));
					
					oOut.writeInt(ObsType.TSSRF);
					for (int nIndex = 0; nIndex < nObsPerType; nIndex++)
						oOut.writeShort(Util.toHpfp(oRd.m_fTssrf[nIndex]));
					
					oOut.writeInt(ObsType.STPVT);
					for (int nIndex = 0; nIndex < nObsPerType; nIndex++)
						oOut.writeShort(Util.toHpfp(oRd.m_nStpvt[nIndex]));
					
					oOut.writeInt(ObsType.DPHLIQ);
					for (int nIndex = 0; nIndex < nObsPerType; nIndex++)
						oOut.writeShort(Util.toHpfp(oRd.m_fDphliq[nIndex]));
					
					oOut.writeInt(ObsType.DPHSN);
					for (int nIndex = 0; nIndex < nObsPerType; nIndex++)
						oOut.writeShort(Util.toHpfp(oRd.m_fDphsn[nIndex]));
					
					oOut.flush();
				}
					
			}

			try (BufferedOutputStream oFileOut = new BufferedOutputStream(Files.newOutputStream(Paths.get(sFilename))))
			{
				oBytes.writeTo(oFileOut);
			}
			
			synchronized (m_oFileNames)
			{
				m_oFileNames.add(sFilename);
			}				
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}

	
	/**
	 * Encapsulates the necessary metadata used for running the METRo model
	 * at a specific location
	 */
	private class MetroLocation
	{
		/**
		 * Longitude of location in decimal degrees scaled to 7 decimal places
		 */
		private final int m_nLon;

		
		/**
		 * Latitude of location in decimal degrees scaled to 7 decimal places
		 */
		private final int m_nLat;

		
		/**
		 * Flag indicating if the location represent a roadway or a bridge deck
		 */
		private final boolean m_bBridge;

		
		/**
		 * Treatment type. 0 = no treatment, 1 = chemically treated
		 */
		private final int m_nTmtType;

		
		/**
		 * Number representing the category of this location, which is an index
		 * used in a list of outputs. The category is determined by 
		 * {@link MetroLocation#m_bBridge} and the indices of the location inside
		 * of the different data files.
		 */
		private int m_nCategory;

		
		/**
		 * Stores information to determine {@link MetroLocation#m_nCategory}.
		 * Format is [bridge flag, x index for file 1, y index for file 1, x index 
		 * for file 2, y index for file 2, ..., x index for file n, y index for 
		 * file n]
		 */
		private int[] m_nCategories;

		
		/**
		 * Constructs a MetroLocation with the given parameters
		 * @param nLon longitude of location in decimal degrees scaled to 7 decimal places
		 * @param nLat latitude of location in decimal degrees scaled to 7 decimal places
		 * @param bBridge true if bridge, otherwise false
		 * @param nTmtType 0 = no treatment, 1 = chemically treated
		 */
		MetroLocation(int nLon, int nLat, boolean bBridge, int nTmtType)
		{
			m_nLon = nLon;
			m_nLat = nLat;
			m_bBridge = bBridge;
			m_nTmtType = nTmtType;
		}
	}
	
	
	/**
	 * Delegate class that updates {@link Metro#m_oTiles} at system startup, 
	 * when a "new ways" notification is received from another base block, and
	 * on a regular system.
	 */
	private class TileUpdate implements Runnable
	{
		@Override
		public void run()
		{
			WayNetworks oWays = (WayNetworks)Directory.getInstance().lookup(m_sWaysBlock);
			ArrayList<MetroTile> oTiles = new ArrayList();
			ArrayList<TileBucket> oBuckets = new ArrayList();
			oWays.getTiles(oBuckets);
			Mercator oMerc = new Mercator();
			double[] dBounds = new double[4];
			int[] nTile = new int[2];
			MetroTile oSearch = new MetroTile();
			for (TileBucket oBucket : oBuckets) // buckets represent a single map tile
			{
				oSearch.m_nX = oBucket.m_nX;
				oSearch.m_nY = oBucket.m_nY;
				int nIndex = Collections.binarySearch(oTiles, oSearch);
				if (nIndex < 0)
				{
					nIndex = ~nIndex;
					oTiles.add(nIndex, new MetroTile(oBucket.m_nX, oBucket.m_nY));
				}
				MetroTile oMTile = oTiles.get(nIndex);
				oMerc.lonLatBounds(oBucket.m_nX, oBucket.m_nY, m_nZoom, dBounds);
				for (OsmWay oWay : oBucket)
				{
					if (GeoUtil.isInside(GeoUtil.fromIntDeg(oWay.m_nMidLon), GeoUtil.fromIntDeg(oWay.m_nMidLat), dBounds[3], dBounds[2], dBounds[1], dBounds[0], 0))
						oMTile.m_oLocations.add(new MetroLocation(oWay.m_nMidLon, oWay.m_nMidLat, oWay.m_bBridge, 0));
				}
			}
			
			int nIndex = oTiles.size();
			while (nIndex-- > 0) // remove empty tiles
			{
				MetroTile oTile = oTiles.get(nIndex);
				if (oTile.m_oLocations.isEmpty())
					oTiles.remove(nIndex);
			}
			
			m_oLogger.info(String.format("Tiles with segments added: %d", oTiles.size()));
			ArrayList<int[]> oHist = new ArrayList();
			int[] nSearch = new int[m_nContribs.length * 2 + 1];
			int[] nIndices = new int[2];
			
			ProjProfiles oProfiles = ProjProfiles.getInstance();
			for (MetroTile oTile : oTiles)
			{
				ArrayList<MetroLocation> oLocsByCategories = new ArrayList();
				for (MetroLocation oLoc : oTile.m_oLocations)
				{
					// determine the category for each location in the tile
					int nCatIndex = 0;
					nSearch[nCatIndex++] = oLoc.m_bBridge ? 1 : 0;
					for (int nContrib : m_nContribs)
					{
						ProjProfile oProj = oProfiles.getProfile(nContrib);
						if (oProj == null)
						{
							m_oLogger.error("Proj Projile not set. Trying again in 2 minutes");
							Scheduling.getInstance().scheduleOnce(m_oTileUpdate, 60 * 1000 * 2);
							return;
						}
						oProj.getPointIndices(GeoUtil.fromIntDeg(oLoc.m_nLon), GeoUtil.fromIntDeg(oLoc.m_nLat), nIndices);
						nSearch[nCatIndex++] = nIndices[0];
						nSearch[nCatIndex++] = nIndices[1];
					}
					oLoc.m_nCategories = nSearch;
					int nSearchIndex = Collections.binarySearch(oLocsByCategories, oLoc, LOCBYARRAY);
					if (nSearchIndex < 0) // add new categories to the list
					{
						int[] nTemp = new int[nSearch.length];
						System.arraycopy(nSearch, 0, nTemp, 0, nTemp.length);
						oLoc.m_nCategories = nTemp;
						oLocsByCategories.add(~nSearchIndex, oLoc);
					}
					else // if the category is already in the list, set the current location's category to it
					{
						oLoc.m_nCategories = oLocsByCategories.get(nSearchIndex).m_nCategories;
					}
				}
				for (MetroLocation oLoc : oTile.m_oLocations)
				{
					oLoc.m_nCategory = Collections.binarySearch(oLocsByCategories, oLoc, LOCBYARRAY);
				}
				for (MetroLocation oLoc : oTile.m_oLocations) // don't need the categories array any more so save memory by setting to null
					oLoc.m_nCategories = null;
				
				Introsort.usort(oTile.m_oLocations, LOCBYCAT);
			}

			synchronized (m_oTiles)
			{
				m_oTiles.clear();
				m_oTiles.addAll(oTiles);
			}
		}
	}
	
	
	/**
	 * Represents a map tile with locations inside of it that will be inputs
	 * to the METRo model
	 */
	class MetroTile implements Comparable<MetroTile>, Runnable
	{
		/**
		 * Locations inside of the tile
		 */
		ArrayList<MetroLocation> m_oLocations;

		
		/**
		 * Current Metro run time 
		 */
		long m_lRunTime;

		
		/**
		 * tile's x index
		 */
		int m_nX;

		
		/**
		 * tile's y index
		 */
		int m_nY;

		
		/**
		 * List that contains the outputs of METRo
		 */
		ArrayList<RoadcastData> m_oResult = new ArrayList();

		
		/**
		 * Object containing the data files needed for input to METRo
		 */
		MetroFileset m_oFiles;

		
		/**
		 * Default constructor, which set {@link MetroTile#m_oLocations} to an
		 * empty {@link java.util.ArrayList}
		 */
		MetroTile()
		{	
			m_oLocations = new ArrayList();
		}

		
		/**
		 * Calls {@link MetroTile#MetroTile()} and sets the x and y index to the
		 * given values.
		 * @param nX tile's x index
		 * @param nY tile's y index
		 */
		MetroTile(int nX, int nY)
		{
			this();
			m_nX = nX;
			m_nY = nY;
		}

		
		/**
		 * Compares MetroTiles by x index, then y index.
		 * @param o the object to be compared.
		 * @return a negative integer, zero, or a positive integer as this 
		 * object is less than, equal to, or greater than the specified object.
		 * 
		 * @see java.lang.Comparable#compareTo(java.lang.Object)
		 */
		@Override
		public int compareTo(MetroTile o)
		{
			int nRet = m_nX - o.m_nX;
			if (nRet == 0)
				nRet = m_nY - o.m_nY;

			return nRet;
		}

		
		/**
		 * For each location in the tile, if a location with the same category
		 * has not been processed, a {@link imrcp.forecast.mdss.DoMetroWrapper} 
		 * is created and used to call the JNI methods to run the METRo model 
		 * via C and Fortran code, otherwise the location's output is set to
		 * the output already computed for that category.
		 */
		@Override
		public void run()
		{
			try
			{
				m_oResult.clear();
				RoadcastData[] oRoadcasts = new RoadcastData[m_oLocations.get(m_oLocations.size() - 1).m_nCategory + 1];
				StringBuilder[] oLogs = new StringBuilder[oRoadcasts.length];
				double[] dPoint = new double[2];
				new Mercator().lonLatMdpt(m_nX, m_nY, m_nZoom, dPoint);
				m_oFiles.fillMetroFiles(m_lRunTime, m_nObsHours, GeoUtil.toIntDeg(dPoint[0]), GeoUtil.toIntDeg(dPoint[1]));
				SimpleDateFormat oSdf = new SimpleDateFormat("yyyyMMdd");
				oSdf.setTimeZone(Directory.m_oUTC);
				WayNetworks oWays = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
				FilenameFormatter oFf = null;
				long lFileTime = (m_lRunTime / 86400000) * 86400000;
				if (m_sLogTrigger != null && !m_sLogTrigger.isEmpty() && Files.exists(Paths.get(m_sLogTrigger)) &&  m_sLogFf != null && !m_sLogFf.isEmpty())
				{
					oFf = new FilenameFormatter(m_sLogFf);
					Files.createDirectories(Paths.get(oFf.format(lFileTime, 0, 0, "")).getParent(), FileUtil.DIRPERS);
				}
				for (MetroLocation oLoc : m_oLocations)
				{
					m_nTotal.incrementAndGet();
					if (oRoadcasts[oLoc.m_nCategory] == null)
					{
						DoMetroWrapper oDmw = new DoMetroWrapper(m_nObsHours, m_nForecastHours);
						boolean bFill = oDmw.fillArrays(oLoc.m_nLon, oLoc.m_nLat, oLoc.m_bBridge, oLoc.m_nTmtType, m_lRunTime, m_oFiles);
						if (bFill)
						{
							oDmw.run();
							oDmw.saveRoadcast(oLoc.m_nLon, oLoc.m_nLat, m_lRunTime);
							m_oResult.add(oDmw.m_oOutput);
							m_nRuns.incrementAndGet();
						}
						else
							m_nFailed.incrementAndGet();
						oRoadcasts[oLoc.m_nCategory] = oDmw.m_oOutput;
						oLogs[oLoc.m_nCategory] = oDmw.log(m_lRunTime);
					}
					else
					{
						m_oResult.add(new RoadcastData(oRoadcasts[oLoc.m_nCategory], oLoc.m_nLon, oLoc.m_nLat));
					}
					if (oFf != null)
					{
						
						OsmWay oWay = oWays.getWay(100, oLoc.m_nLon, oLoc.m_nLat);
						if (oWay == null)
							continue;
						Path oPath = Paths.get(oFf.format(lFileTime, 0, 0, oWay.m_oId.toString()));
						if (oWay.m_oId.toString().compareTo("04ed2c91f441bc31a3e5ebead1543759") == 0)
						{
							try (BufferedWriter oOut = new BufferedWriter(Channels.newWriter(Files.newByteChannel(oPath, FileUtil.APPENDOPTS), "UTF-8")))
							{
								oOut.append(oLogs[oLoc.m_nCategory]);
							}
						}
						
					}
				}
				writeFile(m_oFileFormat.format(m_lRunTime, m_lRunTime, m_lRunTime + (m_nForecastHours - 2) * 3600000, Integer.toString(m_nX), Integer.toString(m_nY), Integer.toString(m_nX), Integer.toString(m_nY)), m_oResult);
			}
			catch (Exception oEx)
			{
				m_oLogger.error(oEx, oEx);
			}
		}
	}
}
