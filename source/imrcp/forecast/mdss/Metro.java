package imrcp.forecast.mdss;

import imrcp.BaseBlock;
import imrcp.FilenameFormatter;
import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.system.CsvReader;
import imrcp.system.Directory;
import imrcp.system.Scheduling;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import ucar.ma2.ArrayDouble;
import ucar.ma2.ArrayFloat;
import ucar.ma2.ArrayInt;
import ucar.ma2.DataType;
import ucar.nc2.Attribute;
import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFileWriter;
import ucar.nc2.Variable;

/**
 * This class handles the execution of the METRo model and saving its outputs.
 */
public class Metro extends BaseBlock
{

	/**
	 * Array that contains groups of 4 integers that represent bounding boxes of
	 * different parts of the study area
	 */
	private int[] m_nStudyArea;

	/**
	 * Thread pool used to run multiple instances of the METRo process in
	 * parallel
	 */
	private ExecutorService m_oThreadPool;

	/**
	 * The number of output values for each variable
	 */
	private int m_nOutputs;

	/**
	 * The number of forecast hours provided to METRo
	 */
	private int m_nForecastHours;

	/**
	 * The number of observation hours provided to METRo
	 */
	private int m_nObsHours;

	/**
	 * Reusable timestamp to mark the start time for a set of METRo processes
	 */
	private long m_lStartTime;

	/**
	 * Formatting object used to generate time dynamic file names
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
	 * List of Callables that the thread pool invokes each time the block is
	 * executed
	 */
	ArrayList<Callable<Object>> m_oToDo;
	
	private int m_nRunsPerPeriod;
	
	private int m_nMaxQueue;
	
	private final ArrayDeque<Long> m_oRunTimes = new ArrayDeque();
	
	private String m_sQueueFile;

	private boolean m_bRealTime;
	
	private static final Comparator<double[]> DOUBLECOMP = (double[] o1, double[] o2) -> 
	{
		int nReturn = o1.length - o2.length;
		if (nReturn != 0)
			return nReturn;
		
		for (int i = 0; i < o1.length; i++)
		{
			nReturn = Double.compare(o1[i], o2[i]);
			if (nReturn != 0)
				return nReturn;
		}
		return nReturn;
	};
	
//	private final TreeMap<double[], RoadcastData> m_oProfiles = new TreeMap(DOUBLECOMP);
	private final ArrayList<RoadcastData> m_oRoadcasts = new ArrayList();

	/**
	 * Resets all configurable variables
	 */
	@Override
	public void reset()
	{
		m_oThreadPool = Executors.newFixedThreadPool(m_oConfig.getInt("threads", 5));
		m_nStudyArea = m_oConfig.getIntArray("box", 0);
		m_nForecastHours = m_oConfig.getInt("fcsthrs", 6);
		m_nObsHours = m_oConfig.getInt("obshrs", 6);
		m_oFileFormat = new FilenameFormatter(m_oConfig.getString("format", ""));
		m_nOffset = m_oConfig.getInt("offset", 3300);
		m_nPeriod = m_oConfig.getInt("period", 3600);
		m_nOutputs = 30 + ((m_nForecastHours - 3) * 3); // saving every 2 minutes for the first 60 minutes and then 20 minute intervals after that. so the first hour has 30 outputs and then each hour after has 3.
		m_nRunsPerPeriod = m_oConfig.getInt("runs", 4);
		m_nMaxQueue = m_oConfig.getInt("maxqueue", 504); // default is a week worth of metro runs
		m_sQueueFile = m_oConfig.getString("queuefile", "/dev/shm/imrcp-prod/metroqueue.txt");
		m_bRealTime = Boolean.parseBoolean(m_oConfig.getString("realtime", "True"));
	}


	/**
	 * Creates a Callable MetroDelegate for each segment in the study area and
	 * schedules this block to run on a regular time interval.
	 *
	 * @return false if there are not enough forecast hours to feed into METRo,
	 * otherwises true.
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
		ArrayList<Segment> oSegments = new ArrayList();
		for (int i = 0; i < m_nStudyArea.length; i += 4) // iterate through the different bounding boxes of the study area
			((SegmentShps)Directory.getInstance().lookup("SegmentShps")).getLinks(oSegments, 0, m_nStudyArea[i], m_nStudyArea[i + 1], m_nStudyArea[i + 2], m_nStudyArea[i + 3]);
		m_oToDo = new ArrayList(oSegments.size());
		for (Segment oSegment : oSegments)
			m_oToDo.add(Executors.callable(new MetroDelegate(oSegment)));
		File oQueue = new File(m_sQueueFile);
		if (!oQueue.exists())
			oQueue.createNewFile();
		try (CsvReader oIn = new CsvReader(new FileInputStream(m_sQueueFile)))
		{
			while (oIn.readLine() > 0)
				m_oRunTimes.addLast(oIn.parseLong(0));
		}
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}


	/**
	 * Calls shutdownNow() on the thread pool
	 *
	 * @return always true
	 */
	@Override
	public boolean stop()
	{
		m_oThreadPool.shutdownNow();
		return true;
	}


	public void queue(String sStart, String sEnd, StringBuilder sBuffer)
	{
		try
		{
			SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
			oSdf.setTimeZone(Directory.m_oUTC);
			int nPeriodInMillis = m_nPeriod * 1000;
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
	 * Wrapper for runMetro()
	 */
	@Override
	public void execute()
	{
		long lNow = System.currentTimeMillis();
		lNow = (lNow / 60000) * 60000;
		int nTimesToRun;
		String sCurrentFile;
		synchronized (m_oRunTimes)
		{
			if (m_bRealTime)
				m_oRunTimes.addFirst(lNow);
			sCurrentFile = m_oFileFormat.format(lNow, lNow - 2400000, lNow + (m_nForecastHours - 2) * 3600000);
			nTimesToRun = Math.min(m_nRunsPerPeriod, m_oRunTimes.size());
		}
		for (int i = 0; i < nTimesToRun; i++)
		{
			boolean bFinished = runMetro(m_oRunTimes.removeFirst());
			if (bFinished && m_bRealTime && i == 0)
				notify("file download", sCurrentFile);
		}
		
		synchronized (m_oRunTimes)
		{
			try (BufferedWriter oOut = new BufferedWriter(new FileWriter(m_sQueueFile)))
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
		}
	}


	/**
	 * Sets the start time for the current run and then the thread pool executes
	 * the METRo model for each segment in the study area. After METRo is
	 * finished processing all the segments a netcdf file is written containing
	 * all the data we want from the METRo outputs.
	 */
	public boolean runMetro(long lRunTime)
	{
		try
		{
			if (!DoMetroWrapper.g_bLibraryLoaded) // don't run if the shared library isn't loaded
				return false;
//			m_oProfiles.clear();
			m_oRoadcasts.clear();
			SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			m_oLogger.info("Running METRo for " + oSdf.format(lRunTime));
			m_lStartTime = lRunTime;
			m_oThreadPool.invokeAll(m_oToDo); // executes all the Callable and blocks until all threads are finished
			boolean bReturn = writeFile(lRunTime);
//			m_oLogger.info("Metro ran " + m_oProfiles.size() + " times ");
			m_oLogger.info("Finished METRo for " + oSdf.format(lRunTime));
			return bReturn;
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
		return false;
	}


	/**
	 * Writes the netcdf file for the current run. If there is no
	 * data from METRo, the file is not written.
	 */
	private boolean writeFile(long lRunTime)
	{
		String sLocation = m_oFileFormat.format(lRunTime, lRunTime - 2400000, lRunTime + (m_nForecastHours - 2) * 3600000);
		File oDir = new File(sLocation.substring(0, sLocation.lastIndexOf("/") + 1));
		oDir.mkdirs();

		try
		{
			Collections.sort(m_oRoadcasts);
			int nNumSegments = m_oRoadcasts.size();
			if (nNumSegments == 0)
			{
				m_oLogger.info("No roadcast data from Metro to write to file.");
				return false;
			}
			try (NetcdfFileWriter oWriter = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, sLocation))
			{
				//create dimensions
				Dimension oNumSegments = oWriter.addDimension(null, "Segment", nNumSegments);
				Dimension oMinutes = oWriter.addDimension(null, "TimeIndex", m_nOutputs);
				//create dimension list, only add Number of Segments for now
				ArrayList<Dimension> oDims = new ArrayList();
				oDims.add(oNumSegments);
				//create all of the variables and attributes
				Variable dMetroStartTime = oWriter.addVariable(null, "Metro_Start_Time", DataType.DOUBLE, new ArrayList());
				dMetroStartTime.addAttribute(new Attribute("long_name", "Timestamp of the start time for this run of METRo"));
				dMetroStartTime.addAttribute(new Attribute("unit", "milliseconds since 1970-1-1 00:00:00"));

				Variable nSegList = oWriter.addVariable(null, "Segment_Ids", DataType.INT, "Segment");
				nSegList.addAttribute(new Attribute("long_name", "List of Segment Ids"));

				Variable nMinutesSince = oWriter.addVariable(null, "Forecast_Minutes", DataType.INT, "TimeIndex");
				nMinutesSince.addAttribute(new Attribute("long name", "Minutes from start of METRo"));

				oDims.add(oMinutes); //now add Minutes to the dimension list because all of the other variables need it as a dimension

				Variable nStpvt = oWriter.addVariable(null, "STPVT", DataType.INT, oDims);
				nStpvt.addAttribute(new Attribute("long_name", "Road Condition"));
				nStpvt.addAttribute(new Attribute("units", "category"));

				Variable fTpvt = oWriter.addVariable(null, "TPVT", DataType.FLOAT, oDims);
				fTpvt.addAttribute(new Attribute("long_name", "Pavement Temperature"));
				fTpvt.addAttribute(new Attribute("units", "degrees Celsius"));

				Variable fTssrf = oWriter.addVariable(null, "TSSRF", DataType.FLOAT, oDims);
				fTssrf.addAttribute(new Attribute("long_name", "Subsurface Temperature"));
				fTssrf.addAttribute(new Attribute("units", "degrees Celsius"));

				Variable fDphliq = oWriter.addVariable(null, "DPHLIQ", DataType.FLOAT, oDims);
				fDphliq.addAttribute(new Attribute("long_name", "Liquid inundation depth"));
				fDphliq.addAttribute(new Attribute("units", "mm"));

				Variable fDphsn = oWriter.addVariable(null, "DPHSN", DataType.FLOAT, oDims);
				fDphsn.addAttribute(new Attribute("long_name", "Snow/Ice inundation depth"));
				fDphsn.addAttribute(new Attribute("units", "cm"));

				oWriter.create(); //create the file
				//add values for all of the variables to the file.
				ArrayDouble.D0 dScalar = new ArrayDouble.D0();
				dScalar.set(lRunTime); //set the StartTime
				oWriter.write(dMetroStartTime, dScalar);

				ArrayInt.D1 nSegments = new ArrayInt.D1(nNumSegments);
				for (int i = 0; i < nNumSegments; i++) // write the segments ids
					nSegments.setInt(i, m_oRoadcasts.get(i).m_nId);
				oWriter.write(nSegList, nSegments);

				ArrayInt.D1 nFcstMinArray = new ArrayInt.D1(m_nOutputs);
				for (int i = 0; i < m_nOutputs; i++) // write the forecast minutes
				{
					if (i < 30)
						nFcstMinArray.setInt(i, i * 2); // the first 30 values are 2 minutes apart
					else
						nFcstMinArray.setInt(i, 60 + ((i - 30) * 20)); // the rest are 20 minutes apart
				}
				oWriter.write(nMinutesSince, nFcstMinArray);

				ArrayInt.D2 nStpvtArray = new ArrayInt.D2(nNumSegments, m_nOutputs);
				ArrayFloat.D2 fTpvtArray = new ArrayFloat.D2(nNumSegments, m_nOutputs);
				ArrayFloat.D2 fTssrfArray = new ArrayFloat.D2(nNumSegments, m_nOutputs);
				ArrayFloat.D2 fDphliqArray = new ArrayFloat.D2(nNumSegments, m_nOutputs);
				ArrayFloat.D2 fDphsnArray = new ArrayFloat.D2(nNumSegments, m_nOutputs);

				for (int i = 0; i < nNumSegments; i++)
				{
					RoadcastData oRD = m_oRoadcasts.get(i);
					for (int j = 0; j < m_nOutputs; j++)
					{
						nStpvtArray.set(i, j, oRD.m_nStpvt[j]);
						fTpvtArray.set(i, j, oRD.m_fTpvt[j]);
						fTssrfArray.set(i, j, oRD.m_fTssrf[j]);
						fDphliqArray.set(i, j, oRD.m_fDphliq[j]);
						fDphsnArray.set(i, j, oRD.m_fDphsn[j]);

					}
				}
				oWriter.write(nStpvt, nStpvtArray);
				oWriter.write(fTpvt, fTpvtArray);
				oWriter.write(fTssrf, fTssrfArray);
				oWriter.write(fDphliq, fDphliqArray);
				oWriter.write(fDphsn, fDphsnArray);

				oWriter.close();	//close the file
				return true;
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
		return false;
	}

	/**
	 * Inner class used to wrap a Segment. It implements runnable so METRo can
	 * be executed for each Segment in the study area.
	 */
	private class MetroDelegate implements Runnable
	{

		/**
		 * The segment to run METRo on
		 */
		private Segment m_oSegment;


		/**
		 * Creates a MetroDelegate for the given Segment
		 *
		 * @param oSegment the Segment
		 */
		MetroDelegate(Segment oSegment)
		{
			m_oSegment = oSegment;
		}


		/**
		 * This function is the process to run the METRo model. It creates a
		 * DoMetroWrapper that gets filled with all the observation and forecast
		 * values METRo needs and then uses JNI to call the C function
		 * doMetroWrapper. The outputs of the METRo run are then saved to a
		 * temporary file.
		 */
		@Override
		public void run()
		{
			try
			{
				DoMetroWrapper oDMW = new DoMetroWrapper(m_nObsHours, m_nForecastHours);
				boolean bFill = oDMW.fillArrays(m_oSegment, m_lStartTime);
				if (bFill)
				{
					oDMW.run();
					oDMW.saveRoadcast(m_oSegment);

					synchronized (m_oRoadcasts)
					{
						m_oRoadcasts.add(oDMW.m_oOutput);
					}
				}
			}
			catch (Exception oException)
			{
				m_oLogger.error(oException, oException);
			}
		}
	}
}
