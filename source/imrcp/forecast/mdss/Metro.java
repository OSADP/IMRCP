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
package imrcp.forecast.mdss;

import imrcp.ImrcpBlock;
import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.system.Util;
import imrcp.system.Directory;
import imrcp.system.Scheduling;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
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
public class Metro extends ImrcpBlock
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
	private SimpleDateFormat m_oFileFormat;

	/**
	 * String used to create the file format SimpleDateFormat
	 */
	private String m_sDestFile;

	/**
	 * Base directory where temporary intermediate data files are written
	 */
	private String m_sBaseDir;

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

	/**
	 * List to store DoMetroWrappers for a set of METRo processes
	 */
	ArrayList<DoMetroWrapper> m_oDMWs = new ArrayList();

	/**
	 * String used to generate time dynamic file names for metro details files
	 * which are only created if ran in test mode
	 */
	private String m_sDetails;


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
		m_sDestFile = m_oConfig.getString("dest", "");
		m_oFileFormat = new SimpleDateFormat(m_sDestFile);
		m_sBaseDir = m_oConfig.getString("dir", "");
		m_nOffset = m_oConfig.getInt("offset", 3300);
		m_nPeriod = m_oConfig.getInt("period", 3600);
		m_nOutputs = 12 + ((m_nForecastHours - 3) * 3); // saving every 2 minutes for the first 20 minutes and then 20 minute intervals after that. so the first hour has 12 outputs and then each hour after has 3.
		m_sDetails = m_oConfig.getString("details", "");
		m_bTest = Boolean.parseBoolean(m_oConfig.getString("test", "False"));
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


	/**
	 * Wrapper for runMetro()
	 */
	@Override
	public void execute()
	{
		runMetro();
	}


	/**
	 * Wrapper for runMetro()
	 */
	@Override
	public void executeTest()
	{
		runMetro();
	}


	/**
	 * Sets the start time for the current run and then the thread pool executes
	 * the METRo model for each segment in the study area. After METRo is
	 * finished processing all the segments a netcdf file is written containing
	 * all the data we want from the METRo outputs.
	 */
	public void runMetro()
	{
		try
		{
			if (!DoMetroWrapper.g_bLibraryLoaded) // don't run if the shared library isn't loaded
				return;
			m_lStartTime = System.currentTimeMillis(); // floor to nearest minute
			m_lStartTime = (m_lStartTime / 60000) * 60000;
			SimpleDateFormat oFormat = new SimpleDateFormat(m_sDetails);

			m_oThreadPool.invokeAll(m_oToDo); // executes all the Callable and blocks until all threads are finished
			if (m_bTest)
			{
				for (DoMetroWrapper oDMW : m_oDMWs)
					oDMW.writeMetroDetails(oFormat.format(m_lStartTime));
			}
			m_oDMWs.clear();
			writeFile();
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}


	/**
	 * Writes the netcdf file for the current run. After the file is written,
	 * subscribers are notified of the new file being available. If there is no
	 * data from METRo, the file is not written.
	 */
	private void writeFile()
	{
		String sLocation = m_oFileFormat.format(m_lStartTime);
		File oDir = new File(sLocation.substring(0, sLocation.lastIndexOf("/") + 1));
		oDir.mkdirs();

		try (NetcdfFileWriter oWriter = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, sLocation))
		{
			File[] oFiles = new File(m_sBaseDir).listFiles();
			ArrayList<RoadcastData> oRoadcasts = new ArrayList();
			int nIndex = oFiles.length;
			while (nIndex-- > 0)
			{
				if (oFiles[nIndex].getName().contains("metrooutput"))
				{
					oRoadcasts.add(new RoadcastData(oFiles[nIndex].getAbsolutePath()));
					oFiles[nIndex].delete();
				}
			}
			Collections.sort(oRoadcasts);
			int nNumSegments = oRoadcasts.size();
			if (nNumSegments == 0)
			{
				m_oLogger.info("No roadcast data from Metro to write to file.");
				return;
			}
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
			dScalar.set(m_lStartTime); //set the StartTime
			oWriter.write(dMetroStartTime, dScalar);

			ArrayInt.D1 nSegments = new ArrayInt.D1(nNumSegments);
			for (int i = 0; i < nNumSegments; i++) // write the segments ids
				nSegments.setInt(i, oRoadcasts.get(i).m_nId);
			oWriter.write(nSegList, nSegments);

			ArrayInt.D1 nFcstMinArray = new ArrayInt.D1(m_nOutputs);
			for (int i = 0; i < m_nOutputs; i++) // write the forecast minutes
			{
				if (i < 10)
					nFcstMinArray.setInt(i, i * 2); // the first 10 values are 2 minutes apart
				else
					nFcstMinArray.setInt(i, 20 + ((i - 10) * 20)); // the rest are 20 minutes apart
			}
			oWriter.write(nMinutesSince, nFcstMinArray);

			ArrayInt.D2 nStpvtArray = new ArrayInt.D2(nNumSegments, m_nOutputs);
			ArrayFloat.D2 fTpvtArray = new ArrayFloat.D2(nNumSegments, m_nOutputs);
			ArrayFloat.D2 fTssrfArray = new ArrayFloat.D2(nNumSegments, m_nOutputs);
			ArrayFloat.D2 fDphliqArray = new ArrayFloat.D2(nNumSegments, m_nOutputs);
			ArrayFloat.D2 fDphsnArray = new ArrayFloat.D2(nNumSegments, m_nOutputs);

			for (int i = 0; i < nNumSegments; i++)
				for (int j = 0; j < m_nOutputs; j++)
				{
					nStpvtArray.set(i, j, oRoadcasts.get(i).m_nStpvt[j]);
					fTpvtArray.set(i, j, oRoadcasts.get(i).m_fTpvt[j]);
					fTssrfArray.set(i, j, oRoadcasts.get(i).m_fTssrf[j]);
					fDphliqArray.set(i, j, oRoadcasts.get(i).m_fDphliq[j]);
					fDphsnArray.set(i, j, oRoadcasts.get(i).m_fDphsn[j]);

				}
			oWriter.write(nStpvt, nStpvtArray);
			oWriter.write(fTpvt, fTpvtArray);
			oWriter.write(fTssrf, fTssrfArray);
			oWriter.write(fDphliq, fDphliqArray);
			oWriter.write(fDphsn, fDphsnArray);

			oWriter.close();	//close the file

			for (int nSubscriber : m_oSubscribers)
				notify(this, nSubscriber, "file download", sLocation); // "file download" sent cause the newly written METRo file to be read into memory

		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
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
					oDMW.saveRoadcast(m_oSegment, m_lStartTime);
					m_oDMWs.add(oDMW);
				}
			}
			catch (Exception oException)
			{
				m_oLogger.error(oException, oException);
			}
		}
	}

	/**
	 * Inner class used to parse the temporary data files that contain the METRo
	 * Roadcast outputs.
	 */
	private class RoadcastData implements Comparable<RoadcastData>
	{

		/**
		 * Imrcp Segment ID
		 */
		int m_nId;

		/**
		 * Array containing pavement state data
		 */
		int[] m_nStpvt = new int[m_nOutputs];

		/**
		 * Array containing pavement temperature data
		 */
		float[] m_fTpvt = new float[m_nOutputs];

		/**
		 * Array containing sub surface temperature data
		 */
		float[] m_fTssrf = new float[m_nOutputs];

		/**
		 * Array containing liquid depth (rain reservoir from METRo) data
		 */
		float[] m_fDphliq = new float[m_nOutputs];

		/**
		 * Array containing snow/ice depth (snow reservoir from METRo) data
		 */
		float[] m_fDphsn = new float[m_nOutputs];


		/**
		 * Creates a new RoadcastData from parsing the data in the given file.
		 *
		 * @param sFilename absolute path of the file to parse
		 * @throws Exception
		 */
		RoadcastData(String sFilename) throws Exception
		{
			try (BufferedReader oIn = new BufferedReader(new InputStreamReader(new FileInputStream(sFilename))))
			{
				m_nId = Integer.parseInt(sFilename.substring(sFilename.indexOf("metrooutput") + "metrooutput".length(), sFilename.lastIndexOf(".csv")));
				String sLine = null;
				int[] nEndpoints = new int[2];
				for (int i = 0; i < m_nOutputs; i++)
				{
					sLine = oIn.readLine();
					nEndpoints[0] = 0;
					nEndpoints[1] = sLine.indexOf(",", nEndpoints[0]);
					m_nStpvt[i] = Integer.parseInt(sLine.substring(nEndpoints[0], nEndpoints[1]));
					Util.moveEndpoints(sLine, nEndpoints);
					m_fTpvt[i] = Float.parseFloat(sLine.substring(nEndpoints[0], nEndpoints[1]));
					Util.moveEndpoints(sLine, nEndpoints);
					m_fTssrf[i] = Float.parseFloat(sLine.substring(nEndpoints[0], nEndpoints[1]));
					Util.moveEndpoints(sLine, nEndpoints);
					m_fDphliq[i] = Float.parseFloat(sLine.substring(nEndpoints[0], nEndpoints[1]));
					nEndpoints[0] = sLine.lastIndexOf(",") + 1;
					m_fDphsn[i] = Float.parseFloat(sLine.substring(nEndpoints[0]));
				}
			}
		}


		/**
		 * Compares RoadcastDatas by Id
		 *
		 * @param o The RoadcastData to compare to
		 * @return
		 */
		@Override
		public int compareTo(RoadcastData o)
		{
			return m_nId - o.m_nId;
		}
	}
}
