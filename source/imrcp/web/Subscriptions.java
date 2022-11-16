package imrcp.web;

import imrcp.geosrv.GeoUtil;
import imrcp.geosrv.osm.OsmWay;
import imrcp.geosrv.WayNetworks;
import imrcp.store.ImrcpObsResultSet;
import imrcp.store.Obs;
import imrcp.store.ObsView;
import imrcp.system.Directory;
import imrcp.system.FileUtil;
import imrcp.system.Id;
import imrcp.system.Introsort;
import imrcp.system.ObsType;
import imrcp.system.Scheduling;
import imrcp.system.Text;
import imrcp.system.Units;
import java.awt.geom.Area;
import java.awt.geom.Path2D;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.HttpStatus;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

/**
 * This servlet manages all of the requests and processing necessary to run the
 * Create and View Reports UIs.
 * @author Federal Highway Administration
 */
public class Subscriptions extends SecureBaseBlock
{
	/**
	 * Number of milliseconds in a day
	 */
	private static final long NUM_OF_MILLI_SECONDS_IN_A_DAY = 1000L * 60 * 60 * 24;

	
	/**
	 * Maps Format enums to the corresponding OutputFormats
	 */
	HashMap<ReportSubscription.Format, OutputFormat> m_oFormatters = new HashMap(5);

	
	/**
	 * Format object used to generate time dependent file names
	 */
	SimpleDateFormat m_oDateFormat = new SimpleDateFormat("yyyyMMdd_HHmm");

	
	/**
	 * Base directory for all of the Reports/Subscriptions files
	 */
	private String m_sBaseDir;
	
	
	/**
	 * Format String used to generate report/subscription configuration file names
	 */
	private String m_sSubFileFormat;
	
	
	/**
	 * Format String used to generate report/subscription data file names
	 */
	private String m_sSubDataFormat;
	
	
	/**
	 * Format String used to generate report/subscription file names that are 
	 * being downloaded
	 */
	private String m_sDownloadFormat;

	
	/**
	 * Schedule offset from midnight in seconds
	 */
	protected int m_nOffset;

	
	/**
	 * Period of execution in seconds
	 */
	protected int m_nPeriod;
	
	
	/**
	 * Factory object used to get {@link JsonGenerator} objects to write JSON
	 * responses
	 */
	private final JsonFactory m_oJsonFactory = new JsonFactory();
	
	
	/**
	 * Value in decimal degrees scaled to 7 decimal places to add to report query's
	 * bounding box
	 */
	private int m_nQueryTol;

	
	/**
	 * Creates the necessary directories for report/subscription files and then 
	 * sets a schedule to execute on a fixed interval.
	 * 
	 * @return true if no Exceptions are thrown
	 * @throws Exception 
	 */
	@Override
	public boolean start() throws Exception
	{
		try
		{
			Files.createDirectories(Paths.get(m_sBaseDir), FileUtil.DIRPERS);
		}
		catch (FileAlreadyExistsException oEx)
		{
		}
		// set the five-minute subscription fulfillment interval
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}


	@Override
	public final void reset()
	{
		super.reset();
		m_nOffset = m_oConfig.getInt("offset", 0);
		m_nPeriod = m_oConfig.getInt("period", 300);

		m_sBaseDir = m_oConfig.getString("basedir", "./");
		if (!m_sBaseDir.endsWith("/"))
			m_sBaseDir += "/";
		m_sSubFileFormat = m_oConfig.getString("subff", "%s/config.bin");
		m_sSubDataFormat = m_oConfig.getString("dataff", "%s/obs_%s%s");
		m_sDownloadFormat = m_oConfig.getString("dlff", "%s/%s");
		m_oFormatters.put(ReportSubscription.Format.CSV, new OutputCsv());
		m_oDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		m_nQueryTol = m_oConfig.getInt("querytol", 10000);
		
	}


	/**
	 * Handles the specific logic for file download requests since users do
	 * not have to be logged in to download subscription files since they can
	 * use the direct link given when the subscription is created. If it is not
	 * a file download request calls {@link SecureBaseBlock#doPost(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)}
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oRes object that contains the response the servlet sends to the client
	 * @throws IOException
	 * @throws ServletException 
	 */
	@Override
	public void doPost(HttpServletRequest oReq, HttpServletResponse oRes)
	   throws IOException, ServletException
	{
		String[] sUrlParts = oReq.getRequestURI().split("/");
		int nIndex = sUrlParts.length;
		while (nIndex-- > 0)
		{
			if (sUrlParts[nIndex].compareTo("download") == 0)
			{
				oRes.setStatus(doDownload(oReq, oRes, null));
				return;			
			}
		}
		
		super.doPost(oReq, oRes);
	}


	/**
	 * Iterates through the directories in the reports/subscriptions base 
	 * directory and processes and fulfills any report/subscription that need to
	 * be processed.
	 */
	@Override
	public void execute()
	{
		m_oLogger.info("run() invoked");

		try
		{
			// process subscriptions here
			GregorianCalendar oNow = new GregorianCalendar(Directory.m_oUTC);
			int nCurrentMinutes = oNow.get(GregorianCalendar.MINUTE);

			List<ReportSubscription> oSubscriptions = getSubscriptions(oNow.getTimeInMillis(), null);
			m_oLogger.debug("Processing " + oSubscriptions.size() + " subscriptions");
			List<Obs> oObsList = new ArrayList(1000);
			Units oUnits = Units.getInstance();
			for (ReportSubscription oSub : oSubscriptions)
			{
				long lStart, lEnd;
				long lRefTime = oSub.m_lRefTime;
				if (oSub.isSubscription())
				{
					if (nCurrentMinutes % oSub.m_nCycle != 0)
					{
						m_oLogger.trace("Skipping " + oSub.m_sUuid + " because sub cycle " + oSub.m_nCycle + " does not match " + nCurrentMinutes);
						continue;
					}

					lStart = oNow.getTimeInMillis() + oSub.m_nOffset * 1000 * 60;
					lEnd = lStart + oSub.m_nDuration * 1000 * 60;
					lRefTime = oNow.getTimeInMillis();
				}
				else // report
				{
					if (oSub.m_lFulfillmentTime > 0)
					{
						m_oLogger.trace("Skipping " + oSub.m_sUuid + " because it has already been fulfilled ");
						continue;
					}

					lStart = oSub.m_lStartTime;
					lEnd = oSub.m_lEndTime;
				}

				m_oLogger.debug("Processing sub " + oSub.m_sUuid);
				int[] nSubObstypes;
				if (oSub.hasObstype())
					nSubObstypes = oSub.m_nObsTypes;
				else
					nSubObstypes = ObsType.ALL_OBSTYPES;

				oObsList.clear();
				double dMin = oSub.m_dMin;
				double dMax = oSub.m_dMax;
				int nQueryMinLon = oSub.m_nMinLon;
				int nQueryMinLat = oSub.m_nMinLat;
				int nQueryMaxLon = oSub.m_nMaxLon;
				int nQueryMaxLat = oSub.m_nMaxLat;
				WayNetworks oWays = (WayNetworks)Directory.getInstance().lookup("WayNetworks");
				ArrayList<OsmWay> oSubWays = new ArrayList();
				
				for (Id oId : oSub.m_oElementIds)
				{
					if (Id.isSegment(oId))
					{
						OsmWay oWay = oWays.getWayById(oId);
						if (oWay == null)
							continue;
						
						if (oWay.m_nMinLon < nQueryMinLon)
							nQueryMinLon = oWay.m_nMinLon;
						if (oWay.m_nMinLat < nQueryMinLat)
							nQueryMinLat = oWay.m_nMinLat;
						if (oWay.m_nMaxLon > nQueryMaxLon)
							nQueryMaxLon = oWay.m_nMaxLon;
						if (oWay.m_nMaxLat > nQueryMaxLat)
							nQueryMaxLat = oWay.m_nMaxLat;
												
						oSubWays.add(oWay);
					}
				}
				
				ArrayList<Area> oWayAreas = new ArrayList(oSubWays.size());
				ArrayList<int[]> oWayBbs = new ArrayList(oSubWays.size());
				for (OsmWay oWay : oSubWays)
				{
					Path2D.Double oPath = new Path2D.Double();
					int[] nBb = new int[]{oWay.m_nMinLon - m_nQueryTol, oWay.m_nMinLat - m_nQueryTol, oWay.m_nMaxLon + m_nQueryTol, oWay.m_nMaxLat + m_nQueryTol};
					oPath.moveTo(nBb[0], nBb[1]);
					oPath.lineTo(nBb[0], nBb[3]);
					oPath.lineTo(nBb[2], nBb[3]);
					oPath.lineTo(nBb[2], nBb[1]);
					oPath.closePath();
					oWayAreas.add(new Area(oPath));
					oWayBbs.add(nBb);
				}
				if (Double.isInfinite(dMax))
					dMax = Double.MAX_VALUE;
				if (Double.isInfinite(dMin))
					dMin = -Double.MAX_VALUE;
				ObsView oObsView = (ObsView)Directory.getInstance().lookup("ObsView");
				Path2D.Double oPath = new Path2D.Double(); // create an Area object to use for polygon intersection algorithm
				oPath.moveTo(oSub.m_nMinLon, oSub.m_nMinLat);
				oPath.lineTo(oSub.m_nMinLon, oSub.m_nMaxLat);
				oPath.lineTo(oSub.m_nMaxLon, oSub.m_nMaxLat);
				oPath.lineTo(oSub.m_nMaxLon, oSub.m_nMinLat);
				oPath.closePath();
				
				Area oSubGeo = new Area(oPath);
				
				nQueryMinLon -= m_nQueryTol;
				nQueryMinLat -= m_nQueryTol;
				nQueryMaxLon += m_nQueryTol;
				nQueryMaxLat += m_nQueryTol;
				for (int nObstype : nSubObstypes)
				{
					String sEnglishUnits = ObsType.getUnits(nObstype, false);
					ImrcpObsResultSet oData = (ImrcpObsResultSet)oObsView.getData(nObstype, lStart, lEnd, nQueryMinLat, nQueryMaxLat, nQueryMinLon, nQueryMaxLon, lRefTime);
					for (Obs oObs : oData)
					{
						String sSourceUnits = oUnits.getSourceUnits(nObstype, oObs.m_nContribId);
						double dConverted = oUnits.convert(sSourceUnits, sEnglishUnits, oObs.m_dValue);
						if (dConverted < dMin || dConverted > dMax) // skip if outside the min/max range
							continue;

						oObsList.add(oObs);						
					}
				}
				
				
				int nIndex = oObsList.size();
				ArrayList<Obs> oObsToWrite = new ArrayList(nIndex);
				while (nIndex-- > 0)
						{
					Obs oObs = oObsList.get(nIndex);
					int[] nObsBb;
					if (oObs.m_nLat2 == Integer.MIN_VALUE || oObs.m_nLat2 == Integer.MAX_VALUE)
						nObsBb = new int[]{oObs.m_nLon1 - 1, oObs.m_nLat1 -1, oObs.m_nLon1 + 1, oObs.m_nLat1 + 1};
					else
						nObsBb = new int[]{oObs.m_nLon1, oObs.m_nLat1, oObs.m_nLon2, oObs.m_nLat2};
					
					boolean bAdded = false;
					int nWayIndex = 0;
					while (nWayIndex < oSubWays.size() && !bAdded)
							{
						int[] nWayBb = oWayBbs.get(nWayIndex);
						OsmWay oWay = oSubWays.get(nWayIndex);
						Area oWayArea = oWayAreas.get(nWayIndex++);
						if (GeoUtil.boundingBoxesIntersect(nObsBb[0], nObsBb[1], nObsBb[2], nObsBb[3], nWayBb[0], nWayBb[1], nWayBb[2], nWayBb[3]))
						{
							if (Id.COMPARATOR.compare(oObs.m_oObjId, Id.NULLID) == 0)
							{
								if (GeoUtil.obsInside(oWayArea, oObs))
								{
									oObsToWrite.add(oObs);
									bAdded = true;
							}
							// do nothing, sensors are included in the reports
						}
							else if (Id.isSensor(oObs.m_oObjId))
							{
								if (oObs.m_nLat2 != Integer.MAX_VALUE || (oObs.m_nLat2 == Integer.MAX_VALUE && oWay.snap(m_nQueryTol, oObs.m_nLon1, oObs.m_nLat1).m_nLonIntersect != Integer.MIN_VALUE))
								{
									oObsToWrite.add(oObs);
									bAdded = true;
								}
							}
							else if (java.util.Arrays.binarySearch(oSub.m_oElementIds, oObs.m_oObjId, Id.COMPARATOR) >= 0)
							{
								if (Text.isEmpty(oObs.m_sDetail))
							oObs.m_sDetail = oWay.m_sName;
								oObsToWrite.add(oObs);
								bAdded = true;
							}
						}
					}
				}

				
				m_oLogger.debug("Found " + oObsToWrite.size() + " obs");

				Introsort.usort(oObsToWrite, Obs.g_oCompObsByTimeTypeContribObj);
				OutputFormat oOutputFormat = m_oFormatters.get(oSub.m_sOutputFormat);
				String sSubFile = m_sBaseDir + String.format(m_sSubDataFormat, oSub.m_sUuid, oSub.m_sName, m_oDateFormat.format(oNow.getTimeInMillis()), oOutputFormat.m_sSuffix);
				Path oSubFile = Paths.get(sSubFile);
				Files.createDirectories(oSubFile.getParent(), FileUtil.DIRPERS);
				m_oLogger.debug("Writing file " + sSubFile);
				try (PrintWriter oPrintWriter = new PrintWriter(Files.newOutputStream(oSubFile)))
				{
					oOutputFormat.fulfill(oPrintWriter, oObsToWrite, oSub);
					oSub.updateFulfillmentTime();
				}
			}
		}
		catch (Exception e)
		{
			m_oLogger.error(e, e);
		}

		m_oLogger.info("run() returning");
	}

	
	/**
	 * Gets an array that contains all of the available data files for the
	 * given id of a report/subscription.
	 * 
	 * @param sId report/subscription id
	 * @return A String array containing all of the available data files for the
	 * report/subscription. The filenames are sorted so that the most recent
	 * file is in position 0.
	 * @throws IOException
	 */
	public String[] getAvailableFiles(String sId)
		throws IOException
	{
		Path oSubDir = Paths.get(m_sBaseDir + String.format(m_sSubFileFormat, sId)).getParent();
		List<Path> oFiles = Files.walk(oSubDir, FileVisitOption.FOLLOW_LINKS).filter((oPath) ->
		{
			String sName = oPath.toString();
			return Files.isRegularFile(oPath) && sName.endsWith(".csv") || sName.endsWith(".cmml") || sName.endsWith(".kml") || sName.endsWith(".xml");
		}).sorted(Comparator.reverseOrder()).collect(Collectors.toList()); // sort by time descending


		String[] sFileNames = new String[oFiles.size()];
		for (int nIndex = 0; nIndex < sFileNames.length; nIndex++)
			sFileNames[nIndex] = oFiles.get(nIndex).getFileName().toString();

		return sFileNames;
	}

	
	/**
	 * Gets the Path of the given filename associated with the given id
	 * 
	 * @param sId report/subscription id
	 * @param sFileName data file name
	 * @return Path representing the data file name that matches the id and filename,
	 * null if the file does not exist
	 */
	public Path getSubscriptionFile(String sId, String sFileName)
	{
		Path oPath = Paths.get(m_sBaseDir + String.format(m_sDownloadFormat, sId, sFileName));
		// ensure the subscription destination exists

		if (!Files.exists(oPath))
			return null;
		else
			return oPath;

	}

	
	/**
	 * Gets the ReportSubscription with the given id
	 * 
	 * @param sUuid report/subscription id
	 * @return The ReportSubscription with the given id
	 * @throws Exception an Exception will be thrown if a ReportSubscription with
	 * the given id doesn't exist or if there is an error when reading its 
	 * configuration file.
	 */
	public ReportSubscription getSubscriptionByUuid(String sUuid) throws Exception
	{
		return new ReportSubscription(Paths.get(m_sBaseDir + String.format(m_sSubFileFormat, sUuid)));
	}

	
	/**
	 * Deletes the report/subscription with the given id and all of the files
	 * associated with it
	 * 
	 * @param sUuid report/subscription id
	 * @return true if the report/subscription is deleted successfully, otherwise
	 * false
	 */
	public boolean deleteSubscription(String sUuid)
	{
		try
		{
			for (Path oPath : Files.walk(Paths.get(m_sBaseDir + String.format(m_sSubFileFormat), sUuid).getParent(), FileVisitOption.FOLLOW_LINKS).sorted(Comparator.reverseOrder()).collect(Collectors.toList()))
				Files.delete(oPath);
			return true;
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
			return false;
		}
	}

	
	/**
	 * Gets a list of all the reports/subscriptions the given user created that 
	 * have been created or accessed recently relative to the given time.
	 * 
	 * @param lExpirationCutoff time in milliseconds since Epoch used to filter
	 * reports/subscriptions
	 * @param sUserName the user to get reports/subscriptions for. If this is
	 * null then all reports/subscriptions are added to the list
	 * @return 
	 * @throws Exception
	 */
	private List<ReportSubscription> getSubscriptions(long lExpirationCutoff, String sUserName) 
		throws Exception
	{
		ArrayList<ReportSubscription> oSubscriptions = new ArrayList<>();
		String sSubFile = m_sSubFileFormat.substring(m_sSubFileFormat.lastIndexOf("/") + 1);
		List<Path> oPaths = Files.walk(Paths.get(m_sBaseDir), FileVisitOption.FOLLOW_LINKS).filter(Files::isRegularFile).filter((oPath) -> oPath.toString().endsWith(sSubFile)).collect(Collectors.toList());
		long lCutoff = lExpirationCutoff - 14 * NUM_OF_MILLI_SECONDS_IN_A_DAY;
		for (Path oPath : oPaths)
		{
			ReportSubscription oSub = new ReportSubscription(oPath);
			if ((oSub.m_lLastAccess >= lCutoff || oSub.m_lCreatedTime >= lCutoff) && (sUserName == null || oSub.m_sUsername.compareTo(sUserName) == 0))
				oSubscriptions.add(oSub);
		}

		return oSubscriptions;
	}

	
	/**
	 * Adds a list of the active reports/subscriptions for the user making the
	 * request to the response as a JSON array of JSON objects.
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oRes object that contains the response the servlet sends to the client
	 * @param oSession object that contains information about the user that made the
	 * request
	 * @return HTTP status code to be included in the response.
	 * @throws ServletException
	 * @throws IOException
	 */
	public int doList(HttpServletRequest oReq, HttpServletResponse oRes, Session oSession)
	   throws ServletException, IOException
	{
		try
		{
			listSubscriptions(oReq, oRes, oSession);
			return HttpServletResponse.SC_OK;
		}
		catch (Exception oEx)
		{
			return HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
		}
	}
	
	
	/**
	 * Adds the report/subscription defined in the request, if valid, 
	 * into the system.
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oRes object that contains the response the servlet sends to the client
	 * @param oSession object that contains information about the user that made the
	 * request
	 * @return HTTP status code to be included in the response.
	 * @throws ServletException
	 * @throws IOException
	 */
	public int doAdd(HttpServletRequest oReq, HttpServletResponse oRes, Session oSession)
	   throws ServletException, IOException
	{
		ReportSubscription oNewSub;
		try
		{
			oNewSub = new ReportSubscription(oReq);
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
			return HttpServletResponse.SC_BAD_REQUEST;
		}

		if(oNewSub.m_nObsTypes.length == 0)
		{
			m_oLogger.error("No obstypes submitted");
			return HttpServletResponse.SC_BAD_REQUEST;
		}

		oNewSub.m_sUsername = oSession.m_sName;
		try
		{
			oNewSub.writeSubConfig(m_sBaseDir, m_sSubFileFormat);
		}
		catch (Exception oEx)
		{
			return HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
		}

		oRes.setContentType("application/json");
		try (JsonGenerator oGenerator = createJsonGenerator(oRes))
		{
			oGenerator.writeStartObject();
			serializeSubSummaryDetails(oGenerator, oNewSub, false);
			oGenerator.writeEndObject();
		}
		return HttpServletResponse.SC_OK;
	}
	
	
	/**
	 * Add a list of the files associated with the subscription id given in the
	 * request to the response as a JSON array.
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oRes object that contains the response the servlet sends to the client
	 * @param oSession object that contains information about the user that made the
	 * request
	 * @return HTTP status code to be included in the response.
	 * @throws ServletException
	 * @throws IOException
	 */
	public int doFiles(HttpServletRequest oReq, HttpServletResponse oRes, Session oSession)
	   throws ServletException, IOException
	{
		String[] sUriParts = oReq.getRequestURI().split("/");
		String sReportId = sUriParts[sUriParts.length - 1];

		ReportSubscription oSub;
		try
		{
			oSub = getSubscriptionByUuid(sReportId);
		}
		catch (Exception oEx)
		{
			return HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
		}
		
		oRes.setContentType("application/json");
		try (JsonGenerator oGenerator = createJsonGenerator(oRes))
		{
			oGenerator.writeStartArray();
			for (String sFileName : getAvailableFiles(oSub.m_sUuid))
				oGenerator.writeString(sFileName);

			oGenerator.writeEndArray();
		}
		return HttpServletResponse.SC_OK;
	}
	
	
	/**
	 * Adds the report/subscription data file defined in the request to the 
	 * response.
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oRes object that contains the response the servlet sends to the client
	 * @param oSession object that contains information about the user that made the
	 * request
	 * @return HTTP status code to be included in the response.
	 * @throws ServletException
	 * @throws IOException
	 */
	public int doDownload(HttpServletRequest oReq, HttpServletResponse oRes, Session oSession)
	   throws ServletException, IOException
	{
		String[] sUriParts = oReq.getRequestURI().split("/");
		String sReportId = sUriParts[sUriParts.length - 2];
		String sFileName = sUriParts[sUriParts.length - 1];
		return retrieveSubscriptionResult(sReportId, sFileName, oRes);
	}

	
	/**
	 * Deletes the report/subscription with the given id in the request
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oRes object that contains the response the servlet sends to the client
	 * @param oSession object that contains information about the user that made the
	 * request
	 * @return HTTP status code to be included in the response.
	 * @throws ServletException
	 * @throws IOException 
	 */
	public int doDelete(HttpServletRequest oReq, HttpServletResponse oRes, Session oSession)
		throws ServletException ,IOException
	{
		oRes.setContentType("text/plain");
		return deleteSubscription(oReq.getParameter("id")) ? HttpServletResponse.SC_OK : HttpServletResponse.SC_NOT_FOUND;
	}

	
	/**
	 * Adds the active reports/subscriptions that belong to the user to the
	 * response as a JSON array of JSON objects.
	 * 
	 * @param oReq object that contains the request the client has made of the servlet
	 * @param oResp object that contains the response the servlet sends to the client
	 * @param oSess object that contains information about the user that made the
	 * request
	 * @throws IOException
	 * @throws Exception
	 */
	public void listSubscriptions(HttpServletRequest oReq, HttpServletResponse oResp, Session oSess) throws IOException, Exception
	{
		oResp.setContentType("application/json");
		try (JsonGenerator oGenerator = createJsonGenerator(oResp))
		{
			oGenerator.writeStartArray();

			List<ReportSubscription> oSubscriptions = getSubscriptions(System.currentTimeMillis(), oSess.m_sName);
			Introsort.usort(oSubscriptions, (ReportSubscription o1, ReportSubscription o2) -> 
			{
				int nReturn = o1.m_sName.toLowerCase().compareTo(o2.m_sName.toLowerCase());
				if (nReturn == 0)
					nReturn = Long.compare(o2.m_lCreatedTime, o1.m_lCreatedTime);
				return nReturn;
			});
			
			
			for (ReportSubscription oSub : oSubscriptions)
			{
				oGenerator.writeStartObject();
				serializeSubSummaryDetails(oGenerator, oSub, false);
				oGenerator.writeEndObject();
			}
			oGenerator.writeEndArray();
		}
	}

	
	/**
	 * Adds the report/subscription data file with the given id and file name to
	 * the response.
	 * 
	 * @param sUuid report/subscription id
	 * @param sFileName file name of the data file
	 * @param oRes object that contains the response the servlet sends to the client
	 * @return HTTP status code to be included in the response.
	 * @throws IOException
	 */
	public int retrieveSubscriptionResult(String sUuid, String sFileName, HttpServletResponse oRes) throws IOException
	{
		try
		{
			ReportSubscription oSub = getSubscriptionByUuid(sUuid);

			if (oSub == null)
				return HttpServletResponse.SC_NOT_FOUND;

			if (sFileName.equals("latest"))
			{
				String[] sFiles = getAvailableFiles(sUuid);
				if (sFiles.length > 0)
					sFileName = sFiles[0];
				else
					return HttpServletResponse.SC_NOT_FOUND;
			}

			Path oReportFile = getSubscriptionFile(sUuid, sFileName);
			if (oReportFile == null)
				return HttpServletResponse.SC_NOT_FOUND;

			if (!sFileName.startsWith(oSub.m_sName))
				sFileName = oSub.m_sName + sFileName.substring(3); // files used to be prepended with "obs" so remove that if old sub/report
			oRes.setHeader("Content-Type", "application/octet-stream");
			// response.setHeader("Content-Length", Long.toString(oReportFile.length()));
			
			oRes.setHeader("Content-Disposition", "attachment; filename=" + sFileName);
			oRes.setContentType("text/plain");
			try (
			   PrintWriter printWriter = new PrintWriter(oRes.getOutputStream());
			   BufferedReader oReader = Files.newBufferedReader(oReportFile))
			{
				String sLine;
				while ((sLine = oReader.readLine()) != null)
					printWriter.write(sLine + "\r\n");
			}
			oSub.updateLastAccess();
			return HttpStatus.SC_OK;
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
			return HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
		}
	}

	
	/**
	 * Writes the fields of the report/subscription to the given JSONGenerator.
	 * 
	 * @param oOutputGenerator JSONGenerator to write the fields to
	 * @param oSub report/subscription to serialize
	 * @param bShort flag indicating if all fields are written (true) or just the
	 * name, description, uuid, format, and if it is a report or subscription (false)
	 * @throws IOException
	 */
	public void serializeSubSummaryDetails(JsonGenerator oOutputGenerator, ReportSubscription oSub, boolean bShort) throws IOException
	{
		oOutputGenerator.writeStringField("name", oSub.m_sName);
		oOutputGenerator.writeStringField("desc", oSub.m_sDescription);
		oOutputGenerator.writeStringField("uuid", oSub.m_sUuid);
		oOutputGenerator.writeStringField("format", oSub.m_sOutputFormat.name());

		oOutputGenerator.writeBooleanField(oSub.isReport() ? "isReport" : "isSubscription", true);

		if (bShort)
			return;

		oOutputGenerator.writeNumberField("created", oSub.m_lCreatedTime);

		if (oSub.m_lLastAccess > 0)
			oOutputGenerator.writeNumberField("lastAccessed", oSub.m_lLastAccess);

		if (oSub.m_lFulfillmentTime > 0)
			oOutputGenerator.writeNumberField("fulfilled", oSub.m_lFulfillmentTime);

		oOutputGenerator.writeNumberField("lat1", GeoUtil.fromIntDeg(oSub.m_nMinLat));
		oOutputGenerator.writeNumberField("lon1", GeoUtil.fromIntDeg(oSub.m_nMinLon));
		oOutputGenerator.writeNumberField("lat2", GeoUtil.fromIntDeg(oSub.m_nMaxLat));
		oOutputGenerator.writeNumberField("lon2", GeoUtil.fromIntDeg(oSub.m_nMaxLon));

		if (oSub.m_oElementIds.length > 0 )
		{
			oOutputGenerator.writeNumberField("elementCount", oSub.m_oElementIds.length);
		}

		if (oSub.hasObstype())
		{
			oOutputGenerator.writeArrayFieldStart("obstypes");
			for (int nObstype : oSub.m_nObsTypes)
			{
				oOutputGenerator.writeStartObject();
				oOutputGenerator.writeNumberField("id", nObstype);
				oOutputGenerator.writeStringField("name", ObsType.getName(nObstype));
				if (oSub.hasMin())
					oOutputGenerator.writeNumberField("min", oSub.m_dMin);
				if (oSub.hasMax())
					oOutputGenerator.writeNumberField("max", oSub.m_dMax);
				oOutputGenerator.writeEndObject();
			}
			oOutputGenerator.writeEndArray();
		}

		if (oSub.isSubscription())
		{
			oOutputGenerator.writeNumberField("cycle", oSub.m_nCycle);
			oOutputGenerator.writeNumberField("offset", oSub.m_nOffset);
			oOutputGenerator.writeNumberField("duration", oSub.m_nDuration);
		}

//	 oOutputGenerator.writeNumberField("refMillis", oSub.getRefTime());
		if (oSub.isReport())
		{
			oOutputGenerator.writeNumberField("startMillis", oSub.m_lStartTime);
			oOutputGenerator.writeNumberField("endMillis", oSub.m_lEndTime);
		}

	}

	
	/**
	 * Wrapper for {@link JsonFactory#createJsonGenerator(java.io.Writer)} using
	 * the response's writer
	 * 
	 * @param oResp object that contains the response the servlet sends to the client
	 * @return A new JsonGenerator wrapping the writer for the response.
	 * @throws IOException
	 */
	protected JsonGenerator createJsonGenerator(HttpServletResponse oResp) throws IOException
	{
		return m_oJsonFactory.createJsonGenerator(oResp.getWriter());
	}
}
