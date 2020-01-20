package imrcp.subs;

import imrcp.BaseBlock;
import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.geosrv.SensorLocation;
import imrcp.geosrv.SensorLocations;
import imrcp.store.Obs;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import imrcp.system.Scheduling;
import imrcp.system.Units;
import imrcp.web.LatLng;
import java.io.File;
import java.io.PrintWriter;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

/**
 * Provides an interface and control class that gathers and processes
 * subscription data on a scheduled basis.
 */
public class Subscriptions extends BaseBlock
{

	/**
	 * Reference to the SegmentShps BaseBlock that allows access to segment
 definitions
	 */
	private final SegmentShps m_oRoads = (SegmentShps)Directory.getInstance().lookup("SegmentShps");

	/**
	 * Number of milliseconds in a day
	 */
	private static final long NUM_OF_MILLI_SECONDS_IN_A_DAY = 1000L * 60 * 60 * 24;

	/**
	 * List of strings that represent the instance names of BaseBlock that are
 SensorLocations
	 */
	private static final String[] g_sSENSORBLOCKS = new String[]
	{
		"KCScoutDetectorLocations", "StormwatchLocations", "AHPSLocations"
	};

	/**
	 * List of Sensor Locations
	 */
	private static final ArrayList<SensorLocations> g_oSENSORS = new ArrayList();


	static
	{
		//Right now this depends on Subscriptions being after any of the detector sensor blocks
		//in the directory config so that they will already be registered when this executes.
		//There is probably a better way to do this
		Directory oDir = Directory.getInstance();
		for (String sSensorBlock : g_sSENSORBLOCKS)
			g_oSENSORS.add((SensorLocations) oDir.lookup(sSensorBlock));
	}

	/**
	 * Subscriptions query format.
	 */
	private static String SUBS_QUERY_BASE = "SELECT id, uuid, name,  description, lat1, lon1, lat2, lon2, obstype_list, min_value, max_value, out_format, cycle, reftime, starttime, endtime, offset, duration, created, last_access, fulfilled, element_type FROM subscription ";

	/**
	 * Output formats. Hash binds file extension to a corresponding output
	 * format instance.
	 * <blockquote>
	 * "CSV" => OutputCsv formatter. <br />
	 * "CMML" => OutputCmml formatter. <br />
	 * "XML" => OutputXml formatter. <br />
	 * </blockquote>
	 *
	 * @see OutputCsv
	 * @see OutputCmml
	 * @see OutputXml
	 */
	HashMap<ReportSubscription.Format, OutputFormat> m_oFormatters = new HashMap<>(5);

	/**
	 * Timestamp format
	 */
	SimpleDateFormat m_oDateFormat = new SimpleDateFormat("yyyyMMdd_HHmm");

	/**
	 * Not currently used - length of time to keep {@code Subscription} records.
	 */
	private long m_lLifetime;

	/**
	 * maximum rows of records to process for a query.
	 */
	private int maxRows;

	/**
	 * maximum time for executing a query.
	 */
	private long maxTime;

	/**
	 * Directory to store subscription files in.
	 */
	private File m_oSubsOutputRoot;

	/**
	 * Reference to ObsView which is the "one-stop shop" for get data from the
	 * data stores
	 */
	private BaseBlock m_oObsView = (BaseBlock)Directory.getInstance().lookup("ObsView");

	/**
	 * Configured subscriptions data source.
	 */
	private DataSource m_iDsSubs;

	/**
	 * Period of execution in seconds
	 */
	protected int m_nOffset;

	/**
	 * Schedule offset from midnight in seconds
	 */
	protected int m_nPeriod;


	/**
	 * <b> Default Constructor </b>
	 * <p>
	 * Configures {@code Subscriptions}. Gets storage directory, and
	 * datasources. Configures the formatter Hash, and timezone.
	 * </p>
	 */
	public Subscriptions()
	{
		try
		{
			InitialContext oInitCtx = new InitialContext();
			Context oCtx = (Context) oInitCtx.lookup("java:comp/env");
			DataSource iDatasource = (DataSource) oCtx.lookup("jdbc/imrcp");

			m_iDsSubs = iDatasource;
			oInitCtx.close();
		}
		catch (NamingException oEx)
		{
			throw new RuntimeException(oEx);
		}

		m_oFormatters.put(ReportSubscription.Format.CSV, new OutputCsv());

		m_oDateFormat.setTimeZone(TimeZone.getDefault().getTimeZone("UTC"));

	}


	/**
	 * Creates the schedule to execute subscription fulfillments on a regular
	 * basis.
	 *
	 * @return always true
	 * @throws java.lang.Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		// set the five-minute subscription fulfillment interval
		m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}


	/**
	 * Resets all configurable variables
	 */
	@Override
	protected final void reset()
	{
		m_nOffset = m_oConfig.getInt("offset", 0);
		m_nPeriod = m_oConfig.getInt("period", 300);

		m_oSubsOutputRoot = new File(m_oConfig.getString("subsRoot", "./"));

		m_lLifetime = m_oConfig.getInt("lifetime", 604800);
		// convert to milliseconds
		m_lLifetime *= 1000;

		maxRows = m_oConfig.getInt("maxrows", 604800);

		maxTime = m_oConfig.getInt("maxtime", 30000);

		m_oFormatters.put(ReportSubscription.Format.CSV, new OutputCsv());

		m_oDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
	}


	/**
	 * Retrieves subscriptions/reports from the database and fulfills them.
	 */
	@Override
	public void execute()
	{
		m_oLogger.info("run() invoked");

		try (Connection iSubsConnection = m_iDsSubs.getConnection())
		{
			// process subscriptions here
			GregorianCalendar oNow = new GregorianCalendar();
			int nCurrentMinutes = oNow.get(GregorianCalendar.MINUTE);
			Timestamp oNowTs = new Timestamp(System.currentTimeMillis());

			List<ReportSubscription> oSubscriptions = getSubscriptions(oNow.getTimeInMillis(), null, iSubsConnection);
			m_oLogger.debug("Processing " + oSubscriptions.size() + " subscriptions");
			List<Obs> oObsList = new ArrayList<>(1000);
			Units oUnits = Units.getInstance();
			for (ReportSubscription oSub : oSubscriptions)
			{

				long lStart, lEnd;
				long lRefTime = oSub.getRefTime();
				if (oSub.isSubscription())
				{
					if (nCurrentMinutes % oSub.getCycle() != 0)
					{
						m_oLogger.trace("Skipping " + oSub.getId() + " because sub cycle " + oSub.getCycle() + " does not match " + nCurrentMinutes);
						continue;
					}

					lStart = oNow.getTimeInMillis() + oSub.getOffset() * 1000 * 60;
					lEnd = lStart + oSub.getDuration() * 1000 * 60;
					lRefTime = oNow.getTimeInMillis();
				}
				else // report
				{
					if (oSub.getFulfillmentTime() > 0)
					{
						m_oLogger.trace("Skipping " + oSub.getId() + " because it has already been fulfilled ");
						continue;
					}

					lStart = oSub.getStartTime();
					lEnd = oSub.getEndTime();
				}

				m_oLogger.debug("Processing sub " + oSub.getId());
				int[] nSubObstypes;
				if (oSub.hasObstype())
				{
					nSubObstypes = oSub.getObsTypes();
				}
				else
					nSubObstypes = ObsType.ALL_OBSTYPES;

				Set<Integer> oElementIds = null;
				List<LatLng> oElementPoints = null;
				if (oSub.getElementIds() != null)
				{
					oElementPoints = new ArrayList<>();

					oElementIds = new HashSet<>();
					for (int nElementId : oSub.getElementIds())
						oElementIds.add(nElementId);

					if (oSub.getElementType() == ReportElementType.Segment)
					{
						for (int nSegId : oSub.getElementIds())
						{
							Segment oSegment = m_oRoads.getLinkById(nSegId);
							oElementPoints.add(new LatLng(oSegment.m_nYmid, oSegment.m_nXmid));
						}
					}
					else if (oSub.getElementType() == ReportElementType.Detector)
					{
						int nSensorLocationPadding = 20;
						ArrayList<SensorLocation> oSensors = new ArrayList();
						for (SensorLocations oSensorLocations : g_oSENSORS)
							oSensorLocations.getSensorLocations(oSensors, oSub.getLat1() - nSensorLocationPadding, oSub.getLat2() + nSensorLocationPadding, oSub.getLon1() - nSensorLocationPadding, oSub.getLon2() + nSensorLocationPadding);

						for (SensorLocation oSensor : oSensors)
						{
							if (oElementIds.contains(oSensor.m_nImrcpId))
								oElementPoints.add(new LatLng(oSensor.m_nLat, oSensor.m_nLon));
						}
					}
				}

				oObsList.clear();
				double dMin = oSub.getMinObsValue();
				double dMax = oSub.getMaxObsValue();
				if (Double.isInfinite(dMax))
					dMax = Double.MAX_VALUE;
				if (Double.isInfinite(dMin))
					dMin = -Double.MAX_VALUE;
				for (int nObstype : nSubObstypes)
				{
					String sEnglishUnits = ObsType.getUnits(nObstype, false);
					try (ResultSet oData = m_oObsView.getData(nObstype, lStart, lEnd, oSub.getLat1(), oSub.getLat2(), oSub.getLon1(), oSub.getLon2(), lRefTime))
					{
						while (oData.next())
						{
							Obs oObs = new Obs(oData.getInt(1), oData.getInt(2), oData.getInt(3), oData.getLong(4), oData.getLong(5), oData.getLong(6), oData.getInt(7), oData.getInt(8), oData.getInt(9), oData.getInt(10), (short)oData.getInt(11), oData.getDouble(12));
							
							String sSourceUnits = oUnits.getSourceUnits(nObstype, oObs.m_nContribId);
							oObs.m_dValue = oUnits.convert(sSourceUnits, sEnglishUnits, oObs.m_dValue);
							if ((oObs.m_dValue < dMin || oObs.m_dValue > dMax))
								continue;

							if (oElementIds != null)
							{
								// if the obs is not tied to a road, include it if its area contains a segment midpoint
								// if the obs is tied to a road, include it if the road id is in the set.
								if (oObs.m_nObjId == Integer.MIN_VALUE)
								{
									if (oObs.m_nLat2 == Integer.MIN_VALUE || oObs.m_nLon2 == Integer.MIN_VALUE)
										continue;

									boolean bContained = false;
									for (LatLng oMidpoint : oElementPoints)
									{
										bContained
										   = oObs.m_nLat1 <= oMidpoint.getLat()
										   && oObs.m_nLat2 >= oMidpoint.getLat()
										   && oObs.m_nLon1 <= oMidpoint.getLng()
										   && oObs.m_nLon2 >= oMidpoint.getLng();

										if (bContained)
											break;
									}
									if (!bContained)
										continue;
								}
								else if (!oElementIds.contains(oObs.m_nObjId))
									continue;
							}

							oObsList.add(oObs);

						}
					}
				}
				m_oLogger.debug("Found " + oObsList.size() + " obs");

				Collections.sort(oObsList, (Obs o1, Obs o2) ->
				{
					int nReturn = Long.compare(o1.m_lObsTime1, o2.m_lObsTime1);
					if (nReturn == 0)
					{
						nReturn = o1.m_nObsTypeId - o2.m_nObsTypeId;
						if (nReturn == 0)
							nReturn = o1.m_nContribId - o2.m_nContribId;
					}
					return nReturn;
				});

				OutputFormat oOutputFormat = m_oFormatters.get(oSub.getOutputFormat());

				File oSubFile = getSubscriptionFile(oSub.getId(), m_oDateFormat.format(oNow.getTime()) + oOutputFormat.m_sSuffix);
				m_oLogger.debug("Writing file " + oSubFile.getAbsolutePath());
				try (PrintWriter oPrintWriter = new PrintWriter(oSubFile))
				{
					oOutputFormat.fulfill(oPrintWriter, oObsList,
					   oSub, oSubFile.getAbsolutePath(), oSub.getId(),
					   lStart);

					updateFulfillment(oSub.getId(), iSubsConnection, oNowTs);
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
	 *
	 * @param nSubId
	 * @return
	 */
	public String[] getAvailableFiles(int nSubId)
	{
		File oSubRoot = new File(m_oSubsOutputRoot, Integer.toString(nSubId));

		File[] oFiles = oSubRoot.listFiles((oFile) -> 
		{
			return oFile.isFile() && oFile.getName().endsWith(".csv") || oFile.getName().endsWith(".cmml") || oFile.getName().endsWith(".kml") || oFile.getName().endsWith(".xml");
		});

		String[] sFileNames = new String[oFiles.length];
		int nInd = -1;
		for (File oFile : oFiles)
			sFileNames[++nInd] = oFile.getName();
		Arrays.sort(sFileNames, (String sLeft, String sRight) -> 
		{
			return sRight.compareTo(sLeft);
		});
		return sFileNames;
	}


	/**
	 *
	 * @param nSubId
	 * @param sFileName
	 * @return
	 */
	public File getSubscriptionFile(int nSubId, String sFileName)
	{
		File oSubRoot = new File(m_oSubsOutputRoot, Integer.toString(nSubId));
		// ensure the subscription destination exists

		if (!oSubRoot.exists() && !oSubRoot.mkdirs())
			return null;
		else
			return new File(oSubRoot, sFileName);

	}


	/**
	 *
	 * @param sUuid
	 * @return
	 * @throws Exception
	 */
	public ReportSubscription getSubscriptionByUuid(String sUuid) throws Exception
	{
		try (Connection iSubsConnection = m_iDsSubs.getConnection();
		   PreparedStatement iSubsQuery = iSubsConnection.prepareStatement(SUBS_QUERY_BASE + "WHERE uuid = ?"))
		{
			iSubsQuery.setString(1, sUuid);
			try (ResultSet iResult = iSubsQuery.executeQuery())
			{
				if (iResult.next())
				{
					ReportSubscription oSub = new ReportSubscription(iResult);
					oSub.setElementIds(getSubElements(iSubsConnection, oSub.getId()));
					return oSub;
				}
				else
					return null;
			}
		}
	}


	public int[] getSubElements(Connection iConnection, int nSubId) throws SQLException
	{
		try (PreparedStatement iSegQuery = iConnection.prepareStatement(
		   "SELECT element_id FROM sub_elements WHERE sub_id = ?"))
		{
			iSegQuery.setInt(1, nSubId);

			List<Integer> oSegmentIds = new ArrayList<>();
			try (ResultSet iSegRs = iSegQuery.executeQuery())
			{
				while (iSegRs.next())
					oSegmentIds.add(iSegRs.getInt(1));
			}

			if (oSegmentIds.isEmpty())
				return null;

			int[] nSegmentIds = new int[oSegmentIds.size()];
			for (ListIterator<Integer> iSegItr = oSegmentIds.listIterator(); iSegItr.hasNext();)
				nSegmentIds[iSegItr.nextIndex()] = iSegItr.next();

			return nSegmentIds;
		}
	}


	/**
	 *
	 * @param nSubscriptionId
	 * @return
	 */
	public boolean deleteSubscription(int nSubscriptionId)
	{
		try (Connection oCon = m_iDsSubs.getConnection();
		   PreparedStatement deleteSubStmt = oCon.prepareStatement("DELETE FROM subscription WHERE id = ?");
		   PreparedStatement deleteSubSegsStmt = oCon.prepareStatement("DELETE FROM sub_elements WHERE sub_id = ?"))
		{
			deleteSubStmt.setInt(1, nSubscriptionId);
			deleteSubStmt.execute();

			deleteSubSegsStmt.setInt(1, nSubscriptionId);
			deleteSubSegsStmt.execute();
			return true;
		}
		catch (SQLException oEx)
		{
			m_oLogger.error(oEx, oEx);
			return false;
		}
	}


	/**
	 *
	 * @param nSubscriptionId
	 * @throws SQLException
	 */
	public void updateLastAccessed(int nSubscriptionId) throws SQLException
	{
		try (Connection oCon = m_iDsSubs.getConnection();
		   PreparedStatement oUpdateStmt = oCon.prepareStatement("UPDATE subscription SET last_access = ? WHERE id = ?"))
		{
			oUpdateStmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
			oUpdateStmt.setInt(2, nSubscriptionId);
			oUpdateStmt.execute();
		}
	}


	private void updateFulfillment(int nSubscriptionId, Connection oCon, Timestamp oTimestamp) throws SQLException
	{
		try (PreparedStatement oUpdateStmt = oCon.prepareStatement("UPDATE subscription SET fulfilled = ? WHERE id = ?"))
		{
			oUpdateStmt.setTimestamp(1, oTimestamp);
			oUpdateStmt.setInt(2, nSubscriptionId);
			oUpdateStmt.execute();
		}
	}


	/**
	 *
	 * @param lExpirationCutoff
	 * @return
	 * @throws SQLException
	 * @throws Exception
	 */
	public List<ReportSubscription> getSubscriptions(long lExpirationCutoff) throws SQLException, Exception
	{
		try (Connection iSubsConnection = m_iDsSubs.getConnection())
		{
			return getSubscriptions(lExpirationCutoff, null, iSubsConnection);
		}
	}


	/**
	 *
	 * @param lExpirationCutoff
	 * @param sUserName
	 * @return
	 * @throws SQLException
	 * @throws Exception
	 */
	public List<ReportSubscription> getSubscriptions(long lExpirationCutoff, String sUserName) throws SQLException, Exception
	{
		try (Connection iSubsConnection = m_iDsSubs.getConnection())
		{
			return getSubscriptions(lExpirationCutoff, sUserName, iSubsConnection);
		}
	}


	private List<ReportSubscription> getSubscriptions(long lExpirationCutoff, String sUserName, Connection iSubsConnection) throws SQLException, Exception
	{
		ArrayList<ReportSubscription> oSubscriptions = new ArrayList<>();
		String sQuery = SUBS_QUERY_BASE + "WHERE (last_access >= ? OR created >= ?)";
		if (sUserName != null)
			sQuery += " AND username = ?";

		try (PreparedStatement iSubsQuery = iSubsConnection.prepareStatement(sQuery))
		{
			Timestamp oCutoff = new Timestamp(lExpirationCutoff - 14 * NUM_OF_MILLI_SECONDS_IN_A_DAY);
			iSubsQuery.setTimestamp(1, oCutoff);
			iSubsQuery.setTimestamp(2, oCutoff);
			if (sUserName != null)
				iSubsQuery.setString(3, sUserName);

			try (ResultSet iSubsResults = iSubsQuery.executeQuery())
			{
				while (iSubsResults.next())
				{
					ReportSubscription oSub = new ReportSubscription(iSubsResults);
					oSub.setElementIds(getSubElements(iSubsConnection, oSub.getId()));
					oSubscriptions.add(oSub);
				}
			}

			return oSubscriptions;
		}
	}


	/**
	 *
	 * @param oSub
	 * @throws Exception
	 */
	public void insertSubscription(ReportSubscription oSub) throws Exception
	{
		oSub.setUuid(UUID.randomUUID().toString());

		try (Connection oCon = m_iDsSubs.getConnection())
		{
			StringBuilder oQueryBuilder = new StringBuilder(500);
			oQueryBuilder.append("INSERT into subscription (");
			oQueryBuilder.append("username,obstype_list, max_value,min_value,out_format,lat1,lon1,lat2,lon2,name,description,uuid,created,reftime,element_type");
			int nParamCount = 15;
			if (oSub.isReport())
			{
				oQueryBuilder.append(",starttime, endtime");
				nParamCount += 2;
			}
			else if (oSub.isSubscription())
			{
				oQueryBuilder.append(",cycle,offset,duration");
				nParamCount += 3;
			}
			else
				throw new Exception("Attempted to save invalid report/subscription");

			oQueryBuilder.append(") VALUES(");
			while (--nParamCount >= 0)
				oQueryBuilder.append("?,");
			oQueryBuilder.setCharAt(oQueryBuilder.length() - 1, ')');

			try (PreparedStatement oInsertStmt = oCon.prepareStatement(oQueryBuilder.toString(), Statement.RETURN_GENERATED_KEYS);
			   PreparedStatement oInsertSegmentStmt = oCon.prepareStatement("INSERT INTO sub_elements (sub_id, element_id) VALUES (?,?)"))
			{
				nParamCount = 0;

				oInsertStmt.setString(++nParamCount, oSub.getUsername());

				if (oSub.getObsTypes() != null)
				{
					StringBuilder oObstypeCsv = new StringBuilder();
					for (int nObstype : oSub.getObsTypes())
						oObstypeCsv.append(",").append(nObstype);

					oInsertStmt.setString(++nParamCount, oObstypeCsv.substring(1));
				}
				else
					oInsertStmt.setNull(++nParamCount, Types.INTEGER);

				if (!Double.isInfinite(oSub.getMaxObsValue()))
					oInsertStmt.setDouble(++nParamCount, oSub.getMaxObsValue());
				else
					oInsertStmt.setNull(++nParamCount, Types.DOUBLE);

				if (!Double.isInfinite(oSub.getMinObsValue()))
					oInsertStmt.setDouble(++nParamCount, oSub.getMinObsValue());
				else
					oInsertStmt.setNull(++nParamCount, Types.DOUBLE);

				// out_format,lat1,lon1,lat2,lon2,name,description,uuid
				oInsertStmt.setString(++nParamCount, oSub.getOutputFormat().name());

				oInsertStmt.setInt(++nParamCount, oSub.getLat1());
				oInsertStmt.setInt(++nParamCount, oSub.getLon1());
				oInsertStmt.setInt(++nParamCount, oSub.getLat2());
				oInsertStmt.setInt(++nParamCount, oSub.getLon2());

				oInsertStmt.setString(++nParamCount, oSub.getName());
				oInsertStmt.setString(++nParamCount, oSub.getDescription());
				oInsertStmt.setString(++nParamCount, oSub.getUuid());

				long lNow = System.currentTimeMillis();
				oSub.setCreatedTime(lNow);
				oInsertStmt.setTimestamp(++nParamCount, new Timestamp(lNow));

				oInsertStmt.setTimestamp(++nParamCount, new Timestamp(oSub.getRefTime()));
				if (oSub.getElementType() == null)
					oInsertStmt.setNull(++nParamCount, Types.INTEGER);
				else
					oInsertStmt.setInt(++nParamCount, oSub.getElementType().getId());

				if (oSub.isReport())
				{
					oInsertStmt.setTimestamp(++nParamCount, new Timestamp(oSub.getStartTime()));
					oInsertStmt.setTimestamp(++nParamCount, new Timestamp(oSub.getEndTime()));
				}
				else
				{
					oInsertStmt.setInt(++nParamCount, oSub.getCycle());
					oInsertStmt.setInt(++nParamCount, oSub.getOffset());
					oInsertStmt.setInt(++nParamCount, oSub.getDuration());
				}

				oInsertStmt.executeUpdate();

				ResultSet iResult = oInsertStmt.getGeneratedKeys();
				if (iResult.next())
				{
					oSub.setId(iResult.getInt(1));

					if (oSub.getElementIds() != null)
					{
						oInsertSegmentStmt.setInt(1, oSub.getId());
						for (int nSegId : oSub.getElementIds())
						{
							oInsertSegmentStmt.setInt(2, nSegId);
							oInsertSegmentStmt.execute();
						}
					}
				}
				else
					m_oLogger.error("Failed to get generated key after insert");
			}
		}
	}
}
