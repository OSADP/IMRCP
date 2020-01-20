package imrcp.comp;

import imrcp.BaseBlock;
import imrcp.FilenameFormatter;
import imrcp.geosrv.Segment;
import imrcp.geosrv.SegmentShps;
import imrcp.store.ImrcpObsResultSet;
import imrcp.store.Obs;
import imrcp.store.ObsView;
import imrcp.system.Directory;
import imrcp.system.Util;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * This block runs at a scheduled configured time to create alerts based off of
 * real time data and Alert Rules that are read in from the configuration file.
 */
public class Alerts extends BaseBlock
{
	private int m_nFileFrequency;
	/**
	 * List of rules
	 */
	private ArrayList<AlertRule> m_oRules = new ArrayList();

	/**
	 * Bounding box of the study area
	 */
	private int[] m_nStudyArea;

	private FilenameFormatter m_oFormatter;

	/**
	 * Array of obs types needed to evaluate the rules for this block
	 */
	private int[] m_nObsTypes;

	/**
	 * Length the area arrays need to be based off of the rules for this block
	 */
	private int m_nArrayLength;

	/**
	 * Reusable array to initial values for a new area
	 */
	private long[] m_lInitialValues;

	/**
	 * Header for the csv file
	 */
	public static final String g_sHEADER = "ObsType,Source,ObjId,ObsTime1,ObsTime2,TimeRecv,Lat1,Lon1,Lat2,Lon2,Elev,Value,Conf\n";


	/**
	 * Comparator used to compare long arrays that represent areas in this
	 * block. Compares first by lon1, then lon2, then lat1, then lat2.
	 */
	public static final Comparator<long[]> COMPBYAREA = (long[] o1, long[] o2) -> 
	{
		int nReturn = Long.compare(o1[2], o2[2]); // compare by lon1
		if (nReturn == 0)
		{
			nReturn = Long.compare(o1[3], o2[3]); // then lon2
			if (nReturn == 0)
			{
				nReturn = Long.compare(o1[0], o2[0]); // then lat1
				if (nReturn == 0)
					nReturn = Long.compare(o1[1], o2[1]); // then lat2
			}
		}

		return nReturn;
	};


	/**
	 * Reads in the rules and creates the necessary objects from the config
	 * file.
	 *
	 * @return true if no errors occur, false otherwise
	 * @throws Exception
	 */
	@Override
	public boolean start() throws Exception
	{
		String[] sRules = m_oConfig.getStringArray("rules", null);
		int nArrayPosition = 5;
		for (String sRule : sRules) // for each configured rule create the object
		{
			String[] sConditions = m_oConfig.getStringArray(sRule, null);
			if (sConditions.length % 4 != 1 || sConditions.length == 0) // rules have this struct {alert type, list of conditions} (conditions have 4 elements each)
			{
				m_oLogger.error("Incorrect length for rule: " + sRule);
				continue;
			}
			AlertRule oAdd = new AlertRule(sConditions, nArrayPosition);
			m_oRules.add(oAdd);
			nArrayPosition += (oAdd.m_oAlgorithm.size() * 2);
		}
		m_nArrayLength = nArrayPosition;
		m_lInitialValues = new long[m_nArrayLength - 5];
		for (int i = 0; i < m_lInitialValues.length;)
		{
			m_lInitialValues[i++] = Long.MAX_VALUE;
			m_lInitialValues[i++] = Long.MIN_VALUE;
		}
		return true;
	}


	/**
	 * Resets are configurable variables
	 */
	@Override
	public void reset()
	{
		m_nFileFrequency = m_oConfig.getInt("freq", 86400000);
		m_oFormatter = new FilenameFormatter(m_oConfig.getString("format", ""));
		String[] sObsTypes = m_oConfig.getStringArray("obs", null);
		m_nObsTypes = new int[sObsTypes.length];
		for (int i = 0; i < sObsTypes.length; i++)
			m_nObsTypes[i] = Integer.valueOf(sObsTypes[i], 36);
		String[] sBox = m_oConfig.getStringArray("box", "");
		m_nStudyArea = new int[4];

		m_nStudyArea[0] = Integer.MAX_VALUE;
		m_nStudyArea[1] = Integer.MIN_VALUE;
		m_nStudyArea[2] = Integer.MAX_VALUE;
		m_nStudyArea[3] = Integer.MIN_VALUE;
		for (int i = 0; i < sBox.length;)
		{
			int nLon = Integer.parseInt(sBox[i++]);
			int nLat = Integer.parseInt(sBox[i++]);

			if (nLon < m_nStudyArea[2])
				m_nStudyArea[2] = nLon;

			if (nLon > m_nStudyArea[3])
				m_nStudyArea[3] = nLon;

			if (nLat < m_nStudyArea[0])
				m_nStudyArea[0] = nLat;

			if (nLat > m_nStudyArea[1])
				m_nStudyArea[1] = nLat;
		}
	}


	/**
	 * Processes Notifications received from other blocks.
	 *
	 * @param oNotification the received Notification
	 */
	@Override
	public void process(String[] sMessage)
	{
		if (sMessage[MESSAGE].compareTo("new data") == 0)
		{
			String sStore = sMessage[FROM];
			long lStartTime = Long.parseLong(sMessage[2]);
			long lEndTime = Long.parseLong(sMessage[3]);
			int[] nObsTypes = new int[sMessage.length - 4];
			int nIndex = 0;
			for (int i = 4; i < sMessage.length; i++)
				nObsTypes[nIndex++] = Integer.parseInt(sMessage[i]);
			createAlerts(sStore, nObsTypes, lStartTime, lEndTime);
		}
	}


	/**
	 * Ran when a notification of new data is sent from a data Store. This
	 * function checks alert rules for the current forecasts in the study area.
	 * We use a long array for each area that could have an alert. The format of
	 * the arrays is: [lat1, lat2, lon1, lon2, objId, (pairs of start and end
	 * times for each condition for each rule)]
	 */
	public void createAlerts(String sStore, int[] nObsTypes, long lStartTime, long lEndTime)
	{
		try
		{
			SegmentShps oShps = (SegmentShps)Directory.getInstance().lookup("SegmentShps");
			BaseBlock oStore = (BaseBlock)Directory.getInstance().lookup(sStore);
			ArrayList<Obs> oAlerts = new ArrayList();
			ArrayList<long[]> oAreas = new ArrayList();
			long[] lSearch = new long[6];

			for (int j = 0; j < nObsTypes.length; j++) // for each obs type
			{
				boolean bCreateAlerts = false;
				for (int nCreateObsType : m_nObsTypes)
				{
					if (nCreateObsType == nObsTypes[j])
					{
						bCreateAlerts = true;
						break;
					}
				}
				if (!bCreateAlerts)
					continue;
				
				ImrcpObsResultSet oData = (ImrcpObsResultSet)oStore.getData(nObsTypes[j], lStartTime, lEndTime, m_nStudyArea[0], m_nStudyArea[1], m_nStudyArea[2], m_nStudyArea[3], System.currentTimeMillis()); // get Obs
				for (Obs oObs : oData)
				{
					if (Util.isSegment(oObs.m_nObjId))
					{
						Segment oSegment = oShps.getLinkById(oObs.m_nObjId);
						if (oSegment == null || !oSegment.m_sType.equals("H")) // skip segments that are not highways
							continue;
					}

					for (AlertRule oRule : m_oRules) // go through all the rules once, setting when each condition was met
					{
						for (int nCondIndex = 0; nCondIndex < oRule.m_oAlgorithm.size(); nCondIndex++)
						{
							AlertCondition oCond = oRule.m_oAlgorithm.get(nCondIndex);
							if (oObs.m_nObsTypeId != oCond.m_nObsType) // not the correct obs type
								continue;
							if (!oCond.evaluate(oObs.m_dValue)) // does not fall within the range of the condition
								continue;

							lSearch[0] = oObs.m_nLat1; // lat1
							lSearch[1] = oObs.m_nLat2; // lat2
							lSearch[2] = oObs.m_nLon1; // lon1
							lSearch[3] = oObs.m_nLon2; // lon2
							if (lSearch[1] == Integer.MIN_VALUE) // point observations
								lSearch[1] = lSearch[0];
							if (lSearch[3] == Integer.MIN_VALUE)
								lSearch[3] = lSearch[2];
							lSearch[4] = oObs.m_lObsTime1; // obstime1
							lSearch[5] = oObs.m_lObsTime2; // obstime2

							int nIndex = Collections.binarySearch(oAreas, lSearch, COMPBYAREA); // search if the an array for the area has been made yet
							if (nIndex < 0)
							{
								nIndex = ~nIndex;
								long[] lTemp = new long[m_nArrayLength];
								System.arraycopy(lSearch, 0, lTemp, 0, 4);
								System.arraycopy(m_lInitialValues, 0, lTemp, 5, m_lInitialValues.length); // initialize all the condition timestamps
								lTemp[4] = oObs.m_nObjId; // objid
								oAreas.add(nIndex, lTemp);
							}

							for (int i = 0; i < oAreas.size(); i++)
							{
								long[] lArea = oAreas.get(i);
								if (lArea[1] >= lSearch[0] && lArea[0] <= lSearch[1] && lArea[3] >= lSearch[2] && lArea[2] <= lSearch[3]) // check if the current area intersects the areas in the list
								{
									int nAreaCond = oRule.m_nArrayPosition + (nCondIndex * 2);
									if (lArea[nAreaCond] > lSearch[4]) // check if the endtime is later than the current endtime
										lArea[nAreaCond] = lSearch[4]; // if so use the earlier endtime
									++nAreaCond;
									if (lArea[nAreaCond] < lSearch[5]) // check if the start time is earlier than the current start time
										lArea[nAreaCond] = lSearch[5]; // if so use the later start time
								}
							}
						}
					}
				}
			}

			for (AlertRule oRule : m_oRules) // evaluate all the rules for each area
			{
				for (long[] lArea : oAreas)
				{
					Obs oObs = oRule.evaluateRuleForArea(lArea, lStartTime);
					if (oObs != null)
						oAlerts.add(oObs);
				}
			}
			long lFileStartTime = (lStartTime / m_nFileFrequency) * m_nFileFrequency;
			long lFileEndTime = lFileStartTime + m_nFileFrequency * 2;
			String sFilename = m_oFormatter.format(lFileStartTime, lFileStartTime, lFileEndTime);
			new File(sFilename.substring(0, sFilename.lastIndexOf("/"))).mkdirs();
			try (BufferedWriter oOut = new BufferedWriter(new FileWriter(sFilename, true))) // write the alert file
			{
				if (new File(sFilename).length() == 0)
					oOut.write(g_sHEADER);
				for (Obs oAlert : oAlerts)
					oAlert.writeCsv(oOut);
			}

			notify("file download", sFilename);
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}
}
