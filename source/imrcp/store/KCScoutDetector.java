package imrcp.store;

import imrcp.geosrv.KCScoutDetectorLocation;
import imrcp.geosrv.KCScoutDetectorLocations;
import imrcp.system.Config;
import imrcp.system.CsvReader;
import imrcp.system.Directory;
import java.io.BufferedWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.TimeZone;

/**
 * This class represents the KCScout Detectors. It contains methods to write
 * detector archive files and to insert detector observations into the database
 *
 */
public class KCScoutDetector implements Comparable<KCScoutDetector>
{

	/**
	 * KCScout Archive Id
	 */
	public int m_nId;

	/**
	 * The lanes the detector covers
	 */
	public Lane[] m_oLanes;

	/**
	 * The average speed of all the lanes at a given time
	 */
	public double m_dAverageSpeed;

	/**
	 * The average occupancy of all the lanes at a given time
	 */
	public double m_dAverageOcc;

	/**
	 * The total volume of all the lanes at a given them
	 */
	public int m_nTotalVolume;

	/**
	 * KCScout's name for the detector
	 */
	public String m_sStation;

	/**
	 * Timestamp of the detector's readings
	 */
	public long m_lTimestamp;

	/**
	 * Boolean telling if the detector is reporting data or not
	 */
	public boolean m_bRunning = false;

	/**
	 * Format for the dates in the archive files
	 */
	private SimpleDateFormat m_oFormat = new SimpleDateFormat("M'/'d'/'yyyy H:mm");

	private static final ArrayList<KCScoutDetectorLocation> m_oDetectorMapping = new ArrayList();

	static int m_nObsLength = Config.getInstance().getInt("imrcp.store.KCScoutDetectorsStore", "imrcp.store.KCScoutDetectorsStore", "obslength", 60000);
	
	public KCScoutDetectorLocation m_oLocation;


	static
	{
		((KCScoutDetectorLocations)Directory.getInstance().lookup("KCScoutDetectorLocations")).getDetectors(m_oDetectorMapping, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE);
		Collections.sort(m_oDetectorMapping, KCScoutDetectorLocation.g_oARCHIVEIDCOMPARATOR);
	}


	/**
	 * Default constructor. Creates the lanes for the detector and sets the
	 * timezone to UTC
	 */
	public KCScoutDetector()
	{
		m_oLanes = new Lane[10]; //max number of lanes in archive file is 10
		for (int i = 0; i < 10; i++)
			m_oLanes[i] = new Lane();
		m_oFormat.setTimeZone(Directory.m_oUTC);
	}


	public KCScoutDetector(CsvReader oIn, boolean bReadUTC) throws Exception
	{
		m_nId = oIn.parseInt(0);
		m_sStation = oIn.parseString(1);
		if (bReadUTC)
			m_oFormat.setTimeZone(Directory.m_oUTC);
		else
			m_oFormat.setTimeZone(TimeZone.getTimeZone("CST6CDT"));
		m_lTimestamp = m_oFormat.parse(oIn.parseString(4)).getTime();
		m_oLanes = new Lane[oIn.parseInt(6)];
		m_nTotalVolume = oIn.isNull(8) ? 0 : oIn.parseInt(8);
		m_dAverageOcc = oIn.isNull(10) ? 0 : oIn.parseDouble(10);
		m_dAverageSpeed = oIn.isNull(11) ? 0 : oIn.parseDouble(11);

		for (int i = 0; i < m_oLanes.length; i++)
		{
			int nStart = (12 * i) + 20;
			m_oLanes[i] = new Lane();
			m_oLanes[i].m_nVolume = oIn.isNull(nStart) ? -1 : oIn.parseInt(nStart);
			m_oLanes[i].m_dOcc = oIn.isNull(nStart + 2) ? -1 : oIn.parseDouble(nStart + 2);
			m_oLanes[i].m_dSpeed = oIn.isNull(nStart + 3) ? -1 : oIn.parseDouble(nStart + 3);
		}
		
		KCScoutDetectorLocation oSearch = new KCScoutDetectorLocation();
		oSearch.m_nArchiveId = m_nId;
		int nIndex = Collections.binarySearch(m_oDetectorMapping, oSearch, KCScoutDetectorLocation.g_oARCHIVEIDCOMPARATOR);
		if (nIndex >= 0)
			m_oLocation = m_oDetectorMapping.get(nIndex);
		else
		{
			m_oLocation = new KCScoutDetectorLocation();
			m_oLocation.m_nImrcpId = Integer.MIN_VALUE;
		}
	}


	/**
	 * Writes a line of an archive file for the detector
	 *
	 * @param oWriter open writer object
	 * @param nLanes number of lanes the detector has
	 * @throws Exception
	 */
	public void writeDetector(BufferedWriter oWriter, int nLanes) throws Exception
	{
		oWriter.write(Integer.toString(m_nId));
		oWriter.write(",");
		if (m_sStation != null)
			oWriter.write(m_sStation);
		oWriter.write(",");
		//oWriter.write("State");
		oWriter.write(",");
		//oWriter.write("Location");
		oWriter.write(",");
		oWriter.write(m_oFormat.format(m_lTimestamp));
		oWriter.write(",");
		oWriter.write("OneMin");
		oWriter.write(",");
		oWriter.write(Integer.toString(nLanes));
		oWriter.write(",");
		//oWriter.write("Dir");
		oWriter.write(",");
		oWriter.write(Integer.toString(m_nTotalVolume));
		oWriter.write(",");
//		oWriter.write("VPH");
		oWriter.write(",");
		oWriter.write(String.format("%.2f", m_dAverageOcc));
		oWriter.write(",");
		oWriter.write(String.format("%.2f", m_dAverageSpeed));
		oWriter.write(",");
		//oWriter.write("VQ");
		oWriter.write(",");
		//oWriter.write("SQ");
		oWriter.write(",");
		//oWriter.write("OQ");
		oWriter.write(",");
		//oWriter.write("VC1 Cnt");
		oWriter.write(",");
		//oWriter.write("VC2 Cnt");
		oWriter.write(",");
		//oWriter.write("VC3 Cnt");
		oWriter.write(",");
		//oWriter.write("VC4 Cnt");
		for (int i = 0; i < nLanes; i++)
		{
			oWriter.write(",");
			oWriter.write(Integer.toString(m_nId));
			oWriter.write(",");
			if (m_oLanes[i].m_nVolume != -1)
				oWriter.write(Integer.toString(m_oLanes[i].m_nVolume));
			oWriter.write(",");

//			oWriter.write("VPH");
			oWriter.write(",");
			if (m_oLanes[i].m_dOcc != -1)
				oWriter.write(Double.toString(m_oLanes[i].m_dOcc));
			oWriter.write(",");
			if (m_oLanes[i].m_dSpeed != -1)
				oWriter.write(Double.toString(m_oLanes[i].m_dSpeed));
			oWriter.write(",");
			//oWriter.write("VQ");
			oWriter.write(",");
			//oWriter.write("SQ");
			oWriter.write(",");
			//oWriter.write("OQ");
			oWriter.write(",");
			//oWriter.write("VC1 Cnt");
			oWriter.write(",");
			//oWriter.write("VC2 Cnt");
			oWriter.write(",");
			//oWriter.write("VC3 Cnt");
			oWriter.write(",");
			//oWriter.write("VC4 Cnt");
		}
		for (int i = 10 - nLanes; i > 0; i--)
		{
			oWriter.write(",");
			oWriter.write(Integer.toString(m_nId));
			oWriter.write(",,,,,,,,,,,");
		}
		oWriter.write("\n");
	}


	@Override
	public int compareTo(KCScoutDetector o)
	{
		int nReturn = m_oLocation.m_nImrcpId - o.m_oLocation.m_nImrcpId;
		if (nReturn == 0)
			nReturn = Long.compare(m_lTimestamp, o.m_lTimestamp);
		
		return nReturn;
	}
}
