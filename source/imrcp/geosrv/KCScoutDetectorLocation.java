package imrcp.geosrv;

import imrcp.system.Config;
import imrcp.system.CsvReader;
import java.io.BufferedWriter;
import java.util.Comparator;

/**
 * This class contains metadata for detectors used in the study area and allows
 * mapping external system ids to internal imrcp ids for the detectors.
 */
public class KCScoutDetectorLocation extends SensorLocation
{

	/**
	 * KCScout archive id
	 */
	public int m_nArchiveId;

	/**
	 * KCScout id used in real time feeds
	 */
	public int m_nRealTimeId;

	/**
	 * KCScout detector name
	 */
	public String m_sDetectorName;

	/**
	 * NUTC iNode id
	 */
	public int m_nINodeId;

	/**
	 * NUTC jNode id
	 */
	public int m_nJNodeId;

	/**
	 * Tells if the detector is on a ramp or not
	 */
	public boolean m_bRamp;

	/**
	 * If the detector is on a ramp, tells if the ramp is metered or not
	 */
	public boolean m_bMetered;
	
	public int m_nPhysicalLat;
	
	public int m_nPhysicalLon;
	
	public int m_nSegmentId;

	/**
	 * Compares DetectorMappings by KCScout Archive Id
	 */
	public static final Comparator<KCScoutDetectorLocation> g_oARCHIVEIDCOMPARATOR;

	/**
	 * Compare DetectorMappings by KCScout Real Time Id
	 */
	public static final Comparator<KCScoutDetectorLocation> g_oREALTIMEIDCOMPARATOR;
	
	public static final Comparator<KCScoutDetectorLocation> g_oNUTCLINKIDCOMPARATOR;
	
	public static final Comparator<KCScoutDetectorLocation> g_oSEGMENTIDCOMPARATOR;
	private static final int g_nMAPVALUE;

	/**
	 * Creates the static Comparators
	 */
	static
	{
		g_oARCHIVEIDCOMPARATOR = (KCScoutDetectorLocation o1, KCScoutDetectorLocation o2) -> 
		{
			return o1.m_nArchiveId - o2.m_nArchiveId;
		};
		g_oREALTIMEIDCOMPARATOR = (KCScoutDetectorLocation o1, KCScoutDetectorLocation o2) -> 
		{
			return o1.m_nRealTimeId - o2.m_nRealTimeId;
		};
		g_oNUTCLINKIDCOMPARATOR = (KCScoutDetectorLocation o1, KCScoutDetectorLocation o2) -> 
		{
			String sId1 = o1.m_nINodeId + "-" + o1.m_nJNodeId;
			String sId2 = o2.m_nINodeId + "-" + o2.m_nJNodeId;
			return sId1.compareTo(sId2);
		};
		g_oSEGMENTIDCOMPARATOR = (KCScoutDetectorLocation o1, KCScoutDetectorLocation o2) -> 
		{
			return o1.m_nSegmentId - o2.m_nSegmentId;
		};
		g_nMAPVALUE = Integer.valueOf(Config.getInstance().getString("imrcp.geosrv.KCScoutDetectorMappings", "imrcp.geosrv.KCScoutDetectorMappings", "mapvalue", "scout"), 36);
	}


	/**
	 * Default constructor
	 */
	public KCScoutDetectorLocation()
	{
	}


	/**
	 * Creates a new DetectorMapping from a csv line
	 *
	 * @param sLine csv line from the detector mapping file that contains the
	 * imrcp ids
	 */
	public KCScoutDetectorLocation(CsvReader oIn)
	{
		m_bRamp = !oIn.isNull(0);
		
		m_bMetered = !oIn.isNull(1);
		m_nArchiveId = oIn.parseInt(2);
		m_nRealTimeId = oIn.parseInt(3);
		m_sDetectorName = oIn.parseString(4);
		m_nPhysicalLon = oIn.parseInt(5);
		m_nPhysicalLat = oIn.parseInt(6);
		m_nINodeId = oIn.parseInt(7);
		m_nJNodeId = oIn.parseInt(8);
		m_nImrcpId = oIn.parseInt(9);
		m_nSegmentId = oIn.parseInt(10);
		m_nLon = oIn.parseInt(11);
		m_nLat = oIn.parseInt(12);
		
		m_sMapDetail = m_sDetectorName;
		setElev();
	}


	@Override
	public int getMapValue()
	{
		return g_nMAPVALUE;
	}
	
	public void write(BufferedWriter oOut) throws Exception
	{
		oOut.write(String.format("\n%s,%s,%d,%d,%s,%d,%d,%d,%d,%d,%d,%d,%d", m_bRamp ? "x" : "", m_bMetered ? "x" : "", m_nArchiveId,
		   m_nRealTimeId, m_sDetectorName, m_nPhysicalLon, m_nPhysicalLat, m_nINodeId == 0 ? Integer.MIN_VALUE : m_nINodeId, m_nJNodeId == 0 ? Integer.MIN_VALUE : m_nJNodeId,
		   m_nImrcpId, m_nSegmentId, m_nLon, m_nLat));
	}
}
