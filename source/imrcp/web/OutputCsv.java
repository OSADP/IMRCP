
package imrcp.web;

import imrcp.geosrv.GeoUtil;
import imrcp.store.Obs;
import imrcp.system.Id;
import imrcp.system.ObsType;
import imrcp.system.Units;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * OutputFormat implementation for writing observations in the report CSV format
 * @author Federal Highway Administration
 */
public class OutputCsv extends OutputFormat
{
	/**
	 * Log4j Logger
	 */
	private static final Logger m_oLogger = LogManager.getLogger(OutputCsv.class);

	
	/**
	 * Format object used for floating point numbers
	 */
	protected static DecimalFormat m_oDecimal = new DecimalFormat("0.#");

	
	/**
	 * CSV header
	 */
	protected String m_sHeader = "Source,ObsType,ObstimeStart, ObstimeEnd,Latitude 1,Longitude 1,Latitude 2,Longitude 2,Elevation,Units,Observation (Numeric),Observation (Text),System Id,Detail";

	
	/**
	 * Format object for timestamps
	 */
	protected SimpleDateFormat m_oDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	
	/**
	 * Default constructor. Sets the suffix and time zone of {@link #m_oDateFormat}
	 * to UTC.
	 */
	OutputCsv()
	{
		m_sSuffix = ".csv";
		m_oDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

	
	/**
	 * Writes the given observations associated with the given report/subscription
	 * to the given Writer.
	 * 
	 * @param oWriter Writer used to write the observations
	 * @param oSubObsList List of observations associated with the report/subscription
	 * @param oSub The report/subscription
	 */
	@Override
	void fulfill(PrintWriter oWriter, List<Obs> oSubObsList,
	   ReportSubscription oSub)
	{
		try
		{
			if (oSubObsList.isEmpty())
			{
				oWriter.println("No records found");
				return;
			}

			// output the header information
			oWriter.println(m_sHeader);
			Units oUnits = Units.getInstance();
			for (int nIndex = 0; nIndex < oSubObsList.size(); nIndex++)
			{
				Obs oSubObs = oSubObsList.get(nIndex);

				// // obs must match the time range and filter criteria
				// if(oSubObs.m_lTimeRecv < lLimit || !oSub.matches(oSubObs))
				// -continue;
				// "SourceId,ObsTypeID,ObsTypeName,"
				// + "Timestamp,Latitude,Longitude,Elevation,Observation,ConfValue";
				oWriter.print(Integer.toString(oSubObs.m_nContribId, 36).toUpperCase());
				oWriter.print(",");
				oWriter.print(Integer.toString(oSubObs.m_nObsTypeId, 36).toUpperCase());
				oWriter.print(",");
				oWriter.print(m_oDateFormat.format(oSubObs.m_lObsTime1));
				oWriter.print(",");
				oWriter.print(m_oDateFormat.format(oSubObs.m_lObsTime2));
				oWriter.print(",");
				oWriter.print(GeoUtil.fromIntDeg(oSubObs.m_nLat1));
				oWriter.print(",");
				oWriter.print(GeoUtil.fromIntDeg(oSubObs.m_nLon1));
				oWriter.print(",");
				if (oSubObs.m_nLat2 != Integer.MIN_VALUE && oSubObs.m_nLat2 != Integer.MAX_VALUE)
					oWriter.print(GeoUtil.fromIntDeg(oSubObs.m_nLat2));
				oWriter.print(",");
				if (oSubObs.m_nLon2 != Integer.MIN_VALUE && oSubObs.m_nLat2 != Integer.MAX_VALUE)
					oWriter.print(GeoUtil.fromIntDeg(oSubObs.m_nLon2));
				oWriter.print(",");
				if (oSubObs.m_tElev != Short.MIN_VALUE)
					oWriter.print(oSubObs.m_tElev);
				oWriter.print(",");
				String sEnglishUnits = ObsType.getUnits(oSubObs.m_nObsTypeId, false);
				oWriter.print(sEnglishUnits);
				oWriter.print(",");
				oWriter.print(m_oDecimal.format(oUnits.convert(oUnits.getSourceUnits(oSubObs.m_nObsTypeId, oSubObs.m_nContribId), sEnglishUnits, oSubObs.m_dValue)));
				oWriter.print(",");
				if (ObsType.hasLookup(oSubObs.m_nObsTypeId))
					oWriter.print(ObsType.lookup(oSubObs.m_nObsTypeId, (int)oSubObs.m_dValue));
				oWriter.print(",");
				oWriter.print(oSubObs.m_oObjId == null || Id.isNull(oSubObs.m_oObjId) ? "" : oSubObs.m_oObjId.toString());
				oWriter.print(",");
				oWriter.print(oSubObs.m_sDetail == null ? "" : oSubObs.m_sDetail);
				oWriter.println();
			}

			// output the end of file
			oWriter.print("---END OF RECORDS---");
		}
		catch (Exception oExp)
		{
			m_oLogger.error(oExp, oExp);
		}
	}
}
