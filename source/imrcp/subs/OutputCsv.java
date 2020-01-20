/**
 * @file OutputCsv.java
 */
package imrcp.subs;

import imrcp.geosrv.GeoUtil;
import imrcp.store.Obs;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Provides methods to format and output observation data to .csv files to a
 * provided output stream.
 * <p>
 * Extends {@code OutputFormat} to enforce a general output-format interface.
 * </p>
 *
 * @see OutputCsv#fulfill(PrintWriter, ArrayList, Subscription, String, int,
 * long)
 */
public class OutputCsv extends OutputFormat
{

	private static final Logger m_oLogger = LogManager.getLogger(OutputCsv.class);

	/**
	 * Decimal number format.
	 */
	protected static DecimalFormat m_oDecimal = new DecimalFormat("0.000");

	/**
	 * File header information format.
	 */
	protected String m_sHeader = "Source,ObsType,ObstimeStart, ObstimeEnd,Latitude 1,Longitude 1,Latitude 2,Longitude 2,Elevation,Observation";

	/**
	 * Timestamp format.
	 */
	protected SimpleDateFormat m_oDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


	/**
	 * Initializes the file suffix. Initializes date format timezone to UTC.
	 */
	OutputCsv()
	{
		m_sSuffix = ".csv";
		m_oDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
	}


	/**
	 * Prints {@code SubObs} data to provided output stream. First prints the
	 * defined header, then traverses the provided {@code SubObs} list, printing
	 * the contained data in a .csv comma-delimited manner, of the format:
	 * <p/>
	 * <blockquote>
	 * observation-type id, observation-type name, sensor id, sensor index,
	 * station id, site id, climate id, contributor id, contributor name,
	 * station code, observation timestamp, latitude, longitude, elevation,
	 * observation value, units, english-unit value, english-units, confidence
	 * level, quality check
	 * </blockquote>
	 * followed by a timestamp footer.
	 * <p/>
	 * <p>
	 * Required for extension of {@link OutputFormat}.
	 * </p>
	 *
	 * @param oWriter output stream, connected, and ready to write data.
	 * @param oSubObsList list of observations to print.
	 * @param oSub subscription - used for filtering.
	 * @param sFilename output filename to write in footer, can be specified as
	 * null
	 * @param nId subscription id.
	 * @param lLimit timestamp lower bound. All observations with received or
	 * completed date less than this limit will not be printed.
	 */
	@Override
	void fulfill(PrintWriter oWriter, List<Obs> oSubObsList,
	   ReportSubscription oSub, String sFilename, int nId, long lLimit)
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
			// ArrayList<String> qualityFlags = QualityFlagDao.getInstance().getQualityFlagStringArray();
			// for (String qf : qualityFlags) {
			// oWriter.println(qf);
			// }
			// oWriter.println("WxDE (5) [2015-10-05 00:00:00+00, 2016-10-05 00:00:00+00) - QchsSequenceComplete,QchsManualFlag,QchsServiceSensorRange,QchsServiceClimateRange,QchsServiceStep,QchsServiceLike,QchsServicePersist,QchsServiceBarnes,QchsServicePressure,Complete,Manual,Sensor_Range,Climate_Range,Step,Like_Instrument,Persistence,IQR_Spatial,Barnes_Spatial,Dew_Point,Sea_Level_Pressure,Precip_Accum,Model_Analysis,Neighboring_Vehicle,Standard_Deviation");

			// oWriter.println("---BEGIN OF RECORDS---");
			// output the obs details
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
				if (oSubObs.m_nLat2 != Integer.MIN_VALUE)
					oWriter.print(GeoUtil.fromIntDeg(oSubObs.m_nLat2));
				oWriter.print(",");
				if (oSubObs.m_nLon2 != Integer.MIN_VALUE)
					oWriter.print(GeoUtil.fromIntDeg(oSubObs.m_nLon2));
				oWriter.print(",");
				oWriter.print(oSubObs.m_tElev);
				oWriter.print(",");
				oWriter.print(m_oDecimal.format(oSubObs.m_dValue));
				oWriter.println();
			}

			// output the end of file
			oWriter.print("---END OF RECORDS---");

			if (sFilename != null)
			{
				// oWriter.println();
				// oWriter.print(" --");
				oWriter.print(nId);
				oWriter.print(":");
				oWriter.println(sFilename);
			}
		}
		catch (Exception oExp)
		{
			m_oLogger.error(oExp, oExp);
		}
	}

}
