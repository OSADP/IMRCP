package imrcp.web;

import imrcp.geosrv.GeoUtil;
import imrcp.store.AlertObs;
import imrcp.store.ObsView;
import imrcp.system.Config;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.OutputStreamWriter;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.zip.CRC32;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 *
 */
//@WebServlet(name = "NotifyServlet", urlPatterns = {
//    "/notify/*"
//})
public class NotificationServlet extends HttpServlet
{

	private static final Logger LOGGER = LogManager.getLogger(NotificationServlet.class);

	private static final String JSON
	   = "\n\t{\"id\":%d,\"typeName\":\"%s\",\"typeId\":\"%d\"," + "\"start\":%d,\"end\":%d,\"cleared\":%d," + "\"description\":\"%s\"," + "\"issued\":%d," + "\"lat1\":%.6f,\"lon1\":%.6f,\"lat2\":%.6f,\"lon2\":%.6f}";

	private final ObsView m_oObsView = (ObsView) Directory.getInstance().lookup("ObsView");

	private final int[] m_nStudyArea = new int[4];

	private boolean m_bTest;


	public NotificationServlet()
	{
		String[] sBox = Config.getInstance().getStringArray("imrcp.ImrcpBlock", "imrcp.ImrcpBlock", "box", "");
		m_bTest = Boolean.parseBoolean(Config.getInstance().getString("NotificationServlet", "NotificationServlet", "test", "False"));
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
	 *
	 * @param iReq
	 * @param iRep
	 */
	@Override
	public void doGet(HttpServletRequest iReq, HttpServletResponse iRep)
	{
		if (!m_bTest)
		{
			try
			{
				iRep.setContentType("application/json");
				String[] sUriParts = iReq.getRequestURI().split("/");

				long lCurrentTime = System.currentTimeMillis();
				lCurrentTime = (lCurrentTime / 60000) * 60000;

				ResultSet oData = m_oObsView.getData(ObsType.NOTIFY, lCurrentTime, lCurrentTime + 28800000, m_nStudyArea[0], m_nStudyArea[1], m_nStudyArea[2], m_nStudyArea[3], System.currentTimeMillis());
				ArrayList<AlertObs> oNotifications = new ArrayList();
				while (oData.next())
					oNotifications.add(new AlertObs(oData.getInt(1), oData.getInt(2), oData.getInt(3), oData.getLong(4), oData.getLong(5), oData.getLong(6), oData.getInt(7), oData.getInt(8), oData.getInt(9), oData.getInt(10), oData.getShort(11), oData.getDouble(12), oData.getShort(13), oData.getString(14), oData.getLong(15)));

				try (BufferedWriter oOut = new BufferedWriter(new OutputStreamWriter(iRep.getOutputStream())))
				{
					oOut.write("["); // start json array
					int nMaxIndex = oNotifications.size() - 1;
					for (int i = 0; i <= nMaxIndex; i++)
					{
						AlertObs oNotification = oNotifications.get(i);
						oOut.write(format(getId(oNotification), ObsType.lookup(ObsType.NOTIFY, (int)oNotification.m_dValue), (int)oNotification.m_dValue, (oNotification.m_lObsTime1 / 60000) * 60000, (oNotification.m_lObsTime2 / 60000) * 60000, oNotification.m_lClearedTime, oNotification.m_sDetail.replace(";", "<br/>"), oNotification.m_lTimeRecv, oNotification.m_nLat1, oNotification.m_nLon1, oNotification.m_nLat2, oNotification.m_nLon2));
						if (i < nMaxIndex)
							oOut.write(",");
					}

					oOut.write("\n]\n"); // finish JSON array
				}
			}
			catch (Exception oEx)
			{
				LOGGER.info(oEx, oEx);
			}
		}
		else
		{
			iRep.setContentType("application/json");
			String[] sUriParts = iReq.getRequestURI().split("/");

			long lRefTime = Long.parseLong(sUriParts[sUriParts.length - 2]);
			long lCurrentTime = Long.parseLong(sUriParts[sUriParts.length - 1]);
			Calendar oCal = new GregorianCalendar(Directory.m_oUTC);
			oCal.setTimeInMillis(lCurrentTime);
			long[] lTimeExtents = new long[6];
			lTimeExtents[0] = oCal.getTimeInMillis() + 60000; // first minute
			lTimeExtents[1] = oCal.getTimeInMillis() + 180000; // 2 minutes after [0]
			lTimeExtents[2] = lTimeExtents[0] + 60000; // second minute
			lTimeExtents[3] = lTimeExtents[2] + 3600000; // 1 hour after [2]
			lTimeExtents[4] = lTimeExtents[0] + 120000; // third minute
			lTimeExtents[5] = lTimeExtents[4] + 1200000; // 20 minutes after [4]

			try (BufferedWriter oOut = new BufferedWriter(new OutputStreamWriter(iRep.getOutputStream())))
			{
				oOut.write("["); // start json array

				int nTimeIndex = (int)(lCurrentTime % 300000) / 60000;

				if (nTimeIndex == 0)
				{

				}

				if (nTimeIndex == 1)
				{
					lTimeExtents[0] = oCal.getTimeInMillis() - 3600000;
					lTimeExtents[1] = oCal.getTimeInMillis() + 120000;
					oOut.write(format(1, "unusual-congestion", 307, lTimeExtents[0], lTimeExtents[1], Long.MIN_VALUE, "I-435 W Between Metcalf Ave & Antioch Rd", 389338590 - 10000, -946767330 - 10000, 389338590 + 10000, -946767330 + 10000));
				}

				if (nTimeIndex == 2)
				{
					lTimeExtents[0] = oCal.getTimeInMillis() - 3540000;
					lTimeExtents[1] = oCal.getTimeInMillis() + 60000;
					lTimeExtents[2] = oCal.getTimeInMillis();
					lTimeExtents[3] = lTimeExtents[2] + 1800000;
					oOut.write(format(1, "unusual-congestion", 307, lTimeExtents[0], lTimeExtents[1], Long.MIN_VALUE, "I-435 W Between Metcalf Ave & Antioch Rd", 389338590 - 10000, -946767330 - 10000, 389338590 + 10000, -946767330 + 10000));
					oOut.write(",");
					oOut.write(format(2, "heavy-rain", 106, lTimeExtents[2], lTimeExtents[3], Long.MIN_VALUE, "3 Locations", 388689479, -947734071, 389867615, -946212850));
					oOut.write(",");
					oOut.write(format(3, "heavy-rain", 106, lTimeExtents[2], lTimeExtents[3], Long.MIN_VALUE, "500 Locations", 388686844, -946217087, 389863657, -944694177));
				}

				if (nTimeIndex == 3)
				{
					lTimeExtents[0] = oCal.getTimeInMillis() - 3480000;
					lTimeExtents[1] = oCal.getTimeInMillis();
					lTimeExtents[2] = oCal.getTimeInMillis() - 60000;
					lTimeExtents[3] = lTimeExtents[2] + 1740000;
					lTimeExtents[4] = oCal.getTimeInMillis();
					lTimeExtents[5] = lTimeExtents[4] + 1200000;

					oOut.write(format(2, "heavy-rain", 106, lTimeExtents[2], lTimeExtents[3], Long.MIN_VALUE, "3 Locations", 388689479, -947734071, 389867615, -946212850));
					oOut.write(",");
					oOut.write(format(3, "heavy-rain", 106, lTimeExtents[2], lTimeExtents[3], Long.MIN_VALUE, "500 Locations", 388686844, -946217087, 389863657, -944694177));
					oOut.write(",");
					oOut.write(format(4, "icy-bridge", 202, lTimeExtents[4], lTimeExtents[5], Long.MIN_VALUE, "E Blue Ridge Blvd WB<br/>E Blue Ridge Blvd EB", 388894572 - 10000, -945806132 - 10000, 388894572 + 10000, -945806132 + 10000));
				}

				if (nTimeIndex == 4)
				{
					lTimeExtents[2] = oCal.getTimeInMillis() - 120000;
					lTimeExtents[3] = lTimeExtents[2] + 1680000;
					lTimeExtents[4] = oCal.getTimeInMillis() - 60000;
					lTimeExtents[5] = lTimeExtents[4] + 1140000;

					oOut.write(format(2, "heavy-rain", 106, lTimeExtents[2], lTimeExtents[3], oCal.getTimeInMillis(), "3 Locations", 388689479, -947734071, 389867615, -946212850));
					oOut.write(",");
					oOut.write(format(3, "heavy-rain", 106, lTimeExtents[2], lTimeExtents[3], oCal.getTimeInMillis(), "500 Locations", 388686844, -946217087, 389863657, -944694177));
					oOut.write(",");
					oOut.write(format(4, "icy-bridge", 202, lTimeExtents[4], lTimeExtents[5], Long.MIN_VALUE, "E Blue Ridge Blvd WB<br/>E Blue Ridge Blvd EB", 388894572 - 10000, -945806132 - 10000, 388894572 + 10000, -945806132 + 10000));
				}

				oOut.write("\n]\n"); // finish JSON array
			}
			catch (Exception oEx)
			{
				LOGGER.info(oEx, oEx);
			}

		}
	}


	private String format(Object... args)
	{
		for (int i = args.length - 4; i < args.length; ++i)
			args[i] = GeoUtil.fromIntDeg((Integer) args[i]);

		if (m_bTest)
		{
			Object[] newArgs = new Object[12];
			System.arraycopy(args, 0, newArgs, 0, 7);
			System.arraycopy(args, 7, newArgs, 8, 4);
			newArgs[7] = (Long) args[3] - 1000 * 60 * 15;
			return String.format(JSON, newArgs);
		}
		return String.format(JSON, args);
	}


	private long getId(AlertObs oNotification) throws Exception
	{
		ByteArrayOutputStream oOutBytes = new ByteArrayOutputStream();
		DataOutputStream oOutData = new DataOutputStream(oOutBytes);
		oOutData.writeInt((int)oNotification.m_dValue);
		oOutData.writeLong(oNotification.m_lTimeRecv);
		oOutData.writeLong(oNotification.m_lObsTime1);
		int nLocations = 1;
		if (oNotification.m_sDetail.contains("Location"))
			nLocations = Integer.parseInt(oNotification.m_sDetail.substring(0, oNotification.m_sDetail.indexOf(" ")));
		else if (oNotification.m_sDetail.contains(";"))
			nLocations = 2;
		oOutData.writeInt(nLocations);
		oOutData.writeInt(oNotification.m_nLat1);
		oOutData.writeInt(oNotification.m_nLon1);
		oOutData.writeInt(oNotification.m_nLat2);
		oOutData.writeInt(oNotification.m_nLon2);
		CRC32 oCRC32 = new CRC32();
		oCRC32.update(oOutBytes.toByteArray());
		return oCRC32.getValue();
	}
}
