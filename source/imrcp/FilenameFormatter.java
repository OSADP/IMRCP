package imrcp;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;
import java.util.TimeZone;


public class FilenameFormatter extends GregorianCalendar
{
	private String m_sPattern;
	private StringBuilder m_sBuf;


	protected FilenameFormatter()
	{
	}


	public FilenameFormatter(String sPattern)
	{
		setTimeZone(TimeZone.getTimeZone("UTC")); // set UTC time zone
		m_sPattern = sPattern; // save pattern
		m_sBuf = new StringBuilder(sPattern.length() * 2); // reserve buffer space
	}


	public synchronized String format(long lRcvd, long lStart, long lEnd, int nOffset)
	{
		m_sBuf.setLength(0); // reset buffer
		m_sBuf.append(m_sPattern);

		int nPos = m_sBuf.indexOf("%");
		while (nPos >= 0)
		{
			setTimeInMillis(lEnd);
			char cWhich = m_sBuf.charAt(nPos + 1); // switch by time reference
			if (cWhich == 'r')
				setTimeInMillis(lRcvd);
			else if (cWhich == 's')
				setTimeInMillis(lStart);

			if (m_sBuf.indexOf("yMdHm", nPos) == nPos + 2) // switch on pattern
				yMdHm(nPos);
			else if (m_sBuf.indexOf("yMdH", nPos) == nPos + 2)
				yMdH(nPos);
			else if (m_sBuf.indexOf("yMd", nPos) == nPos + 2)
				yMd(nPos);
			else if (m_sBuf.indexOf("yM", nPos) == nPos + 2)
				yM(nPos);
			else if (m_sBuf.indexOf("y", nPos) == nPos + 2)
				y(nPos);
			else if (m_sBuf.indexOf("n", nPos) == nPos + 2)
				n(nPos, nOffset);
			else if (m_sBuf.indexOf("H", nPos) == nPos + 2)
				H(nPos);
				
			nPos = m_sBuf.indexOf("%", nPos);		
		}

		return m_sBuf.toString();
	}
	
	
	public synchronized String format(long lRcvd, long lStart, long lEnd)
	{
		return format(lRcvd, lStart, lEnd, 0);
	}


	public synchronized int parse(String sFilename, long[] lTimes)
	{
		m_sBuf.setLength(0);
		m_sBuf.append(sFilename);
		int nPos = m_sBuf.lastIndexOf("/") + 1;
		int nReturn = Integer.valueOf(m_sBuf.substring(nPos, m_sBuf.indexOf("_", nPos)), 36);
		nPos = m_sBuf.length();
		set(Calendar.SECOND, 0);
		set(Calendar.MILLISECOND, 0);
		for (int i = 0; i < 3; i++)
		{
			nPos = m_sBuf.lastIndexOf("_", nPos);
			set(Calendar.YEAR, Integer.parseInt(m_sBuf.substring(nPos + 1, nPos + 5)));
			set(Calendar.MONTH, Integer.parseInt(m_sBuf.substring(nPos + 5, nPos + 7)) - 1);
			set(Calendar.DAY_OF_MONTH, Integer.parseInt(m_sBuf.substring(nPos + 7, nPos + 9)));
			set(Calendar.HOUR_OF_DAY, Integer.parseInt(m_sBuf.substring(nPos + 9, nPos + 11)));
			set(Calendar.MINUTE, Integer.parseInt(m_sBuf.substring(nPos + 11, nPos + 13)));
			lTimes[i] = getTimeInMillis();
			--nPos;
		}
		
		return nReturn;
	}
	
	
	public synchronized int parseRecv(String sFilename, long[] lTimes)
	{
		m_sBuf.setLength(0);
		m_sBuf.append(sFilename);
		int nOffset = 0;
		int nPos = m_sPattern.indexOf("%r");
		int nCount = 0;
		set(Calendar.MINUTE, 0);
		set(Calendar.SECOND, 0);
		set(Calendar.MILLISECOND, 0);
		while (nPos >= 0)
		{
			int nParsePos = nPos - nCount++ * 2;
			if (m_sPattern.indexOf("yyyyMMdd", nPos) == nPos + 2)
				setyyyyMMdd(nParsePos);
			else if (m_sPattern.indexOf("HHmmss", nPos) == nPos + 2)
				setHHmmss(nParsePos);
			else if (m_sPattern.indexOf("HHmm", nPos) == nPos + 2)
				setHHmm(nParsePos);
			else if (m_sPattern.indexOf("HH", nPos) == nPos + 2)
				setHH(nParsePos);
			else if (m_sPattern.indexOf("nn", nPos) == nPos + 2)
				nOffset = Integer.parseInt(m_sBuf.substring(nParsePos, nParsePos + 2));
			
			
			
			
			nPos = m_sPattern.indexOf("%r", nPos + 1);
		}
		
		lTimes[0] = getTimeInMillis();
		return nOffset;
	}
	
	public boolean contains(CharSequence sSeq)
	{
		return m_sPattern.contains(sSeq);
	}

	
	protected void setyyyyMMdd(int nPos)
	{
		set(Calendar.YEAR, Integer.parseInt(m_sBuf.substring(nPos, nPos + 4)));
		set(Calendar.MONTH, Integer.parseInt(m_sBuf.substring(nPos + 4, nPos + 6)) - 1); // months are 0 based so subtract one
		set(Calendar.DAY_OF_MONTH, Integer.parseInt(m_sBuf.substring(nPos + 6, nPos + 8)));
	}
	
	
	protected void setHH(int nPos)
	{
		set(Calendar.HOUR_OF_DAY, Integer.parseInt(m_sBuf.substring(nPos, nPos + 2)));
	}
	
	
	protected void setHHmm(int nPos)
	{
		set(Calendar.HOUR_OF_DAY, Integer.parseInt(m_sBuf.substring(nPos, nPos + 2)));
		set(Calendar.MINUTE, Integer.parseInt(m_sBuf.substring(nPos + 2, nPos + 4)));
	}
	
	
	protected void setHHmmss(int nPos)
	{
		set(Calendar.HOUR_OF_DAY, Integer.parseInt(m_sBuf.substring(nPos, nPos + 2)));
		set(Calendar.MINUTE, Integer.parseInt(m_sBuf.substring(nPos + 2, nPos + 4)));
		set(Calendar.SECOND, Integer.parseInt(m_sBuf.substring(nPos + 4, nPos + 6)));
	}
	protected void yMdHm(int nPos)
	{
		m_sBuf.delete(nPos, nPos + 7); // remove %?yMdHm pattern
		zeroPad(m_sBuf, nPos, get(Calendar.MINUTE));
		zeroPad(m_sBuf, nPos, get(Calendar.HOUR_OF_DAY));
		zeroPad(m_sBuf, nPos, get(Calendar.DAY_OF_MONTH));
		zeroPad(m_sBuf, nPos, get(Calendar.MONTH) + 1);
		m_sBuf.insert(nPos, get(Calendar.YEAR)); // should contain at least 4-digits
	}


	protected void yMdH(int nPos)
	{
		m_sBuf.delete(nPos, nPos + 6); // remove %?yMdH pattern
		zeroPad(m_sBuf, nPos, get(Calendar.HOUR_OF_DAY));
		zeroPad(m_sBuf, nPos, get(Calendar.DAY_OF_MONTH));
		zeroPad(m_sBuf, nPos, get(Calendar.MONTH) + 1);
		m_sBuf.insert(nPos, get(Calendar.YEAR)); // should contain at least 4-digits
	}


	protected void yMd(int nPos)
	{
		m_sBuf.delete(nPos, nPos + 5); // remove %?yMd pattern
		zeroPad(m_sBuf, nPos, get(Calendar.DAY_OF_MONTH));
		zeroPad(m_sBuf, nPos, get(Calendar.MONTH) + 1);
		m_sBuf.insert(nPos, get(Calendar.YEAR)); // should contain at least 4-digits
	}


	protected void yM(int nPos)
	{
		m_sBuf.delete(nPos, nPos + 4); // remove %?yM pattern
		zeroPad(m_sBuf, nPos, get(Calendar.MONTH) + 1);
		m_sBuf.insert(nPos, get(Calendar.YEAR)); // should contain at least 4-digits
	}


	protected void y(int nPos)
	{
		m_sBuf.delete(nPos, nPos + 3); // remove %?y pattern
		m_sBuf.insert(nPos, get(Calendar.YEAR)); // should contain at least 4-digits
	}
	
	
	protected void H(int nPos)
	{
		m_sBuf.delete(nPos, nPos + 3); // remove %?H pattern
		zeroPad(m_sBuf, nPos, get(Calendar.HOUR_OF_DAY));
	}
	
	
	protected void n(int nPos, int nOffset)
	{
		m_sBuf.delete(nPos, nPos + 3); // remove %?n pattern
		zeroPad(m_sBuf, nPos, nOffset);
	}


	private static void zeroPad(StringBuilder sBuf, int nPos, int nVal)
	{
		sBuf.insert(nPos, nVal);
		if (nVal < 10)
			sBuf.insert(nPos, "0"); // zero pad
	}
	
	
	public String getExtension()
	{
		return m_sPattern.substring(m_sPattern.lastIndexOf("."));
	}
	
	public static void main(String[] sArgs)
	{
		FilenameFormatter oFf = new FilenameFormatter("ftp://ftpprd.ncep.noaa.gov/pub/data/nccf/com/rap/prod/rap.%ryyyyMMdd/rap.t%rHHz.awp130pgrbf%rnn.grib2");
		long[] lTimes = new long[3];
		SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd HHmmss");
		oSdf.setTimeZone(new SimpleTimeZone(0, ""));
		int nOffset = oFf.parseRecv("ftp://ftpprd.ncep.noaa.gov/pub/data/nccf/com/rap/prod/rap.20191125/rap.t02z.awp130pgrbf04.grib2", lTimes);
		System.out.println(nOffset);
		System.out.println(oSdf.format(lTimes[0]));
		
		oFf = new FilenameFormatter("https://mrms.ncep.noaa.gov/data/2D/MergedBaseReflectivityQC/MRMS_MergedBaseReflectivityQC_00.00_%ryyyyMMdd-%rHHmm00.grib2.gz");
		lTimes = new long[3];
		nOffset = oFf.parseRecv("https://mrms.ncep.noaa.gov/data/2D/MergedBaseReflectivityQC/MRMS_MergedBaseReflectivityQC_00.00_20191124-221744.grib2.gz", lTimes);
		System.out.println(nOffset);
		System.out.println(oSdf.format(lTimes[0]));
		
		oFf = new FilenameFormatter("ftp://ftp.ncep.noaa.gov/pub/data/nccf/com/rtma/prod/rtma2p5.%ryyyyMMdd/rtma2p5.t%rHHz.2dvarges_ndfd.grb2_wexp");
		lTimes = new long[3];
		nOffset = oFf.parseRecv("ftp://ftp.ncep.noaa.gov/pub/data/nccf/com/rtma/prod/rtma2p5.20191124/rtma2p5.t02z.2dvarges_ndfd.grb2_wexp", lTimes);
		System.out.println(nOffset);
		System.out.println(oSdf.format(lTimes[0]));
		
	}
}
