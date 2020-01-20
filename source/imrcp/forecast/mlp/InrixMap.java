package imrcp.forecast.mlp;


import imrcp.system.CsvReader;
import imrcp.system.Util;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Collections;
import java.util.Comparator;


public class InrixMap// implements Comparator<String>
{
	ArrayList<InrixData> m_oTmcMap = new ArrayList();


	public InrixMap()
	{
	}


	private class InrixData extends ArrayList<String[]> 
		implements Comparable<InrixData>, Comparator<String[]>
	{
		CharSequence m_sCode;


		InrixData()
		{
		}


		public InrixData(CharSequence sCode, int nCapacity)
		{
			super(nCapacity);
			m_sCode = sCode.toString();
		}


		@Override
		public int compareTo(InrixData oMap)
		{
			return Util.compare(m_sCode, oMap.m_sCode);
		}


		@Override
		public int compare(String[] oLhs, String[] oRhs)
		{
			int nComp = Util.compare(oLhs[0], oRhs[0]); // compare week and day
			if (nComp == 0)
				return Util.compare(oLhs[1], oRhs[1]); // then compare hour minute

			return nComp;
		}
	}


	public void loadData(String sFilename)
		throws Exception
	{
		m_oTmcMap.clear();
		SimpleDateFormat oDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		GregorianCalendar oTs = new GregorianCalendar();

//		int nCount = 0;
		StringBuilder sBuf = new StringBuilder();
		InrixData oList = null;
		InrixData oSearch = new InrixData();
		try (CsvReader oIn = new CsvReader(new FileInputStream(sFilename)))
		{
			oIn.readLine(); // skip header row
			
			while (oIn.readLine() > 0)
			{
				oIn.parseString(sBuf, 1);
				sBuf.setLength(sBuf.length() - 1);
				sBuf.deleteCharAt(0); // trim double quotes
				
				oSearch.m_sCode = sBuf;
				if (oList == null || oList.compareTo(oSearch) != 0)
				{
					int nIndex = Collections.binarySearch(m_oTmcMap, oSearch);
					if (nIndex < 0)
					{
						oList = new InrixData(oSearch.m_sCode, 9000);
						m_oTmcMap.add(~nIndex, oList);
					}
					else
						oList = m_oTmcMap.get(nIndex);
				}
				
				oIn.parseString(sBuf, 2);
				sBuf.setLength(sBuf.length() - 1);
				sBuf.deleteCharAt(0); // trim double quotes
				oTs.setTime(oDateFormat.parse(sBuf.toString()));
				
				String[] sDayType = new String[3];
				sDayType[0] = Integer.toString(oTs.get(Calendar.WEEK_OF_MONTH)) + "-" + Integer.toString(oTs.get(Calendar.DAY_OF_WEEK));
				sDayType[1] = sBuf.substring(11); // remove date
				oIn.parseString(sBuf, 3);
				sDayType[2] = sBuf.toString();
				oList.add(sDayType);
				
//			++nCount;
//			if (nCount % 100000 == 0)
//				System.out.println(nCount);
			}
		}
	}


	public int[] getData(String sTmcCode, long lStartTs, int nIntervals)
	{
		InrixData oTmcSearch = new InrixData();
		oTmcSearch.m_sCode = sTmcCode;
		int nIndex = Collections.binarySearch(m_oTmcMap, oTmcSearch);
		if (nIndex < 0)
			return null;


		InrixData oList = m_oTmcMap.get(nIndex);
		GregorianCalendar oTs = new GregorianCalendar();
		oTs.setTimeInMillis(lStartTs);

		String[] sSearch = new String[2];
		sSearch[0] = Integer.toString(oTs.get(Calendar.WEEK_OF_MONTH)) + "-" + Integer.toString(oTs.get(Calendar.DAY_OF_WEEK));
		sSearch[1] = String.format("%02d:%02d:00", oTs.get(Calendar.HOUR_OF_DAY), oTs.get(Calendar.MINUTE));
		int nEnd = Collections.binarySearch(oList, sSearch, oList);
		if (nEnd < 0)
			return null;


		ArrayList<String[]> oResults = new ArrayList(nIntervals);

		if (nIntervals > nEnd) // check for wrap around
		{
			int nDayOfWeek = Integer.parseInt(oList.get(0)[0].substring(2));
			if (nDayOfWeek == Calendar.SUNDAY)
				nDayOfWeek = Calendar.SATURDAY;
			else
				--nDayOfWeek;

			String sDayOfWeek = Integer.toString(nDayOfWeek);
			nIndex = oList.size(); // brute force find the previous day of week
			while (nIndex-- > 0 && !oList.get(nIndex)[0].endsWith(sDayOfWeek));

			int nStart = nIndex - (nIntervals - nEnd);
			while (nStart++ < nIndex)
				oResults.add(oList.get(nStart)); // fetch extra records

			nIntervals = nEnd;
		}

		for (nIndex = nEnd - nIntervals; nIndex < nEnd; nIndex++)
			oResults.add(oList.get(nIndex));

		int[] nData = new int[oResults.size()];
		for (nIndex = 0; nIndex < nData.length; nIndex++) // copy speeds to output array
			nData[nIndex] = Integer.parseInt(oResults.get(nIndex)[2]);

		return nData;
	}


	public static void main(String[] sArgs)
		throws Exception
	{
		GregorianCalendar oNow = new GregorianCalendar();
		oNow.set(Calendar.MINUTE, 15);
		oNow.set(Calendar.SECOND, 0);
		oNow.set(Calendar.MILLISECOND, 0);

		InrixMap oMap = new InrixMap();
		oMap.loadData("C:/Users/bryan.krueger/Inrix1810Truncated.csv");
		int[] nSpeeds = oMap.getData("119+04368", oNow.getTimeInMillis(), 288 * 31);
		System.out.println(nSpeeds.length);

//		ArrayList<String> oLines = new ArrayList();
//		BufferedReader oIn = new BufferedReader(new FileReader("C:/Users/bryan.krueger/Inrix1810.csv"));
//		String sHeader = oIn.readLine();
//		String sLine;
//		while ((sLine = oIn.readLine()) != null)
//			oLines.add(sLine);
//
//		oIn.close();
//		Collections.sort(oLines, oMap);
//
//		BufferedWriter oOut = new BufferedWriter(new FileWriter("C:/Users/bryan.krueger/Inrix1810Sorted.csv"));
//		oOut.write(sHeader);
//		oOut.write("\n");
//		for (String sRow : oLines)
//		{
//			oOut.write(sRow);
//			oOut.write("\n");
//		}
//		oOut.close();
	}
}
