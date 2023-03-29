/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package imrcp.collect;

import imrcp.store.Obs;
import imrcp.system.Directory;
import imrcp.system.FilenameFormatter;
import imrcp.system.ObsType;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.ext.DefaultHandler2;

/**
 *
 * @author Federal Highway Administration
 */
public class LAc2cFEU extends LAc2c
{
	String m_sDateFormat = "yyyyMMddHHmmssZ";

	@Override 
	protected void parseXml(byte[] yXml, long[] lTimes)
		throws Exception
	{
		new Parser().parse(new BufferedInputStream(new ByteArrayInputStream(yXml)), lTimes);
	}
	
	private class Parser extends DefaultHandler2
	{
		/**
		 * Buffer to store characters in when {@link DefaultHandler2#characters(char[], int, int)}
		 * is called
		 */
		private StringBuilder m_sBuf = new StringBuilder();


		/**
		 * Stores data of the current event
		 */
		private Obs m_oEvent = new Obs();

		
		/**
		 * Flag used to mark if the parser is in the "event-headline" tag
		 */
		private boolean m_bEventHeadline = false;

		
		/**
		 * Flag used to mark if the type of the event should be parsed
		 */
		private boolean m_bGetType = false;

		
		/**
		 * Flag used to mark if the parser is in the "event-reference" tag
		 */
		private boolean m_bEventReference = false;

		
		/**
		 * Flag used to mark if the parser can skip the current tag
		 */
		private boolean m_bSkip = false;

		
		/**
		 * Counter used to determine which timestamp is being parsed
		 */
		private int m_nTimePos = -1;

		
		/** 
		 * Flag used to mark if the parser is in the "primary-location" tag to
		 * read the longitude and latitude
		 */
		private boolean m_bReadLonLat = false;

		
		/**
		 * Flag used to mark if the parser is in the "event-lanes" tag
		 */
		private boolean m_bReadLanes = false;

		
		/**
		 * Flag used to mark if the parser is in the "location-on-link" tag
		 */
		private boolean m_bLocationOnLink = false;

		
		/**
		 * Flag used to mark if the current "property-name" is "IsInactiveStatus"
		 */
		private boolean m_bReadInactive = false;

		
		/**
		 * Flag used to mark if the event is inactive
		 */
		private boolean m_bInactive = false;
		
		
		private int m_nLon;
		private int m_nLat;
		StringBuilder m_sStart = new StringBuilder(19);
		StringBuilder m_sEnd = new StringBuilder(19);
		StringBuilder m_sUpdate = new StringBuilder(19);
		SimpleDateFormat m_oSdf;
		long[] m_lTimes;
		
		/**
		 * Default Constructor
		 */
		private Parser()
		{
			super();
			m_oSdf = new SimpleDateFormat(m_sDateFormat);
			m_oSdf.setTimeZone(Directory.m_oUTC);
		}
		
		
		@Override
		public void characters(char[] cBuf, int nPos, int nLen)
		{
			m_sBuf.setLength(0);
			m_sBuf.append(cBuf, nPos, nLen);
		}
		
		
		@Override
		public void startElement(String sUri, String sLocalName, 
			String sQname, Attributes iAtt)
		{
			
			if (sQname.compareTo("event-reference") == 0)
				m_bEventReference = true;
			else if (sQname.compareTo("location-on-link") == 0)
				m_bLocationOnLink = true;
			else if (sQname.compareTo("update-time") == 0)
				m_nTimePos = 0;
			else if (sQname.compareTo("expected-start-time") == 0)
				m_nTimePos = 1;
			else if (sQname.compareTo("expected-end-time") == 0)
				m_nTimePos = 2;
			else if (sQname.compareTo("primary-location") == 0)
				m_bReadLonLat = true;
			else if (sQname.compareTo("event-headline") == 0)
				m_bEventHeadline = true;
			else if (sQname.compareTo("headline") == 0)
				m_bGetType = true;
			else if (sQname.compareTo("event-lanes") == 0)
				m_bReadLanes = true;
			else if (sQname.compareTo("message-header") == 0 || sQname.compareTo("other-references") == 0 || sQname.compareTo("event-properties") == 0)
				m_bSkip = true;
		}
		
		
		@Override
		public void endElement(String sUri, String sLocalName, String sQname)
		{
			try
			{
				if (sQname.compareTo("message-header") == 0 || sQname.compareTo("other-references") == 0 || sQname.compareTo("event-properties") == 0)
					m_bSkip = false;
				if (m_bSkip)
					return;
				if (sQname.compareTo("event-headline") == 0)
				{
					m_bEventHeadline = false;
					return;
				}
				if (sQname.compareTo("headline") == 0)
				{
					m_bGetType = false;
					return;
				}
				if (sQname.compareTo("event-lanes") == 0)
				{
					m_bReadLanes = false;
					return;
				}
				if (sQname.compareTo("event-reference") == 0)
				{
					m_bEventReference = false;
					return;
				}
				
				if (sQname.compareTo("location-on-link") == 0)
				{
					m_bLocationOnLink = false;
					return;
				}
				
				if (m_bEventHeadline && m_bGetType)
				{
					m_oEvent.m_sStrings[2] = sQname;
					return;
				}
				if (sQname.compareTo("update-time") == 0 || sQname.compareTo("expected-start-time") == 0 || sQname.compareTo("expected-end-time") == 0)
				{
					m_nTimePos = -1;
					return;
				}
				if (sQname.compareTo("primary-location") == 0)
				{
					m_bReadLonLat = false;
					return;
				}
				
				if (m_bReadLanes)
				{
					if (sQname.compareTo("lanes-total-affected") == 0)
					{
						m_oEvent.m_sStrings[3] = Integer.toString(Integer.parseInt(m_oEvent.m_sStrings[3]) + Integer.parseInt(m_sBuf.toString()));
					}
					return;
				}
				
				if (m_bEventReference)
				{
					if (sQname.compareTo("event-id") == 0)
						m_oEvent.m_sStrings[0] = m_sBuf.toString();
					return;
				}
				
				if (m_bLocationOnLink && sQname.compareTo("link-direction") == 0)
				{
					String sDir = m_sBuf.toString();
					if (sDir.length() > 2) // don't need to do anything for "regular" directions like n, ne, s, sw
					{
						if (sDir.compareTo("both directions") == 0)
							sDir = "b";
						else
							sDir = "u"; // unknown
					}
					m_oEvent.m_sStrings[4] = sDir;
				}
				
				if (sQname.compareTo("event-name") == 0)
				{
					m_oEvent.m_sStrings[1] = m_sBuf.toString().replaceAll(",", "_");
					return;
				}
				
				if (m_bReadInactive)
				{
					m_bInactive = Boolean.parseBoolean(m_sBuf.toString());
					m_bReadInactive = false;
				}
				
				if (sQname.compareTo("property-name") == 0 && m_sBuf.toString().compareTo("IsInactiveStatus") == 0)
				{
					m_bReadInactive = true;
				}


				StringBuilder sTimeBuf = new StringBuilder();
				if (m_nTimePos == 0)
					sTimeBuf = m_sUpdate;
				else if (m_nTimePos == 1)
					sTimeBuf = m_sStart;
				else if (m_nTimePos == 2)
					sTimeBuf = m_sEnd;
				
				if (sQname.compareTo("date") == 0 || sQname.compareTo("offset") == 0)
					sTimeBuf.append(m_sBuf);
				else if (sQname.compareTo("time") == 0)
					sTimeBuf.append(m_sBuf.subSequence(0, 6));
						
				

				if (m_bReadLonLat)
				{
					if (sQname.compareTo("latitude") == 0)
						m_nLat = Integer.parseInt(m_sBuf.toString()) * 10; // scale to 7 decimal places
					if (sQname.compareTo("longitude") == 0)
						m_nLon = Integer.parseInt(m_sBuf.toString()) * 10;
					return;
				}
				
				if (sQname.compareTo("FEU") == 0)
				{
					if (!m_bInactive)
					{
						m_oEvent.m_lObsTime1 = m_oSdf.parse(m_sStart.toString()).getTime();
						m_oEvent.m_lObsTime2 = m_oSdf.parse(m_sEnd.toString()).getTime();
						m_oEvent.m_lTimeRecv = m_oSdf.parse(m_sUpdate.toString()).getTime();
						m_oEvent.m_oGeo = Obs.createPoint(m_nLon, m_nLat);
						m_oEvent.m_dValue = ObsType.lookup(ObsType.EVT, m_oEvent.m_sStrings[2].compareTo("roadwork") == 0 || m_oEvent.m_sStrings[2].compareTo("workzone") == 0 ? "workzone" : "incident");
						if (m_oEvent.m_sStrings[3] == null)
							m_oEvent.m_sStrings[3] = "0";
						if (m_oEvent.m_sStrings[4] == null)
							m_oEvent.m_sStrings[4] = "u";
						m_oObs.add(m_oEvent);
						
						if (m_oEvent.m_lObsTime1 < m_lTimes[FilenameFormatter.START])
							m_lTimes[FilenameFormatter.START] = m_oEvent.m_lObsTime1;
						if (m_oEvent.m_lObsTime2 > m_lTimes[FilenameFormatter.END])
							m_lTimes[FilenameFormatter.END] = m_oEvent.m_lObsTime2;
					}

					m_oEvent = new Obs();
					m_oEvent.m_sStrings = new String[8];
					m_oEvent.m_sStrings[3] = "0";
					m_bInactive = false;
					m_sStart.setLength(0);
					m_sEnd.setLength(0);
					m_sUpdate.setLength(0);
				}
			}
			catch (Exception oEx)
			{
				oEx.printStackTrace();
			}
		}
		
		
		/**
		 * Wrapper for {@link XMLReader#parse(org.xml.sax.InputSource)}
		 * @param oIn InputStream of xml file
		 * @throws Exception
		 */
		private void parse(InputStream oIn, long[] lTimes)
			throws Exception
		{
			m_oEvent.m_sStrings = new String[8];
			m_oEvent.m_sStrings[3] = "0";
			m_lTimes = lTimes;
			XMLReader iXmlReader = SAXParserFactory.newInstance().newSAXParser().getXMLReader();
			iXmlReader.setContentHandler(this);
			iXmlReader.parse(new InputSource(oIn));	
		}
	}
}
