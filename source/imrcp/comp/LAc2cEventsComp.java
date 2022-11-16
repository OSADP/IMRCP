/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.comp;

import imrcp.store.EventObs;
import imrcp.store.LaDOTDEvent;
import imrcp.system.Text;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.zip.GZIPInputStream;
import javax.xml.parsers.SAXParserFactory;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.ext.DefaultHandler2;

/**
 * Processes the event xml files downloaded from LADOTD's c2c feed and 
 * converts them to IMRCP's incident and work zone .csv file format
 * @author Federal Highway Administration
 */
public class LAc2cEventsComp extends EventComp
{
	@Override
	public void reset()
	{
		super.reset();
		m_sDateFormat = m_oConfig.getString("dateformat", "yyyyMMddHHmmssZ");
	}
	
	
	/**
	 * Wrapper for {@link LAc2cEventsComp.Parser#parse(java.io.InputStream)}
	 * @param sFile File name of LADOTD's xml event feed
	 * @param lTime Start time of the file
	 * @return The time the file was last updated
	 * @throws Exception
	 */
	@Override
	protected long getEvents(String sFile, long lTime)
		throws Exception
	{
		try (BufferedInputStream oIn = new BufferedInputStream(new GZIPInputStream(Files.newInputStream(Paths.get(sFile)))))
		{
			new Parser().parse(oIn);
		}
		
		return lTime;
	}
	
	
	/**
	 * XML parser for LAc2c Event files
	 */
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
		private LaDOTDEvent m_oEvent = new LaDOTDEvent();

		
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
		
		
		/**
		 * Default Constructor
		 */
		private Parser()
		{
			super();
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
					m_oEvent.m_sType = sQname;
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
						m_oEvent.m_nLanesAffected += Integer.parseInt(m_sBuf.toString());
					return;
				}
				
				if (m_bEventReference)
				{
					if (sQname.compareTo("event-id") == 0)
						m_oEvent.m_sExtId = m_sBuf.toString();
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
					m_oEvent.m_sDir = sDir;
				}
				
				if (sQname.compareTo("event-name") == 0)
				{
					m_oEvent.m_sEventName = m_sBuf.toString().replaceAll(",", "_");
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
					sTimeBuf = m_oEvent.m_sUpdateTime;
				else if (m_nTimePos == 1)
					sTimeBuf = m_oEvent.m_sStartTime;
				else if (m_nTimePos == 2)
					sTimeBuf = m_oEvent.m_sEndTime;
				
				if (sQname.compareTo("date") == 0 || sQname.compareTo("offset") == 0)
					sTimeBuf.append(m_sBuf);
				else if (sQname.compareTo("time") == 0)
					sTimeBuf.append(m_sBuf.subSequence(0, 6));
						
				

				if (m_bReadLonLat)
				{
					if (sQname.compareTo("latitude") == 0)
						m_oEvent.m_nLat1 = Integer.parseInt(m_sBuf.toString()) * 10; // scale to 7 decimal places
					if (sQname.compareTo("longitude") == 0)
						m_oEvent.m_nLon1 = Integer.parseInt(m_sBuf.toString()) * 10;
					return;
				}
				
				if (sQname.compareTo("FEU") == 0)
				{
					if (!m_bInactive)
					{
						int nIndex = Collections.binarySearch(m_oEvents, m_oEvent, EventObs.EXTCOMP);
						if (nIndex < 0)
						{
							LaDOTDEvent oAdd = new LaDOTDEvent(m_oEvent);
							m_oEvents.add(~nIndex, oAdd);
						}
						else
						{
							LaDOTDEvent oCur = (LaDOTDEvent)m_oEvents.get(nIndex);
							oCur.m_bOpen = true;
							if (Text.compare(oCur.m_sUpdateTime, m_oEvent.m_sUpdateTime) != 0)
							{
								oCur.copyValues(m_oEvent);
								oCur.m_bUpdated = true;
							}
						}
					}

					m_oEvent.reset();
					m_bInactive = false;
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
		private void parse(InputStream oIn)
			throws Exception
		{
			XMLReader iXmlReader = SAXParserFactory.newInstance().newSAXParser().getXMLReader();
			iXmlReader.setContentHandler(this);
			iXmlReader.parse(new InputSource(oIn));	
		}
	}
}
