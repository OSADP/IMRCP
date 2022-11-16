/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import imrcp.system.dbf.DbfResultSet;
import imrcp.system.shp.Header;
import imrcp.system.shp.Polyline;
import imrcp.system.shp.PolyshapeIterator;
import imrcp.system.Arrays;
import imrcp.system.Directory;
import imrcp.system.Id;
import imrcp.system.ObsType;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

/**
 * FileWrapper for parsing .tar.gz shapefiles containing alerts from the National
 * Weather Service's Common Alerting Protocol system
 * @author Federal Highway Administration
 */
public class CapWrapper extends FileWrapper
{
	/**
	 * Contains the CAP alerts defined in this file
	 */
	ArrayList<CAPObs> m_oObs = new ArrayList();
	
	
	/**
	 * Decompresses the .tar.gz file and loads the .shp and .dbf files into 
	 * memory to be parsed to create observations for the defined CAP alerts
	 * that get added to {@link #m_oObs}
	 */
	@Override
	public void load(long lStartTime, long lEndTime, long lValidTime, String sFilename, int nContribId) throws Exception
	{
		byte[] yShp = null;
		byte[] yDbf = null;
		
		try (TarArchiveInputStream oTar = new TarArchiveInputStream(new GzipCompressorInputStream(new BufferedInputStream(Files.newInputStream(Paths.get(sFilename))))))
		{
			TarArchiveEntry oEntry = null;
			while ((oEntry = oTar.getNextTarEntry()) != null) // search through the files in the .tar
			{
				byte[] yBuffer = null;
				if (oEntry.getName().endsWith(".shp")) // read the .shp file into memory
				{
					long lSize = oEntry.getSize();
					yShp = new byte[(int)lSize];
					yBuffer = yShp;
				}
				if (oEntry.getName().endsWith(".dbf")) // read the .shp file into memory
				{
					long lSize = oEntry.getSize();
					yDbf = new byte[(int)lSize];
					yBuffer = yDbf;
				}
				
				if (yBuffer != null)
				{
					int nOffset = 0;
					int nBytesRead = 0;
					while (nOffset < yBuffer.length && (nBytesRead = oTar.read(yBuffer, nOffset, yBuffer.length - nOffset)) >= 0)
						nOffset += nBytesRead;
				}
			}
		}
		
		if (yShp == null || yDbf == null)
			throw new Exception("Error reading tar.gz file");
		
		SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
		oSdf.setTimeZone(Directory.m_oUTC);
		try (DbfResultSet oDbf = new DbfResultSet(new BufferedInputStream(new ByteArrayInputStream(yDbf)));
			 DataInputStream oShp = new DataInputStream(new BufferedInputStream(new ByteArrayInputStream(yShp))))
		{
			new Header(oShp); // read through shp header
			PolyshapeIterator oIter = null;
			// find the correct column for each of the needed parameters
			int nExCol = oDbf.findColumn("EXPIRATION"); // end time
			int nOnCol = oDbf.findColumn("ONSET"); // start time
			int nIsCol = oDbf.findColumn("ISSUANCE"); // time received
			int nIdCol = oDbf.findColumn("CAP_ID"); // cap id
			int nTypeCol = oDbf.findColumn("PROD_TYPE"); // alert type
			int nUrlCol = oDbf.findColumn("URL"); // cap url
			while (oDbf.next()) // for each record in the .dbf
			{
				Polyline oPoly = new Polyline(oShp, true); // there is a polygon defined in the .shp (Polyline object reads both polylines and polygons
				oIter = oPoly.iterator(oIter);
				String sType = oDbf.getString(nTypeCol);
				CAPObs oObs = new CAPObs(ObsType.EVT, nContribId, Id.NULLID, oSdf.parse(oDbf.getString(nOnCol)).getTime(), oSdf.parse(oDbf.getString(nExCol)).getTime(), oSdf.parse(oDbf.getString(nIsCol)).getTime(),
					oPoly.m_nYmin, oPoly.m_nXmin, oPoly.m_nYmax, oPoly.m_nXmax, Short.MIN_VALUE, ObsType.lookup(ObsType.EVT, sType), Short.MIN_VALUE, sType, oDbf.getString(nIdCol), oDbf.getString(nUrlCol));
				while (oIter.nextPart()) // can be a multipolygon so make sure to read each part of the polygon
				{
					int[] nPart = Arrays.newIntArray();
					while (oIter.nextPoint())
						nPart = Arrays.add(nPart, oIter.getX(), oIter.getY());
					oObs.m_oPoly.add(nPart);
				}
				
				m_oObs.add(oObs);
			}
		}
		setTimes(lValidTime, lStartTime, lEndTime);
		m_sFilename = sFilename;
		m_nContribId = nContribId;
	}

	
	/**
	 * Does nothing for CapWrappers
	 */
	@Override
	public void cleanup(boolean bDelete)
	{
	}
}
