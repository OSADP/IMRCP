/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.collect;

import imrcp.BaseBlock;
import imrcp.FileCache;
import imrcp.FilenameFormatter;
import imrcp.store.FileWrapper;
import imrcp.store.GribEntryData;
import imrcp.store.GribWrapper;
import imrcp.store.NcfEntryData;
import imrcp.store.NcfWrapper;
import imrcp.store.WeatherStore;
import imrcp.store.grib.GribParameter;
import imrcp.system.CsvReader;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import imrcp.system.Scheduling;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;
import ucar.ma2.Index;
import ucar.unidata.geoloc.projection.LambertConformal;

/**
 *
 * @author Federal Highway Administration
 */
public class PcCat extends BaseBlock
{
	private String m_sMRMS;
	private String m_sRAP;
	private FilenameFormatter m_oMrmsFormat;
	private FilenameFormatter m_oRapFormat;
	private FilenameFormatter m_oMrmsPcCatFormat;
	private FilenameFormatter m_oRapPcCatFormat;
	private String m_sRTMAStore;
	/**
	 * Array of ObsType Ids used for the file
	 */
	protected int[] m_nObsTypes;

	/**
	 * Array of titles of ObsTypes used in the NetCDF file
	 */
	protected String[] m_sObsTypes;

	/**
	 * Title for the horizontal axis in the NetCDF file
	 */
	protected String m_sHrz;

	/**
	 * Title for the vertical axis in the NetCDF file
	 */
	protected String m_sVrt;

	/**
	 * Title for the time axis in the NetCDF file
	 */
	protected String m_sTime;
	
	private GribParameter[] m_nParameters;
	private int[] m_nGribObs;
	private int[] m_nSubObsTypes;
	private int m_nRunsPerPeriod;
	private int m_nMaxQueue;
	private final ArrayDeque<String> m_oFilesToRun = new ArrayDeque();
	private String m_sQueueFile;
	private int m_nPeriod;
	private int m_nOffset;
	private String m_sCurrentFile = null;
	
	
	@Override
	public void reset()
	{
		m_sMRMS = m_oConfig.getString("mrms", "RadarPrecip");
		m_sRAP = m_oConfig.getString("rap", "RAP");
		m_sRTMAStore = m_oConfig.getString("rtmastore", "RTMAStore");
		m_oMrmsFormat = new FilenameFormatter(m_oConfig.getString("mrmsformat", ""));
		m_oMrmsPcCatFormat = new FilenameFormatter(m_oConfig.getString("mrmspccat", ""));
		m_oRapFormat = new FilenameFormatter(m_oConfig.getString("rapformat", ""));
		m_oRapPcCatFormat = new FilenameFormatter(m_oConfig.getString("rappccat", ""));
		String[] sObsTypes = m_oConfig.getStringArray("obsid", null);
		m_nObsTypes = new int[sObsTypes.length];
		for (int i = 0; i < sObsTypes.length; i++)
			m_nObsTypes[i] = Integer.valueOf(sObsTypes[i], 36);
		m_sObsTypes = m_oConfig.getStringArray("obs", null);
		m_sHrz = m_oConfig.getString("hrz", "x");
		m_sVrt = m_oConfig.getString("vrt", "y");
		m_sTime = m_oConfig.getString("time", "time");
		String[] sParameterInfo = m_oConfig.getStringArray("parameters", "");
		m_nParameters = new GribParameter[sParameterInfo.length / 4];
		m_nGribObs = new int[m_nParameters.length];
		int nCount = 0;
		for (int i = 0; i < m_nParameters.length; i += 4)
		{
			m_nParameters[nCount] = new GribParameter(Integer.valueOf(sParameterInfo[i], 36), Integer.parseInt(sParameterInfo[i + 1]), Integer.parseInt(sParameterInfo[i + 2]), Integer.parseInt(sParameterInfo[i + 3]));
			m_nGribObs[nCount++] = Integer.valueOf(sParameterInfo[i], 36);
		}
		sObsTypes = m_oConfig.getStringArray("subobs", null);
		m_nSubObsTypes = new int[sObsTypes.length];
		for (int i = 0; i < sObsTypes.length; i++)
			m_nSubObsTypes[i] = Integer.valueOf(sObsTypes[i], 36);
		m_nRunsPerPeriod = m_oConfig.getInt("runs", 4);
		m_nMaxQueue = m_oConfig.getInt("maxqueue", 984);  // default is the number of rap and mrms files in a day
		m_sQueueFile = m_oConfig.getString("queuefile", "");
		m_nPeriod = m_oConfig.getInt("period", 300);
		m_nOffset = m_oConfig.getInt("offset", 0);
	}
	
	
	@Override
	public boolean start() throws Exception
	{
		if (!m_sQueueFile.isEmpty())
		{
			File oQueue = new File(m_sQueueFile);
			if (!oQueue.exists())
				oQueue.createNewFile();
			try (CsvReader oIn = new CsvReader(new FileInputStream(m_sQueueFile)))
			{
				while (oIn.readLine() > 0)
					m_oFilesToRun.addLast(oIn.parseString(0));
			}
		}
		if (m_nPeriod != 0)
			m_nSchedId = Scheduling.getInstance().createSched(this, m_nOffset, m_nPeriod);
		return true;
	}
	
	
	@Override
	public void process(String[] sMessage)
	{
		if (!m_sQueueFile.isEmpty())
			return;
		if (sMessage[MESSAGE].compareTo("file download") == 0)
		{
			ArrayList<String> oFiles = new ArrayList();
			if (sMessage[FROM].compareTo(m_sMRMS) == 0)
			{	
				for (int i = 2; i < sMessage.length; i++)
				{
					String sPcCatFile = createMrmsPcCat(sMessage[i]);
					if (sPcCatFile != null)
						oFiles.add(sPcCatFile);
				}
			}
			if (sMessage[FROM].compareTo(m_sRAP) == 0)
			{
				for (int i = 2; i < sMessage.length; i++)
				{
				   String sPcCatFile = createRapPcCat(sMessage[i]);
				   if (sPcCatFile != null)
					   oFiles.add(sPcCatFile);
				}
			}
			if (oFiles.isEmpty())
				return;
			String[] sFiles = new String[oFiles.size()];
			int nCount = 0;
			for (String sFile : oFiles)
				sFiles[nCount++] = sFile;
			notify("file download", sFiles);
		}			
	}
	
	private String createRapPcCat(String sOriginalFile)
	{
		FileWrapper oRap = getRapWrapper();
		try
		{
			long[] lTimes = new long[3];
			m_oRapFormat.parse(sOriginalFile, lTimes);
			String sFilename = m_oRapPcCatFormat.format(lTimes[FileCache.VALID], lTimes[FileCache.START], lTimes[FileCache.END]);
			if (new File(sFilename).exists())
				return null;
			m_oLogger.info("Creating PcCat file for: " + sOriginalFile);
			oRap.load(lTimes[FileCache.START], lTimes[FileCache.END], lTimes[FileCache.VALID], sOriginalFile, Integer.valueOf("IMRCP", 36));
			new File(sFilename.substring(0, sFilename.lastIndexOf(File.separator))).mkdirs();
			ByteArrayOutputStream oBytes = new ByteArrayOutputStream(524288);
			try (DataOutputStream oBytesOut = new DataOutputStream(new BufferedOutputStream(new GZIPOutputStream(oBytes) {{def.setLevel(Deflater.BEST_COMPRESSION);}})))
			{
				m_sCurrentFile = sFilename;
				NcfEntryData oRain = (NcfEntryData)oRap.getEntryByObsId(1);
				synchronized (oRain)
				{
					oBytesOut.writeInt(ObsType.PCCAT);
					oBytesOut.writeInt(1); // lambert conformal projection
					// lambert conformal parameters
					LambertConformal oLam = (LambertConformal)oRain.m_oProjProfile.m_oProj;
					oBytesOut.writeDouble(oLam.getOriginLat());
					oBytesOut.writeDouble(oLam.getOriginLon());
					oBytesOut.writeDouble(oLam.getParallelOne());
					oBytesOut.writeDouble(oLam.getParallelTwo());
					oBytesOut.writeDouble(oLam.getFalseEasting());
					oBytesOut.writeDouble(oLam.getFalseNorthing());
					oBytesOut.writeInt(oRain.getVrt() + 1);
					for (int nVrt = 0; nVrt <= oRain.getVrt(); nVrt++)
						oBytesOut.writeDouble(oRain.m_oProjProfile.m_dYs[nVrt]);
					oBytesOut.writeInt(oRain.getHrz() + 1);
					for (int nHrz = 0; nHrz <= oRain.getHrz(); nHrz++)
						oBytesOut.writeDouble(oRain.m_oProjProfile.m_dXs[nHrz]);

					NcfEntryData oSnow = (NcfEntryData)oRap.getEntryByObsId(2);
					NcfEntryData oIce = (NcfEntryData)oRap.getEntryByObsId(3);
					NcfEntryData oFRain = (NcfEntryData)oRap.getEntryByObsId(4);
					NcfEntryData oPrecipRate = (NcfEntryData)oRap.getEntryByObsId(ObsType.RTEPC);

					Index oIndex = oRain.m_oIndex;
					int nHrzDim = oRain.m_oVar.findDimensionIndex(m_sHrz);
					int nVrtDim = oRain.m_oVar.findDimensionIndex(m_sVrt);

					for (int nVrt = 0; nVrt <= oRain.getVrt(); nVrt++)
					{
						for (int nHrz = 0; nHrz <= oRain.getHrz(); nHrz++)
						{
							oIndex.setDim(nHrzDim, nHrz);
							oIndex.setDim(nVrtDim, nVrt);
							double dRain = oRain.m_oArray.getDouble(oIndex);
							double dSnow = oSnow.m_oArray.getDouble(oIndex);
							double dIce = oIce.m_oArray.getDouble(oIndex);
							double dFRain = oFRain.m_oArray.getDouble(oIndex);
							double dPrecipRate = oPrecipRate.m_oArray.getDouble(oIndex);
							
							if (oPrecipRate.m_oVar.isFillValue(dPrecipRate) || oPrecipRate.m_oVar.isInvalidData(dPrecipRate) || oPrecipRate.m_oVar.isMissing(dPrecipRate) || Double.isNaN(dPrecipRate))
							{
								oBytesOut.writeFloat(Float.NaN);
								continue;
							}
							
							double dVal = Double.NaN;
							if (dFRain == 1.0)
								dVal = ObsType.lookup(ObsType.TYPPC, "freezing-rain");
							else if (dIce == 1.0)
								dVal = ObsType.lookup(ObsType.TYPPC, "ice-pellets");
							else if (dSnow == 1.0)
								dVal = ObsType.lookup(ObsType.TYPPC, "snow");
							else if (dRain == 1.0)
								dVal = ObsType.lookup(ObsType.TYPPC, "rain");
							else if (Double.isFinite(dFRain) || Double.isFinite(dIce) || Double.isFinite(dSnow) || Double.isFinite(dRain)) //no precip
								dVal = ObsType.lookup(ObsType.TYPPC, "none");

							
							if (dVal == ObsType.lookup(ObsType.TYPPC, "none") || dPrecipRate == 0.0)
								dVal = ObsType.lookup(ObsType.PCCAT, "no-precipitation");
							else if (dVal == ObsType.lookup(ObsType.TYPPC, "rain"))
							{
								if (dPrecipRate > 0 && dPrecipRate <= ObsType.m_dLIGHTRAIN)
									dVal = ObsType.lookup(ObsType.PCCAT, "light-rain");
								else if (dPrecipRate > ObsType.m_dLIGHTRAIN && dPrecipRate <= ObsType.m_dMEDIUMRAIN)
									dVal = ObsType.lookup(ObsType.PCCAT, "moderate-rain");
								else if (dPrecipRate > ObsType.m_dMEDIUMRAIN)
									dVal = ObsType.lookup(ObsType.PCCAT, "heavy-rain");
							}
							else if (dVal == ObsType.lookup(ObsType.TYPPC, "freezing-rain"))
							{
								if (dPrecipRate > 0 && dPrecipRate <= ObsType.m_dLIGHTSNOW)
									dVal = ObsType.lookup(ObsType.PCCAT, "light-freezing-rain");
								else if (dPrecipRate > ObsType.m_dLIGHTSNOW && dPrecipRate <= ObsType.m_dMEDIUMSNOW)
									dVal = ObsType.lookup(ObsType.PCCAT, "moderate-freezing-rain");
								else if (dPrecipRate > ObsType.m_dMEDIUMSNOW)
									dVal = ObsType.lookup(ObsType.PCCAT, "heavy-freezing-rain");
							}
							else if (dVal == ObsType.lookup(ObsType.TYPPC, "snow"))
							{
								if (dPrecipRate > 0 && dPrecipRate <= ObsType.m_dLIGHTSNOW)
									dVal = ObsType.lookup(ObsType.PCCAT, "light-snow");
								else if (dPrecipRate > ObsType.m_dLIGHTSNOW && dPrecipRate <= ObsType.m_dMEDIUMSNOW)
									dVal = ObsType.lookup(ObsType.PCCAT, "moderate-snow");
								else if (dPrecipRate > ObsType.m_dMEDIUMSNOW)
									dVal = ObsType.lookup(ObsType.PCCAT, "heavy-snow");
							}
							else if (dVal == ObsType.lookup(ObsType.TYPPC, "ice-pellets"))
							{
								if (dPrecipRate > 0 && dPrecipRate <= ObsType.m_dLIGHTSNOW)
									dVal = ObsType.lookup(ObsType.PCCAT, "light-ice");
								else if (dPrecipRate > ObsType.m_dLIGHTSNOW && dPrecipRate <= ObsType.m_dMEDIUMSNOW)
									dVal = ObsType.lookup(ObsType.PCCAT, "moderate-ice");
								else if (dPrecipRate > ObsType.m_dMEDIUMSNOW)
									dVal = ObsType.lookup(ObsType.PCCAT, "heavy-ice");
							}
							else
								dVal = Double.NaN;

							oBytesOut.writeFloat((float)dVal);
						}
					}
					oBytesOut.flush();
				}
			}
			try (BufferedOutputStream oFileOut = new BufferedOutputStream(new FileOutputStream(sFilename)))
			{
				oBytes.writeTo(oFileOut);
			}
			m_sCurrentFile = null;
			m_oLogger.info("Finished PcCat file for: " + sOriginalFile);
			return sFilename;
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
			return null;
		}
		finally
		{
			oRap.cleanup(false);
		}
	}
	
	private String createMrmsPcCat(String sOriginalFile)
	{
		FileWrapper oMrms = getMrmsWrapper();
		try
		{
			long[] lTimes = new long[3];
			m_oMrmsFormat.parse(sOriginalFile, lTimes);
			long lTimestamp = lTimes[FileCache.START];
			String sFilename = m_oMrmsPcCatFormat.format(lTimes[FileCache.VALID], lTimestamp, lTimes[FileCache.END]);
			if (new File(sFilename).exists())
				return null;
			m_oLogger.info("Creating PcCat file for: " + sOriginalFile);
			oMrms.load(lTimestamp, lTimes[FileCache.END], lTimes[FileCache.VALID], sOriginalFile, Integer.valueOf("IMRCP", 36));
			new File(sFilename.substring(0, sFilename.lastIndexOf(File.separator))).mkdirs();
			WeatherStore oRtma = (WeatherStore)Directory.getInstance().lookup(m_sRTMAStore);
			long lNow = System.currentTimeMillis();
			
			FileWrapper oRtmaFile = oRtma.getFile(lTimestamp, lNow);
			if (oRtmaFile == null)
			{
				m_oLogger.error("No Rtma: cannot infer Precip Type");
				return null;
			}

			NcfEntryData oTemp = (NcfEntryData)oRtmaFile.getEntryByObsId(ObsType.TAIR);
			int[] nIndices = new int[4];
			int nLastX = Integer.MIN_VALUE;
			int nLastY = Integer.MIN_VALUE;
			double dTemp = Double.NaN;
			ByteArrayOutputStream oBytes = new ByteArrayOutputStream(524288);
			try (DataOutputStream oBytesOut = new DataOutputStream(new BufferedOutputStream(new GZIPOutputStream(oBytes) {{def.setLevel(Deflater.BEST_COMPRESSION);}})))
			{
				m_sCurrentFile = sFilename;
				GribEntryData oPrecipRate = (GribEntryData)oMrms.getEntryByObsId(ObsType.RTEPC);
				oBytesOut.writeInt(ObsType.PCCAT);
				oBytesOut.writeInt(0); // lat/lon projection, has no parameters
				oBytesOut.writeInt(oPrecipRate.getVrt() + 1);
				for (int nVrt = 0; nVrt <= oPrecipRate.getVrt(); nVrt++)
					oBytesOut.writeDouble(oPrecipRate.m_oProjProfile.m_dYs[nVrt]);
				oBytesOut.writeInt(oPrecipRate.getHrz() + 1);
				for (int nHrz = 0; nHrz <= oPrecipRate.getHrz(); nHrz++)
					oBytesOut.writeDouble(oPrecipRate.m_oProjProfile.m_dXs[nHrz]);

				double dNoPrecip = ObsType.lookup(ObsType.PCCAT, "no-precipitation");
				double dLightRain = ObsType.lookup(ObsType.PCCAT, "light-rain");
				double dModerateRain = ObsType.lookup(ObsType.PCCAT, "moderate-rain");
				double dHeavyRain = ObsType.lookup(ObsType.PCCAT, "heavy-rain");
				double dLightFRain = ObsType.lookup(ObsType.PCCAT, "light-freezing-rain");
				double dModerateFRain = ObsType.lookup(ObsType.PCCAT, "moderate-freezing-rain");
				double dHeavyFRain = ObsType.lookup(ObsType.PCCAT, "heavy-freezing-rain");
				double dLightSnow = ObsType.lookup(ObsType.PCCAT, "light-snow");
				double dModerateSnow = ObsType.lookup(ObsType.PCCAT, "moderate-snow");
				double dHeavySnow = ObsType.lookup(ObsType.PCCAT, "heavy-snow");

				for (int nVrt = 0; nVrt <= oPrecipRate.getVrt(); nVrt++)
				{
					for (int nHrz = 0; nHrz <= oPrecipRate.getHrz(); nHrz++)
					{
						double dRate = oPrecipRate.getValue(nHrz, nVrt);
						double dVal;
						
						if (dRate == 0.0)
						{
							oBytesOut.writeFloat((float)dNoPrecip);
							continue;
						}

						if (dRate < 0.0) // missing or fill value
						{
							oBytesOut.writeFloat(Float.NaN);
							continue;
						}

						oTemp.getIndices(oPrecipRate.m_oProjProfile.m_dXs[nHrz], oPrecipRate.m_oProjProfile.m_dYs[nVrt], oPrecipRate.m_oProjProfile.m_dXs[nHrz], oPrecipRate.m_oProjProfile.m_dYs[nVrt], nIndices);
						if (nIndices[0] != nLastX || nIndices[1] != nLastY)
						{
							nLastX = nIndices[0];
							nLastY = nIndices[1];
							if (nIndices[0] == 0 || nIndices[0] > oTemp.getHrz() || nIndices[1] > oTemp.getVrt() || nIndices[0] < 0 || nIndices[1] < 0)
								dTemp = Double.NaN;
							else
							{
								synchronized (oTemp)
								{
									Index oIndex = oTemp.m_oArray.getIndex();
									oIndex.setDim(oTemp.m_nHrzIndex, nIndices[0]);
									oIndex.setDim(oTemp.m_nVrtIndex, nIndices[1]);

									dTemp = oTemp.m_oArray.getDouble(oIndex);
								}
								if (oTemp.m_oVar.isFillValue(dTemp) || oTemp.m_oVar.isInvalidData(dTemp) || oTemp.m_oVar.isMissing(dTemp))
									dTemp = Double.NaN; // no valid data for specified location
							}
						}

						if (Double.isNaN(dTemp) || Double.isNaN(dRate))
						{
							dVal = Double.NaN;
						}
						else if (dTemp > ObsType.m_dRAINTEMP) // temp > 2C
						{
							if (dRate < ObsType.m_dLIGHTRAINMMPERHR)
								dVal = dLightRain;
							else if (dRate < ObsType.m_dMEDIUMRAINMMPERHR)
								dVal = dModerateRain;
							else
								dVal = dHeavyRain;
						}
						else if (dTemp > ObsType.m_dSNOWTEMP) // -2C < temp <= 2C
						{
							if (dRate < ObsType.m_dLIGHTRAINMMPERHR)
								dVal = dLightFRain;
							else if (dRate < ObsType.m_dMEDIUMRAINMMPERHR)
								dVal = dModerateFRain;
							else
								dVal = dHeavyFRain;
						}
						else // temp <= -2C
						{
							if (dRate < ObsType.m_dLIGHTSNOWMMPERHR)
								dVal = dLightSnow;
							else if (dRate < ObsType.m_dMEDIUMSNOWMMPERHR)
								dVal = dModerateSnow;
							else
								dVal = dHeavySnow;
						}
						oBytesOut.writeFloat((float)dVal);
					}
				}
				oBytesOut.flush();
			}
			try (BufferedOutputStream oFileOut = new BufferedOutputStream(new FileOutputStream(sFilename)))
			{
				oBytes.writeTo(oFileOut);
			}
			m_sCurrentFile = null;
			m_oLogger.info("Finished PcCat file for: " + sOriginalFile);
			return sFilename;
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
			return null;
		}
		finally
		{
			oMrms.cleanup(false);
		}
	}
	
	private FileWrapper getRapWrapper()
	{
		return new NcfWrapper(m_nObsTypes, m_sObsTypes, m_sHrz, m_sVrt, m_sTime);
	}
	
	
	private FileWrapper getMrmsWrapper()
	{
		return new GribWrapper(m_nGribObs, m_nParameters);
	}
	
	public void queue(String sStart, String sEnd, StringBuilder sBuffer)
	{
		if (m_sQueueFile.isEmpty())
			return;
		try
		{
			SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm");
			oSdf.setTimeZone(Directory.m_oUTC);
			long lStartTime = oSdf.parse(sStart).getTime();
			lStartTime = (lStartTime / 3600000) * 3600000;
			long lEndTime = oSdf.parse(sEnd).getTime();
			lEndTime = (lEndTime / 3600000) * 3600000;
			int nCount = 0;
			long[] lTimes = new long[3];
			ArrayList<FilenameFormatter> oFormats = new ArrayList();
			oFormats.add(m_oRapFormat);
			oFormats.add(m_oMrmsFormat);
			for (FilenameFormatter oFormat : oFormats)
			{
				long lTimestamp = lStartTime;
				while (lTimestamp < lEndTime && nCount++ < m_nMaxQueue)
				{
					String sDir = oFormat.format(lTimestamp, 0, 0);
					String sExt = oFormat.getExtension();

					sDir = sDir.substring(0, sDir.lastIndexOf("/") + 1);
					File oDir = new File(sDir);
					String[] sFiles = oDir.list();
					if (sFiles != null) // can be null if the directory doesn't exist
					{
						ArrayList<String> oFiles = new ArrayList();
						for (String sFile : sFiles)
						{
							if (sFile.endsWith(sExt))
							{
								oFormat.parse(sFile, lTimes);
								if (lTimes[FileCache.VALID] >= lTimestamp && lTimes[FileCache.VALID] < lTimestamp + 3600000)
									oFiles.add(sFile);
							}
						}
						Collections.sort(oFiles, FileCache.REFTIMECOMP);
						synchronized (m_oFilesToRun)
						{
							for (String sFile : oFiles)
								m_oFilesToRun.addLast(sDir + sFile);
						}
					}
					
					lTimestamp += 3600000;
				}
			}
			synchronized (m_oFilesToRun)
			{
				try (BufferedWriter oOut = new BufferedWriter(new FileWriter(m_sQueueFile)))
				{
					Iterator<String> oIt = m_oFilesToRun.iterator();
					while (oIt.hasNext())
					{
						String sFile = oIt.next();
						oOut.write(sFile);
						oOut.write("\n");
						sBuffer.append(sFile).append("<br></br>");
					}
				}
			}
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
	}
	
	
	public void queueStatus(StringBuilder sBuffer)
	{
		synchronized (m_oFilesToRun)
		{
			sBuffer.append(m_oFilesToRun.size()).append(" files in queue");
			Iterator<String> oIt = m_oFilesToRun.iterator();
			while (oIt.hasNext())
			{
				String sFile = oIt.next();
				sBuffer.append("<br></br>").append(sFile);
			}
		}
	}
	
	@Override
	public void execute()
	{
		if (m_sQueueFile.isEmpty())
			return;
		int nTimesToRun = Math.min(m_nRunsPerPeriod, m_oFilesToRun.size());
		for (int i = 0; i < nTimesToRun; i++)
		{
			String sFile = m_oFilesToRun.removeFirst();
			if (sFile.contains("mrms"))
				createMrmsPcCat(sFile);
			else if (sFile.contains("rap"))
				createRapPcCat(sFile);
		}
		synchronized (m_oFilesToRun)
		{
			try (BufferedWriter oOut = new BufferedWriter(new FileWriter(m_sQueueFile)))
			{
				Iterator<String> oIt = m_oFilesToRun.iterator();
				while (oIt.hasNext())
				{
					oOut.write(oIt.next());
					oOut.write("\n");
				}
			}
			catch (Exception oEx)
			{
				m_oLogger.error(oEx, oEx);
			}
		}
	}
	
	
	@Override
	public boolean stop() throws Exception
	{
		if (m_sCurrentFile != null)
		{
			new File(m_sCurrentFile).delete();
		}
		
		return true;
	}
}
