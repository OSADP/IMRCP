/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.comp;

import imrcp.geosrv.GeoUtil;
import imrcp.system.BaseBlock;
import imrcp.store.FileCache;
import imrcp.system.FilenameFormatter;
import imrcp.store.GriddedFileWrapper;
import imrcp.store.GribEntryData;
import imrcp.store.GribWrapper;
import imrcp.store.NcfEntryData;
import imrcp.store.NcfWrapper;
import imrcp.store.WeatherStore;
import imrcp.store.grib.GribParameter;
import imrcp.system.CsvReader;
import imrcp.system.Directory;
import imrcp.system.FileUtil;
import imrcp.system.ObsType;
import imrcp.system.Scheduling;
import imrcp.system.Util;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import ucar.ma2.Index;
import ucar.unidata.geoloc.projection.LambertConformal;

/**
 * Contains methods for creating Precipitation Category observations from 
 * different National Weather Service sources.
 * @author Federal Highway Administration
 */
public class PcCat extends BaseBlock
{
	/**
	 * Instance name of base block that collects precipitation files from MRMS
	 */
	private String m_sMRMS;

	
	/**
	 * Instance name of base block that collects precipitation files from RAP
	 */
	private String m_sRAP;

	
	/**
	 * Instance name of base block that collects precipitation files from GFS
	 */
	private String m_sGFS;

	
	/**
	 * Object used to create time dependent file names of MRMS files
	 */
	private FilenameFormatter m_oMrmsFormat;

	
	/**
	 * Object used to create time dependent file names of RAP files
	 */
	private FilenameFormatter m_oRapFormat;

	
	/**
	 * Object used to create time dependent file names of PcCat files created 
	 * from MRMS files
	 */
	private FilenameFormatter m_oMrmsPcCatFormat;

	
	/**
	 * Object used to create time dependent file names of PcCat files created 
	 * from RAP files
	 */
	private FilenameFormatter m_oRapPcCatFormat;

	
	/**
	 * Object used to create time dependent file names of GFS files
	 */
	private FilenameFormatter m_oGfsFormat;

	
	/**
	 * Object used to create time dependent file names of PcCat files created 
	 * from GFS files
	 */
	private FilenameFormatter m_oGfsPcCatFormat;

	
	/**
	 * Object used to create time dependent file names of NDFD temperature files
	 */
	private FilenameFormatter m_oNdfdTempFormat;

	
	/**
	 * Object used to create time dependent file names of NDFD quantitative 
	 * precipitation forecast files
	 */
	private FilenameFormatter m_oNdfdQpfFormat;

	
	/**
	 * Object used to create time dependent file names of PcCat files created 
	 * from NDFD files
	 */
	private FilenameFormatter m_oNdfdPcCatFormat;
	
	
	/**
	 * Instance name of FileCache that manages RTMA files
	 */
	private String m_sRTMAStore;
	
	
	/**
	 * Contains the IMRCP observation types that map to the observation type 
	 * labels in {@link PcCat#m_sObsTypes}'s corresponding indices. Used in 
	 * constructing {@link imrcp.store.NcfWrapper}s
	 */
	protected int[] m_nObsTypes;

	
	/**
	 * Contains the observation type labels found in NWS files. Used in 
	 * constructing {@link imrcp.store.NcfWrapper}s
	 */
	protected String[] m_sObsTypes;

	
	/**
	 * Label of the x coordinate in NWS files using Lambert Conformal projections
	 */
	protected String m_sHrz;

	
	/**
	 * Label of the y coordinate in NWS files using Lambert Conformal projections
	 */
	protected String m_sVrt;

	
	/**
	 * Label of the time axis in NWS files using Lambert Conformal projections
	 */
	protected String m_sTime;
	
	
	/**
	 * Label of the x coordinate in GFS files
	 */
	protected String m_sGfsHrz;

	
	/**
	 * Label of the y coordinate in GFS files
	 */
	protected String m_sGfsVrt;

	
	/**
	 * Label of the time axis in GFS files
	 */
	protected String m_sGfsTime;
	
	
	/**
	 * Contains the IMRCP observation types that map to the observation type 
	 * labels in {@link PcCat#m_sNdfdTempTypes}'s corresponding indices. Used in 
	 * constructing {@link imrcp.store.NcfWrapper}s
	 */
	protected int[] m_nNdfdTempTypes;

	
	/**
	 * Contains the IMRCP observation types that map to the observation type 
	 * labels in {@link PcCat#m_sNdfdQpfTypes}'s corresponding indices. Used in 
	 * constructing {@link imrcp.store.NcfWrapper}s
	 */
	protected int[] m_nNdfdQpfTypes;

	
	/**
	 * Contains the observation type labels found in NDFD temperature files. 
	 * Used in constructing {@link imrcp.store.NcfWrapper}s
	 */
	protected String[] m_sNdfdTempTypes;

	
	/**
	 * Contains the observation type labels found in NDFD quantitative precipitation
	 * forecast files. Used in constructing {@link imrcp.store.NcfWrapper}s
	 */
	protected String[] m_sNdfdQpfTypes;
	
	
	/**
	 * Stores different parameters for {@link imrcp.store.GribWrapper} which
	 * are used to open MRMS files.
	 */
	private GribParameter[] m_nParameters;

	
	/**
	 * Stores the IMRCP observation types provided in a .grb2 file
	 */
	private int[] m_nGribObs;

	
	/**
	 * Number of files to process per execution cycle
	 */
	private int m_nRunsPerPeriod;

	
	/**
	 * Maximum number of files that can be placed in the queue at once
	 */
	private int m_nMaxQueue;

	
	/**
	 * Used as a queue for files to processed
	 */
	private final ArrayDeque<String> m_oFilesToRun = new ArrayDeque();

	
	/**
	 * File name to save queued files to for persistent storage
	 */
	private String m_sQueueFile;

	
	/**
	 * Period of execution in seconds. Should be set to 0 unless this 
	 * instantiation is going to be used for reprocessing old files
	 */
	private int m_nPeriod;

	
	/**
	 * Schedule offset from midnight in seconds
	 */
	private int m_nOffset;

	
	/**
	 * Stores the files that is currently being processed
	 */
	private String m_sCurrentFile = null;

	
	/**
	 * Flag used to indicate if RAP files should be reprocessed in 
	 * {@link PcCat#execute()}
	 */
	private boolean m_bRunRap;

	
	/**
	 * * Flag used to indicate if MRMS files should be reprocessed in 
	 * {@link PcCat#execute()}
	 */
	private boolean m_bRunMrms;

	
	/**
	 * * Flag used to indicate if GFS files should be reprocessed in 
	 * {@link PcCat#execute()}
	 */
	private boolean m_bRunGfs;

	
	/**
	 * * Flag used to indicate if NDFD files should be reprocessed in 
	 * {@link PcCat#execute()}
	 */
	private boolean m_bRunNdfd;
	
	
	@Override
	public void reset()
	{
		m_sMRMS = m_oConfig.getString("mrms", "RadarPrecip");
		m_sRAP = m_oConfig.getString("rap", "RAP");
		m_sGFS = m_oConfig.getString("gfs", "GFS");
		m_sRTMAStore = m_oConfig.getString("rtmastore", "RTMAStore");
		m_oMrmsFormat = new FilenameFormatter(m_oConfig.getString("mrmsformat", ""));
		m_oMrmsPcCatFormat = new FilenameFormatter(m_oConfig.getString("mrmspccat", ""));
		m_oRapFormat = new FilenameFormatter(m_oConfig.getString("rapformat", ""));
		m_oRapPcCatFormat = new FilenameFormatter(m_oConfig.getString("rappccat", ""));
		m_oGfsFormat = new FilenameFormatter(m_oConfig.getString("gfsformat", ""));
		m_oGfsPcCatFormat = new FilenameFormatter(m_oConfig.getString("gfspccat", ""));
		m_oNdfdTempFormat = new FilenameFormatter(m_oConfig.getString("ndfdtemp", ""));
		m_oNdfdQpfFormat = new FilenameFormatter(m_oConfig.getString("ndfdqpf", ""));
		m_oNdfdPcCatFormat = new FilenameFormatter(m_oConfig.getString("ndfdpccat", ""));
		String[] sObsTypes = m_oConfig.getStringArray("obsid", null);
		m_nObsTypes = new int[sObsTypes.length];
		for (int i = 0; i < sObsTypes.length; i++)
			m_nObsTypes[i] = Integer.valueOf(sObsTypes[i], 36);
		m_sObsTypes = m_oConfig.getStringArray("obs", null);
		m_sHrz = m_oConfig.getString("hrz", "x");
		m_sVrt = m_oConfig.getString("vrt", "y");
		m_sTime = m_oConfig.getString("time", "time");
		m_sGfsHrz = m_oConfig.getString("gfshrz", "lon");
		m_sGfsVrt = m_oConfig.getString("gfsvrt", "lat");
		m_sGfsTime = m_oConfig.getString("gfstime", "time");
		sObsTypes = m_oConfig.getStringArray("ndfdtempobsid", null);
		m_nNdfdTempTypes = new int[sObsTypes.length];
		for (int i = 0; i < sObsTypes.length; i++)
			m_nNdfdTempTypes[i] = Integer.valueOf(sObsTypes[i], 36);
		
		sObsTypes = m_oConfig.getStringArray("ndfdqpfobsid", null);
		m_nNdfdQpfTypes = new int[sObsTypes.length];
		for (int i = 0; i < sObsTypes.length; i++)
			m_nNdfdQpfTypes[i] = Integer.valueOf(sObsTypes[i], 36);
		m_sNdfdTempTypes = m_oConfig.getStringArray("tempobs", null);
		m_sNdfdQpfTypes = m_oConfig.getStringArray("qpfobs", null);
		String[] sParameterInfo = m_oConfig.getStringArray("parameters", "");
		m_nParameters = new GribParameter[sParameterInfo.length / 4];
		m_nGribObs = new int[m_nParameters.length];
		int nCount = 0;
		for (int i = 0; i < m_nParameters.length; i += 4)
		{
			m_nParameters[nCount] = new GribParameter(Integer.valueOf(sParameterInfo[i], 36), Integer.parseInt(sParameterInfo[i + 1]), Integer.parseInt(sParameterInfo[i + 2]), Integer.parseInt(sParameterInfo[i + 3]));
			m_nGribObs[nCount++] = Integer.valueOf(sParameterInfo[i], 36);
		}

		m_nRunsPerPeriod = m_oConfig.getInt("runs", 4);
		m_nMaxQueue = m_oConfig.getInt("maxqueue", 984);  // default is the number of rap and mrms files in a day
		m_sQueueFile = m_oConfig.getString("queuefile", "");
		m_nPeriod = m_oConfig.getInt("period", 300);
		m_nOffset = m_oConfig.getInt("offset", 0);
		m_bRunRap = Boolean.parseBoolean(m_oConfig.getString("runrap", "True"));
		m_bRunMrms = Boolean.parseBoolean(m_oConfig.getString("runmrms", "True"));
		m_bRunGfs = Boolean.parseBoolean(m_oConfig.getString("rungfs", "True"));
	}
	
	/**
	 * Checks if the configured queue file has any file names in it and them to
	 * the queue. Then if the period is not 0 sets a schedule to execute on a 
	 * fixed interval.
	 * @return true if no exceptions are thrown
	 * @throws Exception
	 */
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
	
	/**
	 * Called when a message from {@link imrcp.system.Directory} is received. 
	 * Determines which method to call depending on which base block sent the 
	 * message.
	 * @param sMessage [BaseBlock message is from, message name, files to process...]
	 */
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
				   String sPcCatFile = createPcCat(sMessage[i], m_oRapFormat, m_oRapPcCatFormat, getRapWrapper(), m_sHrz, m_sVrt, 1);
				   if (sPcCatFile != null)
					   oFiles.add(sPcCatFile);
				}
			}
			if (sMessage[FROM].compareTo(m_sGFS) == 0)
			{
				for (int i = 2; i < sMessage.length; i++)
				{
					String sPcCatFile = createGfsPcCat(sMessage[i], m_oGfsFormat, m_oGfsPcCatFormat, getGfsWrapper(), m_sGfsHrz, m_sGfsVrt, 0);
					if (sPcCatFile != null)
						oFiles.add(sPcCatFile);
				}
			}
			if (sMessage[FROM].contains("NDFD"))
			{
				String sFile = sMessage[2];
				long[] lTimes = new long[3];
				m_oNdfdTempFormat.parse(sFile, lTimes);
				
				String sTempFile = m_oNdfdTempFormat.format(lTimes[FileCache.VALID], lTimes[FileCache.START], lTimes[FileCache.END]);
				String sQpfFile = m_oNdfdQpfFormat.format(lTimes[FileCache.VALID], lTimes[FileCache.START], lTimes[FileCache.END]);
				if (Files.exists(Paths.get(sQpfFile)) && Files.exists(Paths.get(sTempFile))) // both the qpf file and temperature file have to exist to infer the pccat
					createNdfdPcCat(sQpfFile, sTempFile, oFiles);
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
	
	/**
	 * Creates a Precipitation Category observation file in IMRCP's gridded 
	 * binary observation format from a GFS forecast file
	 * @param sOriginalFile GFS file to process
	 * @param oFf FilenameFormatter for GFS files
	 * @param oPcCatFf FilenameFormatter for created PcCat file
	 * @param oFile GriddedFileWrapper ready for 
	 * {@link imrcp.store.GriddedFileWrapper#load(long, long, long, java.lang.String, int)} 
	 * to be called
	 * @param sHrz label for x coordinate in GFS file
	 * @param sVrt label for y coordinate in GFS file
	 * @param nProj proj number: 0 = lat/lon, 1 = lambert conformal. Should be 0
	 * for GFS files
	 * @return Name of the created PcCat file
	 */
	private String createGfsPcCat(String sOriginalFile, FilenameFormatter oFf, FilenameFormatter oPcCatFf, GriddedFileWrapper oFile, String sHrz, String sVrt, int nProj)
	{
		try
		{
			long[] lTimes = new long[3];
			oFf.parse(sOriginalFile, lTimes);
			String sFilename = oPcCatFf.format(lTimes[FileCache.VALID], lTimes[FileCache.START], lTimes[FileCache.END]);
			if (new File(sFilename).exists())
				return null;
			m_oLogger.info("Creating PcCat file for: " + sOriginalFile);
			oFile.load(lTimes[FileCache.START], lTimes[FileCache.END], lTimes[FileCache.VALID], sOriginalFile, Integer.valueOf("IMRCP", 36));
			new File(sFilename.substring(0, sFilename.lastIndexOf(File.separator))).mkdirs();
			ByteArrayOutputStream oBytes = new ByteArrayOutputStream(524288);
			try (DataOutputStream oBytesOut = new DataOutputStream(new BufferedOutputStream(Util.getGZIPOutputStream(oBytes))))
			{
				m_sCurrentFile = sFilename;
				NcfEntryData oRain = (NcfEntryData)oFile.getEntryByObsId(1);
				synchronized (oRain)
				{
					oBytesOut.writeByte(1); // version
					oBytesOut.writeInt(ObsType.PCCAT);
					if (nProj == 1)
					{	
						oBytesOut.writeInt(1); // lambert conformal projection
						// lambert conformal parameters
						LambertConformal oLam = (LambertConformal)oRain.m_oProjProfile.m_oProj;
						oBytesOut.writeDouble(oLam.getOriginLat());
						oBytesOut.writeDouble(oLam.getOriginLon());
						oBytesOut.writeDouble(oLam.getParallelOne());
						oBytesOut.writeDouble(oLam.getParallelTwo());
						oBytesOut.writeDouble(oLam.getFalseEasting());
						oBytesOut.writeDouble(oLam.getFalseNorthing());
					}
					else
					{
						oBytesOut.writeInt(0); // lat/lon projection, has no parameters
					}
					oBytesOut.writeInt(oRain.getVrt() + 1);
					for (int nVrt = 0; nVrt <= oRain.getVrt(); nVrt++)
						oBytesOut.writeDouble(oRain.m_oProjProfile.m_dYs[nVrt]);
					oBytesOut.writeInt(oRain.getHrz() + 1);
					int nDateLine = oRain.m_oProjProfile.m_dXs.length / 2 + 1; // gfs files have lon ranging from 0 to 360 starting at 0 (prime meridian), we need from -180 to 180 so start at the international date line (180 deg E) and adjust lons up to 360 deg E giving the range -180 to 0
					for (int nHrz = nDateLine; nHrz <= oRain.getHrz(); nHrz++)
						oBytesOut.writeDouble(GeoUtil.adjustLon(oRain.m_oProjProfile.m_dXs[nHrz]));
					
					for (int nHrz = 0; nHrz < nDateLine; nHrz++) // then go back to the prime meridian giving the range 0 to 180
						oBytesOut.writeDouble(oRain.m_oProjProfile.m_dXs[nHrz]);

					NcfEntryData oSnow = (NcfEntryData)oFile.getEntryByObsId(2);
					NcfEntryData oIce = (NcfEntryData)oFile.getEntryByObsId(3);
					NcfEntryData oFRain = (NcfEntryData)oFile.getEntryByObsId(4);
					NcfEntryData oPrecipRate = (NcfEntryData)oFile.getEntryByObsId(ObsType.RTEPC);

					Index oIndex = oRain.m_oIndex;
					int nHrzDim = oRain.m_oVar.findDimensionIndex(sHrz);
					int nVrtDim = oRain.m_oVar.findDimensionIndex(sVrt);

					for (int nVrt = 0; nVrt <= oRain.getVrt(); nVrt++)
					{
						oIndex.setDim(nVrtDim, nVrt);
						// this loop starts at the international date line and goes up to the prime meridian traveling east (-180 to 0)
						for (int nHrz = nDateLine; nHrz <= oRain.getHrz(); nHrz++)
						{
							oIndex.setDim(nHrzDim, nHrz);
							double dRain = oRain.m_oArray.getDouble(oIndex);
							double dSnow = oSnow.m_oArray.getDouble(oIndex);
							double dIce = oIce.m_oArray.getDouble(oIndex);
							double dFRain = oFRain.m_oArray.getDouble(oIndex);
							double dPrecipRate = oPrecipRate.m_oArray.getDouble(oIndex);
							
							if (oPrecipRate.m_oVar.isFillValue(dPrecipRate) || oPrecipRate.m_oVar.isInvalidData(dPrecipRate) || oPrecipRate.m_oVar.isMissing(dPrecipRate) || Double.isNaN(dPrecipRate))
							{
								oBytesOut.writeByte(Byte.MIN_VALUE);
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

							// can write bytes since values are from 0 to 12 or Byte.MIN_VALUE
							if (Double.isNaN(dVal))
								oBytesOut.writeByte(Byte.MIN_VALUE);
							else
								oBytesOut.writeByte((byte)dVal);
						}
						// this loop starts at the prime meridian and goes up to the international date line traveling east (0 to 180)
						for (int nHrz = 0; nHrz < nDateLine; nHrz++)
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
								oBytesOut.writeByte(Byte.MIN_VALUE);
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

							// can write bytes since values are from 0 to 12 or Byte.MIN_VALUE
							if (Double.isNaN(dVal))
								oBytesOut.writeByte(Byte.MIN_VALUE);
							else
								oBytesOut.writeByte((byte)dVal);
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
			oFile.cleanup(false); // clean up resources but don't delete index files that might be used by another module
		}
	}
	
	/**
	 * Creates a Precipitation Category observation file in IMRCP's gridded 
	 * binary observation format from a RAP forecast file
	 * 
	 * @param sOriginalFile RAP file to process
	 * @param oFf FilenameFormatter for RAP files
	 * @param oPcCatFf FilenameFormatter for created PcCat file
	 * @param oFile GriddedFileWrapper ready for 
	 * {@link imrcp.store.GriddedFileWrapper#load(long, long, long, java.lang.String, int)} 
	 * to be called
	 * @param sHrz label for x coordinate in RAP file
	 * @param sVrt label for y coordinate in RAP file
	 * @param nProj proj number: 0 = lat/lon, 1 = lambert conformal. Should be 1
	 * for RAP files
	 * @return Name of the created PcCat file
	 */
	private String createPcCat(String sOriginalFile, FilenameFormatter oFf, FilenameFormatter oPcCatFf, GriddedFileWrapper oFile, String sHrz, String sVrt, int nProj)
	{
		try
		{
			long[] lTimes = new long[3];
			oFf.parse(sOriginalFile, lTimes);
			String sFilename = oPcCatFf.format(lTimes[FileCache.VALID], lTimes[FileCache.START], lTimes[FileCache.END]);
			if (new File(sFilename).exists())
				return null;
			m_oLogger.info("Creating PcCat file for: " + sOriginalFile);
			oFile.load(lTimes[FileCache.START], lTimes[FileCache.END], lTimes[FileCache.VALID], sOriginalFile, Integer.valueOf("IMRCP", 36));
			new File(sFilename.substring(0, sFilename.lastIndexOf(File.separator))).mkdirs();
			ByteArrayOutputStream oBytes = new ByteArrayOutputStream(524288);
			try (DataOutputStream oBytesOut = new DataOutputStream(new BufferedOutputStream(Util.getGZIPOutputStream(oBytes))))
			{
				m_sCurrentFile = sFilename;
				NcfEntryData oRain = (NcfEntryData)oFile.getEntryByObsId(1);
				synchronized (oRain)
				{
					oBytesOut.writeByte(1); // version
					oBytesOut.writeInt(ObsType.PCCAT);
					if (nProj == 1)
					{	
						oBytesOut.writeInt(1); // lambert conformal projection
						// lambert conformal parameters
						LambertConformal oLam = (LambertConformal)oRain.m_oProjProfile.m_oProj;
						oBytesOut.writeDouble(oLam.getOriginLat());
						oBytesOut.writeDouble(oLam.getOriginLon());
						oBytesOut.writeDouble(oLam.getParallelOne());
						oBytesOut.writeDouble(oLam.getParallelTwo());
						oBytesOut.writeDouble(oLam.getFalseEasting());
						oBytesOut.writeDouble(oLam.getFalseNorthing());
					}
					else
					{
						oBytesOut.writeInt(0); // lat/lon projection, has no parameters
					}
					oBytesOut.writeInt(oRain.getVrt() + 1);
					for (int nVrt = 0; nVrt <= oRain.getVrt(); nVrt++)
						oBytesOut.writeDouble(oRain.m_oProjProfile.m_dYs[nVrt]);
					oBytesOut.writeInt(oRain.getHrz() + 1);
					for (int nHrz = 0; nHrz <= oRain.getHrz(); nHrz++)
						oBytesOut.writeDouble(oRain.m_oProjProfile.m_dXs[nHrz]);

					NcfEntryData oSnow = (NcfEntryData)oFile.getEntryByObsId(2);
					NcfEntryData oIce = (NcfEntryData)oFile.getEntryByObsId(3);
					NcfEntryData oFRain = (NcfEntryData)oFile.getEntryByObsId(4);
					NcfEntryData oPrecipRate = (NcfEntryData)oFile.getEntryByObsId(ObsType.RTEPC);

					Index oIndex = oRain.m_oIndex;
					int nHrzDim = oRain.m_oVar.findDimensionIndex(sHrz);
					int nVrtDim = oRain.m_oVar.findDimensionIndex(sVrt);

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
								oBytesOut.writeByte(Byte.MIN_VALUE);
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

							// can write bytes since values are from 0 to 12 or Byte.MIN_VALUE
							if (Double.isNaN(dVal))
								oBytesOut.writeByte(Byte.MIN_VALUE);
							else
								oBytesOut.writeByte((byte)dVal);
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
			oFile.cleanup(false); // clean up resources but don't delete index files that might be used by another module
		}
	}
	
	/**
	 * Creates a Precipitation Category observation file in IMRCP's gridded 
	 * binary observation format from a MRMS precipitation rate file and 
	 * temperatures from RTMA to infer the type
	 * @param sOriginalFile MRMS file to process
	 * @return Name of the created PcCat file
	 */
	private String createMrmsPcCat(String sOriginalFile)
	{
		GriddedFileWrapper oMrms = getMrmsWrapper();
		try
		{
			long[] lTimes = new long[3];
			m_oMrmsFormat.parse(sOriginalFile, lTimes);
			long lTimestamp = lTimes[FileCache.START];
			String sFilename = m_oMrmsPcCatFormat.format(lTimes[FileCache.VALID], lTimestamp, lTimes[FileCache.END]);
//			if (new File(sFilename).exists())
//				return null;
			m_oLogger.info("Creating PcCat file for: " + sOriginalFile);
			oMrms.load(lTimestamp, lTimes[FileCache.END], lTimes[FileCache.VALID], sOriginalFile, Integer.valueOf("IMRCP", 36));
			new File(sFilename.substring(0, sFilename.lastIndexOf(File.separator))).mkdirs();
			WeatherStore oRtma = (WeatherStore)Directory.getInstance().lookup(m_sRTMAStore);
			long lNow = System.currentTimeMillis();
			
			GriddedFileWrapper oRtmaFile = (GriddedFileWrapper)oRtma.getFile(lTimestamp, lNow);
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
			try (DataOutputStream oBytesOut = new DataOutputStream(new BufferedOutputStream(Util.getGZIPOutputStream(oBytes))))
			{
				m_sCurrentFile = sFilename;
				GribEntryData oPrecipRate = (GribEntryData)oMrms.getEntryByObsId(ObsType.RTEPC);
				oBytesOut.writeByte(1); // version
				oBytesOut.writeInt(ObsType.PCCAT);
				oBytesOut.writeInt(0); // lat/lon projection, has no parameters
				oBytesOut.writeInt(oPrecipRate.getVrt() + 1);
				for (int nVrt = 0; nVrt <= oPrecipRate.getVrt(); nVrt++)
					oBytesOut.writeDouble(oPrecipRate.m_oProjProfile.m_dYs[nVrt]);
				oBytesOut.writeInt(oPrecipRate.getHrz() + 1);
				for (int nHrz = 0; nHrz <= oPrecipRate.getHrz(); nHrz++)
					oBytesOut.writeDouble(oPrecipRate.m_oProjProfile.m_dXs[nHrz]);

				byte yNoPrecip = (byte)ObsType.lookup(ObsType.PCCAT, "no-precipitation");
				byte yLightRain = (byte)ObsType.lookup(ObsType.PCCAT, "light-rain");
				byte yModerateRain = (byte)ObsType.lookup(ObsType.PCCAT, "moderate-rain");
				byte yHeavyRain = (byte)ObsType.lookup(ObsType.PCCAT, "heavy-rain");
				byte yLightFRain = (byte)ObsType.lookup(ObsType.PCCAT, "light-freezing-rain");
				byte yModerateFRain = (byte)ObsType.lookup(ObsType.PCCAT, "moderate-freezing-rain");
				byte yHeavyFRain = (byte)ObsType.lookup(ObsType.PCCAT, "heavy-freezing-rain");
				byte yLightSnow = (byte)ObsType.lookup(ObsType.PCCAT, "light-snow");
				byte yModerateSnow = (byte)ObsType.lookup(ObsType.PCCAT, "moderate-snow");
				byte yHeavySnow = (byte)ObsType.lookup(ObsType.PCCAT, "heavy-snow");

				for (int nVrt = 0; nVrt <= oPrecipRate.getVrt(); nVrt++)
				{
					for (int nHrz = 0; nHrz <= oPrecipRate.getHrz(); nHrz++)
					{
						double dRate = oPrecipRate.getValue(nHrz, nVrt);
						byte yVal;
						
						if (dRate == 0.0)
						{
							oBytesOut.writeByte(yNoPrecip);
							continue;
						}

						if (dRate < 0.0) // missing or fill value
						{
							oBytesOut.writeByte(Byte.MIN_VALUE);
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
							yVal = Byte.MIN_VALUE;
						}
						else if (dTemp > ObsType.m_dRAINTEMP) // temp > 2C
						{
							if (dRate < ObsType.m_dLIGHTRAINMMPERHR)
								yVal = yLightRain;
							else if (dRate < ObsType.m_dMEDIUMRAINMMPERHR)
								yVal = yModerateRain;
							else
								yVal = yHeavyRain;
						}
						else if (dTemp > ObsType.m_dSNOWTEMP) // -2C < temp <= 2C
						{
							if (dRate < ObsType.m_dLIGHTRAINMMPERHR)
								yVal = yLightFRain;
							else if (dRate < ObsType.m_dMEDIUMRAINMMPERHR)
								yVal = yModerateFRain;
							else
								yVal = yHeavyFRain;
						}
						else // temp <= -2C
						{
							if (dRate < ObsType.m_dLIGHTSNOWMMPERHR)
								yVal = yLightSnow;
							else if (dRate < ObsType.m_dMEDIUMSNOWMMPERHR)
								yVal = yModerateSnow;
							else
								yVal = yHeavySnow;
						}
						oBytesOut.writeByte(yVal);
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
	
	/**
	 * Creates multiple Precipitation Category observation files in IMRCP's gridded 
	 * binary observation format from a NDFD quantitative precipitation forecast 
	 * and temperature file.
	 * temperatures from RTMA to infer the type
	 * @param sQpfFile NDFD quantitative precipitation forecast file to process
	 * @param sTairFile NDFD temperature file to process
	 * @param oFiles List to fill with file names of created PcCat files
	 */
	private void createNdfdPcCat(String sQpfFile, String sTairFile, ArrayList<String> oFiles)
	{
		NcfWrapper oQpf = (NcfWrapper)getNdfdQpfWrapper();
		NcfWrapper oTemp = (NcfWrapper)getNdfdTempWrapper();
		try
		{
			long[] lTimes = new long[3];
			m_oLogger.info("Creating PcCat file for: " + sQpfFile);
			m_oNdfdQpfFormat.parse(sQpfFile, lTimes);
			oQpf.load(lTimes[FileCache.START], lTimes[FileCache.END], lTimes[FileCache.VALID], sQpfFile, Integer.valueOf("NDFD", 36));
			oTemp.load(lTimes[FileCache.START], lTimes[FileCache.END], lTimes[FileCache.VALID], sTairFile, Integer.valueOf("NDFD", 36));
			SimpleDateFormat oSdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
			oSdf.setTimeZone(Directory.m_oUTC);
			long lOutputRefTime = lTimes[FileCache.VALID];

			Path oPcCatDir = Paths.get(m_oNdfdPcCatFormat.format(lTimes[FileCache.VALID], lTimes[FileCache.START], lTimes[FileCache.END])).getParent();
			Files.createDirectories(oPcCatDir, FileUtil.DIRPERS);
			NcfEntryData oTair = (NcfEntryData)oTemp.getEntryByObsId(ObsType.TAIR);
			NcfEntryData oRate = (NcfEntryData)oQpf.getEntryByObsId(ObsType.RTEPC);
			double[] dTimeArray = oTair.m_dTime;
			byte yNoPrecip = (byte)ObsType.lookup(ObsType.PCCAT, "no-precipitation");
			byte yLightRain = (byte)ObsType.lookup(ObsType.PCCAT, "light-rain");
			byte yModerateRain = (byte)ObsType.lookup(ObsType.PCCAT, "moderate-rain");
			byte yHeavyRain = (byte)ObsType.lookup(ObsType.PCCAT, "heavy-rain");
			byte yLightFRain = (byte)ObsType.lookup(ObsType.PCCAT, "light-freezing-rain");
			byte yModerateFRain = (byte)ObsType.lookup(ObsType.PCCAT, "moderate-freezing-rain");
			byte yHeavyFRain = (byte)ObsType.lookup(ObsType.PCCAT, "heavy-freezing-rain");
			byte yLightSnow = (byte)ObsType.lookup(ObsType.PCCAT, "light-snow");
			byte yModerateSnow = (byte)ObsType.lookup(ObsType.PCCAT, "moderate-snow");
			byte yHeavySnow = (byte)ObsType.lookup(ObsType.PCCAT, "heavy-snow");
			for (int nTimeIndex = 0; nTimeIndex < dTimeArray.length; nTimeIndex++)
			{
				long lTimestamp = (long)dTimeArray[nTimeIndex];
				int nHours = nTimeIndex == dTimeArray.length - 1 ? (int)((lTimestamp - dTimeArray[nTimeIndex - 1]) / 3600000) : (int)((dTimeArray[nTimeIndex + 1] - lTimestamp) / 3600000); // determine the correct amount of hours between this record and the next one, unless it is the last record then use the record before it
				String sPcCatFile = m_oNdfdPcCatFormat.format(lOutputRefTime, (long)dTimeArray[nTimeIndex], (long)(dTimeArray[nTimeIndex] + (nHours * 3600000)));
				int nDivisor = 6 / nHours; // qpf files have records every 6 hours.
				int nQpfTimeIndex = oQpf.getTimeIndex(oRate, lTimestamp);
				if (nQpfTimeIndex < 0)
					continue;
				oRate.setTimeDim(nQpfTimeIndex);
				oTair.setTimeDim(nTimeIndex);
				ByteArrayOutputStream oBytes = new ByteArrayOutputStream(524288);
				try (DataOutputStream oBytesOut = new DataOutputStream(new BufferedOutputStream(Util.getGZIPOutputStream(oBytes))))
				{
					m_sCurrentFile = sPcCatFile;
					oBytesOut.writeByte(1); // version
					oBytesOut.writeInt(ObsType.PCCAT);
					oBytesOut.writeInt(1); // lambert conformal projection
					// lambert conformal parameters
					LambertConformal oLam = (LambertConformal)oRate.m_oProjProfile.m_oProj;
					oBytesOut.writeDouble(oLam.getOriginLat());
					oBytesOut.writeDouble(oLam.getOriginLon());
					oBytesOut.writeDouble(oLam.getParallelOne());
					oBytesOut.writeDouble(oLam.getParallelTwo());
					oBytesOut.writeDouble(oLam.getFalseEasting());
					oBytesOut.writeDouble(oLam.getFalseNorthing());

					oBytesOut.writeInt(oRate.getVrt() + 1);
					for (int nVrt = 0; nVrt <= oRate.getVrt(); nVrt++)
						oBytesOut.writeDouble(oRate.m_oProjProfile.m_dYs[nVrt]);
					oBytesOut.writeInt(oRate.getHrz() + 1);
					for (int nHrz = 0; nHrz <= oRate.getHrz(); nHrz++)
						oBytesOut.writeDouble(oRate.m_oProjProfile.m_dXs[nHrz]);
					for (int nVrt = 0; nVrt <= oRate.getVrt(); nVrt++)
					{
						for (int nHrz = 0; nHrz <= oRate.getHrz(); nHrz++)
						{
							double dRate = oRate.getValue(nHrz, nVrt);
							byte yVal;

							if (dRate == 0.0)
							{
								oBytesOut.writeByte(yNoPrecip);
								continue;
							}

							if (dRate < 0.0 || Double.isNaN(dRate)) // missing or fill value
							{
								oBytesOut.writeByte(Byte.MIN_VALUE);
								continue;
							}
							dRate = dRate / nDivisor;
							double dTair = oTair.getValue(nHrz, nVrt);
							if (Double.isNaN(dTair) || Double.isNaN(dRate))
							{
								yVal = Byte.MIN_VALUE;
							}
							else if (dTair > ObsType.m_dRAINTEMP) // temp > 2C
							{
								if (dRate < ObsType.m_dLIGHTRAINMMPERHR)
									yVal = yLightRain;
								else if (dRate < ObsType.m_dMEDIUMRAINMMPERHR)
									yVal = yModerateRain;
								else
									yVal = yHeavyRain;
							}
							else if (dTair > ObsType.m_dSNOWTEMP) // -2C < temp <= 2C
							{
								if (dRate < ObsType.m_dLIGHTRAINMMPERHR)
									yVal = yLightFRain;
								else if (dRate < ObsType.m_dMEDIUMRAINMMPERHR)
									yVal = yModerateFRain;
								else
									yVal = yHeavyFRain;
							}
							else // temp <= -2C
							{
								if (dRate < ObsType.m_dLIGHTSNOWMMPERHR)
									yVal = yLightSnow;
								else if (dRate < ObsType.m_dMEDIUMSNOWMMPERHR)
									yVal = yModerateSnow;
								else
									yVal = yHeavySnow;
							}
							oBytesOut.writeByte(yVal);
						}
					}
					oBytesOut.flush();
				}
				
				try (BufferedOutputStream oFileOut = new BufferedOutputStream(Files.newOutputStream(Paths.get(sPcCatFile), FileUtil.WRITEOPTS)))
				{
					oBytes.writeTo(oFileOut);
				}
				oFiles.add(sPcCatFile);
				m_sCurrentFile = null;
			}
			m_oLogger.info("Finished creating PcCat files for: " + sQpfFile);
		}
		catch (Exception oEx)
		{
			m_oLogger.error(oEx, oEx);
		}
		finally
		{
			oTemp.cleanup(false);
			oQpf.cleanup(false);
		}
	}
	
	/**
	 * Get a GriddedFileWrapper configured for RAP files and ready to call
	 * {@link imrcp.store.GriddedFileWrapper#load(long, long, long, java.lang.String, int)}
	 * @return a GriddedFileWrapper configured for RAP files
	 */
	private GriddedFileWrapper getRapWrapper()
	{
		return new NcfWrapper(m_nObsTypes, m_sObsTypes, m_sHrz, m_sVrt, m_sTime);
	}

	/**
	 * Get a GriddedFileWrapper configured for MRMS files and ready to call
	 * {@link imrcp.store.GriddedFileWrapper#load(long, long, long, java.lang.String, int)}
	 * @return a GriddedFileWrapper configured for MRMS files
	 */
	private GriddedFileWrapper getMrmsWrapper()
	{
		return new GribWrapper(m_nGribObs, m_nParameters);
	}
	
	/**
	 * Get a GriddedFileWrapper configured for GFS files and ready to call
	 * {@link imrcp.store.GriddedFileWrapper#load(long, long, long, java.lang.String, int)}
	 * @return a GriddedFileWrapper configured for GFS files
	 */
	private GriddedFileWrapper getGfsWrapper()
	{
		return new NcfWrapper(m_nObsTypes, m_sObsTypes, m_sGfsHrz, m_sGfsVrt, m_sGfsTime);
	}
	
	/**
	 * Get a GriddedFileWrapper configured for NDFD temperature files and ready to call
	 * {@link imrcp.store.GriddedFileWrapper#load(long, long, long, java.lang.String, int)}
	 * @return a GriddedFileWrapper configured for NDFD temperature files
	 */
	private GriddedFileWrapper getNdfdTempWrapper()
	{
		return new NcfWrapper(m_nNdfdTempTypes, m_sNdfdTempTypes, m_sHrz, m_sVrt, m_sTime);
	}
	
	/**
	 * Get a GriddedFileWrapper configured for NDFD QPF files and ready to call
	 * {@link imrcp.store.GriddedFileWrapper#load(long, long, long, java.lang.String, int)}
	 * @return a GriddedFileWrapper configured for NDFD QPF files
	 */
	private GriddedFileWrapper getNdfdQpfWrapper()
	{
		return new NcfWrapper(m_nNdfdQpfTypes, m_sNdfdQpfTypes, m_sHrz, m_sVrt, m_sTime);
	}

	/**
	 * If this instance is configured to reprocess files, it parses the given
	 * start and end times and queues files within that range to be reprocessed.
	 * The given StringBuilder gets filled with basic html that contains the
	 * files that were queued.
	 * @param sStart start timestamp in the format yyyy-MM-ddTHH:mm 
	 * @param sEnd end timestamp in the format yyyy-MM-ddTHH:mm
	 * @param sBuffer Buffer that gets fills with html containing the queued files
	 */
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
			if (m_bRunRap)
				oFormats.add(m_oRapFormat);
			if (m_bRunMrms)
				oFormats.add(m_oMrmsFormat);
			if (m_bRunGfs)
				oFormats.add(m_oGfsFormat);
			ArrayList<String> oDirs = new ArrayList();
			for (FilenameFormatter oFormat : oFormats)
			{
				long lTimestamp = lStartTime;
				while (lTimestamp < lEndTime && nCount++ < m_nMaxQueue)
				{
					String sDir = oFormat.format(lTimestamp, 0, 0);
					int nContrib = oFormat.parse(sDir, lTimes);
					int nStep = 3600000;
					if (nContrib == Integer.valueOf("GFS", 36))
						nStep = 86400000;
					String sExt = oFormat.getExtension();

					sDir = sDir.substring(0, sDir.lastIndexOf("/") + 1);
					File oDir = new File(sDir);
//					int nSearch = Collections.binarySearch(oDirs, sDir);
//					if (nSearch < 0)
//						oDirs.add(~nSearch, sDir);
					String[] sFiles = oDir.list();
					if (sFiles != null) // can be null if the directory doesn't exist and don't check directories that have already been checked
					{
						ArrayList<String> oFiles = new ArrayList();
						for (String sFile : sFiles)
						{
							if (sFile.endsWith(sExt))
							{
								oFormat.parse(sFile, lTimes);
								if (lTimes[FileCache.VALID] >= lTimestamp && lTimes[FileCache.VALID] < lTimestamp + nStep)
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
					
					lTimestamp += nStep;
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
	
	/**
	 * Adds the current files in the queue to the StringBuilder in basic html
	 * @param sBuffer StringBuilder to fill with current files queued
	 */
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
	
	/**
	 * Processes up to {@link PcCat#m_nRunsPerPeriod} files in the queue.
	 */
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
				createPcCat(sFile, m_oRapFormat, m_oRapPcCatFormat, getRapWrapper(), m_sHrz, m_sVrt, 1);
			else if (sFile.contains("gfs"))
				createGfsPcCat(sFile, m_oGfsFormat, m_oGfsPcCatFormat, getGfsWrapper(), m_sGfsHrz, m_sGfsVrt, 0);
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
	
	/**
	 * Deletes partial files that have not finished processing
	 * @return true if no exceptions are thrown
	 * @throws Exception
	 */
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
