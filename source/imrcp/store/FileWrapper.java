/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import imrcp.system.BlockConfig;
import imrcp.system.Config;
import java.io.File;
import java.util.HashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Base class for files that are loaded into memory by {@link imrcp.store.FileCache}
 * (stores).
 * @author Federal Highway Administration
 */
public abstract class FileWrapper
{
	/**
	 * Time in milliseconds since Epoch of the earliest observation or forecast
	 * contained in this file
	 */
	public long m_lStartTime;

	
	/**
	 * Time in milliseconds since Epoch of the latest observation or forecast 
	 * contained in this file
	 */
	public long m_lEndTime;
	
	
	/**
	 * Time in milliseconds since Epoch that the file starts being valid, usually
	 * when the file is downloaded/created
	 */
	public long m_lValidTime; 

	
	/**
	 * Time in milliseconds since Epoch that the file was last accessed/used
	 */
	public long m_lLastUsed = System.currentTimeMillis();

	
	/**
	 * Logger object
	 */
	protected Logger m_oLogger = LogManager.getLogger(getClass());

	
	/**
	 * Absolute path of the file
	 */
	public String m_sFilename;
	
	
	/**
	 * Contains the IMRCP observation type ids that this file provides
	 */
	public int[] m_nObsTypes;

	
	/**
	 * IMRCP contributor Id which is a computed by converting an up to a 6
	 * character alphanumeric string using base 36.
	 */
	public int m_nContribId;

	
	/**
	 * Static map object that maps IMRCP contributor Ids to a default time in 
	 * milliseconds of how long forecasts last for that contributor
	 */
	public static final HashMap<Integer, Integer> FCSTMINMAP;

	
	/**
	 * Index used for this file in a store's {@link imrcp.store.FileCache#m_oFormatters}
	 */
	public int m_nFormatIndex = 0;
	
	
	/**
	 * Construct and fill in FCSTMINMAP by using the configuration object
	 */
	static
	{
		FCSTMINMAP = new HashMap();
		if (Config.getInstance() != null)
		{	
			BlockConfig oConfig = new BlockConfig(FileWrapper.class.getName(), "FileWrapper");
			String[] sFcsts = oConfig.getStringArray("fcst", "");
			for (String sContrib : sFcsts)
				FCSTMINMAP.put(Integer.valueOf(sContrib, 36), oConfig.getInt(sContrib, 3600000));
			FCSTMINMAP.put(Integer.MIN_VALUE, oConfig.getInt("default", 3600000));
		}
	}
	
	
	/**
	 * Parses and loads observations from the given file into memory.
	 * @param lStartTime time in milliseconds since Epoch that the file starts 
	 * having observations
	 * @param lEndTime time in milliseconds since Epoch that the file stops 
	 * having observations
	 * @param lValidTime time in milliseconds since Epoch that the file starts
	 * being valid (usually the time it is received)
	 * @param sFilename path of the file being loaded
	 * @param nContribId IMRCP contributor Id which is a computed by converting
	 * an up to a 6 character alphanumeric string using base 36.
	 * @throws Exception 
	 */
	public abstract void load(long lStartTime, long lEndTime, long lValidTime, String sFilename, int nContribId) throws Exception;


	/**
	 * Called when a FileWrapper is removed from the cache in memory to clean up
	 * resources if necessary.
	 * @param bDelete flag used to indicate if index files created when the file
	 * is loaded into memory or the file itself should be deleted or not
	 */
	public abstract void cleanup(boolean bDelete);
	
	
	/**
	 * Attempts to delete the given File. Right now the implementation is
	 * commented out so files are not accidentally deleted
	 * @param oFile File to delete
	 */
	public void deleteFile(File oFile)
	{
//		m_oLogger.info("Deleting invalid file: " + oFile.getAbsolutePath());
//		if (oFile.exists())
//			oFile.delete();
	}
	
	
	/**
	 * Sets the start, end, and valid times of the FileWrapper to the given
	 * values
	 * @param lValid Time in milliseconds since Epoch that the file starts 
	 * being valid
	 * @param lStart Time in milliseconds since Epoch of the earliest observation 
	 * or forecast contained in this file
	 * @param lEnd Time in milliseconds since Epoch of the latest observation 
	 * or forecast contained in this file
	 */
	public void setTimes(long lValid, long lStart, long lEnd)
	{
		m_lValidTime = lValid;
		m_lStartTime = lStart;
		m_lEndTime = lEnd;
	}
}
