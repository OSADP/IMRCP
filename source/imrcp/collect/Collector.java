/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.collect;

import imrcp.system.BaseBlock;
import imrcp.system.FilenameFormatter;

/**
 * Base class containing functions and member variables used by various collectors
 * @author Federal Highway Administration
 */
public class Collector extends BaseBlock
{
	/**
	 * Object used to create time dependent file names to save on disk
	 */
	protected FilenameFormatter m_oDestFile;
	
	
	/**
	 * Object used to create time dependent file names collected from remote 
	 * sources
	 */
	protected FilenameFormatter m_oSrcFile;

	
	/**
	 * Base URL used for downloading data from remote sources
	 */
	protected String m_sBaseURL;

	
	/**
	 * Schedule offset from midnight in seconds
	 */
	protected int m_nOffset;

	
	/**
	 * Period of execution in seconds
	 */
	protected int m_nPeriod;
	
	
	/**
	 * Number of milliseconds from collection time until the forecast is valid
	 */
	protected int m_nDelay;
	
	
	/**
	 * Number of milliseconds the forecast is valid 
	 */
	protected int m_nRange;
	
	
	/**
	 * Expected time in milliseconds between collection of consecutive files
	 */
	protected int m_nFileFrequency;
	
	
	/**
	 *
	 */
	@Override
	public void reset()
	{
		m_oDestFile = new FilenameFormatter(m_oConfig.getString("dest", ""));
		m_oSrcFile = new FilenameFormatter(m_oConfig.getString("src", ""));
		m_sBaseURL = m_oConfig.getString("url", "");
		m_nOffset = m_oConfig.getInt("offset", 0);
		m_nPeriod = m_oConfig.getInt("period", 0);
		m_nDelay = m_oConfig.getInt("delay", 0);
		m_nRange = m_oConfig.getInt("range", 0);
		m_nFileFrequency = m_oConfig.getInt("freq", 0);
	}
	
	
	/**
	 * Calls {@link imrcp.collect.Collector#getDestFilename(long, int, java.lang.String...)}
	 * with a file index of zero to get the time dependent file name
	 * @param lFileTime Expected collection time of file in milliseconds since the Epoch
	 * @param sStrings String array of values passed to {@link imrcp.system.FilenameFormatter#format(long, long, long, java.lang.String...)}
	 * @return time dependent file name
	 */
	protected String getDestFilename(long lFileTime, String... sStrings)
	{
		return getDestFilename(lFileTime, 0, sStrings);
	}
	
	
	/**
	 * Uses {@link imrcp.collect.Collector#m_oDestFile} to call {@link imrcp.system.FilenameFormatter#format(long, long, long, java.lang.String...) 
	 * with the correct timestamps based off of {@link imrcp.collect.Collector#m_oDestFile} and
	 * {@link imrcp.collect.Collector#m_oDestFile}
	 * @param lFileTime Expected collection time of file in milliseconds since the Epoch
	 * @param nFileIndex Forecast index from source files
	 * @param sStrings String array of values passed to {@link imrcp.system.FilenameFormatter#format(long, long, long, java.lang.String...)}
	 * @return time dependent file name
	 */
	protected String getDestFilename(long lFileTime, int nFileIndex, String... sStrings)
	{
		return m_oDestFile.format(lFileTime, lFileTime + m_nDelay + (nFileIndex * m_nRange), lFileTime + m_nDelay + m_nRange + (nFileIndex * m_nRange), sStrings);
	}
}
