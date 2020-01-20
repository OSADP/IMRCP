/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.collect;

import imrcp.BaseBlock;
import imrcp.FilenameFormatter;

/**
 *
 * @author Federal Highway Administration
 */
public class Collector extends BaseBlock
{
	protected FilenameFormatter m_oDestFile;
	
	protected FilenameFormatter m_oSrcFile;


	/**
	 * Base URL used for downloading files
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
	
	protected int m_nDelay;
	
	protected int m_nRange;
	
	protected int m_nFileFrequency;
	
	
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
	
	protected String getDestFilename(long lFileTime)
	{
		return getDestFilename(lFileTime, 0);
	}
	
	
	protected String getDestFilename(long lFileTime, int nFileIndex)
	{
		return m_oDestFile.format(lFileTime, lFileTime + m_nDelay + (nFileIndex * m_nRange), lFileTime + m_nDelay + m_nRange + (nFileIndex * m_nRange));
	}
}
