/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.forecast.mlp;

import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

/**
 * Container used to aid with multi-threaded processing of the MLP model.
 * @author Federal Highway Administration
 */
public class Work extends ArrayList<WorkObject>
{
	/**
	 * Run time of the MLP model
	 */
	public long m_lTimestamp;

	
	/**
	 * Thread number
	 */
	public int m_nThread;

	
	/**
	 * Stores the bytes for the input files
	 */
	public ByteArrayOutputStream m_oBoas;

	
	/**
	 * Writer that wraps {@link #m_oBoas}, intended to be used with a GZIP stream
	 */
	public OutputStreamWriter m_oCompressor;

	
	/**
	 * Default constructor. Does nothing.
	 */
	public Work()
	{
	}

	
	/**
	 * Constructs a Work object with the given thread number
	 * @param nThread Thread number for this Work
	 */
	Work(int nThread)
	{
		m_nThread = nThread;
	}
}
