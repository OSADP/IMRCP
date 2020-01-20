/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp;

import imrcp.system.BlockConfig;

/**
 *
 * @author Federal Highway Administration
 */
public interface ImrcpBlock
{
	public static final int FROM = 0;
	public static final int MESSAGE = 1;
	
	public static final int OVERRIDESTATUS = -1;
	public static final int STARTING = 0;
	public static final int RUNNING = 1;
	public static final int IDLE = 2;
	public static final int ERROR = 3;
	public static final int STOPPING = 4;
	public static final int STOPPED = 5;
	public static final int INIT = 6;
	
	/**
	 * Array that contains the different statuses an ImrcpBlock can have
	 */
	public static final String[] STATUSES = new String[]
	{
		"STARTING", "RUNNING", "IDLE", "ERROR", "STOPPING", "STOPPED", "INIT"
	};
	
	public void notify(String sMessageName, String... sResources);
	
	public void receive(String[] sMessage);
	
	public void register();
	
	public void init();
	
	public void destroy();
	
	public long[] status();
	
	public String getName();
	
	public void setName(String sName);
	
	public BlockConfig getConfig();
}
