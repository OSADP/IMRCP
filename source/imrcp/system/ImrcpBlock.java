/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.system;

/**
 * Interface used for components of the IMRCP system. 
 * @author Federal Highway Administration
 */
public interface ImrcpBlock
{
	/**
	 * Index in string array system messages that corresponds to the ImrcpBlock 
	 * the message is from
	 */
	public static final int FROM = 0;

	
	/**
	 * Index in string array system messages that corresponds to the type of 
	 * message
	 */
	public static final int MESSAGE = 1;
	
	
	/**
	 * Status enumeration used to override the current status regardless of 
	 * what it is
	 */
	public static final int OVERRIDESTATUS = -1;

	
	/**
	 * Status enumeration indicating that the ImrcpBlock is starting
	 */
	public static final int STARTING = 0;

	
	/**
	 * Status enumeration indicating that the ImrcpBlock actively executing
	 * its task
	 */
	public static final int RUNNING = 1;

	
	/**
	 * Status enumeration indicating that the ImrcpBlock is idle and ready to
	 * execute its task
	 */
	public static final int IDLE = 2;

	
	/**
	 * Status enumeration indicating that the ImrcpBlock has had an error
	 * occur and is not ready to execute its task
	 */
	public static final int ERROR = 3;

	
	/**
	 * Status enumeration indicating that the ImrcpBlock is actively shuting 
	 * down and stopping its service
	 */
	public static final int STOPPING = 4;

	
	/**
	 * Status enumeration indicating that the ImrcpBlock has finishing stopping
	 * its service
	 */
	public static final int STOPPED = 5;

	
	/**
	 * Status enumeration indicating that the ImrcpBlock is currently initializing
	 */
	public static final int INIT = 6;
	
	
	/**
	 * String names for the different status enumerations
	 */
	public static final String[] STATUSES = new String[]
	{
		"STARTING", "RUNNING", "IDLE", "ERROR", "STOPPING", "STOPPED", "INIT"
	};
	
	
	/**
	 * Method called to notify other subscribing system components with the
	 * given message and resources
	 * @param sMessageName type of message being sent
	 * @param sResources array of strings that represent the resources and details
	 * of the message
	 */
	public void notify(String sMessageName, String... sResources);
	
	
	/**
	 * Method called to receive a message from other system components.
	 * @param sMessage the message to receive
	 */
	public void receive(String[] sMessage);
	
	
	/**
	 * Method called to register the ImrcpBlock to the system Directory
	 */
	public void register();
	
	
	/**
	 * Method called to initialize the ImrcpBlock
	 */
	public void init();
	
	
	/**
	 * Method called when the ImrcpBlock is finished being active in the system
	 */
	public void destroy();
	
	
	/**
	 * Method called to get the current status of the ImrcpBlock
	 * @return [current status, time in milliseconds since Epoch the status last
	 * changed]
	 */
	public long[] status();
	
	
	/**
	 * Method called to get the instance name of the ImrcpBlock
	 * @return Instance name of the ImrcpBlock
	 */
	public String getName();
	
	
	/**
	 * Method called to set the instance name of the ImrcpBlock
	 * @param sName desired nstance name of the ImrcpBlock
	 */
	public void setName(String sName);
	
	
	/**
	 * Method called to get the configuration object for this ImrcpBlock
	 * @return configuration object
	 */
	public BlockConfig getConfig();
}
