/* 
 * Copyright 2017 Synesis-Partners.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package imrcp;

import imrcp.store.ImrcpObsResultSet;
import imrcp.store.ImrcpResultSet;
import imrcp.system.AsyncQ;
import imrcp.system.BlockConfig;
import imrcp.system.Directory;
import imrcp.system.IRunTarget;
import imrcp.system.Scheduling;
import java.sql.ResultSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import javax.servlet.http.HttpServlet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This is the base class for most of the components that are a part of the
 * Imrcp system.
 */
public abstract class BaseBlock extends HttpServlet implements ImrcpBlock, Runnable, IRunTarget<String[]>
{

	/**
	 * Counter used for to keep Ids unique
	 */
	private static AtomicInteger m_nIdCount = new AtomicInteger();

	/**
	 * The Id for the BaseBlock
	 */
	public int m_nId;

	/**
	 * The Scheduling Id set by the Scheduling class when the BaseBlock is set
 to run at a scheduled time
	 */
	protected int m_nSchedId;

	/**
	 * The status of the BaseBlock. Can be 0= STARTING, 1= RUNNING, 2 = IDLE, 3
	 * = ERROR, 4 = STOPPING , or 5 = STOPPED.
	 */
	private AtomicInteger m_nStatus = new AtomicInteger();
	
	private AtomicLong m_lStatusChanged = new AtomicLong();

	/**
	 * Human-readable name of the BaseBlock
	 */
	protected String m_sInstanceName;


	/**
	 * Asynchronous queue used to process notifications as they come in
	 */
	protected final AsyncQ<String[]> m_oNotifications = new AsyncQ(this);
	

	/**
	 * Logger
	 */
	protected Logger m_oLogger;

	/**
	 * Object that contains the configured variables for the BaseBlock
	 */
	protected BlockConfig m_oConfig;

	/**
	 * True if running in test mode
	 */
	protected boolean m_bTest = false;


	/**
	 * Default constructor. Initializes the status of the block to STARTING, and
	 * the block's unique id.
	 */
	protected BaseBlock()
	{
		m_nStatus.set(INIT);
		m_nId = m_nIdCount.getAndIncrement(); //set Id
	}


	/**
	 * This method is called whenever an BaseBlock starts it service. It
 registers the BaseBlock with the directory then calls the overridable
 start() function for the BaseBlock. If the start() function returns true
 then this block's subscribers are notified that the service has started
 and this block's status is set to RUNNING. If any Exceptions are caught
 the status is set to ERROR. An BaseBlock can only start if its status is
 STARTING or STOPPED.
	 */
	public void startService()
	{
		int nStatus = m_nStatus.get();
		if (nStatus == INIT || m_nStatus.get() == STOPPED) //to start the status must be STARTING or STOPPED
		{
			try
			{
				checkAndSetStatus(STARTING, OVERRIDESTATUS); //if status is STOPPED set it to STARTING
				reset(); //sets all of the configurable variables
				m_oLogger.info("Calling start()");
				if (start()) //call the overridable start()
				{
					m_oLogger.info("Exiting start()");
					checkAndSetStatus(IDLE, OVERRIDESTATUS); // set status to IDLE
					notify("service started");
				}
				else
				{
					m_oLogger.error("start() returned false. Setting status to ERROR");
					checkAndSetStatus(ERROR, OVERRIDESTATUS); // set status to ERROR
				}
			}
			catch (Exception oException)
			{
				m_oLogger.error("Error starting service");
				m_oLogger.error(oException, oException);
				checkAndSetStatus(ERROR, OVERRIDESTATUS); // set status to ERROR
			}
		}
		else
		{
			m_oLogger.error("Cannot start. Current status: " + STATUSES[m_nStatus.get()]);
		}
	}


	/**
	 * This function is to be overridden by each BaseBlock to implement what
		the block needs to do to start its service
	 *
	 * @return true if the service starts without error
	 * @throws Exception this function throws any exception to be caught by the
	 * startService() function
	 */
	public boolean start() throws Exception
	{
		return true;
	}
	
	@Override
	public void init()
	{
		setLogger();
		setConfig();
	}
	
	@Override
	public void notify(String sMessageName, String... sResource)
	{
		String[] sMessage = new String[2 + sResource.length];
		sMessage[FROM] = getName();
		sMessage[MESSAGE] = sMessageName;
		System.arraycopy(sResource, 0, sMessage, 2, sResource.length);
		Directory.getInstance().notifyBlocks(sMessage);
	}
	
	
	@Override
	public void receive(String[] sMessage)
	{
		m_oNotifications.queue(sMessage);
	}
	
	@Override
	public void register()
	{
		Directory.getInstance().register(this, m_oConfig.getStringArray("subscribe", null));
	}


	/**
	 * Returns the status of the BaseBlock. Can be STOPPED, STARTING, RUNNING,
	 * IDLE, ERROR, or STOPPING.
	 *
	 * @return the status of the BaseBlock
	 */
	public long[] status()
	{
		return new long[]{m_nStatus.get(), m_lStatusChanged.get()};
	}

	@Override
	public void destroy()
	{
		stopService();
	}

	/**
	 * This method is called whenever an BaseBlock stops it service. It
 unregisters the BaseBlock from the directory, cancels its scheduled task
 if it has one, notifies subscribers that the service has stopped, cancels
 its subscriptions and calls the overridable stop() method. If stop()
 returns true then the status is set to STOPPED. If stop returns false or
 an Exception is caught the status is set to ERROR.
	 */
	public void stopService()
	{
		if (m_nStatus.get() == STOPPED) //can't stop an BaseBlock that is STOPPED
		{
			m_oLogger.error("Cannot stop service because it is already stopped");
			return;
		}
		try
		{
			checkAndSetStatus(STOPPING, OVERRIDESTATUS); //set status to STOPPING
			if (Scheduling.getInstance().cancelSched(this, m_nSchedId)) //cancel scheduled task
				m_oLogger.info("Task canceled");
			m_oNotifications.stop();

			if (stop()) //call the child's overridable stop
				checkAndSetStatus(STOPPED, OVERRIDESTATUS); //set status to STOPPED
			else
				checkAndSetStatus(ERROR, OVERRIDESTATUS); //set status to ERROR
			notify("service stopped");
		}
		catch (Exception oException)
		{
			m_oLogger.error("Error while trying to stop service");
			m_oLogger.error(oException, oException);
			checkAndSetStatus(ERROR, OVERRIDESTATUS); //set status to ERROR
		}
	}


	/**
	 * This function is to be overridden by each BaseBlock to implement what
 the block needs to do to stop its service
	 *
	 * @return true if the service stops without error
	 * @throws Exception this function throws any exception to be caught by the
	 * stopService() function
	 */
	public boolean stop() throws Exception
	{
		return true;
	}


	/**
	 * This function should be overriden if the BaseBlock has any configurable
	 * parameters to set. The function is called when the Block starts its
	 * service.
	 */
	protected void reset()
	{

	}


	/**
	 * Creates a new config object for the block by reading the config file.
	 */
	public void setConfig()
	{
		m_oConfig = new BlockConfig(getClass().getName(), m_sInstanceName);
	}
	
	
	@Override 
    public BlockConfig getConfig()
	{
		return m_oConfig;
	}


	/**
	 * Creates a new ImrcpResultSet depending on what kind of BaseBlock this
 is. Then fills the ImrcpResultSet with obs
	 *
	 * @param nType obstype id
	 * @param lStartTime start time of the query in milliseconds
	 * @param lEndTime end time of the query in milliseconds
	 * @param nStartLat lower bound of latitude (int scaled to 7 decimal places)
	 * @param nEndLat upper bound of latitude (int scaled to 7 decimals places)
	 * @param nStartLon lower bound of longitude (int scaled to 7 decimal
	 * places)
	 * @param nEndLon upper bound of longitude (int scaled to 7 decimal places)
	 * @param lRefTime
	 * @return a ResultSet filled with obs that match the extents of the query
	 */
	public ResultSet getData(int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{
		ImrcpResultSet oReturn = new ImrcpObsResultSet();
		int nStatus = m_nStatus.get();
		if (nStatus == RUNNING || nStatus == IDLE)
			getData(oReturn, nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime);
		return oReturn;
	}
	
	
	public ResultSet getAllData(int nType, long lStartTime, long lEndTime, long lRefTime)
	{
		return getData(nType, lStartTime, lEndTime, -849999999, 849999999, -1800000000, 1799999999, lRefTime);
	}


	/**
	 * This method is meant to be overridden by children classes to implement
	 * how the block returns data. The ImrcpResultSet that is passed as the
	 * first parameter is filled with the obs from the block.
	 *
	 * @param oReturn ImrcpResultSet that will be filled with obs
	 * @param nType obstype id
	 * @param lStartTime start time of the query in milliseconds
	 * @param lEndTime end time of the query in milliseconds
	 * @param nStartLat lower bound of latitude (int scaled to 7 decimal places)
	 * @param nEndLat upper bound of latitude (int scaled to 7 decimals places)
	 * @param nStartLon lower bound of longitude (int scaled to 7 decimal
	 * places)
	 * @param nEndLon upper bound of longitude (int scaled to 7 decimal places)
	 * @param lRefTime reference time
	 */
	protected void getData(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{

	}


	/**
	 * This function allows the BaseBlock to be executed by the ThreadPool.
	 * Usually the Block is scheduled to run on a specific interval. If the test
	 * boolean is set it will call the executeTest() instead of the execute().
	 * execute() will only be called if the block's status is IDLE. While in the
	 * execute() the block's status will be RUNNING, once it has return the status
	 * will go back to IDLE.
	 */
	@Override
	public void run()
	{
		if (checkAndSetStatus(RUNNING, IDLE)) // check if IDLE, if it is set to RUNNING and execute the block's task
		{
			if (m_bTest)
				executeTest();
			else
				execute();
			checkAndSetStatus(IDLE, RUNNING); // if still RUNNING, set back to IDLE
		}
		else
			m_oLogger.info(String.format("%s didn't run. Status was %s", m_sInstanceName, STATUSES[m_nStatus.get()]));
	}
	
	
	protected synchronized void setError()
	{
		checkAndSetStatus(ERROR, OVERRIDESTATUS);
	}
	
	
	protected synchronized boolean checkAndSetStatus(int nStatus, int nCheck)
	{
		if (nCheck == OVERRIDESTATUS)
		{
			
			m_nStatus.set(nStatus);
			m_lStatusChanged.set(System.currentTimeMillis());
			if (nStatus == ERROR)
				m_oLogger.error("Status set to ERROR");
			return true;
		}
		else
		{
			if (m_nStatus.compareAndSet(nCheck, nStatus))
			{
				m_lStatusChanged.set(System.currentTimeMillis());
				if (nStatus == ERROR)
					m_oLogger.error("Status set to ERROR");
				return true;
			}
			return false;
		}
	}


	/**
	 * This method is meant to be overridden by children classes to implement
	 * what the block does when it is executed.
	 */
	public void execute()
	{

	}


	/**
	 * This method is meant to be overridden by children classes to implement
	 * what the block does when it is executed in test mode
	 */
	public void executeTest()
	{

	}


	/**
	 * This method returns the Instance Name of the BaseBlock.
	 *
	 * @return Instance Name
	 */
	@Override
	public String getName()
	{
		return m_sInstanceName;
	}


	/**
	 * Set the instance name of the block
	 *
	 * @param sName the new name of the block
	 */
	@Override
	public void setName(String sName)
	{
		m_sInstanceName = sName;
	}


	/**
	 * Sets the logger for this block. This is called after the instance name
	 * has been set.
	 */
	public void setLogger()
	{
		// should fix the properties file later
		m_oLogger = LogManager.getLogger("imrcp." + m_sInstanceName); // append "imrcp." because of how the properties file for log4j is set up right now.
	}


	/**
	 * This method needs to be overridden in child classes to handle the
	 * Notifications the block can receive and implement the actions to be taken
	 * for the different type of Notification.
	 *
	 * @param e Notification from another block.
	 */
	@Override
	public void run(String[] e)
	{
		if (checkAndSetStatus(RUNNING, IDLE)) // process the notification if the status is IDLE and set status to RUNNING
		{
			process(e);
			checkAndSetStatus(IDLE, RUNNING); // if still RUNNING, set back to IDLE
		}
		else
		{
			int nStatus = m_nStatus.get();
			if (nStatus == STARTING || nStatus == RUNNING || nStatus == IDLE || nStatus == INIT) // if the block is starting or running requeue the notification to be processed later
				m_oNotifications.queue(e);
		}
	}	


	/**
	 * This method is meant to be overridden by the children classes to
	 * implement how it handles and processes different Notifications.
	 *
	 * @param e the Notification to be processed.
	 */
	public void process(String[] e)
	{

	}
}
