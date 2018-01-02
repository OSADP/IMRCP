/* 
 * Copyright 2017 Federal Highway Administration.
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

import imrcp.ImrcpBlock.Notification;
import imrcp.store.CAPStore;
import imrcp.store.ImrcpCapResultSet;
import imrcp.store.ImrcpEventResultSet;
import imrcp.store.ImrcpObsResultSet;
import imrcp.store.ImrcpResultSet;
import imrcp.store.KCScoutIncidentsStore;
import imrcp.system.AsyncQ;
import imrcp.system.Config;
import imrcp.system.Directory;
import imrcp.system.IRunTarget;
import imrcp.system.Scheduling;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This is the base class for most of the components that are a part of the
 * Imrcp system.
 */
public abstract class ImrcpBlock implements Runnable, IRunTarget<Notification>
{

	/**
	 * Counter used for to keep Ids unique
	 */
	private static AtomicInteger m_nIdCount = new AtomicInteger();

	/**
	 * The Id for the ImrcpBlock
	 */
	public int m_nId;

	/**
	 * The Scheduling Id set by the Scheduling class when the ImrcpBlock is set
	 * to run at a scheduled time
	 */
	protected int m_nSchedId;

	/**
	 * The registration Id set by the Directory class when the ImrcpBlock is
	 * registered to the system.
	 */
	public int m_nRegId;

	/**
	 * The status of the ImrcpBlock. Can be 0= STARTING, 1= RUNNING, 2 = IDLE, 3
	 * = ERROR, 4 = STOPPING , or 5 = STOPPED.
	 */
	public AtomicInteger m_nStatus = new AtomicInteger();

	/**
	 * Human-readable name of the ImrcpBlock
	 */
	protected String m_sInstanceName;

	/**
	 * List of subscriptions by Id the ImrcpBlock has
	 */
	protected ArrayList<Subscription> m_oSubscriptions = new ArrayList();

	/**
	 * List of ImrcpBlocks Id that subscribe to this ImrcpBlock
	 */
	protected ArrayList<Integer> m_oSubscribers = new ArrayList();

	/**
	 * Asynchronous queue used to process notifications as they come in
	 */
	protected final AsyncQ<Notification> m_oNotifications = new AsyncQ(this);

	/**
	 * List of ImrcpBlocks that are attached to this block
	 */
	public ArrayList<BlockRef> m_oAttached = new ArrayList();

	/**
	 * Array that contains the different statuses an ImrcpBlock can have
	 */
	public static final String[] m_sSTATUSES = new String[]
	{
		"STARTING", "RUNNING", "IDLE", "ERROR", "STOPPING", "STOPPED"
	};

	/**
	 * Logger
	 */
	protected Logger m_oLogger;

	/**
	 * Object that contains the configured variables for the ImrcpBlock
	 */
	protected BlockConfig m_oConfig;

	/**
	 * True if running in test mode
	 */
	protected boolean m_bTest = false;

	/**
	 * Array of obs type ids that the block has data for
	 */
	public int[] m_nSubObsTypes;

	/**
	 * Counter of how many blocks have notified this block that they have
	 * started
	 */
	public int m_nSubsStartedCount = 0;


	/**
	 * Default constructor. Initializes the status of the block to STARTING, and
	 * the block's unique id.
	 */
	protected ImrcpBlock()
	{
		m_nStatus.set(0); //set status to STARTING
		m_nId = m_nIdCount.getAndIncrement(); //set Id
	}


	/**
	 * This method is called whenever an ImrcpBlock starts it service. It
	 * registers the ImrcpBlock with the directory then calls the overridable
	 * start() function for the ImrcpBlock. If the start() function returns true
	 * then this block's subscribers are notified that the service has started
	 * and this block's status is set to RUNNING. If any Exceptions are caught
	 * the status is set to ERROR. An ImrcpBlock can only start if its status is
	 * STARTING or STOPPED.
	 */
	public void startService()
	{
		if (m_nStatus.get() == 0 || m_nStatus.get() == 5) //to start the status must be STARTING or STOPPED
		{
			try
			{
				m_nStatus.compareAndSet(5, 0); //if status is STOPPED set it to STARTING
				resetBlockConfig(); //read configured variables into the BlockConfig
				reset(); //sets all of the configurable variables
				int nSubscriptions = m_oSubscriptions.size();
				long lTimeout = System.currentTimeMillis() + m_oConfig.getInt("timeout", 1200000);
				while (m_nSubsStartedCount != nSubscriptions)
				{
					if (lTimeout < System.currentTimeMillis())
						break;
					Thread.sleep(1000);
				}
				m_oLogger.info("Calling start()");
				if (start()) //call the overridable start()
				{
					for (int nSub : m_oSubscribers) //notify subscribers the service has started.
						notify(this, nSub, "service started", null);
					m_nStatus.set(2);

				}
				else
				{
					m_oLogger.error("start() returned false. Setting status to ERROR");
					m_nStatus.set(3); //set status to ERROR
				}
			}
			catch (Exception oException)
			{
				m_oLogger.error("Error starting service");
				m_oLogger.error(oException, oException);
				m_nStatus.set(3); //set status to ERROR
			}
		}
		else
		{
			m_oLogger.error("Cannot start. Current status: " + status());
		}
	}


	/**
	 * This function is to be overridden by each ImrcpBlock to implement what
	 * the block needs to do to start its service
	 *
	 * @return true if the service starts without error
	 * @throws Exception this function throws any exception to be caught by the
	 * startService() function
	 */
	public boolean start() throws Exception
	{
		return true;
	}


	/**
	 * Returns the status of the ImrcpBlock. Can be STOPPED, STARTING, RUNNING,
	 * IDLE, ERROR, or STOPPING.
	 *
	 * @return the status of the ImrcpBlock
	 */
	public String status()
	{
		return m_sSTATUSES[m_nStatus.get()];
	}


	/**
	 * This method is called whenever an ImrcpBlock stops it service. It
	 * unregisters the ImrcpBlock from the directory, cancels its scheduled task
	 * if it has one, notifies subscribers that the service has stopped, cancels
	 * its subscriptions and calls the overridable stop() method. If stop()
	 * returns true then the status is set to STOPPED. If stop returns false or
	 * an Exception is caught the status is set to ERROR.
	 */
	public void stopService()
	{
		if (m_nStatus.get() == 5) //can't stop an ImrcpBlock that is STOPPED
		{
			m_oLogger.error("Cannot stop service because it is already stopped");
			return;
		}
		try
		{
			m_nStatus.set(4); //set status to STOPPING
			Directory.getInstance().unregister(this, m_nRegId); //unregister from directory
			if (Scheduling.getInstance().cancelSched(this, m_nSchedId)) //cancel scheduled task
				m_oLogger.info("Task canceled");
			for (int nSub : m_oSubscribers) //notify subscribers that the service has stopped
			{
				notify(this, nSub, "service stopped", null);
			}
			for (Subscription oSub : m_oSubscriptions)
			{
				unsubscribe(this, oSub.m_nSubscriptionId);
			}
			if (stop()) //call the child's overridable stop
				m_nStatus.set(5); //set status to STOPPED
			else
				m_nStatus.set(3); //set status to ERROR
		}
		catch (Exception oException)
		{
			m_oLogger.error("Error while trying to stop service");
			m_oLogger.error(oException, oException);
			m_nStatus.set(3); //set status to ERROR
		}
	}


	/**
	 * This function is to be overridden by each ImrcpBlock to implement what
	 * the block needs to do to stop its service
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
	 * Creates a subscription for this ImrcpBlock to another ImrcpBlock.
	 *
	 * @param nType observation types for the subscription
	 * @param lStartTime timestamp for the start of the subscription
	 * @param lEndTime timestamp for the end of the subscription
	 * @param nStartLat latitude of the bottom of bounding box in micro degrees
	 * @param nEndLat latitude of the top of bounding box in micro degrees
	 * @param nStartLon longitude of the left of bounding box in micro degrees
	 * @param nEndLon longitude of the right of bounding box in micro degrees
	 * @param sSubscribe instance name of the block this is subscribing to
	 * @return 0 no errors occur. -1 if the block this wants to subscribe to is
	 * not found in the Directory
	 */
	public synchronized int subscribe(int[] nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, String sSubscribe)
	{
		ImrcpBlock oSubscribe = Directory.getInstance().lookup(sSubscribe); //find the ImrcpBlock in the Directory
		if (oSubscribe != null)
		{
			m_oLogger.info(getInstanceName() + " subscribing to " + oSubscribe.getInstanceName());
			m_oSubscriptions.add(new Subscription(nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, oSubscribe)); //add the subscription to this blocks subscription list
			oSubscribe.m_oSubscribers.add(this.m_nId); //add this as a subscriber in the subscribed blcok
			return 0;
		}
		else
		{
			m_oLogger.error("Couldn't subscribe to " + sSubscribe + ": not found in directory");
			return -1;
		}
	}


	/**
	 * This method allows one ImrcpBlock to send a Notification to another
	 * ImrcpBlock.
	 *
	 * @param oProvider the ImrcpBlock sending the Notification
	 * @param nSubscriberId the Id of the ImrcpBlock that receiving the
	 * Notification
	 * @param sMessage a message to describe the type of Notification
	 * @param sResource Notification resource for example a file name
	 */
	public void notify(ImrcpBlock oProvider, int nSubscriberId, String sMessage, String sResource)
	{
		ImrcpBlock oNotified = Directory.getInstance().findBlockById(nSubscriberId); //find the block that is being notified
		if (oNotified != null) //if it is found
		{
			synchronized (oNotified.m_oNotifications)
			{
				oNotified.m_oNotifications.queue(new Notification(oProvider.m_nId, System.currentTimeMillis(), sMessage, sResource)); //queue the Notification in the Async queue
				m_oLogger.info(oProvider.getInstanceName() + " notified " + oNotified.getInstanceName() + ": " + sMessage);
			}
		}
		else
			m_oLogger.info("Notify() failed - invalid id");
	}


	/**
	 * Unsubscribes the Subscriber from the given subscription id.
	 *
	 * @param oSubscriber The block that is subscribed
	 * @param nSubscriptionId subscription id of the block to unsubscribe from
	 */
	public void unsubscribe(ImrcpBlock oSubscriber, int nSubscriptionId)
	{
		ImrcpBlock oSubscribed = null;
		int nIndex = m_oSubscriptions.size();
		while (nIndex-- > 0)
		{
			if (m_oSubscriptions.get(nIndex).m_nSubscriptionId == nSubscriptionId)
			{
				oSubscribed = m_oSubscriptions.get(nIndex).m_oSubscribedTo;
				break;
			}
		}
		ArrayList<Integer> oTemp = null;
		if (oSubscribed != null)
		{
			oTemp = oSubscribed.m_oSubscribers;
			nIndex = oTemp.size();
			while (nIndex-- > 0)
				if (oTemp.get(nIndex) == m_nId)
					oTemp.remove(nIndex);
		}
	}


	/**
	 * This function should be overriden if the ImrcpBlock has any configurable
	 * parameters to set. The function is called when the Block starts its
	 * service.
	 */
	protected void reset()
	{

	}


	/**
	 * Creates a new config object for the block by reading the config file.
	 */
	private void resetBlockConfig()
	{
		m_oConfig = new BlockConfig(getClass().getName(), m_sInstanceName);
	}


	/**
	 * Creates a new ImrcpResultSet depending on what kind of ImrcpBlock this
	 * is. Then fills the ImrcpResultSet with obs
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
		ImrcpResultSet oReturn;
		if (this instanceof KCScoutIncidentsStore)
			oReturn = new ImrcpEventResultSet();
		else if (this instanceof CAPStore)
			oReturn = new ImrcpCapResultSet();
		else
			oReturn = new ImrcpObsResultSet();
		if (m_nStatus.get() == 1 || m_nStatus.get() == 2)
			getData(oReturn, nType, lStartTime, lEndTime, nStartLat, nEndLat, nStartLon, nEndLon, lRefTime);
		return oReturn;
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
	protected synchronized void getData(ImrcpResultSet oReturn, int nType, long lStartTime, long lEndTime,
	   int nStartLat, int nEndLat, int nStartLon, int nEndLon, long lRefTime)
	{

	}


	/**
	 * This method attaches the ImrcpBlock that invokes this method to a given
	 * ImrcpBlock for a given filter (instance name that the invoking Block is
	 * interested in)
	 *
	 * @param oAttachTo the ImrcpBlock that this is attaching to
	 * @param sFilterName the filter it is attaching for
	 */
	public void attach(ImrcpBlock oAttachTo, String sFilterName)
	{
		boolean bFound = false;
		if (this == oAttachTo) //check if a Block is trying to attach to itself
			return;
		for (BlockRef oTemp : oAttachTo.m_oAttached)  //check to see if this block is already attached
		{
			if (oTemp.m_nId == m_nId)  //if it is already attached
			{
				for (String sSearch : oTemp.m_oFilters) //see if it is attached for the given filter
					if (sFilterName.compareTo(sSearch) == 0)
						return; //do nothing if it is already attached for that filter

				bFound = true; //set flag so a new Block isn't added to the list
				oTemp.m_oFilters.add(sFilterName); //add the filter to the list of filters this block is attached for
				m_oLogger.info(this.getInstanceName() + " attached to " + oAttachTo.getInstanceName() + " for " + sFilterName);
			}
		}

		if (!bFound) //if this block is not already attached
		{
			oAttachTo.m_oAttached.add(new BlockRef(m_nId, sFilterName));  //create a new reference to add to its attached list
			m_oLogger.info(this.getInstanceName() + " attached to " + oAttachTo.getInstanceName() + " for " + sFilterName);
		}
	}


	/**
	 * This method detaches the ImrcpBlock that invokes the method from a given
	 * ImrcpBlock.
	 *
	 * @param oDetachFrom the ImrcpBlock that this is detaching from
	 */
	public void detach(ImrcpBlock oDetachFrom)
	{
		int nIndex = oDetachFrom.m_oAttached.size();
		while (nIndex-- > 0) //iterate through the list of attached blocks
		{
			if (m_nId == oDetachFrom.m_oAttached.get(nIndex).m_nId) //if this block is in the attached list
			{
				m_oLogger.info(this.m_sInstanceName + " detached from " + oDetachFrom.getInstanceName());
				oDetachFrom.m_oAttached.remove(nIndex); //remove it from the list
				return;
			}
		}
	}


	/**
	 * This function allows the ImrcpBlock to be executed by the ThreadPool.
	 * Usually the Block is scheduled to run on a specific interval. If the test
	 * boolean is set it will call the executeTest() instead of the execute().
	 * execute() will only be called if the block's status is IDLE. While in the
	 * execute() the block's status will be RUNNING, once it has return the status
	 * will go back to IDLE.
	 */
	@Override
	public void run()
	{
		if (m_nStatus.compareAndSet(2, 1)) // check if IDLE, if it is set to RUNNING and execute the block's task
		{
			if (m_bTest)
				executeTest();
			else
				execute();
			m_nStatus.compareAndSet(1, 2); // if still RUNNING, set back to IDLE
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
	 * This method returns the Instance Name of the ImrcpBlock.
	 *
	 * @return Instance Name
	 */
	public String getInstanceName()
	{
		return m_sInstanceName;
	}


	/**
	 * Set the instance name of the block
	 *
	 * @param sName the new name of the block
	 */
	public void setInstanceName(String sName)
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
	public void run(Notification e)
	{
		if (e.m_sMessage.compareTo("service started") == 0) // handle a subscribed service starting
		{
			++m_nSubsStartedCount;
			//m_nStatus.compareAndSet(0, 2); // if all blocks this is subscribed to are ready and this is STARTING, set status to IDLE
			process(e);
		}
		else if (e.m_sMessage.compareTo("service stopped") == 0) // handle a subscribed service stopping
		{
			//m_nStatus.set(0); // a block this is subscribed to is no longer running so set to this status to STARTING
		}
		else if (e.m_sMessage.compareTo("polygons ready") == 0)
			process(e);
		else if (m_nStatus.compareAndSet(2, 1)) // process the notification if the status is IDLE and set status to RUNNING
		{
			process(e);
			m_nStatus.compareAndSet(1, 2); // if still RUNNING, set back to IDLE
		}
		else if (m_nStatus.get() == 1 || m_nStatus.get() == 0) // requeue the notification if the block is STARTING or RUNNING
			m_oNotifications.queue(e);
	}


	/**
	 * This method is meant to be overridden by the children classes to
	 * implement how it handles and processes different Notifications.
	 *
	 * @param e the Notification to be processed.
	 */
	public void process(Notification e)
	{

	}

	/**
	 * Inner class that allows ImrcpBlocks to be stored by just their Id and
	 * filters (which are interfaces or instance names). This is primarily used
	 * for keeping track of ImrcpBlocks that are attached to another ImrcpBlock
	 */
	public class BlockRef
	{

		/**
		 * Id that corresponds to the ImrcpBlock's Id
		 */
		public int m_nId;

		/**
		 * List of filters the ImrcpBlock is attached for
		 */
		private ArrayList<String> m_oFilters = new ArrayList();


		/**
		 * Custom constructor. Sets the Id and adds the filter to the filter
		 * list.
		 *
		 * @param nId the ImrcpBlock's Id
		 * @param sFilter the filter to be added to the list
		 */
		BlockRef(int nId, String sFilter)
		{
			m_nId = nId;
			m_oFilters.add(sFilter);
		}
	}

	/**
	 * Inner class that acts as a notification from one ImrcpBlock to another.
	 * This allows blocks to communicate with each other
	 */
	public class Notification
	{

		/**
		 * Id of the ImrcpBlock that provided the Notification
		 */
		public int m_nProviderId;

		/**
		 * Timestamp of when the Notification was sent
		 */
		public long m_lTimeNotified;

		/**
		 * A message describing the type of Notification
		 */
		public String m_sMessage;

		/**
		 * Resource that goes with the Notification (example file name)
		 */
		public String m_sResource;


		/**
		 * Custom constructor. Allows Notifications to be created with a given
		 * Id and timestamp
		 *
		 * @param nProviderId Id of the ImrcpBlock that provided the
		 * Notification
		 * @param lTimeNotified Timestamp of when the Notification was sent
		 */
		Notification(int nProviderId, long lTimeNotified)
		{
			m_nProviderId = nProviderId;
			m_lTimeNotified = lTimeNotified;
			m_sMessage = "";
			m_sResource = null;
		}


		/**
		 * Custom constructor. Allows Notification to be created with a given
		 * Id, timestamp, and message.
		 *
		 * @param nProviderId Id of the ImrcpBlock that provided the
		 * Notification
		 * @param lTimeNotified Timestamp of when the Notification was sent
		 * @param sMessage message describing the type of Notification
		 */
		Notification(int nProviderId, long lTimeNotified, String sMessage, String sResource)
		{
			m_nProviderId = nProviderId;
			m_lTimeNotified = lTimeNotified;
			m_sMessage = sMessage;
			m_sResource = sResource;
		}
	}

	/**
	 * Subscriptions allow one ImrcpBlock to subscribe to another meaning that
	 * the block that is the subscriber will receive all of the Notifications
	 * sent from the other block.
	 */
	protected class Subscription
	{

		/**
		 * Subscription id
		 */
		public int m_nSubscriptionId;

		/**
		 * Reference of the ImrcpBlock that this is subscribed to
		 */
		public ImrcpBlock m_oSubscribedTo;

		/**
		 * Int array of obs type Ids that the subscribed block produces
		 */
		public int[] m_nObsTypes;

		/**
		 * Start time of the subscription (not used at the moment)
		 */
		public long m_lStartTime;

		/**
		 * End time of the subscription (not used at the moment)
		 */
		public long m_lEndTime;

		/**
		 * Lower bound for latitude of the subscription (not used at the moment)
		 */
		public int m_nStartLat;

		/**
		 * Upper bound for latitude of the subscription (not used at the moment)
		 */
		public int m_nEndLat;

		/**
		 * Lower bound for longitude of the subscription (not used at the moment)
		 */
		public int m_nStartLon;

		/**
		 * Upper bound for longitude of the subscription (not used at the moment)
		 */
		public int m_nEndLon;


		/**
		 * Creates a new Subscription with the given parameters
		 * @param nObsTypes
		 * @param lStartTime
		 * @param lEndTime
		 * @param nStartLat
		 * @param nEndLat
		 * @param nStartLon
		 * @param nEndLon
		 * @param oSubscribedTo
		 */
		Subscription(int[] nObsTypes, long lStartTime, long lEndTime,
		   int nStartLat, int nEndLat, int nStartLon, int nEndLon, ImrcpBlock oSubscribedTo)
		{
			m_nSubscriptionId = m_nIdCount.getAndIncrement();
			m_nObsTypes = nObsTypes;
			m_lStartTime = lStartTime;
			m_lEndTime = lEndTime;
			m_nStartLat = nStartLat;
			m_nEndLat = nEndLat;
			m_nStartLon = nStartLon;
			m_nEndLon = nEndLon;
			m_oSubscribedTo = oSubscribedTo;
		}
	}

	/**
	 * HashMap that contains all of the configuration items for an ImrcpBlock,
	 * mapping the key(a String) to the value(s) (a String array)
	 */
	protected class BlockConfig extends HashMap<String, String[]>
	{
		/**
		 * Creates a new BlockConfig by retrieving the configuration items from
		 * Config based on the fully qualified name and instance name
		 * @param sClass Fully qualified name of the java class
		 * @param sName Instance name of the ImrcpBlock
		 */
		BlockConfig(String sClass, String sName)
		{
			putAll(Config.getInstance().getProps(sClass, sName));
		}


		/**
		 * Returns the configured value as a String for the given key.
		 * @param sKey Key of the value needed
		 * @param sDefault Default value, used if the key cannot be found
		 * @return If the key is found the value as a String, otherwise
		 * the default value
		 */
		public String getString(String sKey, String sDefault)
		{
			String[] sReturn = get(sKey);
			if (sReturn != null)
				return sReturn[0];
			else
				return sDefault;
		}


		/**
		 * Returns the configured value as an int for the given key
		 * @param sKey Key of the value needed
		 * @param nDefault Default value, used if the key cannot be found
		 * @return If the key is found the value as an int, otherwise the 
		 * default value.
		 */
		public int getInt(String sKey, int nDefault)
		{
			String[] sReturn = get(sKey);
			if (sReturn != null)
				return Integer.parseInt(sReturn[0]);
			else
				return nDefault;
		}


		/**
		 * Returns the configured value as a String array for the given key.
		 * @param sKey Key of the value needed
		 * @param sDefault Default value, used if the key cannot be found
		 * @return If the key is found the value as a String array, otherwise
		 * the default value in a String array with length 1. If the default 
		 * value is null, an empty String array.
		 */
		public String[] getStringArray(String sKey, String sDefault)
		{
			String[] sReturn = get(sKey);
			if (sReturn != null)
				return sReturn;
			else if (sDefault != null)
				return new String[]{sDefault};
			else
				return new String[0];
		}


		/**
		 * Returns the configured value as an int array for the given key.
		 * @param sKey Key of the value needed
		 * @param nDefault Default value, used if the key cannot be found
		 * @return If the key is found the value as an int array, otherwise
		 * the default value in an int array with length 1.
		 */
		public int[] getIntArray(String sKey, int nDefault)
		{
			String[] sReturn = get(sKey);
			int[] nReturn;
			if (sReturn != null)
			{
				nReturn = new int[sReturn.length];
				for (int i = 0; i < sReturn.length; i++)
					nReturn[i] = Integer.parseInt(sReturn[i]);
				return nReturn;
			}
			else
				return new int[]{nDefault};
		}
	}
}
