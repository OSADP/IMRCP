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
package imrcp.system;

import imrcp.ImrcpBlock;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.SimpleTimeZone;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServlet;
import javax.sql.DataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This singleton class acts as a Directory for the system. ImrcpBlocks that are
 * using in the system register to the Directory. When an ImrcpBlock is no
 * longer in use, the block will unregister from the Directory. It extends
 * HttpServlet and initializes the system.
 */
public class Directory extends HttpServlet
{

	/**
	 * Singleton instance of Directory
	 */
	private static Directory g_oDirectory;

	/**
	 * Counter variable used for registration Ids
	 */
	private static AtomicInteger m_nIdCount = new AtomicInteger();

	/**
	 * List of all of the Registered Blocks
	 */
	private final ArrayList<BlockFilter> m_oRegistered = new ArrayList();

	/**
	 * List of all of the components that Directory starts
	 */
	private ArrayList<Object> m_oComponents = new ArrayList();

	/**
	 * Logger
	 */
	private Logger m_oLogger = LogManager.getLogger(Directory.class);

	/**
	 * UTC time znoe
	 */
	public static final SimpleTimeZone m_oUTC = new SimpleTimeZone(0, "");

	/**
	 * Central time zone
	 */
	public static final TimeZone m_oCST6CDT = TimeZone.getTimeZone("CST6CDT");

	/**
	 * DataSource for the database
	 */
	private DataSource m_iDatasource;


	/**
	 * Default constructor. Looks up the jdbc context.
	 */
	public Directory()
	{
		g_oDirectory = this;
		try
		{
			Context oCtx = new InitialContext();
			m_iDatasource = (DataSource) oCtx.lookup("java:/comp/env/jdbc/imrcp");
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}


	/**
	 * This method returns a reference to the singleton instance of Directory
	 *
	 * @return singleton reference of Directory
	 */
	public static Directory getInstance()
	{
		return g_oDirectory;
	}


	/**
	 * This method is called when the Servlet is initialized. It loads all of
	 * the classes specified in the Configuration File.
	 */
	@Override
	public void init()
	{
		try
		{
			m_oLogger.info("Starting init()");
			ServletConfig oServletConfig = getServletConfig();
			Config oConfig = new Config(oServletConfig.getInitParameter("config-file"));
			Scheduling oSched = Scheduling.getInstance();
			oConfig.setSchedule();
			String[] sClasses = oConfig.getStringArray(getClass().getName(), "Directory", "classes", null); //get the array of classes that need to be initialized upon system startup
			for (int i = 0; i < sClasses.length; i++) //iterate through the strings
			{
				String sClassName = sClasses[i++]; // increment here because the classes configuration contains pairs: java class name, instance name
				if (sClassName.startsWith("#")) // allows for "comments" in the json array
					continue;
				Object oBlock = null;
				try
				{
					m_oLogger.info("Adding " + sClassName + " to components");
					oBlock = Class.forName(sClassName).newInstance();
					m_oComponents.add(oBlock); //add a new instance of the class into the components list
					((ImrcpBlock) oBlock).setInstanceName(sClasses[i]);
					((ImrcpBlock) oBlock).setLogger();
					String[] sFilter = oConfig.getStringArray(oBlock.getClass().getName(), ((ImrcpBlock) oBlock).getInstanceName(), "attach", null); //read in the filters for this block's instance name
					((ImrcpBlock) oBlock).m_nRegId = register((ImrcpBlock) oBlock, sFilter); //register the block to the Directory
				}
				catch (Exception oException)
				{
					m_oLogger.error(oException, oException);
				}
			}

			// create all of the subscriptions for the registered blocks, then
			// start each block's service
			int nSize = m_oRegistered.size();
			int[] nBlank = new int[0];
			synchronized (m_oRegistered)
			{
				for (int i = 0; i < nSize; i++)
				{
					ImrcpBlock oBlock = m_oRegistered.get(i).m_oBlock;
					String[] sSubscribeTo = Config.getInstance().getStringArray(oBlock.getClass().getName(), oBlock.getInstanceName(), "subscribe", null); //read in the filters for this block's instance name
					String[] sObsTypes = oConfig.getStringArray(oBlock.getClass().getName(), oBlock.getInstanceName(), "subobs", null); //get the obstypes this block subscribes for
					oBlock.m_nSubObsTypes = new int[sObsTypes.length]; //convert String[] to int[]
					for (int j = 0; j < sObsTypes.length; j++)
						oBlock.m_nSubObsTypes[j] = Integer.valueOf(sObsTypes[j], 36);
					if (sSubscribeTo.length > 0)
					{
						for (String sSubscribe : sSubscribeTo) //subscribe to each block
							oBlock.subscribe(oBlock.m_nSubObsTypes, 0, Long.MAX_VALUE, -90000000, 90000000, -180000000, 179999999, sSubscribe);
					}
					if (oBlock.m_nSubObsTypes == null)
						oBlock.m_nSubObsTypes = nBlank;
				}

				for (int i = 0; i < nSize; i++) // call each blocks start function with a different thread
					oSched.execute(new Startup(m_oRegistered.get(i).m_oBlock));
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}


	/**
	 * This method allows an ImrcpBlock to register with the system. Part of
	 * registering is attaching to already registered ImrcpBlocks that have an
	 * interface that is an instance of the registering block. Already
	 * registered ImrcpBlocks also will attach to the registering block if the
	 * registering block has an interface that is an instance of the registered
	 * block. The term instance refers to the name an ImrcpBlock wants to attach
	 * to. The term interface refers to the name an ImrcpBlock has that other
	 * ImrcpBlocks are looking for.
	 *
	 * @param oBlock the ImrcpBlock that is being registered
	 * @param sInstances a list of all the instance names it needs to attach to
	 * @return the registration Id
	 */
	public int register(ImrcpBlock oBlock, String[] sInstances)
	{
		synchronized (m_oRegistered)
		{
			try
			{
				for (BlockFilter oTemp : m_oRegistered) //check if that block has already been registered
				{
					if (oTemp.m_oBlock == oBlock)
						return -1;
				}
				Class[] oInterfaces = oBlock.getClass().getInterfaces(); //get the names of all the interfaces implemented by the block
				String[] sInterfaces;
				if (oInterfaces.length == 0)
				{
					sInterfaces = new String[1];
				}
				else
				{
					sInterfaces = new String[oInterfaces.length + 1];
					for (int i = 1; i < sInterfaces.length; i++)
						sInterfaces[i] = oInterfaces[i - 1].getName();
				}
				sInterfaces[0] = oBlock.getInstanceName(); //include the instance name in the list of interfaces

				BlockFilter oFilter = new BlockFilter(m_nIdCount.incrementAndGet(), oBlock, sInstances);
				for (BlockFilter oSearch : m_oRegistered) //see if this block needs to be attached to each registered block or if each registered block needs to attach to it based off of the two block's interfaces and instances
				{
					Class[] oRegInterfaces = oSearch.m_oBlock.getClass().getInterfaces(); //get the names of all the interfaces implemented by the registered block
					String[] sRegInterfaces;
					if (oRegInterfaces.length == 0)
					{
						sRegInterfaces = new String[1];
					}
					else
					{
						sRegInterfaces = new String[oRegInterfaces.length + 1];
						for (int i = 1; i < sRegInterfaces.length; i++)
							sRegInterfaces[i] = oRegInterfaces[i - 1].getName();
					}
					sRegInterfaces[0] = oSearch.m_oBlock.getInstanceName(); //add the instance name to the list of interfaces

					for (String sInstance : sInstances) //compare each instance("what I want") of the registering block with the interfaces("what I have") of the registered block
					{
						for (String sSearch : sRegInterfaces)
						{
							if (sInstance.compareTo(sSearch) == 0) //if the registered block has an interface which is an instance of the registering block
								oBlock.attach(oSearch.m_oBlock, sInstance);  //attach the registering block to the registered block
						}
					}

					for (String sInterface : sInterfaces) //compare each interface("what I have") of the registering block with the instances("what I want") of the registered block
					{
						for (String sInstance : oSearch.m_sInstances)
						{
							if (sInstance.compareTo(sInterface) == 0) //if the registering block has an interface which is an instance of the registered block
								oSearch.m_oBlock.attach(oBlock, sInterface); //atach the registered block to the registering block.
						}
					}
				}

				m_oRegistered.add(oFilter); //add the Block to the list of registered blocks
				m_oLogger.info(oFilter.m_oBlock.getInstanceName() + " registered");
			}
			catch (Exception oException)
			{
				m_oLogger.error(oException, oException);
			}
			return m_nIdCount.get();
		}
	}


	/**
	 * This method allows the Directory to unregister an ImrcpBlock.
	 *
	 * @param oBlock the ImrcpBlock to be unregistered
	 * @param nRegId the Registration Id of the ImrcpBlock
	 * @return
	 */
	public boolean unregister(ImrcpBlock oBlock, int nRegId)
	{
		boolean bReturn = false;
		BlockFilter oTemp = null;
		for (BlockFilter oFilter : m_oRegistered) //check if the block is registered 
		{
			if (oBlock == oFilter.m_oBlock && oFilter.m_nRegId == nRegId) //if it is registered make a copy of that BlockFilter
			{
				oTemp = oFilter;
				break;
			}
		}

		if (oTemp != null) //if the block is registered
		{
			int nIndex = m_oRegistered.size();
			while (nIndex-- > 0) //iterate through all blocks registered
			{
				if (m_oRegistered.get(nIndex).compareTo(oTemp) == 0) //if it is the block to be unregistered remove it
				{
					m_oLogger.info(m_oRegistered.get(nIndex).m_oBlock.getInstanceName() + " unregistered");
					m_oRegistered.remove(nIndex);
				}
				else //if it is not the block to be unregistered 
				{
					oBlock.detach(m_oRegistered.get(nIndex).m_oBlock); //detach it from the registered block
				}
			}
			bReturn = true;
		}
		for (ImrcpBlock.BlockRef oRef : oBlock.m_oAttached)
		{
			m_oLogger.info(findBlockById(oRef.m_nId).getInstanceName() + " detached from " + oBlock.getInstanceName());
		}
		oBlock.m_oAttached.clear(); //remove all the blocks attached to the block that is unregistered
		return bReturn;
	}


	/**
	 * This method stops the Directory service which shuts down the entire
	 * system.
	 */
	@Override
	public void destroy()
	{
		for (Object oClass : m_oComponents)
		{
			((ImrcpBlock) oClass).stopService();
		}

		int nIndex = m_oRegistered.size();
		while (nIndex-- > 0)
		{
			m_oRegistered.get(nIndex).m_oBlock.stopService();
		}

		m_oComponents.clear(); //clear the list of componen
		m_oRegistered.clear();
		Scheduling.getInstance().stop();
	}


	/**
	 * Searches through the registered ImrcpBlocks to find the one with the
	 * given Id
	 *
	 * @param nId Id of the block to be found
	 * @return reference to the ImrcpBlock with the given Id if found. If the
	 * block is not found null is returned.
	 */
	public synchronized ImrcpBlock findBlockById(int nId)
	{
		Collections.sort(m_oRegistered); //sort the list so binary search can be done
		int nIndex = Collections.binarySearch(m_oRegistered, new BlockFilter(0, new SearchBlock(nId), null)); //the compareTo only check the nId so create a SearchBlock to find the block by id
		if (nIndex >= 0)
			return m_oRegistered.get(nIndex).m_oBlock;
		else
			return null;
	}


	/**
	 * Finds and returns a reference to the registered ImrcpBlock with the given
	 * instance name.
	 *
	 * @param sName name of the desired block
	 * @return reference to the desired ImrcpBlock or null if it couldn't be
	 * found
	 */
	public synchronized ImrcpBlock lookup(String sName)
	{
		try
		{
			for (BlockFilter oBlock : m_oRegistered)
			{
				if (oBlock.m_oBlock.getInstanceName().compareTo(sName) == 0)
					return oBlock.m_oBlock;
			}
			return null; //couldn't find the given Block 
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
			return null;
		}
	}


	/**
	 * Restarts the service for the given ImrcpBlock's instance name
	 *
	 * @param sBlock instance name of the ImrcpBlock that is going to be
	 * restarted
	 */
	public void restartService(String sBlock)
	{
		ImrcpBlock oBlock = lookup(sBlock);
		oBlock.stopService();
		String[] sFilter = Config.getInstance().getStringArray(oBlock.getClass().getName(), ((ImrcpBlock) oBlock).getInstanceName(), "attach", null); //read in the filters for this block's instance name
		oBlock.m_nRegId = register(oBlock, sFilter); //register the block to the Directory
		oBlock.startService();
	}


	/**
	 * Searches through the registered ImrcpBlocks to return a List filled with
	 * the Stores that contain Obs of the given obs type id.
	 *
	 * @param nType integer obs type id
	 * @return a List filled with the Stores that contain Obs of the given obs
	 * type id
	 */
	public synchronized List<ImrcpBlock> getStoresByObs(int nType)
	{
		List<ImrcpBlock> oReturn = new ArrayList();
		for (BlockFilter oBlock : m_oRegistered)
		{
			for (int nSubObsType : oBlock.m_oBlock.m_nSubObsTypes)
			{
				if (nSubObsType == nType)
				{
					oReturn.add(oBlock.m_oBlock);
					break;
				}
			}
		}
		return oReturn;
	}


	/**
	 * Returns a connection to the database
	 *
	 * @return Connection to the database
	 * @throws Exception
	 */
	public Connection getConnection() throws Exception
	{
		return m_iDatasource.getConnection();
	}

	/**
	 * Inner class used to keep track of ImrcpBlocks that are registered along
	 * with their instances so the ImrcpBlock can easily attach to other
	 * ImrcpBlocks
	 */
	public class BlockFilter implements Comparable<BlockFilter>
	{

		/**
		 * Registration Id
		 */
		private int m_nRegId;

		/**
		 * The registered ImrcpBlock
		 */
		private ImrcpBlock m_oBlock;

		/**
		 * Instance names that this block wants to attach to
		 */
		private String[] m_sInstances;


		/**
		 * Default constructor.
		 */
		BlockFilter()
		{
		}


		/**
		 * Custom constructor. Sets all member variables.
		 *
		 * @param nRegId registration Id
		 * @param oBlock ImrcpBlock that is registered
		 * @param sInstances the ImrcpBlock's instance filters
		 */
		BlockFilter(int nRegId, ImrcpBlock oBlock, String[] sInstances)
		{
			m_nRegId = nRegId;
			m_oBlock = oBlock;
			m_sInstances = sInstances;
		}


		/**
		 * Allows BlockFilters to be compared by their block's Id
		 *
		 * @param oFilter the BlockFilter that is compared to this
		 * @return 0 if the Ids are equal, a non zero number if the Ids are not
		 * equal
		 */
		@Override
		public int compareTo(BlockFilter oFilter)
		{
			return m_oBlock.m_nId - oFilter.m_oBlock.m_nId;
		}
	}

	/**
	 * Inner class used to start the services of the ImrcpBlocks. Implements
	 * Runnable so the thread pool can execute them in parallel
	 */
	private class Startup implements Runnable
	{

		ImrcpBlock m_oBlock;


		Startup(ImrcpBlock oBlock)
		{
			m_oBlock = oBlock;
		}


		/**
		 * Calls the ImrcpBlocks startService method
		 */
		@Override
		public void run()
		{
			m_oBlock.startService();
		}
	}

	/**
	 * Inner class used to search for ImrcpBlocks by Id
	 */
	private class SearchBlock extends ImrcpBlock
	{

		SearchBlock(int nId)
		{
			m_nId = nId;
		}
	}
}
