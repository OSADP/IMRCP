package imrcp.system;

import imrcp.BaseBlock;
import imrcp.FileCache;
import java.sql.Connection;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
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
import imrcp.ImrcpBlock;

/**
 * This singleton class acts as a Directory for the system. ImrcpBlocks that are
 using in the system register to the Directory. When an BaseBlock is no
 longer in use, the block will unregister from the Directory. It extends
 HttpServlet and initializes the system.
 */
public class Directory extends HttpServlet implements Runnable, Comparator<ImrcpBlock>
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
	private final ArrayList<RegisteredBlock> m_oRegistered = new ArrayList();


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
	
	private int m_nMonitorLimit = 120;
	
	private RegisteredBlock m_oSearch = new RegisteredBlock(-1, new SearchBlock(-1), 0L);
	
	private HashMap<Integer, Integer> m_oContribPrefMap;
	
	private ArrayDeque<Startup> m_oStartups = new ArrayDeque();


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
			Scheduling.getInstance(); // create the Scheduling singleton
			oConfig.setSchedule();
			m_oContribPrefMap = new HashMap();
			String[] sContribPreferences = oConfig.getStringArray(getClass().getName(), "Directory", "prefer", null);
			for (String sContrib : sContribPreferences)
				m_oContribPrefMap.put(Integer.valueOf(sContrib, 36), oConfig.getInt(getClass().getName(), "Directory", sContrib, 0));
			String[] sClasses = oConfig.getStringArray(getClass().getName(), "Directory", "classes", null); //get the array of classes that need to be initialized upon system startup
			m_nMonitorLimit = oConfig.getInt(getClass().getName(), "Directory", "monlim", 120);
			for (int i = 0; i < sClasses.length; i++) //iterate through the strings
			{
				String sClassName = sClasses[i++]; // increment here because the classes configuration contains pairs: java class name, instance name
				if (sClassName.startsWith("#")) // allows for "comments" in the json array
					continue;
				BaseBlock oBlock = null;
				try
				{
					oBlock = (BaseBlock)Class.forName(sClassName).newInstance();
					oBlock.setName(sClasses[i]);
					oBlock.init();
					oBlock.register();
				}
				catch (Exception oException)
				{
					m_oLogger.error(oException, oException);
				}
			}
			
			synchronized (m_oRegistered)
			{
				int nSize = m_oRegistered.size();
				for (int i = 0; i < nSize; i++)
				{
					RegisteredBlock oRegBlock = m_oRegistered.get(i);					
					if (oRegBlock.m_oDependencies.isEmpty())
						queueStartup(new Startup(m_oRegistered.get(i)));
				}
			}
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
	}

	@Override
	public void run()
	{
		synchronized (m_oStartups)
		{
			if (m_oStartups.isEmpty()) // ensure there is work to be done
			{
				return;
			}
			Startup oStartup = m_oStartups.pollLast();
			Scheduling.getInstance().execute(oStartup);
		}
	}
	
	private void queueStartup(Startup oStartup)
	{
		synchronized (m_oStartups)
		{
			m_oStartups.addFirst(oStartup);
			Scheduling.getInstance().execute(this);
		}	
		
	}
	
	
	/**
	 * This method allows an BaseBlock to register with the system. Part of
 registering is attaching to already registered ImrcpBlocks that have an
 interface that is an instance of the registering block. Already
 registered ImrcpBlocks also will attach to the registering block if the
 registering block has an interface that is an instance of the registered
 block. The term instance refers to the name an BaseBlock wants to attach
 to. The term interface refers to the name an BaseBlock has that other
 ImrcpBlocks are looking for.
	 *
	 * @param oBlock the BaseBlock that is being registered
	 * @param sInstances a list of all the instance names it needs to attach to
	 * @return the registration Id
	 */
	public synchronized int register(ImrcpBlock oBlock, String[] sDependencies)
	{
		try
		{
			m_oSearch.m_oBlock.setName(oBlock.getName());
			int nIndex = Collections.binarySearch(m_oRegistered, m_oSearch);
			if (nIndex >= 0) // a block with the same name has already been registered
				return -1;

			RegisteredBlock oNew = new RegisteredBlock(m_nIdCount.incrementAndGet(), oBlock, System.currentTimeMillis());
			m_oLogger.info("Registering " + oNew.m_oBlock.getName() + " to Directory");
			for (RegisteredBlock oRegistered : m_oRegistered) //see if this block needs to be attached to each registered block or if each registered block needs to attach to it based off of the two block's interfaces and instances
			{
				for (String sDependsOn : sDependencies)
				{
					if (oRegistered.m_oBlock.getName().compareTo(sDependsOn) == 0)
					{
						oRegistered.m_oSubscribers.add(oNew);
						oNew.m_oDependencies.add(oRegistered);
					}
				}

				for (String sDependsOn : oRegistered.m_oBlock.getConfig().getStringArray("subscribe", null))
				{
					if (oNew.m_oBlock.getName().compareTo(sDependsOn) == 0)
					{
						oNew.m_oSubscribers.add(oRegistered);
						oRegistered.m_oDependencies.add(oNew);
					}
				}
			}

			m_oRegistered.add(~nIndex, oNew); //add the Block to the list of registered blocks sorted by name
			m_oLogger.info(oNew.m_oBlock.getName() + " registered");
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
		return m_nIdCount.get();
	}

	
	public void notifyBlocks(String[] sMessage)
	{
		if (sMessage[ImrcpBlock.MESSAGE].compareTo("service started") == 0)
		{
			notifyStart(sMessage);
			return;
		}
		
		synchronized (this)
		{
			for (RegisteredBlock oBlock : m_oRegistered)
			{
				if (oBlock.m_oBlock.getName().compareTo(sMessage[ImrcpBlock.FROM]) == 0)
				{
					for (RegisteredBlock oSubscriber : oBlock.m_oSubscribers)
					{
						m_oLogger.info(sMessage[ImrcpBlock.FROM] + " notified " + oSubscriber.m_oBlock.getName() + " with message " + sMessage[ImrcpBlock.MESSAGE]);
						oSubscriber.m_oBlock.receive(sMessage);
					}
				}
			}
		}
	}
	
	
	public synchronized void notifyStart(String[] sMessage)
	{
		for (RegisteredBlock oBlock : m_oRegistered)
		{
			if (oBlock.m_oBlock.getName().compareTo(sMessage[ImrcpBlock.FROM]) == 0)
			{
				for (RegisteredBlock oSubscriber : oBlock.m_oSubscribers)
				{
					queueStartup(new Startup(oSubscriber));
				}
			}
		}
	}


	/**
	 * This method stops the Directory service which shuts down the entire
	 * system.
	 */
	@Override
	public void destroy()
	{
		synchronized (m_oRegistered)
		{
			int nIndex = m_oRegistered.size();
			while (nIndex-- > 0)
			{
				try
				{
					m_oRegistered.get(nIndex).m_oBlock.destroy();
				}
				catch (Exception oEx)
				{
					m_oLogger.error(oEx, oEx);
				}
			}

			m_oRegistered.clear();
			Scheduling.getInstance().stop();
		}
	}

	
//	public synchronized void registerStateChange(String sName, int nState, long lTime)
//	{
//		m_oSearch.m_oBlock.setName(sName);
//		int nIndex = Collections.binarySearch(m_oRegistered, m_oSearch);
//		if (nIndex >= 0)
//		{
//			RegisteredBlock oTemp = m_oRegistered.get(nIndex);
//			if (oTemp.m_oMonitor.size() == m_nMonitorLimit)
//				oTemp.m_oMonitor.removeLast();
//				
//			oTemp.m_oMonitor.addFirst(new SimpleEntry(lTime, nState));
//		}
//	}
//	
//	public synchronized void getMonitoring(ArrayList<ArrayDeque<SimpleEntry<Long, Integer>>> oMonitorDeques, ArrayList<String> oBlockNames)
//	{
//		for (RegisteredBlock oBlock : m_oRegistered)
//		{
//			oMonitorDeques.add(oBlock.m_oMonitor);
//			oBlockNames.add(oBlock.m_oBlock.getName());
//		}
//	}
	
	
	public int getContribPreference(int nContribId)
	{
		Integer nPref = m_oContribPrefMap.get(nContribId);
		if (nPref == null)
			return Integer.MAX_VALUE - 1;
		
		return nPref;
	}


	/**
	 * Finds and returns a reference to the registered BaseBlock with the given
 instance name.
	 *
	 * @param sName name of the desired block
	 * @return reference to the desired BaseBlock or null if it couldn't be
 found
	 */
	public synchronized ImrcpBlock lookup(String sName)
	{
		try
		{
			m_oSearch.m_oBlock.setName(sName);
			int nIndex = Collections.binarySearch(m_oRegistered, m_oSearch);
			if (nIndex >= 0)
				return m_oRegistered.get(nIndex).m_oBlock;

			return null; //couldn't find the given Block 
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
			return null;
		}
	}


	/**
	 * Searches through the registered ImrcpBlocks to return a List filled with
	 * the Stores that contain Obs of the given obs type id.
	 *
	 * @param nType integer obs type id
	 * @return a List filled with the Stores that contain Obs of the given obs
	 * type id
	 */
	public synchronized List<BaseBlock> getStoresByObs(int nType)
	{
		List<BaseBlock> oReturn = new ArrayList();
		for (RegisteredBlock oBlock : m_oRegistered)
		{
			if (!(oBlock.m_oBlock instanceof FileCache))
				continue;
			int[] nObs = ((FileCache)oBlock.m_oBlock).m_nSubObsTypes;
			if (nObs == null)
				continue;
			for (int nSubObsType : ((FileCache)oBlock.m_oBlock).m_nSubObsTypes)
			{
				if (nSubObsType == nType)
				{
					oReturn.add((BaseBlock)oBlock.m_oBlock);
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


	@Override
	public int compare(ImrcpBlock o1, ImrcpBlock o2)
	{
		return o1.getName().compareTo(o2.getName());
	}

	/**
	 * Inner class used to keep track of ImrcpBlocks that are registered along
 with their instances so the BaseBlock can easily attach to other
 ImrcpBlocks
	 */
	public class RegisteredBlock implements Comparable<RegisteredBlock>
	{
		/**
		 * Registration Id
		 */
		private int m_nRegId;

		/**
		 * The registered BaseBlock
		 */
		private ImrcpBlock m_oBlock;
		
		private ArrayList<RegisteredBlock> m_oSubscribers = new ArrayList();
		
		private ArrayList<RegisteredBlock> m_oDependencies = new ArrayList();


		/**
		 * Default constructor.
		 */
		RegisteredBlock()
		{
		}


		/**
		 * Custom constructor. Sets all member variables.
		 *
		 * @param nRegId registration Id
		 * @param oBlock BaseBlock that is registered
		 * @param sInstances the BaseBlock's instance filters
		 */
		RegisteredBlock(int nRegId, ImrcpBlock oBlock, long lStartTime)
		{
			m_nRegId = nRegId;
			m_oBlock = oBlock;
		}


		/**
		 * Allows BlockFilters to be compared by their block's Id
		 *
		 * @param oFilter the BlockFilter that is compared to this
		 * @return 0 if the Ids are equal, a non zero number if the Ids are not
		 * equal
		 */
		@Override
		public int compareTo(RegisteredBlock oBlock)
		{
			return m_oBlock.getName().compareTo(oBlock.m_oBlock.getName());
		}
	}

	/**
	 * Inner class used to start the services of the ImrcpBlocks. Implements
	 * Runnable so the thread pool can execute them in parallel
	 */
	private class Startup implements Runnable
	{
		RegisteredBlock m_oRegisteredBlock;


		Startup(RegisteredBlock oBlock)
		{
			m_oRegisteredBlock = oBlock;
		}


		/**
		 * Calls the ImrcpBlocks startService method
		 */
		@Override
		public void run()
		{
			int nStatus = (int)m_oRegisteredBlock.m_oBlock.status()[0];
			if (nStatus != ImrcpBlock.INIT) // blocks should only start if their status is init
				return;
			
			boolean bStart = true;
			for (RegisteredBlock oDependency : m_oRegisteredBlock.m_oDependencies) // and if all of their dependencies have finished their start methods
			{
				nStatus = (int)oDependency.m_oBlock.status()[0];
				if (nStatus == ImrcpBlock.INIT || nStatus == ImrcpBlock.STARTING)
					bStart = false;
			}

			if (bStart)
				((BaseBlock)m_oRegisteredBlock.m_oBlock).startService();
		}
	}

	/**
	 * Inner class used to search for ImrcpBlocks by Id
	 */
	private class SearchBlock extends BaseBlock
	{
		SearchBlock(int nId)
		{
			m_nId = nId;
		}
	}
}
