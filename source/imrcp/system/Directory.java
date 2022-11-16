package imrcp.system;

import imrcp.store.FileCache;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.SimpleTimeZone;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicInteger;
import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServlet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.SecureRandom;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * The Directory is the main system component that initializes the system by
 * identifying and managing BaseBlocks and services available within the system.
 * @author Federal Highway Administration
 */
public class Directory extends HttpServlet implements Runnable, Comparator<ImrcpBlock>
{
	/**
	 * Singleton instance of the Directory
	 */
	private static Directory g_oDirectory;

	
	/**
	 * Used to get sequential ids asynchronously for BaseBlocks
	 */
	private static AtomicInteger m_nIdCount = new AtomicInteger();

	
	/**
	 * List used to register and store all of the active system components
	 */
	private final ArrayList<RegisteredBlock> m_oRegistered = new ArrayList();

	
	/**
	 * Log4J logger object
	 */
	private Logger m_oLogger = LogManager.getLogger(Directory.class);

	
	/**
	 * TimeZone object for UTC
	 */
	public static final SimpleTimeZone m_oUTC = new SimpleTimeZone(0, "");

	
	/**
	 * TimeZone object for CST6CDT
	 */
	public static final TimeZone m_oCST6CDT = TimeZone.getTimeZone("CST6CDT");
	
	
	/**
	 * Convenience search object
	 */
	private RegisteredBlock m_oSearch = new RegisteredBlock(-1, new SearchBlock(-1));
	
	
	/**
	 * Maps contributor ids to a preference value. Lower values are more preferred
	 */
	private HashMap<Integer, Integer> m_oContribPrefMap;
	
	
	/**
	 * Maps contributor ids to a preference value for precipitation rates
	 */
	private HashMap<Integer, Integer> m_oPrecipPrefMap;
	
	
	/**
	 * Queue used to start all of the system components
	 */
	private ArrayDeque<Startup> m_oStartups = new ArrayDeque();
	
	
	/**
	 * Random 16 byte hex string used to identify this instance of IMRCP
	 */
	private String m_sImrcpCode;
	
	
	/**
	 * Random 16 byte hex string used to identify a remote instance of IMRCP
	 */
	private String m_sRemoteCode;
	
	
	/**
	 * Contains all of the instance names of BaseBlock instantiated by the 
	 * servlet container manager that other blocks are dependent on.
	 */
	private final ArrayList<String> m_oDependentServlets = new ArrayList();

	
	/**
	 * Default constructor. Sets the singleton instance to this.
	 */
	public Directory()
	{
		g_oDirectory = this;
	}

	
	/**
	 * Get the singleton Directory instance
	 * @return singleton Directory instance
	 */
	public static Directory getInstance()
	{
		return g_oDirectory;
	}

	
	/**
	 * Initializes the Directory. This should be the first thing that happens at
	 * system start up. It sets up all of the BaseBlocks and handles starting
	 * their services.
	 */
	@Override
	public void init()
	{
		try
		{
			m_oLogger.info("Starting init()");
			SecureRandom oRng = new SecureRandom();
			byte[] yBytes = new byte[16];
			oRng.nextBytes(yBytes);
			m_sImrcpCode = Text.toHexString(yBytes);
			ServletConfig oServletConfig = getServletConfig();
			String sRemoteUrl = oServletConfig.getInitParameter("remote-url");
			if (sRemoteUrl != null) // if there is a configured remote instance of IMRCP
			{
				URL oUrl = new URL(sRemoteUrl); // send a POST request
				HttpURLConnection oConn = (HttpURLConnection)oUrl.openConnection();
				oConn.setRequestMethod("POST");
				oConn.setDoOutput(true);

				try (DataOutputStream oOut = new DataOutputStream(new BufferedOutputStream(oConn.getOutputStream()))) // that contains this instance's ImrcpCode
				{
					oOut.writeBytes("remote=" + m_sImrcpCode);
				}

				int nResCode = oConn.getResponseCode();
				if (nResCode == HttpURLConnection.HTTP_OK)
				{
					try (BufferedInputStream oIn = new BufferedInputStream(oConn.getInputStream())) // get the remote instance's ImrcpCode
					{
						int nByte;
						StringBuilder sBuf = new StringBuilder();
						while ((nByte = oIn.read()) >= 0)
							sBuf.append((char)nByte);
						
						m_sRemoteCode = sBuf.toString();
					}
				}
			}
			Config oConfig = new Config(oServletConfig.getInitParameter("config-file"));
			Scheduling.getInstance(); // create the Scheduling singleton
			oConfig.setSchedule();
			m_oContribPrefMap = new HashMap();
			String[] sContribPreferences = oConfig.getStringArray(getClass().getName(), "Directory", "prefer", null);
			for (String sContrib : sContribPreferences)
				m_oContribPrefMap.put(Integer.valueOf(sContrib, 36), oConfig.getInt(getClass().getName(), "Directory", sContrib, 0));
			m_oPrecipPrefMap = new HashMap();
			sContribPreferences = oConfig.getStringArray(getClass().getName(), "Directory", "precipprefer", null);
			for (String sContrib : sContribPreferences)
				m_oPrecipPrefMap.put(Integer.valueOf(sContrib, 36), oConfig.getInt(getClass().getName(), "Directory", sContrib + "_precip", 0));
			String[] sClasses = oConfig.getStringArray(getClass().getName(), "Directory", "classes", null); //get the array of classes that need to be initialized upon system startup
			for (int i = 0; i < sClasses.length; i++) //iterate through the strings
			{
				String sClassName = sClasses[i++]; // increment here because the classes configuration contains pairs: java class name, instance name
				if (sClassName.startsWith("#")) // allows for "comments" in the json array
					continue;
				if (sClassName.startsWith("@"))
				{
					synchronized (m_oDependentServlets)
					{
						m_oDependentServlets.add(sClasses[i]);
					}
					continue;
				}
				BaseBlock oBlock = null;
				try
				{
					oBlock = (BaseBlock)Class.forName(sClassName).getDeclaredConstructor().newInstance();
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

	
	/**
	 * Executes the last {@link Startup} in {@link #m_oStartups} if there are any
	 * Startups in the queue
	 */
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
	
	
	/**
	 * Adds the given Startup to the queue and calls {@link Scheduling#execute(java.lang.Runnable)}
	 * on this(the Directory)
	 * @param oStartup Startup wrapping a BaseBlock that needs to be started
	 */
	private void queueStartup(Startup oStartup)
	{
		synchronized (m_oStartups)
		{
			m_oStartups.addFirst(oStartup);
			Scheduling.getInstance().execute(this);
		}	
		
	}
	
	
	/**
	 * Registers the given ImrcpBlock with the given instance name dependencies.
	 * @param oBlock The ImrcpBlock to reegister
	 * @param sDependencies contains the instance names of the ImrcpBlocks {@code oBlock}
	 * is dependent on
	 * @return the registration id assigned to the ImrcpBlock, -1 if a block with
	 * {@code oBlock}'s instance name is already registered
	 */
	public synchronized int register(ImrcpBlock oBlock, String[] sDependencies)
	{
		try
		{
			m_oSearch.m_oBlock.setName(oBlock.getName());
			int nIndex = Collections.binarySearch(m_oRegistered, m_oSearch);
			if (nIndex >= 0) // a block with the same name has already been registered
				return -1;

			RegisteredBlock oNew = new RegisteredBlock(m_nIdCount.incrementAndGet(), oBlock);
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
			synchronized (m_oDependentServlets)
			{
				nIndex = m_oDependentServlets.size();
				while (nIndex-- > 0)
				{
					if (m_oDependentServlets.get(nIndex).compareTo(oNew.m_oBlock.getName()) == 0)
						m_oDependentServlets.remove(nIndex);
				}
			}
			m_oLogger.info(oNew.m_oBlock.getName() + " registered");
		}
		catch (Exception oException)
		{
			m_oLogger.error(oException, oException);
		}
		return m_nIdCount.get();
	}

	
	/**
	 * Sends the given notification message to the BaseBlocks that are subscribed
	 * to the BaseBlock the message is from
	 * @param sMessage notification message in the format [BaseBlock message is from, message name, {@literal <resources>...}]
	 */
	public void notifyBlocks(String[] sMessage)
	{
		if (sMessage[ImrcpBlock.MESSAGE].compareTo("service started") == 0) // special service started case
		{
			notifyStart(sMessage);
			return;
		}
		
		synchronized (this)
		{
			for (RegisteredBlock oBlock : m_oRegistered) // find the block the message is from
			{
				if (oBlock.m_oBlock.getName().compareTo(sMessage[ImrcpBlock.FROM]) == 0)
				{
					for (RegisteredBlock oSubscriber : oBlock.m_oSubscribers) // for each subscribed block
					{
						m_oLogger.info(sMessage[ImrcpBlock.FROM] + " notified " + oSubscriber.m_oBlock.getName() + " with message " + sMessage[ImrcpBlock.MESSAGE]);
						oSubscriber.m_oBlock.receive(sMessage); // send the message to that block
					}
				}
			}
		}
	}
	
	
	/**
	 * Queues the subscribers of the BaseBlock the "service started" message is from
	 * to start their service
	 * @param sMessage [BaseBlock message is from, "service started"]
	 */
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
	 * Called when the system is being shut down. Calls {@link HttpServlet#destroy()}
	 * for each RegisteredBlock, clears {@link #m_oRegistered} and calls {@link Scheduling#stop()}
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

	
	/**
	 * Gets the preference value for the given IMRCP contributor id.
	 * 
	 * @param nContribId IMRCP contributor id
	 * @return Preference value associated with the contributor id. Lower values
	 * are more preferred.
	 */
	public int getContribPreference(int nContribId)
	{
		Integer nPref = m_oContribPrefMap.get(nContribId);
		if (nPref == null)
			return Integer.MAX_VALUE - 1;
		
		return nPref;
	}
	
	
	/**
	 * Gets the preference value for the given IMRCP contributor id for precipitation
	 * rates.
	 * 
	 * @param nContribId IMRCP contributor id
	 * @return Preference value associated with the contributor id for precipitation
	 * rates. Lower values are more preferred.
	 */
	public int getPrecipPreference(int nContribId)
	{
		Integer nPref = m_oPrecipPrefMap.get(nContribId);
		if (nPref == null)
			return Integer.MAX_VALUE - 1;
		
		return nPref;
	}

	
	/**
	 * Gets the instance of the registered ImrcpBlock with the given instance 
	 * name.
	 * @param sName instance name to lookup in {@link #m_oRegistered}
	 * @return Instance of the registered ImrcpBlock with the instance name if
	 * it exists, otherwise null.
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
	 * Gets a List of all the data stores that provide data for the given 
	 * observation type.
	 * 
	 * @param nType IMRCP Observation type id to search for
	 * @return A List of all the data stores taht provde data for the observation
	 * type
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
	 * Get this IMRCP instance's ImrcpCode
	 * @return the ImrcpCode of this instance of IMRCP
	 */
	public String getImrcpCode()
	{
		return m_sImrcpCode;
	}
	
	
	/**
	 * Get the configured remote IMRCP instance's ImrcpCode
	 * @return the ImrcpCode of the configured remote instance of IMRCP
	 */
	public String getRemoteCode()
	{
		return m_sRemoteCode;
	}
	
	
	/**
	 * 
	 * @param oReq
	 * @param oRes
	 * @throws IOException 
	 */
	@Override
	public void doPost(HttpServletRequest oReq, HttpServletResponse oRes)
		throws IOException
	{
		String sRemote = oReq.getParameter("remote");
		if (sRemote != null)
			m_sRemoteCode = sRemote;
		
		try (PrintWriter oOut = oRes.getWriter())
		{
			oOut.append(m_sImrcpCode);
		}
	}
	
	
	/**
	 * Compares ImrcpBlocks by instance name
	 */
	@Override
	public int compare(ImrcpBlock o1, ImrcpBlock o2)
	{
		return o1.getName().compareTo(o2.getName());
	}

	
	/**
	 * This object is what gets registered into the Directory representing all 
	 * of the active system components.
	 */
	public class RegisteredBlock implements Comparable<RegisteredBlock>
	{
		/**
		 * Registration id
		 */
		private int m_nRegId;

		
		/**
		 * The ImrcpBlock that is registered
		 */
		private ImrcpBlock m_oBlock;
		
		
		/**
		 * Contains references to the RegisteredBlocks that are subscribed to
		 * this RegisteredBlock
		 */
		private ArrayList<RegisteredBlock> m_oSubscribers = new ArrayList();
		
		
		/**
		 * Contains references to the RegisteredBlocks that this RegisteredBlock
		 * is dependent on
		 */
		private ArrayList<RegisteredBlock> m_oDependencies = new ArrayList();

		
		/**
		 * Default constructor. Does nothing.
		 */
		RegisteredBlock()
		{
		}

		
		/**
		 * Constructs a RegisteredBlock with the given id and ImrcpBlock.
		 * @param nRegId registration id
		 * @param oBlock ImrcpBlock being registered
		 */
		RegisteredBlock(int nRegId, ImrcpBlock oBlock)
		{
			m_nRegId = nRegId;
			m_oBlock = oBlock;
		}


		/**
		 * Compares RegisteredBlocks by instance name of their BaseBlock
		 */
		@Override
		public int compareTo(RegisteredBlock oBlock)
		{
			return m_oBlock.getName().compareTo(oBlock.m_oBlock.getName());
		}
	}

	
	/**
	 * Delegate object used at system start up to aid with starting the services 
	 * all of the registered BaseBlocks asynchronously.
	 */
	private class Startup implements Runnable
	{
		/**
		 * The RegisteredBlock that gets its service started by this delegate
		 */
		RegisteredBlock m_oRegisteredBlock;

		
		/**
		 * Constructs a Startup with the given RegisteredBlock
		 * @param oBlock the RegisteredBlock that's service will be started by
		 * this Startup
		 */
		Startup(RegisteredBlock oBlock)
		{
			m_oRegisteredBlock = oBlock;
		}


		/**
		 * Determines if the RegisteredBlock is ready to have {@link BaseBlock#startService()}
		 * called by checking if its status is {@link ImrcpBlock#INIT} and all of 
		 * the BaseBlocks it is dependent on have finished starting their service.
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
			{
				for (String sSub : m_oRegisteredBlock.m_oBlock.getConfig().getStringArray("subscribe", ""))
				{
					synchronized (m_oDependentServlets)
					{
						for (String sServlet : m_oDependentServlets)
						{
							if (sServlet.compareTo(sSub) == 0)
							{
								bStart = false;
								break;
							}
						}
					}
					if (!bStart)
						break;
				}
			}	

			if (bStart)
				((BaseBlock)m_oRegisteredBlock.m_oBlock).startService();
		}
	}

	
	/**
	 * BaseBlock is abstract so need an implementation that can be used for searching
	 */
	private class SearchBlock extends BaseBlock
	{
		/**
		 * Constructs a SearchBlock with the given id.
		 * @param nId id of the SearchBlock
		 */
		SearchBlock(int nId)
		{
			m_nId = nId;
		}
	}
}
