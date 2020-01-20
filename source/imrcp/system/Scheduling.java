package imrcp.system;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class handles the scheduling and execution of tasks for the system. It
 * contains the primary thread pool used by the system.
 */
public class Scheduling implements Executor
{

	/**
	 * Singleton instance of Scheduling
	 */
	private static Scheduling g_oScheduling = new Scheduling();

	/**
	 * Timer used to schedule tasks for future execution
	 */
	private Timer m_oTimer = new Timer();

	/**
	 * System thread pool
	 */
	private ExecutorService m_iExecutor = Executors.newFixedThreadPool(53);

	/**
	 * List of tasks that are scheduled
	 */
	private final ArrayList<Sched> m_oTasks = new ArrayList();

	/**
	 * Counter variable used for schedule Ids
	 */
	private static AtomicInteger m_nIdCount = new AtomicInteger();


	/**
	 * Default constructor
	 */
	private Scheduling()
	{
	}


	/**
	 * Returns the reference to the singleton instance of Scheduling
	 *
	 * @return reference to the singleton instance
	 */
	public static Scheduling getInstance()
	{
		return g_oScheduling;
	}


	/**
	 * Determines the next period to execute based off of the offset from
	 * midnight and the period of execution
	 *
	 * @param nOffset schedule offset from midnight.
	 * @param nPeriod period of execution.
	 * @return Calendar object of the next period .
	 */
	public static Calendar getNextPeriod(int nOffset, int nPeriod)
	{
		Calendar iCalendar = new GregorianCalendar(Directory.m_oUTC);

		// set the current time to midnight UTC and add the schedule offset
		iCalendar.set(Calendar.HOUR_OF_DAY, 0);
		iCalendar.set(Calendar.MINUTE, 0);
		iCalendar.set(Calendar.SECOND, nOffset);
		iCalendar.set(Calendar.MILLISECOND, 0);

		// adjust the period from seconds to milliseconds
		nPeriod *= 1000;
		// determine the timestamp of the next period
		long lOffsetTime = iCalendar.getTimeInMillis();
		long lDeltaTime = System.currentTimeMillis() - lOffsetTime;
		long lPeriods = lDeltaTime / nPeriod;
		if (lDeltaTime % nPeriod > 0)
			++lPeriods;
		iCalendar.setTimeInMillis(lOffsetTime + lPeriods * nPeriod);
		return iCalendar;
	}
	
	
	public static Calendar getLastPeriod(int nOffset, int nPeriod)
	{
		Calendar iCalendar = new GregorianCalendar(Directory.m_oUTC);

		// set the current time to midnight UTC and add the schedule offset
		iCalendar.set(Calendar.HOUR_OF_DAY, 0);
		iCalendar.set(Calendar.MINUTE, 0);
		iCalendar.set(Calendar.SECOND, nOffset);
		iCalendar.set(Calendar.MILLISECOND, 0);

		// adjust the period from seconds to milliseconds
		nPeriod *= 1000;
		// determine the timestamp of the last period
		long lOffsetTime = iCalendar.getTimeInMillis();
		long lDeltaTime = System.currentTimeMillis() - lOffsetTime;
		long lPeriods = lDeltaTime / nPeriod;

		iCalendar.setTimeInMillis(lOffsetTime + lPeriods * nPeriod);
		return iCalendar;
	}


	/**
	 * Creates a new Sched that is scheduled to run at a fixed rate, starting 
	 * at the next period of execution based off of the midnight offset and
	 * period.
	 * @param iRunnable the Runnable to execute
	 * @param nOffset schedule offset from midnight in seconds
	 * @param nPeriod period of execution in seconds
	 * @return the Id of the Sched created
	 */
	public synchronized int createSched(Runnable iRunnable, int nOffset, int nPeriod)
	{
		Calendar iCalendar = getNextPeriod(nOffset, nPeriod);
		Sched oTask = null;
		oTask = new Sched(iRunnable, m_nIdCount.getAndIncrement());
		m_oTasks.add(oTask);
		Collections.sort(m_oTasks);
		m_oTimer.scheduleAtFixedRate(oTask, iCalendar.getTime(), nPeriod * 1000);

		return oTask.m_nId;
	}

	
	/**
	 * Creats a new Sched that is scheduled to run at a fixed rate, starting 
	 * at the time represented by the Date object passed in and then at regular
	 * intervals separated by the given period.
	 * @param iRunnable the Runnable to execute
	 * @param oTime Date object representing the first time to execute the Runnable
	 * @param nPeriod period of execution in milliseconds
	 * @return 
	 */
	public synchronized int createSched(Runnable iRunnable, Date oTime, int nPeriod)
	{
		Sched oTask = new Sched(iRunnable, m_nIdCount.getAndIncrement());
		m_oTasks.add(oTask);
		Collections.sort(m_oTasks);
		m_oTimer.scheduleAtFixedRate(oTask, oTime, nPeriod);

		return oTask.m_nId;
	}


	/**
	 * Executes the given runnable once after waiting the given amount of milliseconds
	 * @param iRunnable Runnable to execute
	 * @param nDelay number of milliseconds to delay before executing the Runnable
	 */
	public void scheduleOnce(Runnable iRunnable, int nDelay)
	{
		Sched oTask = new Sched(iRunnable, -1);  // id doesn't matter since it is a one time task
		m_oTimer.schedule(oTask, nDelay);
	}

	
	/**
	 * Wrapper for the ExecutorService execute function
	 * @param iRunnable Runnable to execute
	 */
	@Override
	public void execute(Runnable iRunnable)
	{
		m_iExecutor.execute(iRunnable);
	}


	/**
	 * Wrapper for the ExecutorSerive shutdown function.
	 */
	public void stop()
	{
		m_iExecutor.shutdownNow();
	}


	/**
	 * Attempts to cancel the scheduled task for the given Runnable with the
	 * given id.
	 * @param iRunnable Runnable with a scheduled task to cancel
	 * @param nSchedId the Sched's id for the Runnable
	 * @return true if the task was canceled, otherwise false
	 */
	public synchronized boolean cancelSched(Runnable iRunnable, int nSchedId)
	{
		Sched oSearch = new Sched(iRunnable, nSchedId);
		int nIndex = Collections.binarySearch(m_oTasks, oSearch);
		if (nIndex >= 0 && m_oTasks.get(nIndex).m_iRunnable == iRunnable)
		{
			m_oTasks.get(nIndex).cancel();
			m_oTasks.remove(nIndex);
			return true;
		}

		return false;
	}


	/**
	 * Returns the number of tasks that are scheduled to run by Scheduling
	 * @return number of tasks that are scheduled to run by Scheduling
	 */
	public int getNumberOfTasks()
	{
		return m_oTasks.size();
	}

	/**
	 * Allows scheduled thread-pool execution of processes.
	 * <p>
	 * Extends {@code TimerTask} to allow scheduling for one-time or repeated
	 * execution by a Timer.
	 * </p>
	 */
	private class Sched extends TimerTask implements Comparable<Sched>
	{

		/**
		 * Object of scheduled execution.
		 */
		private Runnable m_iRunnable;

		private int m_nId;


		/**
		 * <b> Default Constructor </b>
		 * <p>
		 * Creates new instances of {@code Sched}
		 * </p>
		 */
		private Sched()
		{
		}


		/**
		 * <b> Constructor </b>
		 * <p>
		 * Initializes the runnable object to the provided value.
		 * </p>
		 *
		 * @param iRunnable object to be scheduled to run.
		 */
		private Sched(Runnable iRunnable, int nId)
		{
			m_iRunnable = iRunnable;
			m_nId = nId;
		}


		/**
		 * Queues the runnable object to be executed by a thread from the thread
		 * pool.
		 */
		@Override
		public void run()
		{
			m_iExecutor.execute(m_iRunnable);
		}


		@Override
		public int compareTo(Sched oSched)
		{
			return m_nId - oSched.m_nId;
		}
	}
}
