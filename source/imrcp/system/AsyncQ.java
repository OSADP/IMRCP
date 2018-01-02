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

import java.util.ArrayDeque;
import java.util.concurrent.Executor;

/**
 * Asynchronous queue object processing. Objects are processed as soon as they
 * are added to the queue.
 *
 * @param <T> template type. Must be specified when creating a new instance of
 * {@code AsyncQ}.
 * @author bryan.krueger
 * @version 1.0
 */
public class AsyncQ<T> implements Runnable, IRunTarget<T>
{

	/**
	 * Queue data structure.
	 */
	private final ArrayDeque<T> m_oDeque = new ArrayDeque<T>();

	/**
	 * Queue objects are processed by the run method of this object.
	 */
	private IRunTarget<T> m_iDelegate;

	/**
	 * Counter for limiting the maximum number of working threads.
	 */
	private int m_nMaxThreads = 1;

	/**
	 * Refernce to the thread pool.
	 */
	private Executor m_oThreadPool = Scheduling.getInstance();


	/**
	 * <b> Default Constructor </b>
	 * <p>
	 * Creates a new instance of AsyncQ.
	 * </p>
	 */
	protected AsyncQ()
	{
		m_iDelegate = this;
	}


	/**
	 * Creates a new instance of AsyncQ with the defined run target.
	 *
	 * @param iDelegate an object whose run method will be executed to process
	 * the objects in the queue.
	 */
	public AsyncQ(IRunTarget<T> iDelegate)
	{
		m_iDelegate = iDelegate;
	}


	/**
	 * Creates a new instance of AsyncQ with the defined run target and the
	 * specified thread queue depth.
	 *
	 * @param nMaxThreads max threads to allocate to the new instance
	 * @param iDelegate an object whos run method will be executed to process
	 * the objects in the queue.
	 */
	public AsyncQ(int nMaxThreads, IRunTarget<T> iDelegate)
	{
		m_iDelegate = iDelegate;
		setMaxThreads(nMaxThreads);
	}


	/**
	 * Sets the max number of threads to the supplied integer value.
	 *
	 * @param nMaxThreads new max threads.
	 */
	public void setMaxThreads(int nMaxThreads)
	{
		// at least one thread, the default, must be allowed to operate
		if (nMaxThreads > 0)
			m_nMaxThreads = nMaxThreads;
	}


	/**
	 * Adds an object to the queue for processing. If there are no current
	 * objects in the queue, processing will begin.
	 *
	 * @param oT the object of type T to add to the queue
	 */
	public void queue(T oT)
	{
		synchronized (m_oDeque)
		{
			m_oDeque.add(oT);
			// nothing needs to be done if other threads are currently working
			if (m_nMaxThreads > 0)
			{
				--m_nMaxThreads;
				m_oThreadPool.execute(this);
			}
		}
	}


	/**
	 * Process all objects that are in the queue.
	 */
	public void run()
	{
		T oT = null;

		synchronized (m_oDeque)
		{
			oT = m_oDeque.poll();

			// dispatch another worker thread when there is more work to do
			if (m_nMaxThreads > 0 && m_oDeque.size() > 0)
			{
				--m_nMaxThreads;
				m_oThreadPool.execute(this);
			}
		}

		// Run the target delegate
		if (oT != null)
			m_iDelegate.run(oT);

		synchronized (m_oDeque)
		{
			// continue processing until there are no more queued objects
			if (m_oDeque.size() > 0)
				m_oThreadPool.execute(this);
			else
				++m_nMaxThreads;
		}
	}


	/**
	 * The default run target performs no operations on the object. This should
	 * be overridden in subclasses to perform necessary processing.
	 *
	 * @param e target object to run.
	 */
	public void run(T e)
	{
	}
}
