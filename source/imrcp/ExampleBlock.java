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

import imrcp.system.Scheduling;
import java.util.ArrayList;

/**
 * Skeleton code and examples for creating an ImrcpBlock. Attaching or
 * subscribing to another block is handled by setting the "attach" and
 * "subscribe" keys respectively in the config.json file. Both must be a String
 * array containing the instance names of the ImrcpBlocks that this ImrcpBlock
 * needs to attach or subscribe to.
 *
 */
public class ExampleBlock extends ImrcpBlock
{

	private int m_nMemberInt; //example configurable member variable

	private String m_sMemberString; //example configurable member variable


	ExampleBlock()
	{
	}


	/**
	 * Starts the service for the ImrcpBlock. This function is called after the
	 * ImrcpBlock has been registered with the Directory.
	 *
	 * @return true if the service starts without any errors
	 * @throws Exception this function throws any exception to be caught by the
	 * startService() function
	 */
	@Override
	public boolean start() throws Exception
	{
		/**
		 * Do things at startup. Configurable variables are already set when
		 * reset() is called (which is called in the the function that calls
		 * this function). Configuration object is m_oConfig. Log4j logger is
		 * called m_oLogger.
		 */

		//Example of creating a scheduled task: 0 is the midnight offset and 
		//3600 is the period in seconds
		m_nSchedId = Scheduling.getInstance().createSched(this, 0, 3600);

		return true;
	}


	/**
	 * Stops the service for the ImrcpBlock. This function is called after the
	 * ImrcpBlock has been unregistered from the Directory, has had its
	 * scheduled task canceled(if it had one), has notified its subscribers it
	 * has stopped, and has canceled its subscriptions.
	 *
	 * @return true if the service stops without any errors
	 * @throws Exception this function throws any exception to be caught by the
	 * stopService() function
	 */
	@Override
	public boolean stop() throws Exception
	{
		/**
		 * Do things to stop the service
		 */
		return true;
	}


	/**
	 * This function is called when the ImrcpBlock is executed by the
	 * ThreadPool, usually on a scheduled interval.
	 */
	@Override
	public void execute()
	{

	}


	/**
	 * This function is called when the ImrcpBlock's Notification queue
	 * processes a Notification.
	 *
	 * @param oNotification The Notification to be processed
	 */
	@Override
	public void process(Notification oNotification)
	{
		if (oNotification.m_sMessage.compareTo("file download") == 0) //handle new files downloaded
		{
			/**
			 * Do something with the new file. Filename =
			 * oNotification.m_sResource
			 */
		}
		else if (oNotification.m_sMessage.compareTo("new data") == 0)
		{
			ArrayList oObs = new ArrayList(); //create a list to hold obs
			String[] sObsTypes = m_oConfig.getStringArray("obsid", null); //read configured obstypes
			if (sObsTypes != null) //if there are obstypes
			{
				//convert String[] to int[]
				int[] nObsTypes = new int[sObsTypes.length];
				for (int i = 0; i < sObsTypes.length; i++)
					nObsTypes[i] = Integer.parseInt(sObsTypes[i]);
				for (Subscription oSub : m_oSubscriptions)
				{
//					if (oSub.m_oSubscribedTo.m_nId == oNotification.m_nProviderId) //find the ImrcpBlock that sent the Notification
//						oObs.addAll(oSub.m_oSubscribedTo.getData( //get list of obs
//							nObsTypes, oNotification.m_lTimeNotified, oNotification.m_lTimeNotified + 3600000, //obstype ids, start timestamp, end timestamp
//							-90000000, 90000000, -180000000, 179999999));  //lat/lon bounding box in micro degrees
				}
			}
			if (!oObs.isEmpty())
			{
				/**
				 * Do something with the list of Obs
				 */
			}
		}
	}


	/**
	 * This function is called when the service starts. It resets any
	 * configurable variables for the block.
	 */
	@Override
	protected void reset()
	{
		m_nMemberInt = m_oConfig.getInt("intkey", 0);
		m_sMemberString = m_oConfig.getString("stringkey", "default");
	}
}
