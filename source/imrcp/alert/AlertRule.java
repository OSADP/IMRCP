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
package imrcp.alert;

import imrcp.geosrv.NED;
import imrcp.store.Obs;
import imrcp.system.Directory;
import imrcp.system.ObsType;
import java.util.ArrayList;

/**
 * Object that contains the alert type and a list of Conditions to be met for an
 * alert to be generated
 */
public class AlertRule
{

	/**
	 * Reference to the National Elevation Database ImrcpBlock
	 */
	public static final NED g_oNED = (NED)Directory.getInstance().lookup("NED");

	/**
	 * The position in the array used in Alerts where the rule starts
	 */
	int m_nArrayPosition;

	/**
	 * Alert type, looked up from ObsType
	 */
	public int m_nType;

	/**
	 * List of AlertConditions that must be met for the rule to be met
	 */
	public ArrayList<AlertCondition> m_oAlgorithm = new ArrayList();


	/**
	 * Creates the rule by creating all of the configured Conditions
	 *
	 * @param sConditions String array that contains the configured data for the
	 * rule.
	 */
	AlertRule(String[] sConditions, int nPositionCount)
	{
		m_nType = ObsType.lookup(ObsType.EVT, sConditions[0]); // the first entry is the alert(event) type
		AlertConditionIterator oIt = new AlertConditionIterator(sConditions);
		while (oIt.hasNext()) // add each condition to the Algorithm list
		{
			String[] sCondition = oIt.next();
			m_oAlgorithm.add(new AlertCondition(sCondition));
		}
		m_nArrayPosition = nPositionCount;
	}


	/**
	 * Evaluates the rule for the given long array representing an area. If the
	 * rule is met, creates and returns an Obs representing the alert.
	 *
	 * @param lArea an area array from Alerts. Format is [lat1, lat2, lon1,
	 * lon2, objId, (pairs of start and end times for each condition for each
	 * rule)]
	 * @param lTime timestamp of when the alerts are being created
	 * @return An Obs representing an alert, or null if the rule wasn't met
	 * @throws Exception
	 */
	public Obs evaluateRuleForArea(long[] lArea, long lTime) throws Exception
	{
		Obs oReturn = null;

		int nEndPosition = m_nArrayPosition + (m_oAlgorithm.size() * 2);
		long lStartTime = Long.MIN_VALUE;
		long lEndTime = Long.MAX_VALUE;
		for (int i = m_nArrayPosition; i < nEndPosition; i += 2)
		{
			if (lArea[i] < lArea[i + 1]) // the condition was met
			{
				if (lArea[i] < lEndTime && lArea[i + 1] >= lStartTime) // the condition intersects the time range of other conditions
				{
					if (lArea[i + 1] < lEndTime) // update endtime to the earliest endtime of conditions being met
						lEndTime = lArea[i + 1];
					if (lArea[i] > lStartTime) // update starttime to the latest starttime of conditions being met
						lStartTime = lArea[i];
				}
			}
			else
				break;

			short tElev = (short)Double.parseDouble(g_oNED.getAlt((int)(lArea[0] + lArea[1]) / 2, (int)(lArea[2] + lArea[3]) / 2));
			int nLat2;
			int nLon2;
			if (lArea[4] != Integer.MIN_VALUE) // if the objId is set, that means the alert is for a segment
			{
				nLat2 = Integer.MIN_VALUE;
				nLon2 = Integer.MIN_VALUE;
			}
			else
			{
				nLat2 = (int)lArea[1];
				nLon2 = (int)lArea[3];
			}
			oReturn = new Obs(ObsType.EVT, Integer.valueOf("alerts", 36), (int)lArea[4], lStartTime, lEndTime, lTime, (int)lArea[0], (int)lArea[2], nLat2, nLon2, tElev, m_nType);
		}

		return oReturn;
	}
}
