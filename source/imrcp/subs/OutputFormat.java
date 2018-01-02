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
package imrcp.subs;

import imrcp.store.Obs;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Creates guidelines for output classes to follow to allow more generalized
 * use.
 * <p/>
 * <p>
 * Forces extensions to implement  {@link OutputFormat#fulfill(PrintWriter, ArrayList,
 * Subscription, String, int, long)}
 * </p>
 * <p>
 * Implements {@code Comparator} interface to enforce a standard interface for
 * comparisons. Extension of {@code OutputFormat} must implement the required
 * {@code compareTo} method.
 * </p>
 */
public abstract class OutputFormat implements Comparator<Obs>
{

	/**
	 *
	 */
	protected static String warning1 = "Request exceeded system limit - only the first %d rows are included";

	/**
	 *
	 */
	protected static String warning2 = "Request exceeded system limit - only those filtered from the first %d seconds are included";

	/**
	 * File extension suffix.
	 */
	protected String m_sSuffix;


	/**
	 * <b> Default Constructor </b>
	 * <p>
	 * Creates new instances of {@code OutputFormat}
	 * </p>
	 */
	protected OutputFormat()
	{
	}


	/**
	 * <b> Accessor </b>
	 *
	 * @return file extension suffix attribute.
	 */
	String getSuffix()
	{
		return m_sSuffix;
	}


	/**
	 * Extension must implement this method to print in the observation data in
	 * the corresponding format.
	 *
	 * @param oWriter Output stream to write data to.
	 * @param oSubObsList List containing observation data to print.
	 * @param oSub Subscription filter.
	 * @param sFilename output filename printed on footer.
	 * @param nId subscription id.
	 * @param lLimit timerange for observations.
	 */
	abstract void fulfill(PrintWriter oWriter, List<Obs> oSubObsList,
	   ReportSubscription oSub, String sFilename, int nId, long lLimit);


	/**
	 * Compares the two {@code SubObs}.
	 *
	 * @param oSubObsL object to compare to {@code oSubObsR}.
	 * @param oSubObsR object to compare to {@code oSubObsL}.
	 * @return 0 if the {@code SubObs} match by contributor id, observation-type
	 * id, and timestamp.
	 */
	@Override
	public int compare(Obs oSubObsL, Obs oSubObsR)
	{
		int nCompare = (int)(oSubObsL.m_lObsTime1 - oSubObsR.m_lObsTime1);
		if (nCompare != 0)
			return 0;

		// sort the observations for neat output by contrib, obstype, timestamp
		nCompare = oSubObsL.m_nContribId - oSubObsR.m_nContribId;
		if (nCompare != 0)
			return nCompare;

		nCompare = oSubObsL.m_nObsTypeId - oSubObsR.m_nObsTypeId;
		if (nCompare != 0)
			return nCompare;

		nCompare = (int)(oSubObsL.m_lObsTime2 - oSubObsR.m_lObsTime2);
		if (nCompare != 0)
			return 0;

		return nCompare;
	}
}
