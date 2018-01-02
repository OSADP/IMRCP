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
package imrcp.forecast.bayes;

/**
 * This class is used to store the outputs of a Bayes run. An id is not needed
 * because the RealTimeBayes uses an array of BayesOutputs that uses the index
 * as the id
 */
public class BayesOutput
{

	/**
	 * Flow output category 1-Very low 2-Low 3-High 4-Very high
	 */
	public int m_nFlowCat;

	/**
	 * Probability of the flow output category
	 */
	public float m_fFlowProb;

	/**
	 * Speed output category 1-Very low 2-Low 3-High 4-Very high
	 */
	public int m_nSpeedCat;

	/**
	 * Probability of the speed output category
	 */
	public float m_fSpeedProb;

	/**
	 * Occupancy output category 1-Very low 2-Low 3-High 4-Very high
	 */
	public int m_nOccCat;

	/**
	 * Probability of the occupancy output category
	 */
	public float m_fOccProb;


	/**
	 * Sets all of the output parameters from a String Array
	 *
	 * @param sOutputs String Array that contains the columns of a csv line from
	 * a Bayes output file
	 */
	public void setOutputs(String[] sOutputs)
	{
		m_nFlowCat = (int)Float.parseFloat(sOutputs[9]);
		m_fFlowProb = Float.parseFloat(sOutputs[10]);
		m_nSpeedCat = (int)Float.parseFloat(sOutputs[11]);
		m_fSpeedProb = Float.parseFloat(sOutputs[12]);
		m_nOccCat = (int)Float.parseFloat(sOutputs[13]);
		m_fOccProb = Float.parseFloat(sOutputs[14]);
	}
}
