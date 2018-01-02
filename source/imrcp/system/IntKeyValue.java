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

/**
 * Maps an integer key to a value of templated type T, that can be ordered based
 * upon this key.
 * <p/>
 * <p>
 * Implements {@code Comparable<IntKeyValue>} to enforce an ordering upon
 * {@code IntKeyValue} objects.
 * </p>
 *
 * @param <T> template type - must be specified when a new instance of
 * {@code IntKeyValue} is created.
 */
public class IntKeyValue<T> implements Comparable<IntKeyValue>
{

	/**
	 * Key to be mapped to the value {@code m_oT}.
	 */
	private int m_nKey;

	/**
	 * The value associated with the mapped key.
	 */
	private T m_oT;


	/**
	 * <b> Default Constructor </b>
	 * <p>
	 * Creates new instances of {@code IntKeyValue}
	 * </p>
	 */
	public IntKeyValue()
	{
	}


	/**
	 * <b> Constructor </b>
	 * <p>
	 * Initializes attributes of new instances to the provided values.
	 * </p>
	 *
	 * @param nKey new value for the key attribute.
	 * @param oT new value to be mapped.
	 */
	public IntKeyValue(int nKey, T oT)
	{
		m_oT = oT;
		setKey(nKey);
	}


	/**
	 * <b> Mutator </b>
	 *
	 * @param nKey sets the key attribute to the supplied value.
	 */
	public final void setKey(int nKey)
	{
		m_nKey = nKey;
	}


	/**
	 *
	 * @return
	 */
	public int getKey()
	{
		return m_nKey;
	}


	/**
	 * <b> Accessor </b>
	 *
	 * @return the value contained by the {@code IntKeyValue} instance.
	 */
	public T value()
	{
		return m_oT;
	}


	/**
	 * Enforces an ordering on {@code IntKeyValue} objects based off the key.
	 *
	 * @param oRhs the object to compare to <i> this </i>
	 * @return 0 if the keys match. &lt 0 means <i> this </i> key is smaller.
	 */
	@Override
	public int compareTo(IntKeyValue oRhs)
	{
		return (m_nKey - oRhs.m_nKey);
	}
}
