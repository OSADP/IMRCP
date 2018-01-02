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
 * This class contains static utility methods for other classes to use
 */
public class MathUtil
{

	/**
	 * Converts {@code dValue} from its standard units to micro-units.
	 *
	 * @param dValue value to convert to micro units.
	 * @return the converted value.
	 */
	public static int toMicro(double dValue)
	{
		return ((int)Math.round(dValue * 1000000.0));
	}


	/**
	 * converts {@code nValue} from micro-units to its standard units.
	 *
	 * @param nValue value to convert from micro units.
	 * @return the converted value.
	 */
	public static double fromMicro(int nValue)
	{
		return (((double)nValue) / 1000000.0);
	}
}
