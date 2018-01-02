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
 * Runnable target interface
 *
 * @param <E> Template type. Implementations must specify a concrete type at
 * declaration time in place of type: T.
 * @author bryan.krueger
 * @version 1.0
 */
public interface IRunTarget<E>
{

	/**
	 * Run method for target object.
	 *
	 * @param e target object to run
	 */
	public void run(E e);
}
