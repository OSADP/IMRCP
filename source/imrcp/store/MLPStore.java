/*
 * Copyright 2018 Synesis-Partners.
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
package imrcp.store;

/**
 * FileCache that manages CSV files produced by the different MLP traffic processes
 * @author Federal Highway Administration
 */
public class MLPStore extends CsvStore
{
	/**
	 * @return a new {@link MLPCsv} with the configured observation types
	 */
	@Override
	protected FileWrapper getNewFileWrapper()
	{
		return new MLPCsv(m_nSubObsTypes);
	}
}
