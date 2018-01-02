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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.StringReader;
import java.util.HashMap;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class provides a way of parsing a single configuration file written in
 * JSON format, gathering the entries and their associated values, and storing
 * them for later retrieval.
 */
public class Config implements Runnable
{

	/**
	 * Path for the configuration file
	 */
	private String m_sFilename;

	/**
	 * Singleton instance of Config
	 */
	private static Config g_oConfig;

	/**
	 * Stores the contents of the configuration file
	 */
	private JsonArray m_oConfigArray;

	/**
	 * Timestamp of the previous value of lastModified() of the configuration
	 * file
	 */
	private long m_lLastModified;

	/**
	 * The Scheduling Id
	 */
	private int m_nSchedId;

	/**
	 * Logger
	 */
	private Logger m_oLogger = LogManager.getLogger(Config.class);


	/**
	 * Default constructor
	 */
	private Config()
	{
	}


	/**
	 * Sets the static global instance and reads in the configuration file.
	 */
	Config(String sFilename)
	{
		g_oConfig = this;
		m_sFilename = sFilename;
		run();
	}


	/**
	 * Thie method returns the reference to the singleton instance of Config
	 *
	 * @return reference to Config instance
	 */
	public static Config getInstance()
	{
		return g_oConfig;
	}


	/**
	 * This method returns the JsonArray that contains the configuration file
	 * keys and values.
	 *
	 * @return reference to the JsonArray that contains the configuration file
	 * keys and values
	 */
	private synchronized JsonArray getConfigArray()
	{
		return m_oConfigArray;
	}


	/**
	 * If the schedule id is set, this method cancels the scheduled task for
	 * this block
	 */
	public void endSchedule()
	{
		if (m_nSchedId > 0)
			Scheduling.getInstance().cancelSched(this, m_nSchedId);
	}


	/**
	 * Creates a scheduled task for this block to execute every 5 minutes
	 */
	public void setSchedule()
	{
		m_nSchedId = Scheduling.getInstance().createSched(this, 0, 300);
	}


	/**
	 * Retrieves the integer of the given key for the given class or name. The
	 * method first searches for the class and then the name. If both the class
	 * and name are present in the configuration file, the int retrieved by name
	 * will override the int retrieved by class. If the neither the class or
	 * name are found, the default int will be returned.
	 *
	 * @param sClass the class name for the object being configured
	 * @param sName the instance name for the object being configured
	 * @param sKey the desired key
	 * @param nDefault default configuration value
	 * @return the int of the given key for the given class or name. Values
	 * retrieved by name override values retrieved by class. If the class, name,
	 * or key are not found, the default int is returned
	 */
	public int getInt(String sClass, String sName, String sKey, int nDefault)
	{
		JsonArray oConfigArray = getConfigArray();
		int nReturn = nDefault;

		//find configuration value by class
		for (int i = 0; i < oConfigArray.size(); i++)
			if (oConfigArray.getJsonObject(i).containsKey(sClass))  //find the class
				if (oConfigArray.getJsonObject(i).getJsonObject(sClass).containsKey(sKey) //find the key
				   && oConfigArray.getJsonObject(i).getJsonObject(sClass).get(sKey).getValueType().compareTo(JsonValue.ValueType.NUMBER) == 0) //ensure the value is a number
					nReturn = oConfigArray.getJsonObject(i).getJsonObject(sClass).getInt(sKey); //set the value

		//find configuration by name, this will override configuration by class
		for (int i = 0; i < oConfigArray.size(); i++)
			if (oConfigArray.getJsonObject(i).containsKey(sName))  //find the name
				if (oConfigArray.getJsonObject(i).getJsonObject(sName).containsKey(sKey) //find the key
				   && oConfigArray.getJsonObject(i).getJsonObject(sName).get(sKey).getValueType().compareTo(JsonValue.ValueType.NUMBER) == 0) //ensure the value is a number
					nReturn = oConfigArray.getJsonObject(i).getJsonObject(sName).getInt(sKey); //set the value

		return nReturn;
	}


	/**
	 * Retrieves the String of the given key for the given class or name. The
	 * method first searches for the class and then the name. If both the class
	 * and name are present in the configuration file, the String retrieved by
	 * name will override the String retrieved by class. If the neither the
	 * class or name are found, the default String will be returned.
	 *
	 * @param sClass the class name for the object being configured
	 * @param sName the instance name for the object being configured
	 * @param sKey the desired key
	 * @param sDefault default configuration value
	 * @return the String of the given key for the given class or name. Values
	 * retrieved by name override values retrieved by class. If the class, name,
	 * or key are not found, the default String is returned
	 */
	public String getString(String sClass, String sName, String sKey, String sDefault)
	{
		JsonArray oConfigArray = getConfigArray();
		String sReturn = sDefault;

		//find configuration value by class
		for (int i = 0; i < oConfigArray.size(); i++)
			if (oConfigArray.getJsonObject(i).containsKey(sClass)) //find the class
				if (oConfigArray.getJsonObject(i).getJsonObject(sClass).containsKey(sKey) //find the key
				   && oConfigArray.getJsonObject(i).getJsonObject(sClass).get(sKey).getValueType().compareTo(JsonValue.ValueType.STRING) == 0) //ensure the value is a String
					sReturn = oConfigArray.getJsonObject(i).getJsonObject(sClass).getString(sKey); //set the value

		//find configuration value by name, this will override configuration by class
		for (int i = 0; i < oConfigArray.size(); i++)
			if (oConfigArray.getJsonObject(i).containsKey(sName)) //find the name
				if (oConfigArray.getJsonObject(i).getJsonObject(sName).containsKey(sKey) //find the key
				   && oConfigArray.getJsonObject(i).getJsonObject(sName).get(sKey).getValueType().compareTo(JsonValue.ValueType.STRING) == 0) //ensure the value is a String
					sReturn = oConfigArray.getJsonObject(i).getJsonObject(sName).getString(sKey); //set the value

		return sReturn;
	}


	/**
	 * Retrieves the String array of the given key for the given class or name.
	 * The method first searches for the class and then the name. If both the
	 * class and name are present in the configuration file, the String
	 * retrieved by name will override the String array retrieved by class. If
	 * the neither the class or name are found, the default String will be
	 * returned as a String array with size 1. If the default String is null, an
	 * array with size 0 is returned
	 *
	 * @param sClass the class name for the object being configured
	 * @param sName the instance name for the object being configured
	 * @param sKey the desired key
	 * @param sDefault default configuration value
	 * @return the String array of the given key for the given class or name.
	 * Values retrieved by name override values retrieved by class. If the
	 * class, name, or key are not found, the default String is returned as a
	 * String array with size 1. If the default String is null, an array with
	 * size 0 is returned
	 */
	public String[] getStringArray(String sClass, String sName, String sKey, String sDefault)
	{
		JsonArray oConfigArray = getConfigArray(); //make local copy for ConfigArray
		String[] sReturn = null;
		boolean bFound = false;

		//find configuration values by class
		for (int i = 0; i < oConfigArray.size(); i++)
			if (oConfigArray.getJsonObject(i).containsKey(sClass)) //find the class
				if (oConfigArray.getJsonObject(i).getJsonObject(sClass).containsKey(sKey) //find the key
				   && oConfigArray.getJsonObject(i).getJsonObject(sClass).get(sKey).getValueType().compareTo(JsonValue.ValueType.ARRAY) == 0) //ensure the value is an array
				{
					JsonArray oTemp = oConfigArray.getJsonObject(i).getJsonObject(sClass).getJsonArray(sKey);
					sReturn = new String[oTemp.size()];
					for (int j = 0; j < oTemp.size(); j++)
						sReturn[j] = oTemp.getString(j); //fill the array
					bFound = true;
				}

		//find configuration values by name, this will override configuration by class
		for (int i = 0; i < oConfigArray.size(); i++)
			if (oConfigArray.getJsonObject(i).containsKey(sName)) //find the name
				if (oConfigArray.getJsonObject(i).getJsonObject(sName).containsKey(sKey) //find the key
				   && oConfigArray.getJsonObject(i).getJsonObject(sName).get(sKey).getValueType().compareTo(JsonValue.ValueType.ARRAY) == 0) //ensure the value is an array
				{

					JsonArray oTemp = oConfigArray.getJsonObject(i).getJsonObject(sName).getJsonArray(sKey);
					sReturn = new String[oTemp.size()];
					for (int j = 0; j < oTemp.size(); j++)
						sReturn[j] = oTemp.getString(j); //fill the array
					bFound = true;
				}

		if (!bFound) //if the class, name, or key was not found create the default return
		{
			if (sDefault != null)
			{
				sReturn = new String[1];
				sReturn[0] = sDefault;
			}
			else
			{
				sReturn = new String[0];
			}
		}

		return sReturn;
	}


	/**
	 * This method checks to see if the configuration file has been modified. If
	 * the file has been modified, the method read the configuration file and
	 * stores the configuration keys and values in a JsonArray for future
	 * retrieval.
	 */
	@Override
	public final void run()
	{
		File oFile = new File(m_sFilename);
		if (oFile.lastModified() > m_lLastModified) //check to see if the file has been modified
		{
			StringBuilder sSB = new StringBuilder();
			try
			{
				BufferedInputStream oIn = new BufferedInputStream(new FileInputStream(oFile)); //read the file
				int nByte;
				while ((nByte = oIn.read()) >= 0)
					sSB.append((char)nByte);

				oIn.close(); //cleanup resources
				m_lLastModified = oFile.lastModified(); //update the last modified time
				StringReader oSR = new StringReader(sSB.toString());
				JsonReader oReader = Json.createReader(oSR);
				JsonArray oTempArray = oReader.readArray(); //store contents in the JsonArray
				synchronized (this)
				{
					m_oConfigArray = oTempArray;
				}
				oReader.close(); //cleanup resources
			}
			catch (Exception oException)
			{
				m_oLogger.error(oException, oException);
			}
		}
	}


	/**
	 * Creates and returns a HashMap that contains all of the configuration
	 * items for the given class and name as well as the general ImrcpBlock
	 * configurations
	 *
	 * @param sClass desired class name
	 * @param sName desired instance name
	 * @return HashMap that contains all the configuration items for the general
	 * ImrcpBlock and the given class and name
	 */
	public HashMap<String, String[]> getProps(String sClass, String sName)
	{
		HashMap<String, String[]> oMap = new HashMap();
		String[] sValue = null;
		JsonArray oConfigArray = getConfigArray();
		JsonObject oTemp = null;

		for (int i = 0; i < oConfigArray.size(); i++)
		{
			if (oConfigArray.getJsonObject(i).containsKey("imrcp.ImrcpBlock")) //find the class
			{
				oTemp = oConfigArray.getJsonObject(i).getJsonObject("imrcp.ImrcpBlock");
				for (String sKey : oTemp.keySet())
				{
					if (oTemp.get(sKey).getValueType().compareTo(JsonValue.ValueType.STRING) == 0)
					{
						sValue = new String[1];
						sValue[0] = oTemp.getString(sKey);
					}
					else if (oTemp.get(sKey).getValueType().compareTo(JsonValue.ValueType.NUMBER) == 0)
					{
						sValue = new String[1];
						sValue[0] = oTemp.getJsonNumber(sKey).toString();
					}
					else if (oTemp.get(sKey).getValueType().compareTo(JsonValue.ValueType.ARRAY) == 0)
					{
						JsonArray oArray = oTemp.getJsonArray(sKey);
						sValue = new String[oArray.size()];
						for (int j = 0; j < oArray.size(); j++)
							sValue[j] = oArray.getString(j);
					}
					oMap.put(sKey, sValue);
				}
				break;
			}
		}

		for (int i = 0; i < oConfigArray.size(); i++)
		{
			if (oConfigArray.getJsonObject(i).containsKey(sClass)) //find the class
			{
				oTemp = oConfigArray.getJsonObject(i).getJsonObject(sClass);
				for (String sKey : oTemp.keySet())
				{
					if (oTemp.get(sKey).getValueType().compareTo(JsonValue.ValueType.STRING) == 0)
					{
						sValue = new String[1];
						sValue[0] = oTemp.getString(sKey);
					}
					else if (oTemp.get(sKey).getValueType().compareTo(JsonValue.ValueType.NUMBER) == 0)
					{
						sValue = new String[1];
						sValue[0] = oTemp.getJsonNumber(sKey).toString();
					}
					else if (oTemp.get(sKey).getValueType().compareTo(JsonValue.ValueType.ARRAY) == 0)
					{
						JsonArray oArray = oTemp.getJsonArray(sKey);
						sValue = new String[oArray.size()];
						for (int j = 0; j < oArray.size(); j++)
							sValue[j] = oArray.getString(j);
					}
					oMap.put(sKey, sValue);
				}
			}
		}

		for (int i = 0; i < oConfigArray.size(); i++)
		{
			if (oConfigArray.getJsonObject(i).containsKey(sName)) //find the name
			{
				oTemp = oConfigArray.getJsonObject(i).getJsonObject(sName);
				for (String sKey : oTemp.keySet())
				{
					if (oTemp.get(sKey).getValueType().compareTo(JsonValue.ValueType.STRING) == 0)
					{
						sValue = new String[1];
						sValue[0] = oTemp.getString(sKey);
					}
					else if (oTemp.get(sKey).getValueType().compareTo(JsonValue.ValueType.NUMBER) == 0)
					{
						sValue = new String[1];
						sValue[0] = oTemp.getJsonNumber(sKey).toString();
					}
					else if (oTemp.get(sKey).getValueType().compareTo(JsonValue.ValueType.ARRAY) == 0)
					{
						JsonArray oArray = oTemp.getJsonArray(sKey);
						sValue = new String[oArray.size()];
						for (int j = 0; j < oArray.size(); j++)
							sValue[j] = oArray.getString(j);
					}
					oMap.put(sKey, sValue);
				}
			}
		}
		return oMap;
	}
}
