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
 * This class is a singleton that manages the configurable parameters for the
 * entire system.
 * @author Federal Highway Administration
 */
public class Config implements Runnable
{
	/**
	 * Path to the IMRCP JSON configuration file
	 */
	private String m_sFilename;

	
	/**
	 * Singleton reference
	 */
	private static Config g_oConfig;

	
	/**
	 * JsonArray object that stores the configuration file in memory
	 */
	private JsonArray m_oConfigArray;

	
	/**
	 * Stores the last time the configuration file was modified in milliseconds
	 * since Epoch
	 */
	private long m_lLastModified;

	
	/**
	 * Scheduling id
	 */
	private int m_nSchedId;

	
	/**
	 * Log4j Logger object
	 */
	private Logger m_oLogger = LogManager.getLogger(Config.class);

	
	/**
	 * Constructs a Config with the given IMRCP JSON configuration file path
	 * @param sFilename
	 */
	public Config(String sFilename)
	{
		g_oConfig = this;
		m_sFilename = sFilename;
		run();
	}
	

	/**
	 * Returns the singleton instance
	 * @return The singleton instance, {@link #g_oConfig}
	 */
	public static Config getInstance()
	{
		return g_oConfig;
	}

	
	/**
	 * Get the JsonArray that contains the configuration for the system.
	 * @return The JsonARray containing the cofiguration for the system, {@link #m_oConfigArray}
	 */
	private synchronized JsonArray getConfigArray()
	{
		return m_oConfigArray;
	}

	
	/**
	 * Sets a schedule to execute on a fixed interval.
	 */
	public void setSchedule()
	{
		m_nSchedId = Scheduling.getInstance().createSched(this, 0, 300);
	}

	
	/**
	 * Gets the integer configuration value with the given key for the given
	 * class and instance name. First the return value is set based on the class
	 * name and key. Then if a configuration entry exists based on the instance 
	 * name and key, the return value is set to that value. If the key cannot be 
	 * found for class or instance name, the default is returned.
	 * 
	 * @param sClass Fully Qualified Class Name of the Java class the configuration
	 * is being looked up for
	 * @param sName Instance name the configuration is being looked up for
	 * @param sKey Name of the configuration value being looked up
	 * @param nDefault Value to return if the key cannot be found
	 * @return The configuration value that matches the given key for the given
	 * class name and instance name. Configuration by instance name overrides
	 * configuration by class name. If the key cannot be found for class or instance
	 * name, the default value is returned.
	 */
	public int getInt(String sClass, String sName, String sKey, int nDefault)
	{
		JsonArray oConfigArray = getConfigArray();
		int nReturn = nDefault;

		// find configuration value by class
		for (int i = 0; i < oConfigArray.size(); i++)
			if (oConfigArray.getJsonObject(i).containsKey(sClass))  //find the class
				if (oConfigArray.getJsonObject(i).getJsonObject(sClass).containsKey(sKey) //find the key
				   && oConfigArray.getJsonObject(i).getJsonObject(sClass).get(sKey).getValueType().compareTo(JsonValue.ValueType.NUMBER) == 0) //ensure the value is a number
					nReturn = oConfigArray.getJsonObject(i).getJsonObject(sClass).getInt(sKey); //set the value

		// find configuration by name, this will override configuration by class
		for (int i = 0; i < oConfigArray.size(); i++)
			if (oConfigArray.getJsonObject(i).containsKey(sName))  //find the name
				if (oConfigArray.getJsonObject(i).getJsonObject(sName).containsKey(sKey) //find the key
				   && oConfigArray.getJsonObject(i).getJsonObject(sName).get(sKey).getValueType().compareTo(JsonValue.ValueType.NUMBER) == 0) //ensure the value is a number
					nReturn = oConfigArray.getJsonObject(i).getJsonObject(sName).getInt(sKey); //set the value

		return nReturn;
	}

	
	/**
	 * Gets the String configuration value with the given key for the given
	 * class and instance name. First the return value is set based on the class
	 * name and key. Then if a configuration entry exists based on the instance 
	 * name and key, the return value is set to that value. If the key cannot be 
	 * found for class or instance name, the default is returned.
	 * 
	 * @param sClass Fully Qualified Class Name of the Java class the configuration
	 * is being looked up for
	 * @param sName Instance name the configuration is being looked up for
	 * @param sKey Name of the configuration value being looked up
	 * @param sDefault Value to return if the key cannot be found
	 * @return The configuration value that matches the given key for the given
	 * class name and instance name. Configuration by instance name overrides
	 * configuration by class name. If the key cannot be found for class or instance
	 * name, the default value is returned.
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
	 * Gets the String array configuration value with the given key for the given
	 * class and instance name. First the return value is set based on the class
	 * name and key. Then if a configuration entry exists based on the instance 
	 * name and key, the return value is set to that value. If the key cannot be 
	 * found for class or instance name, the default is returned.
	 * 
	 * @param sClass Fully Qualified Class Name of the Java class the configuration
	 * is being looked up for
	 * @param sName Instance name the configuration is being looked up for
	 * @param sKey Name of the configuration value being looked up
	 * @param sDefault Value to return if the key cannot be found
	 * @return The configuration value that matches the given key for the given
	 * class name and instance name. Configuration by instance name overrides
	 * configuration by class name. If the key cannot be found for class or instance
	 * name, the default value is returned. If the default value is not null it
	 * is returned as a String[] with length 1 storing the default value.
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
	 * If the IMRCP JSON configuration file has been modified, reads the file
	 * and updates the reference of {@link #m_oConfigArray} to a new JsonArray
	 * that contains the contents of the file.
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
	 * Adds all of the configuration items as String[]s for the given class and 
	 * instance name to a HashMap and returns that HashMap
	 * 
	 * @param sClass Fully Qualified Class Name of the Java class the configuration
	 * is being looked up for
	 * @param sName Instance name the configuration is being looked up for
	 * @return A HashMap with all of the configuration items for the given class
	 * and instance name with the keys being the name of the configuration item
	 * and the values being String[] representations of the configuration values
	 */
	public HashMap<String, String[]> getProps(String sClass, String sName)
	{
		HashMap<String, String[]> oMap = new HashMap();
		String[] sValue = null;
		JsonArray oConfigArray = getConfigArray();
		JsonObject oTemp = null;

		for (int i = 0; i < oConfigArray.size(); i++)
		{
			if (oConfigArray.getJsonObject(i).containsKey("imrcp.ImrcpBlock")) // get the default values for ImrcpBlocks
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
