/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.system;

import java.util.HashMap;

/**
 * This class is used to store key/value pairs read from configuration files.
 * Base blocks can then retrieve the values using the different get methods
 * @author Federal Highway Administration
 */
public class BlockConfig extends HashMap<String, String[]>
{
	/**
	 * Constructs a BlockConfig for the given class and instance name by
	 * retrieving configuration values from {@link imrcp.system.Config}
	 * @param sClass fully quanlified class name
	 * @param sName instance name of base block
	 */
	public BlockConfig(String sClass, String sName)
	{
		putAll(Config.getInstance().getProps(sClass, sName));
	}

	
	/**
	 * Gets the associated value of the given key as a String. If the key is not
	 * found, the default is returned.
	 * @param sKey key to search for
	 * @param sDefault default value
	 * @return The associated value of the given key if the key exists in the map,
	 * otherwise the default value.
	 */
	public String getString(String sKey, String sDefault)
	{
		String[] sReturn = get(sKey);
		if (sReturn != null)
			return sReturn[0];
		else
			return sDefault;
	}

	
	/**
	 * Gets the associated value of the given key as an integer. If the key is 
	 * not found, the default is returned.
	 * @param sKey key to search for
	 * @param nDefault default value
	 * @return The associated value of the given key if the key exists in the map,
	 * otherwise the default value.
	 */
	public int getInt(String sKey, int nDefault)
	{
		String[] sReturn = get(sKey);
		if (sReturn != null)
			return Integer.parseInt(sReturn[0]);
		else
			return nDefault;
	}
   
	
	/**
	 * Gets the associated value of the given key as a double. If the key is not
	 * found, the default is returned.
	 * @param sKey key to search for
	 * @param dDefault default value
	 * @return The associated value of the given key if the key exists in the map,
	 * otherwise the default value.
	 */
	public double getDouble(String sKey, double dDefault)
	{
		String[] sReturn = get(sKey);
		if (sReturn != null)
			return Double.parseDouble(sReturn[0]);
		else
			return dDefault;
	}

	
	/**
	 * Gets the associated value of the given key as a String array. If the key 
	 * is not found, the default is returned in an array of size 1.
	 * @param sKey key to search for
	 * @param sDefault default value
	 * @return The associated value of the given key if the key exists in the map,
	 * otherwise the default value. If the default is null, an empty array is
	 * returned.
	 */
	public String[] getStringArray(String sKey, String sDefault)
	{
		String[] sReturn = get(sKey);
		if (sReturn != null)
			return sReturn;
		else if (sDefault != null)
			return new String[]{sDefault};
		else
			return new String[0];
	}


	/**
	 * Gets the associated value of the given key as an integer array. If the key 
	 * is not found, the default is returned in an array of size 1.
	 * @param sKey key to search for
	 * @param nDefault default value
	 * @return The associated value of the given key if the key exists in the map,
	 * otherwise the default value. If the default is null, an empty array is
	 * returned.
	 */
	public int[] getIntArray(String sKey, int nDefault)
	{
		String[] sReturn = get(sKey);
		int[] nReturn;
		if (sReturn != null)
		{
			nReturn = new int[sReturn.length];
			for (int i = 0; i < sReturn.length; i++)
				nReturn[i] = Integer.parseInt(sReturn[i]);
			return nReturn;
		}
		else
			return new int[]{nDefault};
	}
}
