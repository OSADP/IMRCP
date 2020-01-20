/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.system;

import java.util.HashMap;

/**
 *
 * @author Federal Highway Administration
 */
/**
* HashMap that contains all of the configuration items for an ImrcpBlock,
* mapping the key(a String) to the value(s) (a String array)
*/
public class BlockConfig extends HashMap<String, String[]>
{
   /**
	* Creates a new BlockConfig by retrieving the configuration items from
	* Config based on the fully qualified name and instance name
	* @param sClass Fully qualified name of the java class
	* @param sName Instance name of the ImrcpBlock
	*/
   public BlockConfig(String sClass, String sName)
   {
	   putAll(Config.getInstance().getProps(sClass, sName));
   }


   /**
	* Returns the configured value as a String for the given key.
	* @param sKey Key of the value needed
	* @param sDefault Default value, used if the key cannot be found
	* @return If the key is found the value as a String, otherwise
	* the default value
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
	* Returns the configured value as an int for the given key
	* @param sKey Key of the value needed
	* @param nDefault Default value, used if the key cannot be found
	* @return If the key is found the value as an int, otherwise the 
	* default value.
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
	* Returns the configured value as a String array for the given key.
	* @param sKey Key of the value needed
	* @param sDefault Default value, used if the key cannot be found
	* @return If the key is found the value as a String array, otherwise
	* the default value in a String array with length 1. If the default 
	* value is null, an empty String array.
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
	* Returns the configured value as an int array for the given key.
	* @param sKey Key of the value needed
	* @param nDefault Default value, used if the key cannot be found
	* @return If the key is found the value as an int array, otherwise
	* the default value in an int array with length 1.
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
