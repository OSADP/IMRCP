/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.web;

import imrcp.system.CsvReader;
import imrcp.system.Text;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Object that contains information about system components the user is allowed 
 * to access.
 * @author Federal Highway Administration
 */
public class UserProfile
{
	/**
	 * The Network ids the User has permission to access
	 */
	public String[] m_sNetworks;

	
	/**
	 * The dashboard graphs the User has permission to access
	 */
	public ArrayList<String> m_oGraphs;
    
	
	/**
	 * Default constructor. Gives the user access to no networks.
	 */
	public UserProfile()
	{
		m_sNetworks = new String[0];
		m_oGraphs = new ArrayList(4);
	}
	
	
	/**
	 * Constructs a UserProfile from the given user profile CSV file
	 * @param oIn CsvReader wrapping the InputStream of the user profile CSV file
	 * @throws IOException
	 */
	public UserProfile(CsvReader oIn)
		throws IOException
    {
		int nCols = oIn.readLine(); // read the first line
		m_sNetworks = new String[nCols]; // number of columns is the number of networks
		for (int nIndex = 0; nIndex < nCols; nIndex++)
			m_sNetworks[nIndex] = oIn.parseString(nIndex); // read each network
		
		nCols = oIn.readLine(); // read the second line
		m_oGraphs = new ArrayList(nCols); // number of columns is the number of graphs
		for (int nIndex = 0; nIndex < nCols; nIndex++)
		{
			String sTemp = oIn.parseString(nIndex);
			if (!Text.isEmpty(sTemp)) // read each graph if the String is not empty
				m_oGraphs.add(sTemp);
		}
    }
}
