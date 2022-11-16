/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store;

import java.text.SimpleDateFormat;

/**
 * Represents an event received from the LADOTD 511 system.
 * @author Federal Highway Administration
 */
public class LADOTD511Event extends EventObs
{
	/**
	 * Nothing needs to be done to close this kind of event so this function
	 * does nothing.
	 */
	@Override
	public void close(long lTime, SimpleDateFormat oSdf)
	{
	}
}
