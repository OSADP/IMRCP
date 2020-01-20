package imrcp.web;

import imrcp.system.Config;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * Web application lifecycle listener.
 *
 * @author scot.lange
 */
public class ContextListener implements ServletContextListener
{

	/**
	 *
	 * @param sce
	 */
	@Override
	public void contextInitialized(ServletContextEvent sce)
	{

	}


	/**
	 *
	 * @param sce
	 */
	@Override
	public void contextDestroyed(ServletContextEvent sce)
	{
		Config.getInstance().endSchedule();
	}
}
