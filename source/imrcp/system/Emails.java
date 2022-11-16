package imrcp.system;

import java.util.Date;
import java.util.Properties;
import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Singleton class that allows basic emails to be sent
 * @author Federal Highway Administration
 */
public class Emails
{
	/**
	 * Singleton instance
	 */
	private static final Emails g_oINSTANCE = new Emails();

	
	/**
	 * Contains the properties necessary mail.smtp properties for sending email
	 */
	private static Properties m_oProps;

	
	/**
	 * Log4j Logger
	 */
	private static final Logger m_oLOGGER = LogManager.getLogger(Emails.class);
	
	
	/**
	 * Constructs the instances creating and setting the necessary properties
	 */
	private Emails()
	{
		m_oProps = new Properties();
		m_oProps.put("mail.smtp.auth", "true");
		m_oProps.put("mail.smtp.starttls.enable", "true");
		m_oProps.put("mail.smtp.host", "smtp.ionos.com");
		m_oProps.put("mail.smtp.port", "587");
		m_oProps.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
	}
	
	
	/**
	 * Gets the singleton instance
	 * @return Singleton instance
	 */
	public static Emails getInstance()
	{
		return g_oINSTANCE;
	}
	
	
	/**
	 * Sends the given email
	 * @param oEmail Email to send
	 * @throws Exception
	 */
	public static void send(Email oEmail) throws Exception
	{
		InternetAddress[] oAddresses = new InternetAddress[oEmail.m_oTo.size()]; // create the necessary address objects
		for (int i = 0; i < oAddresses.length; i++)
			oAddresses[i] = new InternetAddress(oEmail.m_oTo.get(i));
		
		if (oAddresses.length == 0)
		{
			m_oLOGGER.info("send() invoked with empty recipients");
			return;
		}
		
		Session oSession = Session.getInstance(m_oProps, new Authenticator() 
		{
			final String strUsername = "payroll@synesis-partners.com";
			final String strPassword = "nagios_SP*2";

			@Override
			protected PasswordAuthentication getPasswordAuthentication() 
			{
				return new PasswordAuthentication(strUsername, strPassword);
			}
		});

		// construct the email
		try
		{
			MimeMessage oMessage = new MimeMessage(oSession);
			oMessage.setSentDate(new Date());
			oMessage.setRecipients(Message.RecipientType.TO, oAddresses);

			oMessage.setSubject(oEmail.m_sSubject);
			oMessage.setText(oEmail.m_sBody);
			oMessage.setFrom("imrcp-support@data-env.com");
			
			Transport oTransport = oSession.getTransport("smtp");
			oTransport.connect();
			oTransport.sendMessage(oMessage, oMessage.getAllRecipients());
			oTransport.close();
		}
		catch (Exception oEx)
		{
			m_oLOGGER.error(oEx, oEx);
		}
    }
}
