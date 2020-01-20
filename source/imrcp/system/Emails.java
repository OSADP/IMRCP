package imrcp.system;

import java.util.ArrayList;
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

public class Emails
{
	private static final Emails g_oINSTANCE = new Emails();
	private static Properties m_oProps;
	private static final Logger m_oLOGGER = LogManager.getLogger(Emails.class);
	
	private Emails()
	{
		m_oProps = new Properties();
		m_oProps.put("mail.smtp.auth", "true");
		m_oProps.put("mail.smtp.starttls.enable", "true");
		m_oProps.put("mail.smtp.host", "smtp.gmail.com");
		m_oProps.put("mail.smtp.port", "587");
		m_oProps.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
	}
	
	public static Emails getInstance()
	{
		return g_oINSTANCE;
	}
	
	public static void send(Email oEmail) throws Exception
	{
		InternetAddress[] oAddresses = new InternetAddress[oEmail.m_oTo.size()];
		for (int i = 0; i < oAddresses.length; i++)
			oAddresses[i] = new InternetAddress(oEmail.m_oTo.get(i));
		
		if (oAddresses.length == 0)
		{
			m_oLOGGER.info("send() invoked with empty recipients");
			return;
		}
		
		Session oSession = Session.getInstance(m_oProps, new Authenticator() 
		{
			final String strUsername = "cherneysecretsanta@gmail.com";
			final String strPassword = "Secret!@#";

			@Override
			protected PasswordAuthentication getPasswordAuthentication() 
			{
				return new PasswordAuthentication(strUsername, strPassword);
			}
		});

		try
		{
			MimeMessage oMessage = new MimeMessage(oSession);
			oMessage.setSentDate(new Date());
			oMessage.setRecipients(Message.RecipientType.TO, oAddresses);

			oMessage.setSubject(oEmail.m_sSubject);
			oMessage.setText(oEmail.m_sBody);
			
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
	
	public static void main(String[] sArgs) throws Exception
	{
		ArrayList<String> oList = new ArrayList();
		oList.add("aaron.cherney@synesis-partners.com");
		oList.add("acherney07@gmail.com");
		Emails.send(new Email(oList, "2 recipients", "Hello Aaron,\nThe email worked! Good job!"));
	}
}
