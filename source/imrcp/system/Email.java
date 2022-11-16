package imrcp.system;

import java.util.ArrayList;

/**
 * Contains the data for a basic email (recipients, subject, and body).
 * @author Federal Highway Administration
 */
public class Email
{
	/**
	 * List of recipients
	 */
	public ArrayList<String> m_oTo;

	
	/**
	 * Email subject
	 */
	public String m_sSubject;

	
	/**
	 * Text of the email's body
	 */
	public String m_sBody;
	
	
	/**
	 * Constructs an Email with the given parameters
	 * @param oTo list of recipients
	 * @param sSubject subject of email
	 * @param sBody body of email
	 */
	public Email(ArrayList<String> oTo, String sSubject, String sBody)
	{
		m_oTo = new ArrayList();
		m_oTo.addAll(oTo);
		m_sSubject = sSubject;
		m_sBody = sBody;
	}
	
	
	/**
	 * Constructs an Email with the given parameters
	 * @param sTo array of recipients
	 * @param sSubject subject of email
	 * @param sBody body of email
	 */
	public Email(String[] sTo, String sSubject, String sBody)
	{
		m_oTo = new ArrayList();
		for (String sAddress : sTo)
			m_oTo.add(sAddress);
		m_sSubject = sSubject;
		m_sBody = sBody;
	}
}
