package imrcp.system;

import java.util.ArrayList;

public class Email
{
	public ArrayList<String> m_oTo;
    public String m_sSubject;
    public String m_sBody;
	
	public Email(ArrayList<String> oTo, String sSubject, String sBody)
	{
		m_oTo = new ArrayList();
		m_oTo.addAll(oTo);
		m_sSubject = sSubject;
		m_sBody = sBody;
	}
}
