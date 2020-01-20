<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8" trimDirectiveWhitespaces="true" %>
<%
	java.util.Date oNow = new java.util.Date();
	boolean bAuthorized = false;
	String sUser = request.getParameter("identity");

	try
	{
		request.login(sUser, request.getParameter("codeword"));
		bAuthorized = true;

		StringBuilder sBuf = new StringBuilder(new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(oNow));
		sBuf.append(',').append(sUser).append(',').append(bAuthorized).append(',').append(request.getHeader("x-forwarded-for")).append('\n');
		String sLog = sBuf.toString();
		synchronized (System.err)
		{
			java.io.FileWriter oOut = new java.io.FileWriter(new java.io.File(new java.text.SimpleDateFormat("'/opt/imrcp-prod/logs/keep/'yyyyMMdd'-users.log'").format(oNow)), true);
			oOut.write(sLog);
			oOut.close();
		}
	}
	catch (Exception oEx)
	{
		System.out.println(oEx);
	}
%>
{
	"login": <%= bAuthorized %>
}
