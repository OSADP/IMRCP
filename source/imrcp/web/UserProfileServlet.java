package imrcp.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.security.Principal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Optional;
import javax.naming.NamingException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.http.HttpStatus;

/**
 *
 * @author scot.lange
 */
@WebServlet(name = "Profile", urlPatterns = {
    "/profile/*"
})
public class UserProfileServlet extends BaseRestResourceServlet
{

  public UserProfileServlet() throws NamingException
  {
  }

  @Override
  protected String getRequestPattern()
  {
    return "^map-settings$";
  }

  @Override
  protected int processRequest(String sMethod, String[] sUrlParts, HttpServletRequest oReq, HttpServletResponse oResp) throws IOException
  {
    Principal oUser = oReq.getUserPrincipal();
    if(oUser == null)
      return HttpStatus.SC_FORBIDDEN;

    String sUserName = oUser.getName();

    try{
    switch(sUrlParts.length)
    {
      case 1:
        switch(sMethod)
        {
          case "GET":
            returnUserMapSettings(oResp, sUserName);
            return HttpStatus.SC_OK;
          case "PUT":
            saveUserSettings(oReq, sUserName);
            return HttpStatus.SC_NO_CONTENT;
          default:
            return HttpStatus.SC_BAD_REQUEST;
        }
      default:
        return HttpStatus.SC_BAD_REQUEST;
    }
    }
    catch(IOException | SQLException ex)
    {
      return HttpStatus.SC_INTERNAL_SERVER_ERROR;
    }

  }

  private void saveUserSettings(HttpServletRequest oReq, String sUserName) throws SQLException, IOException
  {
    Optional<String> sRequestBody = readRequestBody(oReq);
    try(
        Connection oCon = m_oDatasource.getConnection();
        PreparedStatement oSettingsStmt = oCon.prepareStatement(
            "UPDATE user_profile set map_settings = ? WHERE username = ?"))
    {
      if(sRequestBody.isPresent())
        oSettingsStmt.setString(1, sRequestBody.get());
      else
        oSettingsStmt.setNull(1, Types.VARCHAR);
      oSettingsStmt.setString(2, sUserName);
      if(oSettingsStmt.executeUpdate() == 0 && sRequestBody.isPresent())
      {
        try(PreparedStatement oInsertSettingStmt = oCon.prepareStatement(
                "INSERT INTO user_profile (map_settings, username) VALUES (?,?)"))
        {
          oInsertSettingStmt.setString(1, sRequestBody.get());
          oInsertSettingStmt.setString(2, sUserName);
          oInsertSettingStmt.executeUpdate();
        }
      }
    }
  }

  private void returnUserMapSettings(HttpServletResponse oResp, String sUserName) throws SQLException, IOException
  {
    try(
        Connection oCon = m_oDatasource.getConnection();
        PreparedStatement oSettingsStmt = oCon.prepareStatement(
            "SELECT map_settings FROM user_profile WHERE username = ?"))
    {
      oSettingsStmt.setString(1, sUserName);
      try(ResultSet oSettingsRs = oSettingsStmt.executeQuery())
      {
        String sResponse = null;
        if(oSettingsRs.next())
          sResponse = oSettingsRs.getString("map_settings");
        if(sResponse == null || sResponse.isEmpty())
          sResponse = "{}";
        try(PrintWriter oWriter = oResp.getWriter())
        {
          oWriter.println(sResponse);
        }
      }
    }
  }

}
