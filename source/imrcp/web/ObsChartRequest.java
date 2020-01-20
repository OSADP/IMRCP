package imrcp.web;

/**
 *
 * @author scot.lange
 */
public class ObsChartRequest extends ObsRequest
{
  private int m_nObstypeId;

  /**
   * @return the m_nObstypeId
   */
  public int getObstypeId()
  {
    return m_nObstypeId;
  }

  /**
   * @param m_nObstypeId the m_nObstypeId to set
   */
  public void setObstypeId(int m_nObstypeId)
  {
    this.m_nObstypeId = m_nObstypeId;
  }

}
