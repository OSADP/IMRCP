package imrcp.store;

import imrcp.system.Id;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Map;
import java.util.function.Function;

/**
 * Template class that implements the ResultSet interface with an ArrayList. 
 * Many of the functions are not implement since only a subset of the interface
 * functions were needed.
 * @author Federal Highway Administration
 * @param <T> type of object stored by the ResultSet
 */
public class ImrcpResultSet<T> extends ArrayList<T> implements ResultSet
{
	/**
	 * Keeps track of the current row(index) of the ArrayList
	 */
	protected int m_nCursor;

	
	/**
	 * Contains the column names of the objects stored in the list
	 */
	protected String[] m_sColNames;

	
	/**
	 * Child classes implement an interface for the specific T to get int values
	 * from the columns
	 */
	protected Function<T, Integer>[] m_oIntDelegates;

	
	/**
	 * Child classes implement an interface for the specific T to get String values
	 * from the columns
	 */
	protected Function<T, String>[] m_oStringDelegates;

	
	/**
	 * Child classes implement an interface for the specific T to get double values
	 * from the columns
	 */
	protected Function<T, Double>[] m_oDoubleDelegates;

	
	/**
	 * Child classes implement an interface for the specific T to get long values
	 * from the columns
	 */
	protected Function<T, Long>[] m_oLongDelegates;

	
	/**
	 * Child classes implement an interface for the specific T to get short values
	 * from the columns
	 */
	protected Function<T, Short>[] m_oShortDelegates;
	
	
	/**
	 * Child classes implement an interface for the specific T to get Id values
	 * from the columns
	 */
	protected Function<T, Id>[] m_oIdDelegates;

	
	/**
	 * Default constructor, sets the cursor to before the first row
	 */
	ImrcpResultSet()
	{
		m_nCursor = -1;
	}

	
	/**
	 * Looks up the index for the given column name.
	 * @param sCol Column to look up
	 * @return Index that corresponds to the column name. If the column name does
	 * not exist -1
	 */
	protected int lookup(String sCol)
	{
		int nIndex = m_sColNames.length;
		while (nIndex-- > 0)
		{
			if (m_sColNames[nIndex].compareTo(sCol) == 0)
				return nIndex;
		}

		return nIndex;
	}

	
	/**
	 * Tests if the cursor and given column index are in range and returns the
	 * correct index for the column. Indices are 1 based for ResultSets, since 
	 * the underlaying list is zero based the correct index is nColIndex - 1
	 * @param nColIndex 1 based column index
	 * @return 0 based column index
	 * @throws SQLException if the cursor or column index is out of range
	 */
	protected int rangeTest(int nColIndex) throws SQLException
	{
		if (m_nCursor < 0 || m_nCursor >= size() || --nColIndex < 0 || nColIndex >= m_sColNames.length)
			throw new SQLException();

		return nColIndex;
	}

	
	/**
	 * Advances the cursor and checks if there are any more rows available
	 * @return true if the cursor is not past the end of the list
	 * @throws SQLException
	 */
	@Override
	public boolean next() throws SQLException
	{
		return ++m_nCursor < size();
	}


	/**
	 * Wrapper for {@link ArrayList#clear()}
	 * @throws SQLException 
	 */
	@Override
	public void close() throws SQLException
	{
		clear();
	}

	
	/**
	 * Gets the String value for the given column
	 * @param columnIndex 1 based column index
	 * @return String value for the given column
	 * @throws SQLException
	 */
	@Override
	public String getString(int columnIndex) throws SQLException
	{
		return m_oStringDelegates[rangeTest(columnIndex)].apply(get(m_nCursor));
	}

	
	/**
	 * Gets the short value for the given column
	 * @param columnIndex 1 based column index
	 * @return short value for the given column
	 * @throws SQLException
	 */
	@Override
	public short getShort(int columnIndex) throws SQLException
	{
		return m_oShortDelegates[rangeTest(columnIndex)].apply(get(m_nCursor));
	}

	
	/**
	 * Gets the int value for the given column
	 * @param columnIndex 1 based column index
	 * @return int value for the given column
	 * @throws SQLException
	 */
	@Override
	public int getInt(int columnIndex) throws SQLException
	{
		return m_oIntDelegates[rangeTest(columnIndex)].apply(get(m_nCursor));
	}

	
	/**
	 * Gets the long value for the given column
	 * @param columnIndex 1 based column index
	 * @return
	 * @throws SQLException
	 */
	@Override
	public long getLong(int columnIndex) throws SQLException
	{
		return m_oLongDelegates[rangeTest(columnIndex)].apply(get(m_nCursor));
	}

	
	/**
	 * Gets the double value for the given column
	 * @param columnIndex 1 based column index
	 * @return double value for the given column
	 * @throws SQLException
	 */
	@Override
	public double getDouble(int columnIndex) throws SQLException
	{
		return m_oDoubleDelegates[rangeTest(columnIndex)].apply(get(m_nCursor));
	}

	
	/**
	 * NOT IMPLEMENTED
	 */
	public int[] getIntArray(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	
	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public boolean getBoolean(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	
	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public byte getByte(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	
	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public float getFloat(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	
	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public boolean wasNull() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	
	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	
	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public byte[] getBytes(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	
	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Date getDate(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	
	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Time getTime(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	
	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	
	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	
	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}

	
	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public String getString(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public boolean getBoolean(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public byte getByte(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public short getShort(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public int getInt(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public long getLong(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public float getFloat(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public double getDouble(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public byte[] getBytes(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Date getDate(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Time getTime(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public InputStream getUnicodeStream(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public SQLWarning getWarnings() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void clearWarnings() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public String getCursorName() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public ResultSetMetaData getMetaData() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Object getObject(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Object getObject(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public int findColumn(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public boolean isBeforeFirst() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public boolean isAfterLast() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public boolean isFirst() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public boolean isLast() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void beforeFirst() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void afterLast() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public boolean first() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public boolean last() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public int getRow() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public boolean absolute(int row) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public boolean relative(int rows) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public boolean previous() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void setFetchDirection(int direction) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public int getFetchDirection() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void setFetchSize(int rows) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public int getFetchSize() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public int getType() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public int getConcurrency() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public boolean rowUpdated() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public boolean rowInserted() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public boolean rowDeleted() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateNull(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateShort(int columnIndex, short x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateInt(int columnIndex, int x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateLong(int columnIndex, long x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateString(int columnIndex, String x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateNull(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateBoolean(String columnLabel, boolean x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateShort(String columnLabel, short x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateInt(String columnLabel, int x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateLong(String columnLabel, long x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateString(String columnLabel, String x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void insertRow() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateRow() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void deleteRow() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void refreshRow() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void cancelRowUpdates() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void moveToInsertRow() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void moveToCurrentRow() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Statement getStatement() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Ref getRef(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Blob getBlob(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Clob getClob(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Array getArray(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Ref getRef(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Blob getBlob(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Clob getClob(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Array getArray(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public URL getURL(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public URL getURL(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public RowId getRowId(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public RowId getRowId(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public int getHoldability() throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public boolean isClosed() throws SQLException
	{
		return isEmpty();
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateNString(int columnIndex, String nString) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateNString(String columnLabel, String nString) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateNClob(String columnLabel, NClob nClob) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public NClob getNClob(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public NClob getNClob(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public String getNString(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public String getNString(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateClob(String columnLabel, Reader reader) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public void updateNClob(String columnLabel, Reader reader) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}


	/**
	 * NOT IMPLEMENTED
	 */
	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException
	{
		throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	}
}
