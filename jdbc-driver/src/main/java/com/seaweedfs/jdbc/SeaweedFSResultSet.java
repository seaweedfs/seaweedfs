package com.seaweedfs.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * ResultSet implementation for SeaweedFS JDBC
 */
public class SeaweedFSResultSet implements ResultSet {
    
    private final SeaweedFSStatement statement;
    private final List<String> columnNames;
    private final List<List<String>> rows;
    private int currentRowIndex = -1; // Before first row
    private boolean closed = false;
    private boolean wasNull = false;
    
    public SeaweedFSResultSet(SeaweedFSStatement statement, byte[] data) throws SQLException {
        this.statement = statement;
        this.columnNames = new ArrayList<>();
        this.rows = new ArrayList<>();
        
        parseResultSetData(data);
    }
    
    private void parseResultSetData(byte[] data) throws SQLException {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            
            // Read column count
            int columnCount = buffer.getInt();
            
            // Read column names
            for (int i = 0; i < columnCount; i++) {
                int nameLength = buffer.getInt();
                byte[] nameBytes = new byte[nameLength];
                buffer.get(nameBytes);
                columnNames.add(new String(nameBytes));
            }
            
            // Read row count
            int rowCount = buffer.getInt();
            
            // Read rows
            for (int i = 0; i < rowCount; i++) {
                List<String> row = new ArrayList<>();
                for (int j = 0; j < columnCount; j++) {
                    int valueLength = buffer.getInt();
                    if (valueLength > 0) {
                        byte[] valueBytes = new byte[valueLength];
                        buffer.get(valueBytes);
                        row.add(new String(valueBytes));
                    } else {
                        row.add(null); // Empty value = null
                    }
                }
                rows.add(row);
            }
            
        } catch (Exception e) {
            throw new SQLException("Failed to parse result set data", e);
        }
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        if (currentRowIndex + 1 < rows.size()) {
            currentRowIndex++;
            return true;
        }
        return false;
    }

    @Override
    public void close() throws SQLException {
        closed = true;
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkClosed();
        return wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkClosed();
        checkRowPosition();
        checkColumnIndex(columnIndex);
        
        String value = getCurrentRow().get(columnIndex - 1);
        wasNull = (value == null);
        return value;
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        String value = getString(columnIndex);
        if (wasNull) return false;
        return Boolean.parseBoolean(value);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        String value = getString(columnIndex);
        if (wasNull) return 0;
        try {
            return Byte.parseByte(value);
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert '" + value + "' to byte", e);
        }
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        String value = getString(columnIndex);
        if (wasNull) return 0;
        try {
            return Short.parseShort(value);
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert '" + value + "' to short", e);
        }
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        String value = getString(columnIndex);
        if (wasNull) return 0;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert '" + value + "' to int", e);
        }
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        String value = getString(columnIndex);
        if (wasNull) return 0;
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert '" + value + "' to long", e);
        }
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        String value = getString(columnIndex);
        if (wasNull) return 0;
        try {
            return Float.parseFloat(value);
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert '" + value + "' to float", e);
        }
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        String value = getString(columnIndex);
        if (wasNull) return 0;
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert '" + value + "' to double", e);
        }
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        String value = getString(columnIndex);
        if (wasNull) return null;
        try {
            return new BigDecimal(value).setScale(scale, BigDecimal.ROUND_HALF_UP);
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert '" + value + "' to BigDecimal", e);
        }
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        String value = getString(columnIndex);
        if (wasNull) return null;
        return value.getBytes();
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        String value = getString(columnIndex);
        if (wasNull) return null;
        try {
            return Date.valueOf(value);
        } catch (IllegalArgumentException e) {
            throw new SQLException("Cannot convert '" + value + "' to Date", e);
        }
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        String value = getString(columnIndex);
        if (wasNull) return null;
        try {
            return Time.valueOf(value);
        } catch (IllegalArgumentException e) {
            throw new SQLException("Cannot convert '" + value + "' to Time", e);
        }
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        String value = getString(columnIndex);
        if (wasNull) return null;
        try {
            // Try parsing as timestamp first
            return Timestamp.valueOf(value);
        } catch (IllegalArgumentException e) {
            // If that fails, try parsing as long (nanoseconds)
            try {
                long nanos = Long.parseLong(value);
                return new Timestamp(nanos / 1000000); // Convert nanos to millis
            } catch (NumberFormatException e2) {
                throw new SQLException("Cannot convert '" + value + "' to Timestamp", e);
            }
        }
    }

    // String-based column access
    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkClosed();
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equalsIgnoreCase(columnLabel)) {
                return i + 1; // JDBC uses 1-based indexing
            }
        }
        throw new SQLException("Column not found: " + columnLabel);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return new SeaweedFSResultSetMetaData(columnNames);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getString(columnLabel);
    }

    // Navigation methods
    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkClosed();
        return currentRowIndex == -1 && !rows.isEmpty();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkClosed();
        return currentRowIndex >= rows.size() && !rows.isEmpty();
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkClosed();
        return currentRowIndex == 0 && !rows.isEmpty();
    }

    @Override
    public boolean isLast() throws SQLException {
        checkClosed();
        return currentRowIndex == rows.size() - 1 && !rows.isEmpty();
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkClosed();
        currentRowIndex = -1;
    }

    @Override
    public void afterLast() throws SQLException {
        checkClosed();
        currentRowIndex = rows.size();
    }

    @Override
    public boolean first() throws SQLException {
        checkClosed();
        if (!rows.isEmpty()) {
            currentRowIndex = 0;
            return true;
        }
        return false;
    }

    @Override
    public boolean last() throws SQLException {
        checkClosed();
        if (!rows.isEmpty()) {
            currentRowIndex = rows.size() - 1;
            return true;
        }
        return false;
    }

    @Override
    public int getRow() throws SQLException {
        checkClosed();
        return currentRowIndex >= 0 && currentRowIndex < rows.size() ? currentRowIndex + 1 : 0;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        checkClosed();
        if (row > 0 && row <= rows.size()) {
            currentRowIndex = row - 1;
            return true;
        } else if (row < 0 && Math.abs(row) <= rows.size()) {
            currentRowIndex = rows.size() + row;
            return true;
        } else {
            if (row > rows.size()) {
                currentRowIndex = rows.size(); // After last
            } else {
                currentRowIndex = -1; // Before first
            }
            return false;
        }
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        return absolute(getRow() + rows);
    }

    @Override
    public boolean previous() throws SQLException {
        checkClosed();
        if (currentRowIndex > 0) {
            currentRowIndex--;
            return true;
        }
        return false;
    }

    // Unsupported operations
    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("ASCII streams are not supported");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Unicode streams are not supported");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Binary streams are not supported");
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("ASCII streams are not supported");
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Unicode streams are not supported");
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Binary streams are not supported");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // No-op
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException("Named cursors are not supported");
    }

    // Additional getters with default implementations
    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams are not supported");
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Character streams are not supported");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        String value = getString(columnIndex);
        if (wasNull) return null;
        try {
            return new BigDecimal(value);
        } catch (NumberFormatException e) {
            throw new SQLException("Cannot convert '" + value + "' to BigDecimal", e);
        }
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    // Update operations (not supported - SeaweedFS is read-only)
    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    // String-based update operations (all throw exceptions)
    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void refreshRow() throws SQLException {
        // No-op
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        // No-op
    }

    @Override
    public Statement getStatement() throws SQLException {
        checkClosed();
        return statement;
    }

    // Additional methods with empty/default implementations
    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndex);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref objects are not supported");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob objects are not supported");
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob objects are not supported");
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array objects are not supported");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnLabel);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref objects are not supported");
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob objects are not supported");
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob objects are not supported");
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array objects are not supported");
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDate(columnIndex);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(columnLabel);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return getTime(columnIndex);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(columnLabel);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(columnLabel);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        String value = getString(columnIndex);
        if (wasNull) return null;
        try {
            return new URL(value);
        } catch (Exception e) {
            throw new SQLException("Cannot convert '" + value + "' to URL", e);
        }
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getURL(findColumn(columnLabel));
    }

    // More update operations (all not supported)
    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    // More modern JDBC methods
    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId objects are not supported");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId objects are not supported");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob objects are not supported");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob objects are not supported");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML objects are not supported");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML objects are not supported");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getString(columnLabel);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacter streams are not supported");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacter streams are not supported");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported - SeaweedFS is read-only");
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        Object value = getObject(columnIndex);
        if (value == null || wasNull) {
            return null;
        }
        
        if (type.isInstance(value)) {
            return type.cast(value);
        }
        
        // Basic type conversions
        if (type == String.class) {
            return type.cast(value.toString());
        } else if (type == Integer.class && value instanceof String) {
            return type.cast(Integer.valueOf((String)value));
        } else if (type == Long.class && value instanceof String) {
            return type.cast(Long.valueOf((String)value));
        } else if (type == Boolean.class && value instanceof String) {
            return type.cast(Boolean.valueOf((String)value));
        }
        
        throw new SQLException("Cannot convert " + value.getClass().getName() + " to " + type.getName());
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        checkClosed();
        return false; // SeaweedFS is read-only, no deletions possible
    }

    @Override
    public boolean rowInserted() throws SQLException {
        checkClosed();
        return false; // SeaweedFS is read-only, no insertions possible
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        checkClosed();
        return false; // SeaweedFS is read-only, no updates possible
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkClosed();
        return ResultSet.CONCUR_READ_ONLY; // SeaweedFS is read-only
    }

    @Override
    public int getType() throws SQLException {
        checkClosed();
        return ResultSet.TYPE_FORWARD_ONLY; // Forward-only scrolling
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return 1000; // Default fetch size
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
        // No-op for now, could be enhanced to affect performance
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return ResultSet.FETCH_FORWARD; // Always forward-only
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkClosed();
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLException("Only FETCH_FORWARD is supported");
        }
    }
    
    // Helper methods
    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("ResultSet is closed");
        }
    }
    
    private void checkRowPosition() throws SQLException {
        if (currentRowIndex < 0 || currentRowIndex >= rows.size()) {
            throw new SQLException("ResultSet is not positioned on a valid row");
        }
    }
    
    private void checkColumnIndex(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columnNames.size()) {
            throw new SQLException("Column index " + columnIndex + " is out of range (1-" + columnNames.size() + ")");
        }
    }
    
    private List<String> getCurrentRow() {
        return rows.get(currentRowIndex);
    }
    
    // Get result set information
    public int getColumnCount() {
        return columnNames.size();
    }
    
    public List<String> getColumnNames() {
        return new ArrayList<>(columnNames);
    }
    
    public int getRowCount() {
        return rows.size();
    }
}
