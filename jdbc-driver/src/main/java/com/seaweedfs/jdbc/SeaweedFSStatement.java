package com.seaweedfs.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * JDBC Statement implementation for SeaweedFS
 */
public class SeaweedFSStatement implements Statement {
    
    private static final Logger logger = LoggerFactory.getLogger(SeaweedFSStatement.class);
    
    protected final SeaweedFSConnection connection;
    private boolean closed = false;
    private ResultSet currentResultSet = null;
    private int updateCount = -1;
    private int maxRows = 0;
    private int queryTimeout = 0;
    private int fetchSize = 1000;
    private List<String> batch = new ArrayList<>();
    
    public SeaweedFSStatement(SeaweedFSConnection connection) {
        this.connection = connection;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();
        logger.debug("Executing query: {}", sql);
        
        try {
            // Send query to server
            connection.sendMessage((byte)0x03, sql.getBytes()); // JDBC_MSG_EXECUTE_QUERY
            
            // Read response
            SeaweedFSConnection.Response response = connection.readResponse();
            
            if (response.type == (byte)0x01) { // JDBC_RESP_ERROR
                throw new SQLException("Query failed: " + new String(response.data));
            } else if (response.type == (byte)0x02) { // JDBC_RESP_RESULT_SET
                // Parse result set data
                currentResultSet = new SeaweedFSResultSet(this, response.data);
                updateCount = -1;
                return currentResultSet;
            } else {
                throw new SQLException("Unexpected response type: " + response.type);
            }
            
        } catch (Exception e) {
            throw new SQLException("Failed to execute query: " + e.getMessage(), e);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        logger.debug("Executing update: {}", sql);
        
        try {
            // Send update to server
            connection.sendMessage((byte)0x04, sql.getBytes()); // JDBC_MSG_EXECUTE_UPDATE
            
            // Read response
            SeaweedFSConnection.Response response = connection.readResponse();
            
            if (response.type == (byte)0x01) { // JDBC_RESP_ERROR
                throw new SQLException("Update failed: " + new String(response.data));
            } else if (response.type == (byte)0x03) { // JDBC_RESP_UPDATE_COUNT
                // Parse update count
                updateCount = parseUpdateCount(response.data);
                currentResultSet = null;
                return updateCount;
            } else {
                throw new SQLException("Unexpected response type: " + response.type);
            }
            
        } catch (Exception e) {
            throw new SQLException("Failed to execute update: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws SQLException {
        if (!closed) {
            if (currentResultSet != null) {
                currentResultSet.close();
                currentResultSet = null;
            }
            closed = true;
            logger.debug("Statement closed");
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        checkClosed();
        return 0; // No limit
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        checkClosed();
        // No-op
    }

    @Override
    public int getMaxRows() throws SQLException {
        checkClosed();
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        checkClosed();
        this.maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        checkClosed();
        // No-op
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        checkClosed();
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        checkClosed();
        this.queryTimeout = seconds;
    }

    @Override
    public void cancel() throws SQLException {
        checkClosed();
        // No-op - cancellation not supported
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
        // No-op
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        checkClosed();
        // No-op - cursors not supported
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        logger.debug("Executing: {}", sql);
        
        // Determine if this is likely a query or update
        String trimmedSql = sql.trim().toUpperCase();
        if (trimmedSql.startsWith("SELECT") || 
            trimmedSql.startsWith("SHOW") || 
            trimmedSql.startsWith("DESCRIBE") ||
            trimmedSql.startsWith("DESC") ||
            trimmedSql.startsWith("EXPLAIN")) {
            // It's a query
            executeQuery(sql);
            return true;
        } else {
            // It's an update
            executeUpdate(sql);
            return false;
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();
        return currentResultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed();
        return updateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        checkClosed();
        if (currentResultSet != null) {
            currentResultSet.close();
            currentResultSet = null;
        }
        updateCount = -1;
        return false; // No more results
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkClosed();
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLException("Only FETCH_FORWARD is supported");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkClosed();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        checkClosed();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        checkClosed();
        batch.add(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        checkClosed();
        batch.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkClosed();
        int[] results = new int[batch.size()];
        
        for (int i = 0; i < batch.size(); i++) {
            try {
                results[i] = executeUpdate(batch.get(i));
            } catch (SQLException e) {
                results[i] = EXECUTE_FAILED;
            }
        }
        
        batch.clear();
        return results;
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkClosed();
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        checkClosed();
        return getMoreResults();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException("Generated keys are not supported");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        checkClosed();
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        checkClosed();
        // No-op
    }

    @Override
    public boolean isPoolable() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        checkClosed();
        // No-op
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        checkClosed();
        return false;
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
    
    protected void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Statement is closed");
        }
        if (connection.isClosed()) {
            throw new SQLException("Connection is closed");
        }
    }
    
    private int parseUpdateCount(byte[] data) {
        if (data.length >= 4) {
            return ((data[0] & 0xFF) << 24) |
                   ((data[1] & 0xFF) << 16) |
                   ((data[2] & 0xFF) << 8) |
                   (data[3] & 0xFF);
        }
        return 0;
    }
}
