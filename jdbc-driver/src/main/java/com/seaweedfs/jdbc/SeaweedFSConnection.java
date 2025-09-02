package com.seaweedfs.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * JDBC Connection implementation for SeaweedFS
 */
public class SeaweedFSConnection implements Connection {

    private static final Logger logger = LoggerFactory.getLogger(SeaweedFSConnection.class);
    
    // Protocol constants (must match server implementation)
    private static final byte JDBC_MSG_CONNECT = 0x01;
    private static final byte JDBC_MSG_DISCONNECT = 0x02;
    private static final byte JDBC_MSG_EXECUTE_QUERY = 0x03;
    private static final byte JDBC_MSG_EXECUTE_UPDATE = 0x04;
    private static final byte JDBC_MSG_GET_METADATA = 0x07;
    private static final byte JDBC_MSG_SET_AUTOCOMMIT = 0x08;
    private static final byte JDBC_MSG_COMMIT = 0x09;
    private static final byte JDBC_MSG_ROLLBACK = 0x0A;
    
    private static final byte JDBC_RESP_OK = 0x00;
    private static final byte JDBC_RESP_ERROR = 0x01;
    private static final byte JDBC_RESP_RESULT_SET = 0x02;
    private static final byte JDBC_RESP_UPDATE_COUNT = 0x03;
    private static final byte JDBC_RESP_METADATA = 0x04;
    
    private final SeaweedFSConnectionInfo connectionInfo;
    private Socket socket;
    private DataInputStream inputStream;
    private DataOutputStream outputStream;
    private boolean closed = false;
    private boolean autoCommit = true;
    private String catalog = null;
    private int transactionIsolation = Connection.TRANSACTION_NONE;
    private boolean readOnly = true; // SeaweedFS is read-only
    
    public SeaweedFSConnection(SeaweedFSConnectionInfo connectionInfo) throws SQLException {
        this.connectionInfo = connectionInfo;
        connect();
    }
    
    private void connect() throws SQLException {
        try {
            logger.debug("Connecting to SeaweedFS at {}:{}", connectionInfo.getHost(), connectionInfo.getPort());
            
            // Create socket connection
            socket = new Socket();
            socket.connect(new java.net.InetSocketAddress(connectionInfo.getHost(), connectionInfo.getPort()), 
                          connectionInfo.getConnectTimeout());
            
            if (connectionInfo.getSocketTimeout() > 0) {
                socket.setSoTimeout(connectionInfo.getSocketTimeout());
            }
            
            // Create streams
            inputStream = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            outputStream = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
            
            // Send connection message
            sendMessage(JDBC_MSG_CONNECT, connectionInfo.getDatabase().getBytes());
            
            // Read response
            Response response = readResponse();
            if (response.type == JDBC_RESP_ERROR) {
                throw new SQLException("Failed to connect: " + new String(response.data));
            }
            
            logger.info("Successfully connected to SeaweedFS: {}", connectionInfo.getConnectionString());
            
        } catch (Exception e) {
            if (socket != null && !socket.isClosed()) {
                try {
                    socket.close();
                } catch (IOException ignored) {}
            }
            throw new SQLException("Failed to connect to SeaweedFS: " + e.getMessage(), e);
        }
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        return new SeaweedFSStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        return new SeaweedFSPreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Callable statements are not supported");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        checkClosed();
        return sql; // No translation needed
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
        if (this.autoCommit != autoCommit) {
            sendMessage(JDBC_MSG_SET_AUTOCOMMIT, new byte[]{(byte)(autoCommit ? 1 : 0)});
            Response response = readResponse();
            if (response.type == JDBC_RESP_ERROR) {
                throw new SQLException("Failed to set auto-commit: " + new String(response.data));
            }
            this.autoCommit = autoCommit;
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
        if (autoCommit) {
            throw new SQLException("Cannot commit when auto-commit is enabled");
        }
        sendMessage(JDBC_MSG_COMMIT, new byte[0]);
        Response response = readResponse();
        if (response.type == JDBC_RESP_ERROR) {
            throw new SQLException("Failed to commit: " + new String(response.data));
        }
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
        if (autoCommit) {
            throw new SQLException("Cannot rollback when auto-commit is enabled");
        }
        sendMessage(JDBC_MSG_ROLLBACK, new byte[0]);
        Response response = readResponse();
        if (response.type == JDBC_RESP_ERROR) {
            throw new SQLException("Failed to rollback: " + new String(response.data));
        }
    }

    @Override
    public void close() throws SQLException {
        if (!closed) {
            try {
                if (outputStream != null) {
                    sendMessage(JDBC_MSG_DISCONNECT, new byte[0]);
                    outputStream.close();
                }
                if (inputStream != null) {
                    inputStream.close();
                }
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                }
            } catch (Exception e) {
                logger.warn("Error closing connection: {}", e.getMessage());
            } finally {
                closed = true;
                logger.debug("Connection closed");
            }
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed || (socket != null && socket.isClosed());
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new SeaweedFSDatabaseMetaData(this);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkClosed();
        // SeaweedFS is always read-only, so we ignore attempts to change this
        this.readOnly = true;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkClosed();
        return readOnly;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkClosed();
        this.catalog = catalog;
    }

    @Override
    public String getCatalog() throws SQLException {
        checkClosed();
        return catalog != null ? catalog : connectionInfo.getDatabase();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
        this.transactionIsolation = level;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return transactionIsolation;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return null; // No warnings for now
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
        // No-op
    }

    // Methods not commonly used - basic implementations
    
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("Callable statements are not supported");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException("Type maps are not supported");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Type maps are not supported");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        // No-op
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Callable statements are not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob creation is not supported");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob creation is not supported");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob creation is not supported");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML creation is not supported");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return !closed && socket != null && !socket.isClosed();
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        // No-op
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        // No-op
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return new Properties();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array creation is not supported");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Struct creation is not supported");
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        // No-op
    }

    @Override
    public String getSchema() throws SQLException {
        return connectionInfo.getDatabase();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        try {
            if (socket != null) {
                socket.setSoTimeout(milliseconds);
            }
        } catch (Exception e) {
            throw new SQLException("Failed to set network timeout", e);
        }
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        try {
            return socket != null ? socket.getSoTimeout() : 0;
        } catch (Exception e) {
            throw new SQLException("Failed to get network timeout", e);
        }
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
    
    // Package-private methods for use by Statement and other classes
    
    void sendMessage(byte messageType, byte[] data) throws SQLException {
        try {
            synchronized (outputStream) {
                // Write header: message type (1 byte) + data length (4 bytes)
                outputStream.writeByte(messageType);
                outputStream.writeInt(data.length);
                
                // Write data
                if (data.length > 0) {
                    outputStream.write(data);
                }
                
                outputStream.flush();
            }
        } catch (IOException e) {
            throw new SQLException("Failed to send message to server", e);
        }
    }
    
    Response readResponse() throws SQLException {
        try {
            synchronized (inputStream) {
                // Read response type
                byte responseType = inputStream.readByte();
                
                // Read data length
                int dataLength = inputStream.readInt();
                
                // Read data
                byte[] data = new byte[dataLength];
                if (dataLength > 0) {
                    inputStream.readFully(data);
                }
                
                return new Response(responseType, data);
            }
        } catch (SocketTimeoutException e) {
            throw new SQLException("Read timeout from server", e);
        } catch (IOException e) {
            throw new SQLException("Failed to read response from server", e);
        }
    }
    
    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Connection is closed");
        }
    }
    
    SeaweedFSConnectionInfo getConnectionInfo() {
        return connectionInfo;
    }
    
    // Helper class for responses
    static class Response {
        final byte type;
        final byte[] data;
        
        Response(byte type, byte[] data) {
            this.type = type;
            this.data = data;
        }
    }
}
