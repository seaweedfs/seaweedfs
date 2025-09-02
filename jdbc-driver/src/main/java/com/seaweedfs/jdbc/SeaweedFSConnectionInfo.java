package com.seaweedfs.jdbc;

import java.util.Properties;

/**
 * Connection information holder for SeaweedFS JDBC connections
 */
public class SeaweedFSConnectionInfo {
    
    private final String host;
    private final int port;
    private final String database;
    private final String user;
    private final String password;
    private final int connectTimeout;
    private final int socketTimeout;
    private final Properties properties;
    
    public SeaweedFSConnectionInfo(Properties props) {
        this.properties = new Properties(props);
        this.host = props.getProperty(SeaweedFSDriver.PROP_HOST, "localhost");
        this.port = Integer.parseInt(props.getProperty(SeaweedFSDriver.PROP_PORT, "8089"));
        this.database = props.getProperty(SeaweedFSDriver.PROP_DATABASE, "default");
        this.user = props.getProperty(SeaweedFSDriver.PROP_USER, "");
        this.password = props.getProperty(SeaweedFSDriver.PROP_PASSWORD, "");
        this.connectTimeout = Integer.parseInt(props.getProperty(SeaweedFSDriver.PROP_CONNECT_TIMEOUT, "30000"));
        this.socketTimeout = Integer.parseInt(props.getProperty(SeaweedFSDriver.PROP_SOCKET_TIMEOUT, "0"));
    }
    
    public String getHost() {
        return host;
    }
    
    public int getPort() {
        return port;
    }
    
    public String getDatabase() {
        return database;
    }
    
    public String getUser() {
        return user;
    }
    
    public String getPassword() {
        return password;
    }
    
    public int getConnectTimeout() {
        return connectTimeout;
    }
    
    public int getSocketTimeout() {
        return socketTimeout;
    }
    
    public Properties getProperties() {
        return new Properties(properties);
    }
    
    public String getConnectionString() {
        return String.format("jdbc:seaweedfs://%s:%d/%s", host, port, database);
    }
    
    @Override
    public String toString() {
        return String.format("SeaweedFSConnectionInfo{host='%s', port=%d, database='%s', user='%s', connectTimeout=%d, socketTimeout=%d}",
                host, port, database, user, connectTimeout, socketTimeout);
    }
}
