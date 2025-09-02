package com.seaweedfs.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

/**
 * SeaweedFS JDBC Driver
 * 
 * Provides JDBC connectivity to SeaweedFS SQL engine for querying MQ topics.
 * 
 * JDBC URL format: jdbc:seaweedfs://host:port/database
 * 
 * Example usage:
 * <pre>
 * Class.forName("com.seaweedfs.jdbc.SeaweedFSDriver");
 * Connection conn = DriverManager.getConnection("jdbc:seaweedfs://localhost:8089/default");
 * Statement stmt = conn.createStatement();
 * ResultSet rs = stmt.executeQuery("SELECT * FROM my_topic LIMIT 10");
 * </pre>
 */
public class SeaweedFSDriver implements Driver {

    private static final Logger logger = LoggerFactory.getLogger(SeaweedFSDriver.class);
    
    // Driver information
    public static final String DRIVER_NAME = "SeaweedFS JDBC Driver";
    public static final String DRIVER_VERSION = "1.0.0";
    public static final int DRIVER_MAJOR_VERSION = 1;
    public static final int DRIVER_MINOR_VERSION = 0;
    
    // URL prefix for SeaweedFS JDBC connections
    public static final String URL_PREFIX = "jdbc:seaweedfs://";
    
    // Default connection properties
    public static final String PROP_HOST = "host";
    public static final String PROP_PORT = "port";
    public static final String PROP_DATABASE = "database";
    public static final String PROP_USER = "user";
    public static final String PROP_PASSWORD = "password";
    public static final String PROP_CONNECT_TIMEOUT = "connectTimeout";
    public static final String PROP_SOCKET_TIMEOUT = "socketTimeout";
    
    static {
        try {
            // Register the driver with the DriverManager
            DriverManager.registerDriver(new SeaweedFSDriver());
            logger.info("SeaweedFS JDBC Driver {} registered successfully", DRIVER_VERSION);
        } catch (SQLException e) {
            logger.error("Failed to register SeaweedFS JDBC Driver", e);
            throw new RuntimeException("Failed to register SeaweedFS JDBC Driver", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null; // Not our URL, let another driver handle it
        }
        
        logger.debug("Attempting to connect to: {}", url);
        
        try {
            // Parse the URL to extract connection parameters
            SeaweedFSConnectionInfo connectionInfo = parseURL(url, info);
            
            // Create and return the connection
            return new SeaweedFSConnection(connectionInfo);
            
        } catch (Exception e) {
            logger.error("Failed to connect to SeaweedFS: {}", e.getMessage(), e);
            throw new SQLException("Failed to connect to SeaweedFS: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[] {
            createPropertyInfo(PROP_HOST, "localhost", "SeaweedFS JDBC server hostname", null, false),
            createPropertyInfo(PROP_PORT, "8089", "SeaweedFS JDBC server port", null, false),
            createPropertyInfo(PROP_DATABASE, "default", "Database/namespace name", null, false),
            createPropertyInfo(PROP_USER, "", "Username (optional)", null, false),
            createPropertyInfo(PROP_PASSWORD, "", "Password (optional)", null, false),
            createPropertyInfo(PROP_CONNECT_TIMEOUT, "30000", "Connection timeout in milliseconds", null, false),
            createPropertyInfo(PROP_SOCKET_TIMEOUT, "0", "Socket timeout in milliseconds (0 = infinite)", null, false)
        };
    }
    
    private DriverPropertyInfo createPropertyInfo(String name, String defaultValue, String description, String[] choices, boolean required) {
        DriverPropertyInfo info = new DriverPropertyInfo(name, defaultValue);
        info.description = description;
        info.choices = choices;
        info.required = required;
        return info;
    }

    @Override
    public int getMajorVersion() {
        return DRIVER_MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return DRIVER_MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        // We implement a subset of JDBC, so we're not fully compliant
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("getParentLogger is not supported");
    }
    
    /**
     * Parse JDBC URL and extract connection information
     * 
     * Expected format: jdbc:seaweedfs://host:port/database[?property=value&...]
     */
    private SeaweedFSConnectionInfo parseURL(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            throw new SQLException("Invalid SeaweedFS JDBC URL: " + url);
        }
        
        try {
            // Remove the jdbc:seaweedfs:// prefix
            String remaining = url.substring(URL_PREFIX.length());
            
            // Split into host:port/database and query parameters
            String[] parts = remaining.split("\\?", 2);
            String hostPortDb = parts[0];
            String queryParams = parts.length > 1 ? parts[1] : "";
            
            // Parse host, port, and database
            String host = "localhost";
            int port = 8089;
            String database = "default";
            
            if (hostPortDb.contains("/")) {
                String[] hostPortDbParts = hostPortDb.split("/", 2);
                String hostPort = hostPortDbParts[0];
                database = hostPortDbParts[1];
                
                if (hostPort.contains(":")) {
                    String[] hostPortParts = hostPort.split(":", 2);
                    host = hostPortParts[0];
                    port = Integer.parseInt(hostPortParts[1]);
                } else {
                    host = hostPort;
                }
            } else if (hostPortDb.contains(":")) {
                String[] hostPortParts = hostPortDb.split(":", 2);
                host = hostPortParts[0];
                port = Integer.parseInt(hostPortParts[1]);
            } else if (!hostPortDb.isEmpty()) {
                host = hostPortDb;
            }
            
            // Create properties with defaults
            Properties connectionProps = new Properties();
            connectionProps.setProperty(PROP_HOST, host);
            connectionProps.setProperty(PROP_PORT, String.valueOf(port));
            connectionProps.setProperty(PROP_DATABASE, database);
            connectionProps.setProperty(PROP_USER, "");
            connectionProps.setProperty(PROP_PASSWORD, "");
            connectionProps.setProperty(PROP_CONNECT_TIMEOUT, "30000");
            connectionProps.setProperty(PROP_SOCKET_TIMEOUT, "0");
            
            // Override with provided properties
            if (info != null) {
                connectionProps.putAll(info);
            }
            
            // Parse query parameters
            if (!queryParams.isEmpty()) {
                for (String param : queryParams.split("&")) {
                    String[] keyValue = param.split("=", 2);
                    if (keyValue.length == 2) {
                        connectionProps.setProperty(keyValue[0], keyValue[1]);
                    }
                }
            }
            
            return new SeaweedFSConnectionInfo(connectionProps);
            
        } catch (Exception e) {
            throw new SQLException("Failed to parse SeaweedFS JDBC URL: " + url, e);
        }
    }
    
    /**
     * Get driver information string
     */
    public static String getDriverInfo() {
        return String.format("%s v%s", DRIVER_NAME, DRIVER_VERSION);
    }
}
