package com.seaweedfs.jdbc;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Basic tests for SeaweedFS JDBC driver
 */
public class SeaweedFSDriverTest {

    @Test
    public void testDriverRegistration() {
        // Driver should be automatically registered via META-INF/services
        assertDoesNotThrow(() -> {
            Class.forName("com.seaweedfs.jdbc.SeaweedFSDriver");
        });
    }

    @Test
    public void testURLAcceptance() throws SQLException {
        SeaweedFSDriver driver = new SeaweedFSDriver();
        
        // Valid URLs
        assertTrue(driver.acceptsURL("jdbc:seaweedfs://localhost:8089/default"));
        assertTrue(driver.acceptsURL("jdbc:seaweedfs://server:9000/test"));
        assertTrue(driver.acceptsURL("jdbc:seaweedfs://192.168.1.100:8089/mydb"));
        
        // Invalid URLs
        assertFalse(driver.acceptsURL("jdbc:mysql://localhost:3306/test"));
        assertFalse(driver.acceptsURL("jdbc:postgresql://localhost:5432/test"));
        assertFalse(driver.acceptsURL(null));
        assertFalse(driver.acceptsURL(""));
        assertFalse(driver.acceptsURL("not-a-url"));
    }

    @Test
    public void testDriverInfo() {
        SeaweedFSDriver driver = new SeaweedFSDriver();
        
        assertEquals(SeaweedFSDriver.DRIVER_MAJOR_VERSION, driver.getMajorVersion());
        assertEquals(SeaweedFSDriver.DRIVER_MINOR_VERSION, driver.getMinorVersion());
        assertFalse(driver.jdbcCompliant()); // We're not fully JDBC compliant
        
        assertNotNull(SeaweedFSDriver.getDriverInfo());
        assertTrue(SeaweedFSDriver.getDriverInfo().contains("SeaweedFS"));
        assertTrue(SeaweedFSDriver.getDriverInfo().contains("JDBC"));
    }

    @Test
    public void testPropertyInfo() throws SQLException {
        SeaweedFSDriver driver = new SeaweedFSDriver();
        
        var properties = driver.getPropertyInfo("jdbc:seaweedfs://localhost:8089/default", null);
        assertNotNull(properties);
        assertTrue(properties.length > 0);
        
        // Check that basic properties are present
        boolean foundHost = false, foundPort = false, foundDatabase = false;
        for (var prop : properties) {
            if ("host".equals(prop.name)) foundHost = true;
            if ("port".equals(prop.name)) foundPort = true;
            if ("database".equals(prop.name)) foundDatabase = true;
        }
        
        assertTrue(foundHost, "Host property should be present");
        assertTrue(foundPort, "Port property should be present");
        assertTrue(foundDatabase, "Database property should be present");
    }

    // Note: Connection tests would require a running SeaweedFS JDBC server
    // These tests would be part of integration tests, not unit tests
}
