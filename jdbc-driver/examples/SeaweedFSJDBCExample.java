package com.seaweedfs.jdbc.examples;

import java.sql.*;
import java.util.Properties;

/**
 * Complete example demonstrating SeaweedFS JDBC driver usage
 */
public class SeaweedFSJDBCExample {
    
    public static void main(String[] args) {
        // JDBC URL for SeaweedFS
        String url = "jdbc:seaweedfs://localhost:8089/default";
        
        try {
            // 1. Load the driver (optional - auto-registration via META-INF/services)
            Class.forName("com.seaweedfs.jdbc.SeaweedFSDriver");
            System.out.println("✓ SeaweedFS JDBC Driver loaded successfully");
            
            // 2. Connect to SeaweedFS
            System.out.println("\n📡 Connecting to SeaweedFS...");
            Connection conn = DriverManager.getConnection(url);
            System.out.println("✓ Connected to: " + url);
            
            // 3. Get database metadata
            DatabaseMetaData dbMeta = conn.getMetaData();
            System.out.println("\n📊 Database Information:");
            System.out.println("  Database: " + dbMeta.getDatabaseProductName());
            System.out.println("  Version: " + dbMeta.getDatabaseProductVersion());
            System.out.println("  Driver: " + dbMeta.getDriverName() + " v" + dbMeta.getDriverVersion());
            System.out.println("  JDBC Version: " + dbMeta.getJDBCMajorVersion() + "." + dbMeta.getJDBCMinorVersion());
            System.out.println("  Read-only: " + dbMeta.isReadOnly());
            
            // 4. List available databases/schemas
            System.out.println("\n🗄️  Available Databases:");
            ResultSet catalogs = dbMeta.getCatalogs();
            while (catalogs.next()) {
                System.out.println("  • " + catalogs.getString("TABLE_CAT"));
            }
            catalogs.close();
            
            // 5. Execute basic queries
            System.out.println("\n🔍 Executing SQL Queries:");
            
            Statement stmt = conn.createStatement();
            
            // Show databases
            System.out.println("\n  📋 SHOW DATABASES:");
            ResultSet rs = stmt.executeQuery("SHOW DATABASES");
            while (rs.next()) {
                System.out.println("    " + rs.getString(1));
            }
            rs.close();
            
            // Show tables (topics)
            System.out.println("\n  📋 SHOW TABLES:");
            rs = stmt.executeQuery("SHOW TABLES");
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            
            // Print headers
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(String.format("%-20s", rsmd.getColumnName(i)));
            }
            System.out.println();
            System.out.println("-".repeat(20 * columnCount));
            
            // Print rows
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(String.format("%-20s", rs.getString(i)));
                }
                System.out.println();
            }
            rs.close();
            
            // 6. Query a specific topic (if exists)
            String topicQuery = "SELECT * FROM test_topic LIMIT 5";
            System.out.println("\n  📋 " + topicQuery + ":");
            
            try {
                rs = stmt.executeQuery(topicQuery);
                rsmd = rs.getMetaData();
                columnCount = rsmd.getColumnCount();
                
                // Print column headers
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(String.format("%-15s", rsmd.getColumnName(i)));
                }
                System.out.println();
                System.out.println("-".repeat(15 * columnCount));
                
                // Print data rows
                int rowCount = 0;
                while (rs.next() && rowCount < 5) {
                    for (int i = 1; i <= columnCount; i++) {
                        String value = rs.getString(i);
                        if (value != null && value.length() > 12) {
                            value = value.substring(0, 12) + "...";
                        }
                        System.out.print(String.format("%-15s", value != null ? value : "NULL"));
                    }
                    System.out.println();
                    rowCount++;
                }
                
                if (rowCount == 0) {
                    System.out.println("    (No data found)");
                }
                
                rs.close();
            } catch (SQLException e) {
                System.out.println("    ⚠️  Topic 'test_topic' not found: " + e.getMessage());
            }
            
            // 7. Demonstrate aggregation queries
            System.out.println("\n  🧮 Aggregation Example:");
            try {
                rs = stmt.executeQuery("SELECT COUNT(*) as total_records FROM test_topic");
                if (rs.next()) {
                    System.out.println("    Total records: " + rs.getLong("total_records"));
                }
                rs.close();
            } catch (SQLException e) {
                System.out.println("    ⚠️  Aggregation example skipped: " + e.getMessage());
            }
            
            // 8. Demonstrate PreparedStatement
            System.out.println("\n  📝 PreparedStatement Example:");
            String preparedQuery = "SELECT * FROM test_topic WHERE id > ? LIMIT ?";
            
            try {
                PreparedStatement pstmt = conn.prepareStatement(preparedQuery);
                pstmt.setLong(1, 100);
                pstmt.setInt(2, 3);
                
                System.out.println("    Query: " + preparedQuery);
                System.out.println("    Parameters: id > 100, LIMIT 3");
                
                rs = pstmt.executeQuery();
                rsmd = rs.getMetaData();
                columnCount = rsmd.getColumnCount();
                
                int count = 0;
                while (rs.next()) {
                    if (count == 0) {
                        // Print headers for first row
                        for (int i = 1; i <= columnCount; i++) {
                            System.out.print(String.format("%-15s", rsmd.getColumnName(i)));
                        }
                        System.out.println();
                        System.out.println("-".repeat(15 * columnCount));
                    }
                    
                    for (int i = 1; i <= columnCount; i++) {
                        String value = rs.getString(i);
                        if (value != null && value.length() > 12) {
                            value = value.substring(0, 12) + "...";
                        }
                        System.out.print(String.format("%-15s", value != null ? value : "NULL"));
                    }
                    System.out.println();
                    count++;
                }
                
                if (count == 0) {
                    System.out.println("    (No records match criteria)");
                }
                
                rs.close();
                pstmt.close();
            } catch (SQLException e) {
                System.out.println("    ⚠️  PreparedStatement example skipped: " + e.getMessage());
            }
            
            // 9. System columns example
            System.out.println("\n  🔧 System Columns Example:");
            try {
                rs = stmt.executeQuery("SELECT id, _timestamp_ns, _key, _source FROM test_topic LIMIT 3");
                rsmd = rs.getMetaData();
                columnCount = rsmd.getColumnCount();
                
                // Print headers
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(String.format("%-20s", rsmd.getColumnName(i)));
                }
                System.out.println();
                System.out.println("-".repeat(20 * columnCount));
                
                int count = 0;
                while (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        String value = rs.getString(i);
                        if (value != null && value.length() > 17) {
                            value = value.substring(0, 17) + "...";
                        }
                        System.out.print(String.format("%-20s", value != null ? value : "NULL"));
                    }
                    System.out.println();
                    count++;
                }
                
                if (count == 0) {
                    System.out.println("    (No data available for system columns demo)");
                }
                
                rs.close();
            } catch (SQLException e) {
                System.out.println("    ⚠️  System columns example skipped: " + e.getMessage());
            }
            
            // 10. Connection properties example
            System.out.println("\n⚙️  Connection Properties:");
            System.out.println("  Auto-commit: " + conn.getAutoCommit());
            System.out.println("  Read-only: " + conn.isReadOnly());
            System.out.println("  Transaction isolation: " + conn.getTransactionIsolation());
            System.out.println("  Catalog: " + conn.getCatalog());
            
            // 11. Clean up
            stmt.close();
            conn.close();
            
            System.out.println("\n✅ SeaweedFS JDBC Example completed successfully!");
            System.out.println("\n💡 Next Steps:");
            System.out.println("  • Try connecting with DBeaver or other JDBC tools");
            System.out.println("  • Use in your Java applications with connection pooling");
            System.out.println("  • Integrate with BI tools like Tableau or Power BI");
            System.out.println("  • Build data pipelines using SeaweedFS as a data source");
            
        } catch (ClassNotFoundException e) {
            System.err.println("❌ SeaweedFS JDBC Driver not found: " + e.getMessage());
            System.err.println("   Make sure seaweedfs-jdbc.jar is in your classpath");
        } catch (SQLException e) {
            System.err.println("❌ Database error: " + e.getMessage());
            System.err.println("   Make sure SeaweedFS JDBC server is running:");
            System.err.println("   weed jdbc -port=8089 -master=localhost:9333");
        } catch (Exception e) {
            System.err.println("❌ Unexpected error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Example with connection pooling using HikariCP
     */
    public static void connectionPoolingExample() {
        try {
            // This would require HikariCP dependency
            /*
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl("jdbc:seaweedfs://localhost:8089/default");
            config.setMaximumPoolSize(10);
            config.setMinimumIdle(2);
            config.setConnectionTimeout(30000);
            config.setIdleTimeout(600000);
            
            HikariDataSource dataSource = new HikariDataSource(config);
            
            try (Connection conn = dataSource.getConnection()) {
                // Use connection from pool
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM my_topic");
                if (rs.next()) {
                    System.out.println("Record count: " + rs.getLong(1));
                }
                rs.close();
                stmt.close();
            }
            
            dataSource.close();
            */
            
            System.out.println("Connection pooling example (commented out - requires HikariCP dependency)");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Example configuration for different database tools
     */
    public static void printToolConfiguration() {
        System.out.println("\n🛠️  Database Tool Configuration:");
        
        System.out.println("\n📊 DBeaver:");
        System.out.println("  1. New Connection → Generic JDBC");
        System.out.println("  2. URL: jdbc:seaweedfs://localhost:8089/default");
        System.out.println("  3. Driver Class: com.seaweedfs.jdbc.SeaweedFSDriver");
        System.out.println("  4. Add seaweedfs-jdbc.jar to Libraries");
        
        System.out.println("\n💻 IntelliJ DataGrip:");
        System.out.println("  1. New Data Source → Generic");
        System.out.println("  2. URL: jdbc:seaweedfs://localhost:8089/default");
        System.out.println("  3. Add Driver: seaweedfs-jdbc.jar");
        System.out.println("  4. Class: com.seaweedfs.jdbc.SeaweedFSDriver");
        
        System.out.println("\n📈 Tableau:");
        System.out.println("  1. Connect to Data → More... → Generic JDBC");
        System.out.println("  2. URL: jdbc:seaweedfs://localhost:8089/default");
        System.out.println("  3. Driver Path: /path/to/seaweedfs-jdbc.jar");
        System.out.println("  4. Class Name: com.seaweedfs.jdbc.SeaweedFSDriver");
        
        System.out.println("\n☕ Java Application:");
        System.out.println("  Class.forName(\"com.seaweedfs.jdbc.SeaweedFSDriver\");");
        System.out.println("  Connection conn = DriverManager.getConnection(");
        System.out.println("      \"jdbc:seaweedfs://localhost:8089/default\");");
    }
}
