package seaweed.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test reading LOCAL_ONLY files directly via file:// protocol
 * to verify the files themselves are valid.
 */
public class DirectFileReadTest extends SparkTestBase {

    @Test
    public void testReadLocalOnlyFileDirectly() {
        skipIfTestsDisabled();

        // First write using LOCAL_ONLY mode (through SeaweedFS path)
        java.util.List<SparkSQLTest.Employee> employees = java.util.Arrays.asList(
                new SparkSQLTest.Employee(1, "Alice", "Engineering", 100000),
                new SparkSQLTest.Employee(2, "Bob", "Sales", 80000),
                new SparkSQLTest.Employee(3, "Charlie", "Engineering", 120000),
                new SparkSQLTest.Employee(4, "David", "Sales", 75000));

        Dataset<Row> df = spark.createDataFrame(employees, SparkSQLTest.Employee.class);

        String tablePath = getTestPath("employees_direct_test");
        df.write().mode(org.apache.spark.sql.SaveMode.Overwrite).parquet(tablePath);

        System.out.println("✅ Write completed to: " + tablePath);

        // Now try to read the LOCAL_ONLY .debug file directly using file:// protocol
        // This bypasses LocalOnlyInputStream and uses native file system
        String debugFilePath = "file:///workspace/target/debug-local/";
        
        try {
            // List files in debug directory
            java.io.File debugDir = new java.io.File("/workspace/target/debug-local/");
            java.io.File[] files = debugDir.listFiles((dir, name) -> name.endsWith(".parquet.debug"));
            
            if (files != null && files.length > 0) {
                String localFile = "file://" + files[0].getAbsolutePath();
                System.out.println("📁 Found LOCAL_ONLY file: " + localFile);
                System.out.println("📏 File size: " + files[0].length() + " bytes");
                
                // Try to read it directly
                Dataset<Row> directRead = spark.read().parquet(localFile);
                long count = directRead.count();
                System.out.println("✅ Direct read successful! Row count: " + count);
                
                // Try SQL query on it
                directRead.createOrReplaceTempView("employees_direct");
                Dataset<Row> filtered = spark.sql(
                    "SELECT name, salary FROM employees_direct WHERE department = 'Engineering'");
                long engineeringCount = filtered.count();
                System.out.println("✅ SQL query successful! Engineering employees: " + engineeringCount);
                
                assertEquals("Should have 2 engineering employees", 2, engineeringCount);
                
            } else {
                fail("No .debug files found in /workspace/target/debug-local/");
            }
            
        } catch (Exception e) {
            System.err.println("❌ Direct read failed: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Direct file read failed", e);
        }
    }
}

