package seaweed.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Test Spark DataFrame.write() with LOCAL filesystem to see if the issue is SeaweedFS-specific.
 * This is the CRITICAL test to determine if the 78-byte error occurs with local files.
 */
public class SparkLocalFileSystemTest extends SparkTestBase {

    private String localTestDir;

    @Before
    public void setUp() throws Exception {
        super.setUpSpark();
        localTestDir = "/tmp/spark-local-test-" + System.currentTimeMillis();
        new File(localTestDir).mkdirs();
        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  CRITICAL TEST: Spark DataFrame.write() to LOCAL filesystem  ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println("Local test directory: " + localTestDir);
    }

    @After
    public void tearDown() throws Exception {
        // Clean up
        if (localTestDir != null) {
            deleteDirectory(new File(localTestDir));
        }
        super.tearDownSpark();
    }

    @Test
    public void testSparkWriteToLocalFilesystem() {
        System.out.println("\n=== TEST: Write Parquet to Local Filesystem ===");

        // Create test data (same as SparkSQLTest)
        List<Employee> employees = Arrays.asList(
                new Employee(1, "Alice", "Engineering", 100000),
                new Employee(2, "Bob", "Sales", 80000),
                new Employee(3, "Charlie", "Engineering", 120000),
                new Employee(4, "David", "Sales", 75000));

        Dataset<Row> df = spark.createDataFrame(employees, Employee.class);

        // Write to LOCAL filesystem using file:// protocol
        String localPath = "file://" + localTestDir + "/employees";
        System.out.println("Writing to: " + localPath);
        
        try {
            df.write().mode(SaveMode.Overwrite).parquet(localPath);
            System.out.println("✅ Write completed successfully!");
        } catch (Exception e) {
            System.err.println("❌ Write FAILED: " + e.getMessage());
            e.printStackTrace();
            fail("Write to local filesystem failed: " + e.getMessage());
        }

        // Now try to READ back
        System.out.println("\n=== TEST: Read Parquet from Local Filesystem ===");
        System.out.println("Reading from: " + localPath);
        
        try {
            Dataset<Row> employeesDf = spark.read().parquet(localPath);
            employeesDf.createOrReplaceTempView("employees");

            // Run SQL query
            Dataset<Row> engineeringEmployees = spark.sql(
                    "SELECT name, salary FROM employees WHERE department = 'Engineering'");

            long count = engineeringEmployees.count();
            System.out.println("✅ Read completed successfully! Found " + count + " engineering employees");
            
            assertEquals("Should find 2 engineering employees", 2, count);
            
            System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
            System.out.println("║  ✅ SUCCESS! Local filesystem works perfectly!                ║");
            System.out.println("║  This proves the issue is SeaweedFS-specific!                ║");
            System.out.println("╚══════════════════════════════════════════════════════════════╝");
            
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("EOFException") && e.getMessage().contains("78 bytes")) {
                System.err.println("\n╔══════════════════════════════════════════════════════════════╗");
                System.err.println("║  ❌ CRITICAL: 78-byte error ALSO occurs with local files!    ║");
                System.err.println("║  This proves the issue is NOT SeaweedFS-specific!            ║");
                System.err.println("║  The issue is in Spark itself or our test setup!             ║");
                System.err.println("╚══════════════════════════════════════════════════════════════╝");
            }
            System.err.println("❌ Read FAILED: " + e.getMessage());
            e.printStackTrace();
            fail("Read from local filesystem failed: " + e.getMessage());
        }
    }

    @Test
    public void testSparkWriteReadMultipleTimes() {
        System.out.println("\n=== TEST: Multiple Write/Read Cycles ===");

        for (int i = 1; i <= 3; i++) {
            System.out.println("\n--- Cycle " + i + " ---");
            
            List<Employee> employees = Arrays.asList(
                    new Employee(i * 10 + 1, "Person" + (i * 10 + 1), "Dept" + i, 50000 + i * 10000),
                    new Employee(i * 10 + 2, "Person" + (i * 10 + 2), "Dept" + i, 60000 + i * 10000));

            Dataset<Row> df = spark.createDataFrame(employees, Employee.class);
            String localPath = "file://" + localTestDir + "/cycle" + i;

            // Write
            df.write().mode(SaveMode.Overwrite).parquet(localPath);
            System.out.println("✅ Cycle " + i + " write completed");

            // Read back immediately
            Dataset<Row> readDf = spark.read().parquet(localPath);
            long count = readDf.count();
            System.out.println("✅ Cycle " + i + " read completed: " + count + " rows");
            
            assertEquals("Should have 2 rows", 2, count);
        }

        System.out.println("\n✅ All cycles completed successfully!");
    }

    private void deleteDirectory(File directory) {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            directory.delete();
        }
    }

    // Employee class for testing
    public static class Employee implements java.io.Serializable {
        private int id;
        private String name;
        private String department;
        private int salary;

        public Employee() {}

        public Employee(int id, String name, String department, int salary) {
            this.id = id;
            this.name = name;
            this.department = department;
            this.salary = salary;
        }

        public int getId() { return id; }
        public void setId(int id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getDepartment() { return department; }
        public void setDepartment(String department) { this.department = department; }
        public int getSalary() { return salary; }
        public void setSalary(int salary) { this.salary = salary; }
    }
}

