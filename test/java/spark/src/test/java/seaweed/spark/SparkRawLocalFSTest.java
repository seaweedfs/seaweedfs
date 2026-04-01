package seaweed.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test Spark with Hadoop's RawLocalFileSystem to see if 78-byte error can be reproduced.
 * This uses the EXACT same implementation as native local files.
 */
public class SparkRawLocalFSTest extends SparkTestBase {

    private Path testPath;
    private FileSystem rawLocalFs;

    @Before
    public void setUp() throws IOException {
        if (!TESTS_ENABLED) {
            return;
        }
        super.setUpSpark();
        
        // Use RawLocalFileSystem explicitly
        Configuration conf = new Configuration();
        rawLocalFs = new RawLocalFileSystem();
        rawLocalFs.initialize(java.net.URI.create("file:///"), conf);
        
        testPath = new Path("/tmp/spark-rawlocal-test-" + System.currentTimeMillis());
        rawLocalFs.delete(testPath, true);
        rawLocalFs.mkdirs(testPath);
        
        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  CRITICAL TEST: Spark with RawLocalFileSystem               ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println("Test directory: " + testPath);
    }

    @After
    public void tearDown() throws IOException {
        if (!TESTS_ENABLED) {
            return;
        }
        if (rawLocalFs != null) {
            rawLocalFs.delete(testPath, true);
            rawLocalFs.close();
        }
        super.tearDownSpark();
    }

    @Test
    public void testSparkWithRawLocalFileSystem() throws IOException {
        skipIfTestsDisabled();

        System.out.println("\n=== TEST: Write Parquet using RawLocalFileSystem ===");

        // Create test data (same as SparkSQLTest)
        List<Employee> employees = Arrays.asList(
                new Employee(1, "Alice", "Engineering", 100000),
                new Employee(2, "Bob", "Sales", 80000),
                new Employee(3, "Charlie", "Engineering", 120000),
                new Employee(4, "David", "Sales", 75000));

        Dataset<Row> df = spark.createDataFrame(employees, Employee.class);

        // CRITICAL: Use file:// prefix to force local filesystem
        String outputPath = "file://" + testPath.toString() + "/employees";
        System.out.println("Writing to: " + outputPath);

        // Write using Spark (will use file:// scheme, which uses RawLocalFileSystem)
        df.write().mode(SaveMode.Overwrite).parquet(outputPath);

        System.out.println("✅ Write completed successfully!");

        // Verify by reading back
        System.out.println("\n=== TEST: Read Parquet using RawLocalFileSystem ===");
        System.out.println("Reading from: " + outputPath);
        Dataset<Row> employeesDf = spark.read().parquet(outputPath);
        employeesDf.createOrReplaceTempView("employees");

        // Run SQL queries
        Dataset<Row> engineeringEmployees = spark.sql(
                "SELECT name, salary FROM employees WHERE department = 'Engineering'");

        long count = engineeringEmployees.count();
        assertEquals(2, count);
        System.out.println("✅ Read completed successfully! Found " + count + " engineering employees");

        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  ✅ SUCCESS! RawLocalFileSystem works perfectly!              ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
    }

    // Employee class for Spark DataFrame
    public static class Employee implements java.io.Serializable {
        private int id;
        private String name;
        private String department;
        private int salary;

        public Employee() {} // Required for Spark

        public Employee(int id, String name, String department, int salary) {
            this.id = id;
            this.name = name;
            this.department = department;
            this.salary = salary;
        }

        // Getters and Setters (required for Spark)
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
