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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * CRITICAL DIAGNOSTIC TEST: Compare the exact sequence of FileSystem operations
 * between RawLocalFS (works) and LOCAL_ONLY (fails) during SQL query execution.
 * 
 * This test will help us understand what's different about how Spark SQL
 * interacts with SeaweedFileSystem vs RawLocalFileSystem.
 */
public class SparkSQLReadDifferenceTest extends SparkTestBase {

    private String rawLocalDir;
    private String localOnlyDir;
    private FileSystem rawLocalFs;

    @Before
    public void setUp() throws Exception {
        // Enable detailed logging
        System.setProperty("seaweedfs.detailed.logging", "true");
        super.setUpSpark();
        
        // Set up RawLocalFileSystem directory
        rawLocalDir = "/tmp/spark-sql-diff-rawlocal-" + System.currentTimeMillis();
        new File(rawLocalDir).mkdirs();
        
        Configuration conf = spark.sparkContext().hadoopConfiguration();
        rawLocalFs = new RawLocalFileSystem();
        rawLocalFs.initialize(new URI("file:///"), conf);
        rawLocalFs.delete(new Path(rawLocalDir), true);
        rawLocalFs.mkdirs(new Path(rawLocalDir));
        
        // Set up LOCAL_ONLY directory
        localOnlyDir = "/workspace/target/debug-sql-diff";
        new File(localOnlyDir).mkdirs();
        for (File f : new File(localOnlyDir).listFiles()) {
            f.delete();
        }
        
        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  SQL READ DIFFERENCE TEST: RawLocalFS vs LOCAL_ONLY         ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
    }

    @After
    public void tearDown() throws Exception {
        if (rawLocalFs != null) {
            rawLocalFs.delete(new Path(rawLocalDir), true);
            rawLocalFs.close();
        }
        super.tearDownSpark();
    }

    @Test
    public void testSQLReadDifference() throws IOException {
        // Create test data
        List<Employee> employees = Arrays.asList(
                new Employee(1, "Alice", "Engineering", 100000),
                new Employee(2, "Bob", "Sales", 80000),
                new Employee(3, "Charlie", "Engineering", 120000),
                new Employee(4, "David", "Sales", 75000));

        Dataset<Row> df = spark.createDataFrame(employees, Employee.class);

        // ========================================================================
        // PART 1: RawLocalFS - SQL Query (WORKS)
        // ========================================================================
        System.out.println("\n" + "=".repeat(70));
        System.out.println("PART 1: RawLocalFS - SQL Query (Expected to WORK)");
        System.out.println("=".repeat(70));
        
        String rawLocalPath = "file://" + rawLocalDir + "/employees";
        System.out.println("Writing to: " + rawLocalPath);
        df.write().mode(SaveMode.Overwrite).parquet(rawLocalPath);
        System.out.println("✅ Write completed\n");

        System.out.println("--- Executing SQL Query on RawLocalFS ---");
        try {
            Dataset<Row> rawDf = spark.read().parquet(rawLocalPath);
            System.out.println("✅ Initial read successful");
            
            rawDf.createOrReplaceTempView("employees_raw");
            System.out.println("✅ Temp view created");
            
            System.out.println("\nExecuting: SELECT name, salary FROM employees_raw WHERE department = 'Engineering'");
            Dataset<Row> rawResult = spark.sql("SELECT name, salary FROM employees_raw WHERE department = 'Engineering'");
            
            System.out.println("Triggering execution with count()...");
            long rawCount = rawResult.count();
            
            System.out.println("✅ RawLocalFS SQL query SUCCESSFUL! Row count: " + rawCount);
            assertEquals("Should have 2 engineering employees", 2, rawCount);
            
            System.out.println("\n✅✅✅ RawLocalFS: ALL OPERATIONS SUCCESSFUL ✅✅✅\n");
        } catch (Exception e) {
            System.err.println("❌ RawLocalFS SQL query FAILED (unexpected!): " + e.getMessage());
            e.printStackTrace();
            fail("RawLocalFS should not fail!");
        }

        // ========================================================================
        // PART 2: LOCAL_ONLY - SQL Query (FAILS)
        // ========================================================================
        System.out.println("\n" + "=".repeat(70));
        System.out.println("PART 2: LOCAL_ONLY - SQL Query (Expected to FAIL with 78-byte error)");
        System.out.println("=".repeat(70));
        
        // Enable LOCAL_ONLY mode
        System.setProperty("SEAWEEDFS_DEBUG_MODE", "LOCAL_ONLY");
        spark.sparkContext().hadoopConfiguration().set("fs.seaweedfs.debug.dir", localOnlyDir);
        
        String localOnlyPath = getTestPath("employees_localonly");
        System.out.println("Writing to: " + localOnlyPath);
        df.write().mode(SaveMode.Overwrite).parquet(localOnlyPath);
        System.out.println("✅ Write completed\n");

        System.out.println("--- Executing SQL Query on LOCAL_ONLY ---");
        try {
            Dataset<Row> localDf = spark.read().parquet(localOnlyPath);
            System.out.println("✅ Initial read successful");
            
            localDf.createOrReplaceTempView("employees_local");
            System.out.println("✅ Temp view created");
            
            System.out.println("\nExecuting: SELECT name, salary FROM employees_local WHERE department = 'Engineering'");
            Dataset<Row> localResult = spark.sql("SELECT name, salary FROM employees_local WHERE department = 'Engineering'");
            
            System.out.println("Triggering execution with count()...");
            long localCount = localResult.count();
            
            System.out.println("✅ LOCAL_ONLY SQL query SUCCESSFUL! Row count: " + localCount);
            assertEquals("Should have 2 engineering employees", 2, localCount);
            
            System.out.println("\n✅✅✅ LOCAL_ONLY: ALL OPERATIONS SUCCESSFUL ✅✅✅\n");
        } catch (Exception e) {
            System.err.println("\n❌❌❌ LOCAL_ONLY SQL query FAILED ❌❌❌");
            System.err.println("Error: " + e.getMessage());
            
            if (e.getMessage() != null && e.getMessage().contains("78 bytes")) {
                System.err.println("\n🔍 CONFIRMED: 78-byte EOF error!");
                System.err.println("This error occurs during SQL query execution on LOCAL_ONLY mode.");
            }
            
            System.err.println("\nFull stack trace:");
            e.printStackTrace();
            
            System.err.println("\n" + "=".repeat(70));
            System.err.println("ANALYSIS: Comparing RawLocalFS (works) vs LOCAL_ONLY (fails)");
            System.err.println("=".repeat(70));
            System.err.println();
            System.err.println("Both tests:");
            System.err.println("  - Write identical data (same DataFrame)");
            System.err.println("  - Execute identical SQL query");
            System.err.println("  - Use identical Spark configuration");
            System.err.println();
            System.err.println("Key differences:");
            System.err.println("  1. Path scheme:");
            System.err.println("     - RawLocalFS: file:///tmp/...");
            System.err.println("     - LOCAL_ONLY: seaweedfs://seaweedfs-filer:8888/...");
            System.err.println();
            System.err.println("  2. FileSystem implementation:");
            System.err.println("     - RawLocalFS: Hadoop's native RawLocalFileSystem");
            System.err.println("     - LOCAL_ONLY: SeaweedFileSystem (but writes to local disk)");
            System.err.println();
            System.err.println("  3. InputStream type:");
            System.err.println("     - RawLocalFS: LocalFSFileInputStream");
            System.err.println("     - LOCAL_ONLY: SeaweedHadoopInputStream -> LocalOnlyInputStream");
            System.err.println();
            System.err.println("The 78-byte error suggests that:");
            System.err.println("  - Spark SQL expects to read 78 more bytes");
            System.err.println("  - But the InputStream reports EOF");
            System.err.println("  - This happens even though the file is correct (1260 bytes)");
            System.err.println();
            System.err.println("Possible causes:");
            System.err.println("  1. getFileStatus() returns wrong file size");
            System.err.println("  2. InputStream.available() returns wrong value");
            System.err.println("  3. Seek operations don't work correctly");
            System.err.println("  4. Multiple InputStreams interfere with each other");
            System.err.println("  5. Metadata is cached incorrectly between operations");
            System.err.println();
            
            // Don't fail the test - we want to see the full output
            // fail("LOCAL_ONLY failed as expected");
        }

        // ========================================================================
        // PART 3: Compare Files
        // ========================================================================
        System.out.println("\n" + "=".repeat(70));
        System.out.println("PART 3: File Comparison");
        System.out.println("=".repeat(70));
        
        File rawLocalParquetDir = new File(rawLocalDir + "/employees");
        File[] rawLocalFiles = rawLocalParquetDir.listFiles((dir, name) -> name.endsWith(".parquet"));
        
        File[] localOnlyFiles = new File(localOnlyDir).listFiles((dir, name) -> name.endsWith(".parquet.debug"));
        
        if (rawLocalFiles != null && rawLocalFiles.length > 0 && 
            localOnlyFiles != null && localOnlyFiles.length > 0) {
            
            File rawFile = rawLocalFiles[0];
            File localFile = localOnlyFiles[0];
            
            System.out.println("\nRawLocalFS file:  " + rawFile.getName() + " (" + rawFile.length() + " bytes)");
            System.out.println("LOCAL_ONLY file:  " + localFile.getName() + " (" + localFile.length() + " bytes)");
            
            if (rawFile.length() == localFile.length()) {
                System.out.println("✅ File sizes match!");
            } else {
                System.out.println("❌ File size mismatch: " + (rawFile.length() - localFile.length()) + " bytes");
            }
        }

        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  TEST COMPLETE - Check logs above for differences           ║");
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

