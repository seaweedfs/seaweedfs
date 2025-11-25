package seaweed.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * CRITICAL READ COMPARISON TEST: Compare all read operations between RawLocalFileSystem
 * and SeaweedFS LOCAL_ONLY mode.
 * 
 * This test:
 * 1. Writes identical data to both RawLocalFS and LOCAL_ONLY
 * 2. Performs the same read operations on both
 * 3. Compares the results of each read operation
 * 4. Identifies where the divergence happens
 */
public class SparkShadowReadComparisonTest extends SparkTestBase {

    private String rawLocalDir;
    private String localOnlyDir;
    private FileSystem rawLocalFs;
    private FileSystem seaweedFs;
    private String rawLocalParquetFile;
    private String localOnlyParquetFile;

    @Before
    public void setUp() throws Exception {
        super.setUpSpark();
        
        // Set up RawLocalFileSystem directory
        rawLocalDir = "/tmp/spark-shadow-read-rawlocal-" + System.currentTimeMillis();
        new File(rawLocalDir).mkdirs();
        
        Configuration conf = spark.sparkContext().hadoopConfiguration();
        rawLocalFs = new RawLocalFileSystem();
        rawLocalFs.initialize(new URI("file:///"), conf);
        rawLocalFs.delete(new Path(rawLocalDir), true);
        rawLocalFs.mkdirs(new Path(rawLocalDir));
        
        // Set up LOCAL_ONLY directory
        localOnlyDir = "/workspace/target/debug-shadow-read";
        new File(localOnlyDir).mkdirs();
        for (File f : new File(localOnlyDir).listFiles()) {
            f.delete();
        }
        
        // Get SeaweedFS instance
        seaweedFs = FileSystem.get(URI.create("seaweedfs://seaweedfs-filer:8888"), conf);
        
        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  SHADOW READ COMPARISON: RawLocalFS vs LOCAL_ONLY           ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println("RawLocalFS directory: " + rawLocalDir);
        System.out.println("LOCAL_ONLY directory: " + localOnlyDir);
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
    public void testShadowReadComparison() throws IOException {
        System.out.println("\n=== PHASE 1: Write Identical Data to Both FileSystems ===");
        
        // Create test data
        List<Employee> employees = Arrays.asList(
                new Employee(1, "Alice", "Engineering", 100000),
                new Employee(2, "Bob", "Sales", 80000),
                new Employee(3, "Charlie", "Engineering", 120000),
                new Employee(4, "David", "Sales", 75000));

        Dataset<Row> df = spark.createDataFrame(employees, Employee.class);

        // Write to RawLocalFileSystem
        String rawLocalPath = "file://" + rawLocalDir + "/employees";
        System.out.println("Writing to RawLocalFS: " + rawLocalPath);
        df.write().mode(SaveMode.Overwrite).parquet(rawLocalPath);
        System.out.println("✅ RawLocalFS write completed");

        // Set environment for LOCAL_ONLY mode
        System.setProperty("SEAWEEDFS_DEBUG_MODE", "LOCAL_ONLY");
        spark.sparkContext().hadoopConfiguration().set("fs.seaweedfs.debug.dir", localOnlyDir);
        
        // Write to LOCAL_ONLY
        String localOnlyPath = getTestPath("employees_read_test");
        System.out.println("Writing to LOCAL_ONLY: " + localOnlyPath);
        df.write().mode(SaveMode.Overwrite).parquet(localOnlyPath);
        System.out.println("✅ LOCAL_ONLY write completed");

        // Find the parquet files
        File rawLocalParquetDir = new File(rawLocalDir + "/employees");
        File[] rawLocalFiles = rawLocalParquetDir.listFiles((dir, name) -> name.endsWith(".parquet"));
        assertNotNull("RawLocalFS should have written files", rawLocalFiles);
        assertTrue("RawLocalFS should have at least one parquet file", rawLocalFiles.length > 0);
        rawLocalParquetFile = rawLocalFiles[0].getAbsolutePath();
        
        File[] localOnlyFiles = new File(localOnlyDir).listFiles((dir, name) -> name.endsWith(".parquet.debug"));
        assertNotNull("LOCAL_ONLY should have written files", localOnlyFiles);
        assertTrue("LOCAL_ONLY should have at least one parquet file", localOnlyFiles.length > 0);
        localOnlyParquetFile = localOnlyFiles[0].getAbsolutePath();
        
        System.out.println("RawLocalFS file: " + rawLocalParquetFile);
        System.out.println("LOCAL_ONLY file: " + localOnlyParquetFile);

        System.out.println("\n=== PHASE 2: Compare Low-Level Read Operations ===");
        
        // Open both files for reading
        FSDataInputStream rawStream = rawLocalFs.open(new Path(rawLocalParquetFile));
        
        // For LOCAL_ONLY, we need to read the .debug file directly using RawLocalFS
        // because it's just a local file
        FSDataInputStream localOnlyStream = rawLocalFs.open(new Path(localOnlyParquetFile));
        
        try {
            // Test 1: Read file length
            System.out.println("\n--- Test 1: File Length ---");
            long rawLength = rawLocalFs.getFileStatus(new Path(rawLocalParquetFile)).getLen();
            long localOnlyLength = rawLocalFs.getFileStatus(new Path(localOnlyParquetFile)).getLen();
            System.out.println("RawLocalFS length:  " + rawLength);
            System.out.println("LOCAL_ONLY length:  " + localOnlyLength);
            if (rawLength == localOnlyLength) {
                System.out.println("✅ Lengths match!");
            } else {
                System.out.println("❌ Length mismatch: " + (rawLength - localOnlyLength) + " bytes");
            }
            assertEquals("File lengths should match", rawLength, localOnlyLength);

            // Test 2: Read first 100 bytes
            System.out.println("\n--- Test 2: Read First 100 Bytes ---");
            byte[] rawBuffer1 = new byte[100];
            byte[] localOnlyBuffer1 = new byte[100];
            rawStream.readFully(0, rawBuffer1);
            localOnlyStream.readFully(0, localOnlyBuffer1);
            boolean firstBytesMatch = Arrays.equals(rawBuffer1, localOnlyBuffer1);
            System.out.println("First 100 bytes match: " + (firstBytesMatch ? "✅" : "❌"));
            if (!firstBytesMatch) {
                System.out.println("First difference at byte: " + findFirstDifference(rawBuffer1, localOnlyBuffer1));
            }
            assertTrue("First 100 bytes should match", firstBytesMatch);

            // Test 3: Read last 100 bytes (Parquet footer)
            System.out.println("\n--- Test 3: Read Last 100 Bytes (Parquet Footer) ---");
            byte[] rawBuffer2 = new byte[100];
            byte[] localOnlyBuffer2 = new byte[100];
            rawStream.readFully(rawLength - 100, rawBuffer2);
            localOnlyStream.readFully(localOnlyLength - 100, localOnlyBuffer2);
            boolean lastBytesMatch = Arrays.equals(rawBuffer2, localOnlyBuffer2);
            System.out.println("Last 100 bytes match: " + (lastBytesMatch ? "✅" : "❌"));
            if (!lastBytesMatch) {
                System.out.println("First difference at byte: " + findFirstDifference(rawBuffer2, localOnlyBuffer2));
                System.out.println("RawLocalFS last 20 bytes:");
                printHex(rawBuffer2, 80, 100);
                System.out.println("LOCAL_ONLY last 20 bytes:");
                printHex(localOnlyBuffer2, 80, 100);
            }
            assertTrue("Last 100 bytes should match", lastBytesMatch);

            // Test 4: Read entire file
            System.out.println("\n--- Test 4: Read Entire File ---");
            byte[] rawFull = new byte[(int) rawLength];
            byte[] localOnlyFull = new byte[(int) localOnlyLength];
            rawStream.readFully(0, rawFull);
            localOnlyStream.readFully(0, localOnlyFull);
            boolean fullMatch = Arrays.equals(rawFull, localOnlyFull);
            System.out.println("Full file match: " + (fullMatch ? "✅" : "❌"));
            if (!fullMatch) {
                int firstDiff = findFirstDifference(rawFull, localOnlyFull);
                System.out.println("First difference at byte: " + firstDiff);
            }
            assertTrue("Full file should match", fullMatch);

            // Test 5: Sequential reads
            System.out.println("\n--- Test 5: Sequential Reads (10 bytes at a time) ---");
            rawStream.seek(0);
            localOnlyStream.seek(0);
            boolean sequentialMatch = true;
            int chunkSize = 10;
            int chunksRead = 0;
            while (rawStream.getPos() < rawLength && localOnlyStream.getPos() < localOnlyLength) {
                byte[] rawChunk = new byte[chunkSize];
                byte[] localOnlyChunk = new byte[chunkSize];
                int rawRead = rawStream.read(rawChunk);
                int localOnlyRead = localOnlyStream.read(localOnlyChunk);
                
                if (rawRead != localOnlyRead) {
                    System.out.println("❌ Read size mismatch at chunk " + chunksRead + ": raw=" + rawRead + " localOnly=" + localOnlyRead);
                    sequentialMatch = false;
                    break;
                }
                
                if (!Arrays.equals(rawChunk, localOnlyChunk)) {
                    System.out.println("❌ Content mismatch at chunk " + chunksRead + " (byte offset " + (chunksRead * chunkSize) + ")");
                    sequentialMatch = false;
                    break;
                }
                chunksRead++;
            }
            System.out.println("Sequential reads (" + chunksRead + " chunks): " + (sequentialMatch ? "✅" : "❌"));
            assertTrue("Sequential reads should match", sequentialMatch);

        } finally {
            rawStream.close();
            localOnlyStream.close();
        }

        System.out.println("\n=== PHASE 3: Compare Spark Read Operations ===");
        
        // Test 6: Spark read from RawLocalFS
        System.out.println("\n--- Test 6: Spark Read from RawLocalFS ---");
        try {
            Dataset<Row> rawDf = spark.read().parquet(rawLocalPath);
            long rawCount = rawDf.count();
            System.out.println("✅ RawLocalFS Spark read successful! Row count: " + rawCount);
            assertEquals("Should have 4 employees", 4, rawCount);
        } catch (Exception e) {
            System.err.println("❌ RawLocalFS Spark read FAILED: " + e.getMessage());
            e.printStackTrace();
            fail("RawLocalFS Spark read should not fail!");
        }

        // Test 7: Spark read from LOCAL_ONLY
        System.out.println("\n--- Test 7: Spark Read from LOCAL_ONLY ---");
        try {
            Dataset<Row> localOnlyDf = spark.read().parquet(localOnlyPath);
            long localOnlyCount = localOnlyDf.count();
            System.out.println("✅ LOCAL_ONLY Spark read successful! Row count: " + localOnlyCount);
            assertEquals("Should have 4 employees", 4, localOnlyCount);
        } catch (Exception e) {
            System.err.println("❌ LOCAL_ONLY Spark read FAILED: " + e.getMessage());
            if (e.getMessage() != null && e.getMessage().contains("78 bytes")) {
                System.err.println("🔍 FOUND IT! 78-byte error occurs during Spark read!");
                System.err.println("But low-level reads worked, so the issue is in Spark's Parquet reader!");
            }
            e.printStackTrace();
            // Don't fail - we want to see the full output
        }

        // Test 8: SQL query on RawLocalFS
        System.out.println("\n--- Test 8: SQL Query on RawLocalFS ---");
        try {
            Dataset<Row> rawDf = spark.read().parquet(rawLocalPath);
            rawDf.createOrReplaceTempView("employees_raw");
            Dataset<Row> rawResult = spark.sql("SELECT name, salary FROM employees_raw WHERE department = 'Engineering'");
            long rawResultCount = rawResult.count();
            System.out.println("✅ RawLocalFS SQL query successful! Row count: " + rawResultCount);
            assertEquals("Should have 2 engineering employees", 2, rawResultCount);
        } catch (Exception e) {
            System.err.println("❌ RawLocalFS SQL query FAILED: " + e.getMessage());
            e.printStackTrace();
            fail("RawLocalFS SQL query should not fail!");
        }

        // Test 9: SQL query on LOCAL_ONLY
        System.out.println("\n--- Test 9: SQL Query on LOCAL_ONLY ---");
        try {
            Dataset<Row> localOnlyDf = spark.read().parquet(localOnlyPath);
            localOnlyDf.createOrReplaceTempView("employees_localonly");
            Dataset<Row> localOnlyResult = spark.sql("SELECT name, salary FROM employees_localonly WHERE department = 'Engineering'");
            long localOnlyResultCount = localOnlyResult.count();
            System.out.println("✅ LOCAL_ONLY SQL query successful! Row count: " + localOnlyResultCount);
            assertEquals("Should have 2 engineering employees", 2, localOnlyResultCount);
        } catch (Exception e) {
            System.err.println("❌ LOCAL_ONLY SQL query FAILED: " + e.getMessage());
            if (e.getMessage() != null && e.getMessage().contains("78 bytes")) {
                System.err.println("🔍 78-byte error in SQL query!");
            }
            e.printStackTrace();
            // Don't fail - we want to see the full output
        }

        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  SHADOW READ COMPARISON COMPLETE                             ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
    }

    private int findFirstDifference(byte[] a, byte[] b) {
        int minLen = Math.min(a.length, b.length);
        for (int i = 0; i < minLen; i++) {
            if (a[i] != b[i]) {
                return i;
            }
        }
        return minLen;
    }

    private void printHex(byte[] data, int start, int end) {
        System.out.print("  ");
        for (int i = start; i < end && i < data.length; i++) {
            System.out.printf("%02X ", data[i]);
        }
        System.out.println();
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

