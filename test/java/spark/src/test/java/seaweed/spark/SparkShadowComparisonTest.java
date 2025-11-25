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
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * CRITICAL COMPARISON TEST: Use RawLocalFileSystem as a "shadow" to compare
 * all I/O operations with LOCAL_ONLY mode.
 * 
 * This test writes the same data to both:
 * 1. RawLocalFileSystem (file://) - Known to work
 * 2. SeaweedFS LOCAL_ONLY mode (seaweedfs://) - Has 78-byte error
 * 
 * Then compares the resulting files byte-by-byte to find the exact difference.
 */
public class SparkShadowComparisonTest extends SparkTestBase {

    private String rawLocalDir;
    private String localOnlyDir;
    private FileSystem rawLocalFs;

    @Before
    public void setUp() throws Exception {
        super.setUpSpark();
        
        // Set up RawLocalFileSystem directory
        rawLocalDir = "/tmp/spark-shadow-rawlocal-" + System.currentTimeMillis();
        new File(rawLocalDir).mkdirs();
        
        Configuration conf = spark.sparkContext().hadoopConfiguration();
        rawLocalFs = new RawLocalFileSystem();
        rawLocalFs.initialize(new URI("file:///"), conf);
        rawLocalFs.delete(new Path(rawLocalDir), true);
        rawLocalFs.mkdirs(new Path(rawLocalDir));
        
        // Set up LOCAL_ONLY directory (will be in debug dir)
        localOnlyDir = "/workspace/target/debug-shadow";
        new File(localOnlyDir).mkdirs();
        
        // Clean up previous runs
        for (File f : new File(localOnlyDir).listFiles()) {
            f.delete();
        }
        
        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  SHADOW COMPARISON: RawLocalFS vs LOCAL_ONLY                ║");
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
    public void testShadowComparison() throws IOException {
        System.out.println("\n=== PHASE 1: Write to RawLocalFileSystem ===");
        
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
        
        try {
            df.write().mode(SaveMode.Overwrite).parquet(rawLocalPath);
            System.out.println("✅ RawLocalFS write completed successfully!");
        } catch (Exception e) {
            System.err.println("❌ RawLocalFS write FAILED: " + e.getMessage());
            e.printStackTrace();
            fail("RawLocalFS write should not fail!");
        }

        // List files written by RawLocalFS
        File rawLocalParquetDir = new File(rawLocalDir + "/employees");
        File[] rawLocalFiles = rawLocalParquetDir.listFiles((dir, name) -> name.endsWith(".parquet"));
        assertNotNull("RawLocalFS should have written files", rawLocalFiles);
        assertTrue("RawLocalFS should have at least one parquet file", rawLocalFiles.length > 0);
        
        System.out.println("RawLocalFS wrote " + rawLocalFiles.length + " parquet file(s):");
        for (File f : rawLocalFiles) {
            System.out.println("  - " + f.getName() + " (" + f.length() + " bytes)");
        }

        System.out.println("\n=== PHASE 2: Write to LOCAL_ONLY mode ===");
        
        // Set environment for LOCAL_ONLY mode
        System.setProperty("SEAWEEDFS_DEBUG_MODE", "LOCAL_ONLY");
        spark.sparkContext().hadoopConfiguration().set("fs.seaweedfs.debug.dir", localOnlyDir);
        
        // Write to LOCAL_ONLY
        String localOnlyPath = getTestPath("employees_localonly");
        System.out.println("Writing to LOCAL_ONLY: " + localOnlyPath);
        
        boolean localOnlyWriteSucceeded = false;
        try {
            df.write().mode(SaveMode.Overwrite).parquet(localOnlyPath);
            System.out.println("✅ LOCAL_ONLY write completed successfully!");
            localOnlyWriteSucceeded = true;
        } catch (Exception e) {
            System.err.println("⚠️  LOCAL_ONLY write completed but may have issues: " + e.getMessage());
            // Don't fail here - we want to compare files even if write "succeeded"
        }

        // List files written by LOCAL_ONLY
        File[] localOnlyFiles = new File(localOnlyDir).listFiles((dir, name) -> name.endsWith(".debug"));
        if (localOnlyFiles == null || localOnlyFiles.length == 0) {
            System.err.println("❌ LOCAL_ONLY did not write any .debug files!");
            fail("LOCAL_ONLY should have written .debug files");
        }
        
        System.out.println("LOCAL_ONLY wrote " + localOnlyFiles.length + " .debug file(s):");
        for (File f : localOnlyFiles) {
            System.out.println("  - " + f.getName() + " (" + f.length() + " bytes)");
        }

        System.out.println("\n=== PHASE 3: Compare Files Byte-by-Byte ===");
        
        // Match files by pattern (both should have part-00000-*.snappy.parquet)
        File rawFile = rawLocalFiles[0]; // Should only be one file
        File localOnlyFile = null;
        
        // Find the .debug file that looks like a parquet file
        for (File f : localOnlyFiles) {
            if (f.getName().contains("part-") && f.getName().endsWith(".parquet.debug")) {
                localOnlyFile = f;
                break;
            }
        }
        
        if (localOnlyFile == null) {
            System.out.println("❌ Could not find LOCAL_ONLY parquet file!");
            System.out.println("Available .debug files:");
            for (File f : localOnlyFiles) {
                System.out.println("  - " + f.getName());
            }
            fail("LOCAL_ONLY should have written a parquet .debug file");
        }
        
        System.out.println("\nComparing:");
        System.out.println("  RawLocalFS:  " + rawFile.getName() + " (" + rawFile.length() + " bytes)");
        System.out.println("  LOCAL_ONLY:  " + localOnlyFile.getName() + " (" + localOnlyFile.length() + " bytes)");
        
        // Compare file sizes
        long sizeDiff = rawFile.length() - localOnlyFile.length();
        if (sizeDiff != 0) {
            System.out.println("  ⚠️  SIZE DIFFERENCE: " + sizeDiff + " bytes");
            System.out.println("     RawLocalFS is " + (sizeDiff > 0 ? "LARGER" : "SMALLER") + " by " + Math.abs(sizeDiff) + " bytes");
            
            if (Math.abs(sizeDiff) == 78) {
                System.out.println("     🔍 THIS IS THE 78-BYTE DIFFERENCE!");
            }
        } else {
            System.out.println("  ✅ File sizes match!");
        }
        
        // Compare file contents byte-by-byte
        byte[] rawBytes = Files.readAllBytes(rawFile.toPath());
        byte[] localOnlyBytes = Files.readAllBytes(localOnlyFile.toPath());
        
        int minLen = Math.min(rawBytes.length, localOnlyBytes.length);
        int firstDiffIndex = -1;
        
        for (int i = 0; i < minLen; i++) {
            if (rawBytes[i] != localOnlyBytes[i]) {
                firstDiffIndex = i;
                break;
            }
        }
        
        if (firstDiffIndex >= 0) {
            System.out.println("  ⚠️  CONTENT DIFFERS at byte offset: " + firstDiffIndex);
            System.out.println("     Showing 32 bytes around difference:");
            
            int start = Math.max(0, firstDiffIndex - 16);
            int end = Math.min(minLen, firstDiffIndex + 16);
            
            System.out.print("     RawLocalFS:  ");
            for (int i = start; i < end; i++) {
                System.out.printf("%02X ", rawBytes[i]);
                if (i == firstDiffIndex) System.out.print("| ");
            }
            System.out.println();
            
            System.out.print("     LOCAL_ONLY:  ");
            for (int i = start; i < end; i++) {
                System.out.printf("%02X ", localOnlyBytes[i]);
                if (i == firstDiffIndex) System.out.print("| ");
            }
            System.out.println();
        } else if (rawBytes.length == localOnlyBytes.length) {
            System.out.println("  ✅ File contents are IDENTICAL!");
        } else {
            System.out.println("  ⚠️  Files match up to " + minLen + " bytes, but differ in length");
            
            // Show the extra bytes
            if (rawBytes.length > localOnlyBytes.length) {
                System.out.println("     RawLocalFS has " + (rawBytes.length - minLen) + " extra bytes at end:");
                System.out.print("     ");
                for (int i = minLen; i < Math.min(rawBytes.length, minLen + 32); i++) {
                    System.out.printf("%02X ", rawBytes[i]);
                }
                System.out.println();
            } else {
                System.out.println("     LOCAL_ONLY has " + (localOnlyBytes.length - minLen) + " extra bytes at end:");
                System.out.print("     ");
                for (int i = minLen; i < Math.min(localOnlyBytes.length, minLen + 32); i++) {
                    System.out.printf("%02X ", localOnlyBytes[i]);
                }
                System.out.println();
            }
        }

        System.out.println("\n=== PHASE 4: Try Reading Both Files ===");
        
        // Try reading RawLocalFS file
        System.out.println("\nReading from RawLocalFS:");
        try {
            Dataset<Row> rawDf = spark.read().parquet(rawLocalPath);
            long rawCount = rawDf.count();
            System.out.println("✅ RawLocalFS read successful! Row count: " + rawCount);
            assertEquals("Should have 4 employees", 4, rawCount);
        } catch (Exception e) {
            System.err.println("❌ RawLocalFS read FAILED: " + e.getMessage());
            e.printStackTrace();
            fail("RawLocalFS read should not fail!");
        }
        
        // Try reading LOCAL_ONLY file
        System.out.println("\nReading from LOCAL_ONLY:");
        try {
            Dataset<Row> localOnlyDf = spark.read().parquet(localOnlyPath);
            long localOnlyCount = localOnlyDf.count();
            System.out.println("✅ LOCAL_ONLY read successful! Row count: " + localOnlyCount);
            assertEquals("Should have 4 employees", 4, localOnlyCount);
        } catch (Exception e) {
            System.err.println("❌ LOCAL_ONLY read FAILED: " + e.getMessage());
            if (e.getMessage() != null && e.getMessage().contains("78 bytes")) {
                System.err.println("🔍 CONFIRMED: 78-byte error occurs during READ, not WRITE!");
            }
            // Don't fail - we expect this to fail
        }

        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  SHADOW COMPARISON COMPLETE                                  ║");
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

