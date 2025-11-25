package seaweed.spark;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * CRITICAL TEST: Compare shadow file (reference) with LOCAL_ONLY mode output.
 * 
 * This test:
 * 1. Writes with SHADOW mode enabled → produces reference file
 * 2. Writes with LOCAL_ONLY mode → produces local-only file
 * 3. Compares the two files byte-by-byte
 * 4. Attempts to read both with Spark SQL
 */
public class ShadowVsLocalOnlyComparisonTest extends SparkTestBase {

    private String shadowDir;
    private String localOnlyDir;

    @Before
    public void setUp() throws Exception {
        super.setUpSpark();
        shadowDir = "/workspace/target/shadow-comparison";
        localOnlyDir = "/workspace/target/local-only-comparison";
        
        // Clean up previous runs
        deleteDirectory(new File(shadowDir));
        deleteDirectory(new File(localOnlyDir));
        
        new File(shadowDir).mkdirs();
        new File(localOnlyDir).mkdirs();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDownSpark();
    }

    @Test
    public void testShadowVsLocalOnlyComparison() throws IOException {
        skipIfTestsDisabled();

        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  CRITICAL: Shadow vs LOCAL_ONLY Comparison                   ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");

        List<Employee> employees = Arrays.asList(
                new Employee(1, "Alice", "Engineering", 100000),
                new Employee(2, "Bob", "Sales", 80000),
                new Employee(3, "Charlie", "Engineering", 120000),
                new Employee(4, "David", "Sales", 75000));

        Dataset<Row> df = spark.createDataFrame(employees, Employee.class);

        // PHASE 1: Write with SHADOW mode
        System.out.println("\n=== PHASE 1: Write with SHADOW mode (creates reference) ===");
        System.setProperty("SEAWEEDFS_SHADOW_MODE", "true");
        System.setProperty("SEAWEEDFS_DEBUG_MODE", "SEAWEED_ONLY");
        spark.conf().set("fs.seaweedfs.shadow.dir", shadowDir);
        
        String shadowOutputPath = "seaweedfs://seaweedfs-filer:8888/test-spark/shadow-test/employees";
        df.write().mode(SaveMode.Overwrite).parquet(shadowOutputPath);
        
        File[] shadowFiles = new File(shadowDir).listFiles((dir, name) -> name.endsWith(".shadow"));
        assertNotNull("Shadow files should exist", shadowFiles);
        assertTrue("Should have at least one shadow file", shadowFiles.length > 0);
        File shadowFile = shadowFiles[0];
        System.out.println("Shadow file: " + shadowFile.getName() + " (" + shadowFile.length() + " bytes)");

        // PHASE 2: Write with LOCAL_ONLY mode
        System.out.println("\n=== PHASE 2: Write with LOCAL_ONLY mode ===");
        System.setProperty("SEAWEEDFS_SHADOW_MODE", "false");
        System.setProperty("SEAWEEDFS_DEBUG_MODE", "LOCAL_ONLY");
        spark.conf().set("fs.seaweedfs.debug.dir", localOnlyDir);
        
        String localOnlyOutputPath = "seaweedfs://seaweedfs-filer:8888/test-spark/local-only-test/employees";
        df.write().mode(SaveMode.Overwrite).parquet(localOnlyOutputPath);
        
        File[] localOnlyFiles = new File(localOnlyDir).listFiles((dir, name) -> name.endsWith(".debug"));
        assertNotNull("LOCAL_ONLY files should exist", localOnlyFiles);
        assertTrue("Should have at least one LOCAL_ONLY file", localOnlyFiles.length > 0);
        File localOnlyFile = localOnlyFiles[0];
        System.out.println("LOCAL_ONLY file: " + localOnlyFile.getName() + " (" + localOnlyFile.length() + " bytes)");

        // PHASE 3: Compare files byte-by-byte
        System.out.println("\n=== PHASE 3: Compare files byte-by-byte ===");
        assertEquals("File sizes should match", shadowFile.length(), localOnlyFile.length());
        
        byte[] shadowBytes = Files.readAllBytes(shadowFile.toPath());
        byte[] localOnlyBytes = Files.readAllBytes(localOnlyFile.toPath());
        
        System.out.println("Comparing " + shadowBytes.length + " bytes...");
        
        // Compare byte-by-byte and report first difference
        boolean identical = true;
        for (int i = 0; i < shadowBytes.length; i++) {
            if (shadowBytes[i] != localOnlyBytes[i]) {
                identical = false;
                System.err.println("❌ FIRST DIFFERENCE at byte " + i + ":");
                System.err.println("  Shadow:     0x" + String.format("%02x", shadowBytes[i] & 0xFF));
                System.err.println("  LOCAL_ONLY: 0x" + String.format("%02x", localOnlyBytes[i] & 0xFF));
                
                // Show context
                int contextStart = Math.max(0, i - 10);
                int contextEnd = Math.min(shadowBytes.length, i + 10);
                System.err.println("  Context (shadow):");
                for (int j = contextStart; j < contextEnd; j++) {
                    System.err.print(String.format("%02x ", shadowBytes[j] & 0xFF));
                }
                System.err.println();
                System.err.println("  Context (local_only):");
                for (int j = contextStart; j < contextEnd; j++) {
                    System.err.print(String.format("%02x ", localOnlyBytes[j] & 0xFF));
                }
                System.err.println();
                break;
            }
        }
        
        if (identical) {
            System.out.println("✅ Files are IDENTICAL!");
        } else {
            fail("Files are NOT identical");
        }

        // PHASE 4: Try reading shadow file with Spark
        System.out.println("\n=== PHASE 4: Try reading shadow file with Spark ===");
        try {
            // Copy shadow file to a location Spark can read
            String testPath = "file://" + shadowDir + "/test.parquet";
            Files.copy(shadowFile.toPath(), new File(shadowDir + "/test.parquet").toPath());
            
            Dataset<Row> shadowDf = spark.read().parquet(testPath);
            shadowDf.createOrReplaceTempView("shadow_test");
            Dataset<Row> shadowResult = spark.sql("SELECT * FROM shadow_test WHERE department = 'Engineering'");
            System.out.println("✅ Shadow file SQL query: " + shadowResult.count() + " rows");
        } catch (Exception e) {
            System.err.println("❌ Shadow file SQL query FAILED: " + e.getMessage());
            e.printStackTrace();
        }

        // PHASE 5: Try reading LOCAL_ONLY file with Spark
        System.out.println("\n=== PHASE 5: Try reading LOCAL_ONLY file with Spark ===");
        try {
            Dataset<Row> localOnlyDf = spark.read().parquet(localOnlyOutputPath);
            localOnlyDf.createOrReplaceTempView("local_only_test");
            Dataset<Row> localOnlyResult = spark.sql("SELECT * FROM local_only_test WHERE department = 'Engineering'");
            System.out.println("✅ LOCAL_ONLY SQL query: " + localOnlyResult.count() + " rows");
        } catch (Exception e) {
            System.err.println("❌ LOCAL_ONLY SQL query FAILED: " + e.getMessage());
            assertTrue("Expected 78-byte EOF error", e.getMessage().contains("78 bytes left"));
        }

        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  Comparison complete. See logs for details.                   ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
    }

    private void deleteDirectory(File dir) {
        if (dir.exists()) {
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            dir.delete();
        }
    }

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

