package seaweed.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Test to verify if file chunks are preserved during rename operations.
 * This could explain why Parquet files become unreadable after Spark's commit.
 */
public class RenameChunkVerificationTest extends SparkTestBase {

    @Before
    public void setUp() throws IOException {
        if (!TESTS_ENABLED) {
            return;
        }
        super.setUpSpark();
    }

    @After
    public void tearDown() throws IOException {
        if (!TESTS_ENABLED) {
            return;
        }
        super.tearDownSpark();
    }

    @Test
    public void testSparkWriteAndRenamePreservesChunks() throws Exception {
        skipIfTestsDisabled();

        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  TESTING: Chunk Preservation During Spark Write & Rename    ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");

        // Write using Spark (which uses rename for commit)
        List<SparkSQLTest.Employee> employees = Arrays.asList(
                new SparkSQLTest.Employee(1, "Alice", "Engineering", 100000),
                new SparkSQLTest.Employee(2, "Bob", "Sales", 80000),
                new SparkSQLTest.Employee(3, "Charlie", "Engineering", 120000),
                new SparkSQLTest.Employee(4, "David", "Sales", 75000));

        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> df = 
                spark.createDataFrame(employees, SparkSQLTest.Employee.class);

        String tablePath = getTestPath("chunk-test");

        System.out.println("\n1. Writing Parquet file using Spark...");
        df.write().mode(org.apache.spark.sql.SaveMode.Overwrite).parquet(tablePath);
        System.out.println("   ✅ Write complete");

        // Get file system
        Configuration conf = new Configuration();
        conf.set("fs.seaweedfs.impl", "seaweed.hdfs.SeaweedFileSystem");
        conf.set("fs.seaweed.filer.host", SEAWEEDFS_HOST);
        conf.set("fs.seaweed.filer.port", String.valueOf(SEAWEEDFS_PORT));
        FileSystem fs = FileSystem.get(URI.create(String.format("seaweedfs://%s:%s", 
                SEAWEEDFS_HOST, SEAWEEDFS_PORT)), conf);

        // Find the parquet file
        Path parquetFile = null;
        org.apache.hadoop.fs.FileStatus[] files = fs.listStatus(new Path(tablePath));
        for (org.apache.hadoop.fs.FileStatus file : files) {
            if (file.getPath().getName().endsWith(".parquet") && 
                !file.getPath().getName().startsWith("_")) {
                parquetFile = file.getPath();
                break;
            }
        }

        assertNotNull("Parquet file not found", parquetFile);

        System.out.println("\n2. Checking file metadata after Spark write...");
        org.apache.hadoop.fs.FileStatus fileStatus = fs.getFileStatus(parquetFile);
        long fileSize = fileStatus.getLen();
        System.out.println("   File: " + parquetFile.getName());
        System.out.println("   Size: " + fileSize + " bytes");

        // Try to read the file
        System.out.println("\n3. Attempting to read file with Spark...");
        try {
            org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> readDf = 
                    spark.read().parquet(tablePath);
            long count = readDf.count();
            System.out.println("   ✅ Read SUCCESS - " + count + " rows");
            readDf.show();
        } catch (Exception e) {
            System.out.println("   ❌ Read FAILED: " + e.getMessage());
            System.out.println("\n   Error details:");
            e.printStackTrace();
            
            // This is expected to fail - let's investigate why
            System.out.println("\n4. Investigating chunk availability...");
            
            // Try to read the raw bytes
            System.out.println("\n   Attempting to read raw bytes...");
            try (org.apache.hadoop.fs.FSDataInputStream in = fs.open(parquetFile)) {
                byte[] header = new byte[4];
                int read = in.read(header);
                System.out.println("   Read " + read + " bytes");
                System.out.println("   Header: " + bytesToHex(header));
                
                if (read == 4 && Arrays.equals(header, "PAR1".getBytes())) {
                    System.out.println("   ✅ Magic bytes are correct (PAR1)");
                } else {
                    System.out.println("   ❌ Magic bytes are WRONG!");
                }
                
                // Try to read footer
                in.seek(fileSize - 8);
                byte[] footer = new byte[8];
                read = in.read(footer);
                System.out.println("\n   Footer (last 8 bytes): " + bytesToHex(footer));
                
                // Try to read entire file
                in.seek(0);
                byte[] allBytes = new byte[(int)fileSize];
                int totalRead = 0;
                while (totalRead < fileSize) {
                    int bytesRead = in.read(allBytes, totalRead, (int)(fileSize - totalRead));
                    if (bytesRead == -1) {
                        System.out.println("   ❌ Premature EOF at byte " + totalRead + " (expected " + fileSize + ")");
                        break;
                    }
                    totalRead += bytesRead;
                }
                
                if (totalRead == fileSize) {
                    System.out.println("   ✅ Successfully read all " + totalRead + " bytes");
                } else {
                    System.out.println("   ❌ Only read " + totalRead + " of " + fileSize + " bytes");
                }
                
            } catch (Exception readEx) {
                System.out.println("   ❌ Raw read failed: " + readEx.getMessage());
                readEx.printStackTrace();
            }
        }

        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  TEST COMPLETE                                               ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
    }

    @Test
    public void testManualRenamePreservesChunks() throws Exception {
        skipIfTestsDisabled();

        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  TESTING: Manual Rename Chunk Preservation                  ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");

        // Get file system
        Configuration conf = new Configuration();
        conf.set("fs.seaweedfs.impl", "seaweed.hdfs.SeaweedFileSystem");
        conf.set("fs.seaweed.filer.host", SEAWEEDFS_HOST);
        conf.set("fs.seaweed.filer.port", String.valueOf(SEAWEEDFS_PORT));
        FileSystem fs = FileSystem.get(URI.create(String.format("seaweedfs://%s:%s", 
                SEAWEEDFS_HOST, SEAWEEDFS_PORT)), conf);

        Path sourcePath = new Path(getTestPath("rename-source.dat"));
        Path destPath = new Path(getTestPath("rename-dest.dat"));

        // Clean up
        fs.delete(sourcePath, false);
        fs.delete(destPath, false);

        System.out.println("\n1. Creating test file...");
        byte[] testData = new byte[1260];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte)(i % 256);
        }

        try (org.apache.hadoop.fs.FSDataOutputStream out = fs.create(sourcePath, true)) {
            out.write(testData);
        }
        System.out.println("   ✅ Created source file: " + sourcePath);

        // Check source file
        System.out.println("\n2. Verifying source file...");
        org.apache.hadoop.fs.FileStatus sourceStatus = fs.getFileStatus(sourcePath);
        System.out.println("   Size: " + sourceStatus.getLen() + " bytes");

        // Read source file
        try (org.apache.hadoop.fs.FSDataInputStream in = fs.open(sourcePath)) {
            byte[] readData = new byte[1260];
            int totalRead = 0;
            while (totalRead < 1260) {
                int bytesRead = in.read(readData, totalRead, 1260 - totalRead);
                if (bytesRead == -1) break;
                totalRead += bytesRead;
            }
            System.out.println("   Read: " + totalRead + " bytes");
            
            if (Arrays.equals(testData, readData)) {
                System.out.println("   ✅ Source file data is correct");
            } else {
                System.out.println("   ❌ Source file data is CORRUPTED");
            }
        }

        // Perform rename
        System.out.println("\n3. Renaming file...");
        boolean renamed = fs.rename(sourcePath, destPath);
        System.out.println("   Rename result: " + renamed);

        if (!renamed) {
            System.out.println("   ❌ Rename FAILED");
            return;
        }

        // Check destination file
        System.out.println("\n4. Verifying destination file...");
        org.apache.hadoop.fs.FileStatus destStatus = fs.getFileStatus(destPath);
        System.out.println("   Size: " + destStatus.getLen() + " bytes");

        if (destStatus.getLen() != sourceStatus.getLen()) {
            System.out.println("   ❌ File size CHANGED during rename!");
            System.out.println("      Source: " + sourceStatus.getLen());
            System.out.println("      Dest:   " + destStatus.getLen());
        } else {
            System.out.println("   ✅ File size preserved");
        }

        // Read destination file
        try (org.apache.hadoop.fs.FSDataInputStream in = fs.open(destPath)) {
            byte[] readData = new byte[1260];
            int totalRead = 0;
            while (totalRead < 1260) {
                int bytesRead = in.read(readData, totalRead, 1260 - totalRead);
                if (bytesRead == -1) {
                    System.out.println("   ❌ Premature EOF at byte " + totalRead);
                    break;
                }
                totalRead += bytesRead;
            }
            System.out.println("   Read: " + totalRead + " bytes");
            
            if (totalRead == 1260 && Arrays.equals(testData, readData)) {
                System.out.println("   ✅ Destination file data is CORRECT");
            } else {
                System.out.println("   ❌ Destination file data is CORRUPTED or INCOMPLETE");
                
                // Show first difference
                for (int i = 0; i < Math.min(totalRead, 1260); i++) {
                    if (testData[i] != readData[i]) {
                        System.out.println("      First difference at byte " + i);
                        System.out.println("      Expected: " + String.format("0x%02X", testData[i]));
                        System.out.println("      Got:      " + String.format("0x%02X", readData[i]));
                        break;
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("   ❌ Read FAILED: " + e.getMessage());
            e.printStackTrace();
        }

        // Clean up
        fs.delete(destPath, false);

        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  TEST COMPLETE                                               ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X ", b));
        }
        return sb.toString().trim();
    }
}

