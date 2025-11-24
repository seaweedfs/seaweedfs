package seaweed.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

/**
 * Test to compare in-memory Parquet file with SeaweedFS-stored Parquet file
 * to identify what metadata differences cause the 78-byte EOF error.
 */
public class ParquetMemoryComparisonTest extends SparkTestBase {

    private static final String SCHEMA_STRING = 
        "message Employee { " +
        "  required int32 id; " +
        "  required binary name (UTF8); " +
        "  required int32 age; " +
        "}";
    
    private static final MessageType SCHEMA = MessageTypeParser.parseMessageType(SCHEMA_STRING);
    
    private FileSystem localFs;
    private FileSystem seaweedFs;
    
    @Before
    public void setUp() throws Exception {
        if (!TESTS_ENABLED) {
            return;
        }
        
        Configuration conf = new Configuration();
        
        // Local filesystem
        localFs = FileSystem.getLocal(conf);
        
        // SeaweedFS
        conf.set("fs.seaweedfs.impl", "seaweed.hdfs.SeaweedFileSystem");
        conf.set("fs.seaweed.filer.host", SEAWEEDFS_HOST);
        conf.set("fs.seaweed.filer.port", SEAWEEDFS_PORT);
        seaweedFs = FileSystem.get(java.net.URI.create("seaweedfs://" + SEAWEEDFS_HOST + ":" + SEAWEEDFS_PORT), conf);
        
        System.out.println("=== Test Setup Complete ===");
        System.out.println("Local FS: " + localFs.getClass().getName());
        System.out.println("SeaweedFS: " + seaweedFs.getClass().getName());
    }
    
    @After
    public void tearDown() throws Exception {
        if (localFs != null) {
            localFs.close();
        }
        if (seaweedFs != null) {
            seaweedFs.close();
        }
    }
    
    @Test
    public void testCompareMemoryVsSeaweedFSParquet() throws Exception {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        System.out.println("\n=== PARQUET MEMORY vs SEAWEEDFS COMPARISON TEST ===\n");
        
        // 1. Write identical Parquet file to local temp and SeaweedFS
        Path localPath = new Path("/tmp/test-local-" + System.currentTimeMillis() + ".parquet");
        Path seaweedPath = new Path("seaweedfs://" + SEAWEEDFS_HOST + ":" + SEAWEEDFS_PORT + 
                                     "/test-spark/comparison-test.parquet");
        
        System.out.println("Writing to local: " + localPath);
        System.out.println("Writing to SeaweedFS: " + seaweedPath);
        
        // Write same data to both locations
        writeTestParquetFile(localFs, localPath);
        writeTestParquetFile(seaweedFs, seaweedPath);
        
        System.out.println("\n=== Files Written Successfully ===\n");
        
        // 2. Read raw bytes from both files
        byte[] localBytes = readAllBytes(localFs, localPath);
        byte[] seaweedBytes = readAllBytes(seaweedFs, seaweedPath);
        
        System.out.println("Local file size: " + localBytes.length + " bytes");
        System.out.println("SeaweedFS file size: " + seaweedBytes.length + " bytes");
        
        // 3. Compare byte-by-byte
        if (localBytes.length != seaweedBytes.length) {
            System.out.println("\n❌ SIZE MISMATCH!");
            System.out.println("Difference: " + Math.abs(localBytes.length - seaweedBytes.length) + " bytes");
        } else {
            System.out.println("\n✅ Sizes match!");
        }
        
        // Find first difference
        int firstDiff = -1;
        int minLen = Math.min(localBytes.length, seaweedBytes.length);
        for (int i = 0; i < minLen; i++) {
            if (localBytes[i] != seaweedBytes[i]) {
                firstDiff = i;
                break;
            }
        }
        
        if (firstDiff >= 0) {
            System.out.println("\n❌ CONTENT DIFFERS at byte offset: " + firstDiff);
            System.out.println("Context (20 bytes before and after):");
            printByteContext(localBytes, seaweedBytes, firstDiff, 20);
        } else if (localBytes.length == seaweedBytes.length) {
            System.out.println("\n✅ Files are IDENTICAL!");
        }
        
        // 4. Parse Parquet metadata from both
        System.out.println("\n=== Parquet Metadata Comparison ===\n");
        
        ParquetMetadata localMeta = readParquetMetadata(localFs, localPath);
        ParquetMetadata seaweedMeta = readParquetMetadata(seaweedFs, seaweedPath);
        
        System.out.println("Local metadata:");
        printParquetMetadata(localMeta);
        
        System.out.println("\nSeaweedFS metadata:");
        printParquetMetadata(seaweedMeta);
        
        // 5. Try reading both files with Parquet reader
        System.out.println("\n=== Reading Files with ParquetFileReader ===\n");
        
        try {
            System.out.println("Reading local file...");
            int localRows = countParquetRows(localFs, localPath);
            System.out.println("✅ Local file: " + localRows + " rows read successfully");
        } catch (Exception e) {
            System.out.println("❌ Local file read failed: " + e.getMessage());
            e.printStackTrace();
        }
        
        try {
            System.out.println("\nReading SeaweedFS file...");
            int seaweedRows = countParquetRows(seaweedFs, seaweedPath);
            System.out.println("✅ SeaweedFS file: " + seaweedRows + " rows read successfully");
        } catch (Exception e) {
            System.out.println("❌ SeaweedFS file read failed: " + e.getMessage());
            System.out.println("Error type: " + e.getClass().getName());
            if (e.getMessage() != null && e.getMessage().contains("bytes left")) {
                System.out.println("🎯 THIS IS THE 78-BYTE EOF ERROR!");
            }
            e.printStackTrace();
        }
        
        // Cleanup
        localFs.delete(localPath, false);
        seaweedFs.delete(seaweedPath, false);
        
        System.out.println("\n=== Test Complete ===\n");
    }
    
    private void writeTestParquetFile(FileSystem fs, Path path) throws IOException {
        Configuration conf = fs.getConf();
        GroupWriteSupport.setSchema(SCHEMA, conf);
        
        try (ParquetWriter<Group> writer = org.apache.parquet.hadoop.example.ExampleParquetWriter.builder(path)
                .withConf(conf)
                .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE)
                .build()) {
            
            SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
            
            // Write 3 rows (same as Spark test)
            Group group1 = factory.newGroup()
                .append("id", 1)
                .append("name", "Alice")
                .append("age", 30);
            writer.write(group1);
            
            Group group2 = factory.newGroup()
                .append("id", 2)
                .append("name", "Bob")
                .append("age", 25);
            writer.write(group2);
            
            Group group3 = factory.newGroup()
                .append("id", 3)
                .append("name", "Charlie")
                .append("age", 35);
            writer.write(group3);
        }
    }
    
    private byte[] readAllBytes(FileSystem fs, Path path) throws IOException {
        try (FSDataInputStream in = fs.open(path)) {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            byte[] chunk = new byte[8192];
            int bytesRead;
            while ((bytesRead = in.read(chunk)) != -1) {
                buffer.write(chunk, 0, bytesRead);
            }
            return buffer.toByteArray();
        }
    }
    
    private ParquetMetadata readParquetMetadata(FileSystem fs, Path path) throws IOException {
        Configuration conf = fs.getConf();
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
            return reader.getFooter();
        }
    }
    
    private void printParquetMetadata(ParquetMetadata meta) {
        System.out.println("  Blocks: " + meta.getBlocks().size());
        meta.getBlocks().forEach(block -> {
            System.out.println("    Block rows: " + block.getRowCount());
            System.out.println("    Block total size: " + block.getTotalByteSize());
            System.out.println("    Block compressed size: " + block.getCompressedSize());
            System.out.println("    Columns: " + block.getColumns().size());
            block.getColumns().forEach(col -> {
                System.out.println("      Column: " + col.getPath());
                System.out.println("        Starting pos: " + col.getStartingPos());
                System.out.println("        Total size: " + col.getTotalSize());
                System.out.println("        Total uncompressed: " + col.getTotalUncompressedSize());
            });
        });
    }
    
    private int countParquetRows(FileSystem fs, Path path) throws IOException {
        Configuration conf = fs.getConf();
        int rowCount = 0;
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
            org.apache.parquet.column.page.PageReadStore pages;
            while ((pages = reader.readNextRowGroup()) != null) {
                rowCount += pages.getRowCount();
            }
        }
        return rowCount;
    }
    
    private void printByteContext(byte[] local, byte[] seaweed, int offset, int context) {
        int start = Math.max(0, offset - context);
        int endLocal = Math.min(local.length, offset + context);
        int endSeaweed = Math.min(seaweed.length, offset + context);
        
        System.out.println("\nLocal bytes [" + start + " to " + endLocal + "]:");
        printHexDump(local, start, endLocal, offset);
        
        System.out.println("\nSeaweedFS bytes [" + start + " to " + endSeaweed + "]:");
        printHexDump(seaweed, start, endSeaweed, offset);
    }
    
    private void printHexDump(byte[] bytes, int start, int end, int highlight) {
        StringBuilder hex = new StringBuilder();
        StringBuilder ascii = new StringBuilder();
        
        for (int i = start; i < end; i++) {
            if (i > start && i % 16 == 0) {
                System.out.printf("%04x: %-48s  %s\n", i - 16, hex.toString(), ascii.toString());
                hex.setLength(0);
                ascii.setLength(0);
            }
            
            byte b = bytes[i];
            String hexStr = String.format("%02x ", b & 0xFF);
            if (i == highlight) {
                hexStr = "[" + hexStr.trim() + "] ";
            }
            hex.append(hexStr);
            
            char c = (b >= 32 && b < 127) ? (char) b : '.';
            if (i == highlight) {
                ascii.append('[').append(c).append(']');
            } else {
                ascii.append(c);
            }
        }
        
        if (hex.length() > 0) {
            System.out.printf("%04x: %-48s  %s\n", (end / 16) * 16, hex.toString(), ascii.toString());
        }
    }
}

