package seaweed.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Test if Spark can read a Parquet file that was written directly
 * (not by Spark) to SeaweedFS.
 * 
 * This isolates whether the 78-byte EOF error is in:
 * - Spark's WRITE path (if this test passes)
 * - Spark's READ path (if this test also fails)
 */
public class SparkReadDirectParquetTest extends SparkTestBase {

    private static final String SCHEMA_STRING = 
        "message Employee { " +
        "  required int32 id; " +
        "  required binary name (UTF8); " +
        "  required int32 age; " +
        "}";
    
    private static final MessageType SCHEMA = MessageTypeParser.parseMessageType(SCHEMA_STRING);
    
    @Test
    public void testSparkReadDirectlyWrittenParquet() throws Exception {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  SPARK READS DIRECTLY-WRITTEN PARQUET FILE TEST             ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝\n");
        
        String testPath = getSeaweedFSPath("/direct-write-test/employees.parquet");
        
        // Step 1: Write Parquet file directly (not via Spark)
        System.out.println("=== Step 1: Writing Parquet file directly (bypassing Spark) ===");
        writeParquetFileDirect(testPath);
        System.out.println("✅ File written successfully: " + testPath);
        
        // Step 2: Try to read it with Spark
        System.out.println("\n=== Step 2: Reading file with Spark ===");
        try {
            Dataset<Row> df = spark.read().parquet(testPath);
            
            System.out.println("Schema:");
            df.printSchema();
            
            long count = df.count();
            System.out.println("Row count: " + count);
            
            System.out.println("\nData:");
            df.show();
            
            // Verify data
            assertEquals("Should have 3 rows", 3, count);
            
            System.out.println("\n✅ SUCCESS! Spark can read directly-written Parquet file!");
            System.out.println("✅ This proves the issue is in SPARK'S WRITE PATH, not read path!");
            
        } catch (Exception e) {
            System.out.println("\n❌ FAILED! Spark cannot read directly-written Parquet file!");
            System.out.println("Error: " + e.getMessage());
            
            if (e.getMessage() != null && e.getMessage().contains("bytes left")) {
                System.out.println("🎯 This is the 78-byte EOF error!");
                System.out.println("❌ This means the issue is in SPARK'S READ PATH!");
            }
            
            e.printStackTrace();
            throw e;
        }
    }
    
    @Test
    public void testSparkWriteThenRead() throws Exception {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }
        
        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  SPARK WRITES THEN READS PARQUET FILE TEST (BASELINE)       ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝\n");
        
        String testPath = getSeaweedFSPath("/spark-write-test/employees");
        
        // Step 1: Write with Spark
        System.out.println("=== Step 1: Writing Parquet file with Spark ===");
        spark.sql("CREATE TABLE IF NOT EXISTS test_employees (id INT, name STRING, age INT) USING parquet LOCATION '" + testPath + "'");
        spark.sql("INSERT INTO test_employees VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)");
        System.out.println("✅ File written by Spark");
        
        // Step 2: Try to read it with Spark
        System.out.println("\n=== Step 2: Reading file with Spark ===");
        try {
            Dataset<Row> df = spark.read().parquet(testPath);
            
            System.out.println("Schema:");
            df.printSchema();
            
            long count = df.count();
            System.out.println("Row count: " + count);
            
            System.out.println("\nData:");
            df.show();
            
            assertEquals("Should have 3 rows", 3, count);
            
            System.out.println("\n✅ SUCCESS! Spark can read its own Parquet file!");
            
        } catch (Exception e) {
            System.out.println("\n❌ FAILED! Spark cannot read its own Parquet file!");
            System.out.println("Error: " + e.getMessage());
            
            if (e.getMessage() != null && e.getMessage().contains("bytes left")) {
                System.out.println("🎯 This is the 78-byte EOF error!");
            }
            
            e.printStackTrace();
            throw e;
        } finally {
            spark.sql("DROP TABLE IF EXISTS test_employees");
        }
    }
    
    private void writeParquetFileDirect(String seaweedPath) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.seaweedfs.impl", "seaweed.hdfs.SeaweedFileSystem");
        conf.set("fs.seaweed.filer.host", SEAWEEDFS_HOST);
        conf.set("fs.seaweed.filer.port", SEAWEEDFS_PORT);
        
        FileSystem fs = FileSystem.get(java.net.URI.create("seaweedfs://" + SEAWEEDFS_HOST + ":" + SEAWEEDFS_PORT), conf);
        Path path = new Path(seaweedPath);
        
        // Ensure parent directory exists
        fs.mkdirs(path.getParent());
        
        GroupWriteSupport.setSchema(SCHEMA, conf);
        
        try (ParquetWriter<Group> writer = org.apache.parquet.hadoop.example.ExampleParquetWriter.builder(path)
                .withConf(conf)
                .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE)
                .build()) {
            
            SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
            
            // Write same 3 rows as Spark test
            System.out.println("  Writing row 1: id=1, name=Alice, age=30");
            Group group1 = factory.newGroup()
                .append("id", 1)
                .append("name", "Alice")
                .append("age", 30);
            writer.write(group1);
            
            System.out.println("  Writing row 2: id=2, name=Bob, age=25");
            Group group2 = factory.newGroup()
                .append("id", 2)
                .append("name", "Bob")
                .append("age", 25);
            writer.write(group2);
            
            System.out.println("  Writing row 3: id=3, name=Charlie, age=35");
            Group group3 = factory.newGroup()
                .append("id", 3)
                .append("name", "Charlie")
                .append("age", 35);
            writer.write(group3);
        }
        
        // Verify file was written
        org.apache.hadoop.fs.FileStatus status = fs.getFileStatus(path);
        System.out.println("  File size: " + status.getLen() + " bytes");
        
        fs.close();
    }
}

