package seaweed.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;

/**
 * Minimal reproducer for Parquet EOF issue.
 * 
 * This test writes a simple Parquet file to SeaweedFS and then tries to read it back.
 * It should reproduce the "EOFException: Still have: 78 bytes left" error.
 */
public class ParquetReproducerTest {

    private static final boolean TESTS_ENABLED = 
        "true".equalsIgnoreCase(System.getenv("SEAWEEDFS_TEST_ENABLED"));
    private static final String FILER_HOST = System.getenv().getOrDefault("SEAWEEDFS_FILER_HOST", "localhost");
    private static final String FILER_PORT = System.getenv().getOrDefault("SEAWEEDFS_FILER_PORT", "8888");
    private static final String FILER_GRPC_PORT = System.getenv().getOrDefault("SEAWEEDFS_FILER_GRPC_PORT", "18888");
    private static final String TEST_FILE = "/test-parquet-reproducer/employees.parquet";

    private Configuration conf;
    private org.apache.hadoop.fs.FileSystem fs;

    @Before
    public void setUp() throws Exception {
        if (!TESTS_ENABLED) {
            return;
        }
        
        System.out.println("\n=== Parquet EOF Reproducer Test ===");
        
        // Create configuration
        conf = new Configuration();
        conf.set("fs.seaweedfs.impl", "seaweed.hdfs.SeaweedFileSystem");
        conf.set("fs.seaweed.filer.host", FILER_HOST);
        conf.set("fs.seaweed.filer.port", FILER_PORT);
        conf.set("fs.seaweed.filer.port.grpc", FILER_GRPC_PORT);
        conf.set("fs.seaweed.buffer.size", "1048576");
        
        // Get filesystem
        Path testPath = new Path("seaweedfs://" + FILER_HOST + ":" + FILER_PORT + TEST_FILE);
        fs = testPath.getFileSystem(conf);
        
        // Clean up any existing test file
        if (fs.exists(testPath)) {
            fs.delete(testPath, true);
        }
    }

    @After
    public void tearDown() throws Exception {
        if (!TESTS_ENABLED) {
            return;
        }
        
        // Clean up test file
        Path testPath = new Path("seaweedfs://" + FILER_HOST + ":" + FILER_PORT + TEST_FILE);
        if (fs.exists(testPath)) {
            fs.delete(testPath, true);
        }
        
        if (fs != null) {
            fs.close();
        }
    }

    @Test
    public void testParquetWriteAndRead() throws IOException {
        if (!TESTS_ENABLED) {
            return;
        }

        System.out.println("\n1. Writing Parquet file to SeaweedFS...");
        writeParquetFile();
        
        System.out.println("\n2. Reading Parquet file metadata...");
        readParquetMetadata();
        
        System.out.println("\n3. Attempting to read row groups...");
        readParquetData();
        
        System.out.println("\n4. Test completed successfully!");
    }

    private void writeParquetFile() throws IOException {
        // Define schema: same as Spark SQL test (id, name, department, salary)
        MessageType schema = Types.buildMessage()
            .required(INT32).named("id")
            .optional(BINARY).as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType()).named("name")
            .optional(BINARY).as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType()).named("department")
            .required(INT32).named("salary")
            .named("employee");

        Path path = new Path("seaweedfs://" + FILER_HOST + ":" + FILER_PORT + TEST_FILE);
        
        Configuration writerConf = new Configuration(conf);
        GroupWriteSupport.setSchema(schema, writerConf);
        
        System.out.println("Creating ParquetWriter...");
        System.out.println("Path: " + path);
        
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        GroupWriteSupport.setSchema(schema, writerConf);
        
        try (ParquetWriter<Group> writer = new org.apache.parquet.hadoop.example.GroupWriteSupport.Builder(path)
                .withConf(writerConf)
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .build()) {
            
            SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
            
            // Write 4 employees (same data as Spark test)
            Group group1 = groupFactory.newGroup()
                .append("id", 1)
                .append("name", "Alice")
                .append("department", "Engineering")
                .append("salary", 100000);
            writer.write(group1);
            System.out.println("Wrote employee 1: Alice");
            
            Group group2 = groupFactory.newGroup()
                .append("id", 2)
                .append("name", "Bob")
                .append("department", "Sales")
                .append("salary", 80000);
            writer.write(group2);
            System.out.println("Wrote employee 2: Bob");
            
            Group group3 = groupFactory.newGroup()
                .append("id", 3)
                .append("name", "Charlie")
                .append("department", "Engineering")
                .append("salary", 120000);
            writer.write(group3);
            System.out.println("Wrote employee 3: Charlie");
            
            Group group4 = groupFactory.newGroup()
                .append("id", 4)
                .append("name", "David")
                .append("department", "Sales")
                .append("salary", 75000);
            writer.write(group4);
            System.out.println("Wrote employee 4: David");
        }
        
        System.out.println("ParquetWriter closed successfully");
        
        // Verify file exists and get size
        long fileSize = fs.getFileStatus(new Path("seaweedfs://" + FILER_HOST + ":" + FILER_PORT + TEST_FILE)).getLen();
        System.out.println("File written successfully. Size: " + fileSize + " bytes");
    }

    private void readParquetMetadata() throws IOException {
        Path path = new Path("seaweedfs://" + FILER_HOST + ":" + FILER_PORT + TEST_FILE);
        
        System.out.println("Opening file for metadata read...");
        HadoopInputFile inputFile = HadoopInputFile.fromPath(path, conf);
        
        System.out.println("Creating ParquetFileReader...");
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            ParquetMetadata footer = reader.getFooter();
            
            System.out.println("\n=== Parquet Footer Metadata ===");
            System.out.println("Blocks (row groups): " + footer.getBlocks().size());
            System.out.println("Schema: " + footer.getFileMetaData().getSchema());
            
            footer.getBlocks().forEach(block -> {
                System.out.println("\nRow Group:");
                System.out.println("  Row count: " + block.getRowCount());
                System.out.println("  Total byte size: " + block.getTotalByteSize());
                System.out.println("  Columns: " + block.getColumns().size());
                
                block.getColumns().forEach(column -> {
                    System.out.println("    Column: " + column.getPath());
                    System.out.println("      Data page offset: " + column.getFirstDataPageOffset());
                    System.out.println("      Dictionary page offset: " + column.getDictionaryPageOffset());
                    System.out.println("      Total size: " + column.getTotalSize());
                    System.out.println("      Total uncompressed size: " + column.getTotalUncompressedSize());
                });
            });
            
            System.out.println("\nMetadata read completed successfully");
        } catch (Exception e) {
            System.err.println("\n!!! EXCEPTION in readParquetMetadata !!!");
            System.err.println("Exception type: " + e.getClass().getName());
            System.err.println("Message: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    private void readParquetData() throws IOException {
        Path path = new Path("seaweedfs://" + FILER_HOST + ":" + FILER_PORT + TEST_FILE);
        
        System.out.println("Opening file for data read...");
        HadoopInputFile inputFile = HadoopInputFile.fromPath(path, conf);
        
        System.out.println("Creating ParquetFileReader...");
        try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
            System.out.println("Attempting to read row group...");
            org.apache.parquet.hadoop.metadata.BlockMetaData block = reader.readNextRowGroup();
            
            if (block != null) {
                System.out.println("SUCCESS: Read row group with " + block.getRowCount() + " rows");
            } else {
                System.out.println("WARNING: No row groups found");
            }
            
        } catch (Exception e) {
            System.err.println("\n!!! EXCEPTION in readParquetData !!!");
            System.err.println("Exception type: " + e.getClass().getName());
            System.err.println("Message: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
        
        System.out.println("ParquetFileReader closed successfully");
    }
}

