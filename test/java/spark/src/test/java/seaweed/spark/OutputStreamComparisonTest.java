package seaweed.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Compare OutputStream behavior between local disk and SeaweedFS
 * to understand why Parquet files written to SeaweedFS have incorrect metadata.
 */
public class OutputStreamComparisonTest extends SparkTestBase {

    private static class WriteOperation {
        String source;
        String operation;
        long positionBefore;
        long positionAfter;
        int bytesWritten;
        long timestamp;
        String details;

        WriteOperation(String source, String operation, long positionBefore, long positionAfter, 
                      int bytesWritten, String details) {
            this.source = source;
            this.operation = operation;
            this.positionBefore = positionBefore;
            this.positionAfter = positionAfter;
            this.bytesWritten = bytesWritten;
            this.timestamp = System.nanoTime();
            this.details = details;
        }

        @Override
        public String toString() {
            return String.format("[%s] %s: posBefore=%d, posAfter=%d, written=%d %s",
                    source, operation, positionBefore, positionAfter, bytesWritten, 
                    details != null ? "(" + details + ")" : "");
        }
    }

    private static class LoggingOutputStream extends OutputStream {
        private final FSDataOutputStream wrapped;
        private final String source;
        private final List<WriteOperation> operations;

        LoggingOutputStream(FSDataOutputStream wrapped, String source, List<WriteOperation> operations) {
            this.wrapped = wrapped;
            this.source = source;
            this.operations = operations;
        }

        @Override
        public void write(int b) throws IOException {
            long posBefore = wrapped.getPos();
            wrapped.write(b);
            long posAfter = wrapped.getPos();
            operations.add(new WriteOperation(source, "write(int)", posBefore, posAfter, 1, null));
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            long posBefore = wrapped.getPos();
            wrapped.write(b, off, len);
            long posAfter = wrapped.getPos();
            operations.add(new WriteOperation(source, "write(byte[])", posBefore, posAfter, len, 
                    "len=" + len));
        }

        @Override
        public void flush() throws IOException {
            long posBefore = wrapped.getPos();
            wrapped.flush();
            long posAfter = wrapped.getPos();
            operations.add(new WriteOperation(source, "flush()", posBefore, posAfter, 0, null));
        }

        @Override
        public void close() throws IOException {
            long posBefore = wrapped.getPos();
            wrapped.close();
            long posAfter = 0; // Can't call getPos() after close
            operations.add(new WriteOperation(source, "close()", posBefore, posAfter, 0, 
                    "finalPos=" + posBefore));
        }

        public long getPos() throws IOException {
            long pos = wrapped.getPos();
            operations.add(new WriteOperation(source, "getPos()", pos, pos, 0, "returned=" + pos));
            return pos;
        }

        public void hflush() throws IOException {
            long posBefore = wrapped.getPos();
            wrapped.hflush();
            long posAfter = wrapped.getPos();
            operations.add(new WriteOperation(source, "hflush()", posBefore, posAfter, 0, null));
        }

        public void hsync() throws IOException {
            long posBefore = wrapped.getPos();
            wrapped.hsync();
            long posAfter = wrapped.getPos();
            operations.add(new WriteOperation(source, "hsync()", posBefore, posAfter, 0, null));
        }
    }

    private static final MessageType SCHEMA = MessageTypeParser.parseMessageType(
            "message schema {"
                    + "required int32 id;"
                    + "required binary name;"
                    + "required int32 age;"
                    + "}"
    );

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
    public void testCompareOutputStreamBehavior() throws Exception {
        skipIfTestsDisabled();

        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  REAL-TIME OUTPUTSTREAM COMPARISON: LOCAL vs SEAWEEDFS      ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");

        // Prepare file systems
        Configuration conf = new Configuration();
        FileSystem localFs = FileSystem.getLocal(conf);
        
        conf.set("fs.seaweedfs.impl", "seaweed.hdfs.SeaweedFileSystem");
        conf.set("fs.seaweed.filer.host", SEAWEEDFS_HOST);
        conf.set("fs.seaweed.filer.port", String.valueOf(SEAWEEDFS_PORT));
        FileSystem seaweedFs = FileSystem.get(URI.create(String.format("seaweedfs://%s:%s", 
                SEAWEEDFS_HOST, SEAWEEDFS_PORT)), conf);

        // Prepare paths
        new java.io.File("/workspace/target/test-output").mkdirs();
        Path localPath = new Path("file:///workspace/target/test-output/write-comparison-local.parquet");
        Path seaweedPath = new Path(getTestPath("write-comparison-seaweed.parquet"));

        // Delete if exists
        localFs.delete(localPath, false);
        seaweedFs.delete(seaweedPath, false);

        List<WriteOperation> localOps = new ArrayList<>();
        List<WriteOperation> seaweedOps = new ArrayList<>();

        System.out.println("\n1. Writing Parquet files with synchronized operations...\n");

        // Write using ParquetWriter with custom OutputStreams
        GroupWriteSupport.setSchema(SCHEMA, conf);
        
        // Create data
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(SCHEMA);
        List<Group> groups = new ArrayList<>();
        groups.add(groupFactory.newGroup().append("id", 1).append("name", "Alice").append("age", 30));
        groups.add(groupFactory.newGroup().append("id", 2).append("name", "Bob").append("age", 25));
        groups.add(groupFactory.newGroup().append("id", 3).append("name", "Charlie").append("age", 35));

        // Write to local disk
        System.out.println("   Writing to LOCAL DISK...");
        try (ParquetWriter<Group> localWriter = new ParquetWriter<>(
                localPath,
                new GroupWriteSupport(),
                CompressionCodecName.SNAPPY,
                1024 * 1024, // Block size
                1024, // Page size
                1024, // Dictionary page size
                true, // Enable dictionary
                false, // Don't validate
                ParquetWriter.DEFAULT_WRITER_VERSION,
                conf)) {
            for (Group group : groups) {
                localWriter.write(group);
            }
        }
        System.out.println("   ✅ Local write complete");

        // Write to SeaweedFS
        System.out.println("\n   Writing to SEAWEEDFS...");
        try (ParquetWriter<Group> seaweedWriter = new ParquetWriter<>(
                seaweedPath,
                new GroupWriteSupport(),
                CompressionCodecName.SNAPPY,
                1024 * 1024, // Block size
                1024, // Page size
                1024, // Dictionary page size
                true, // Enable dictionary
                false, // Don't validate
                ParquetWriter.DEFAULT_WRITER_VERSION,
                conf)) {
            for (Group group : groups) {
                seaweedWriter.write(group);
            }
        }
        System.out.println("   ✅ SeaweedFS write complete");

        // Compare file sizes
        System.out.println("\n2. Comparing final file sizes...");
        long localSize = localFs.getFileStatus(localPath).getLen();
        long seaweedSize = seaweedFs.getFileStatus(seaweedPath).getLen();
        System.out.println("   LOCAL:    " + localSize + " bytes");
        System.out.println("   SEAWEED:  " + seaweedSize + " bytes");

        if (localSize == seaweedSize) {
            System.out.println("   ✅ File sizes MATCH");
        } else {
            System.out.println("   ❌ File sizes DIFFER by " + Math.abs(localSize - seaweedSize) + " bytes");
        }

        // Now test reading both files
        System.out.println("\n3. Testing if both files can be read by Spark...");
        
        System.out.println("\n   Reading LOCAL file:");
        try {
            org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> localDf = 
                    spark.read().parquet(localPath.toString());
            long localCount = localDf.count();
            System.out.println("   ✅ LOCAL read SUCCESS - " + localCount + " rows");
            localDf.show();
        } catch (Exception e) {
            System.out.println("   ❌ LOCAL read FAILED: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("\n   Reading SEAWEEDFS file:");
        try {
            org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> seaweedDf = 
                    spark.read().parquet(seaweedPath.toString());
            long seaweedCount = seaweedDf.count();
            System.out.println("   ✅ SEAWEEDFS read SUCCESS - " + seaweedCount + " rows");
            seaweedDf.show();
        } catch (Exception e) {
            System.out.println("   ❌ SEAWEEDFS read FAILED: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  COMPARISON COMPLETE                                         ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
    }

    @Test
    public void testCompareRawOutputStreamOperations() throws Exception {
        skipIfTestsDisabled();

        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  RAW OUTPUTSTREAM COMPARISON: LOCAL vs SEAWEEDFS            ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");

        // Prepare file systems
        Configuration conf = new Configuration();
        FileSystem localFs = FileSystem.getLocal(conf);
        
        conf.set("fs.seaweedfs.impl", "seaweed.hdfs.SeaweedFileSystem");
        conf.set("fs.seaweed.filer.host", SEAWEEDFS_HOST);
        conf.set("fs.seaweed.filer.port", String.valueOf(SEAWEEDFS_PORT));
        FileSystem seaweedFs = FileSystem.get(URI.create(String.format("seaweedfs://%s:%s", 
                SEAWEEDFS_HOST, SEAWEEDFS_PORT)), conf);

        // Prepare paths
        new java.io.File("/workspace/target/test-output").mkdirs();
        Path localPath = new Path("file:///workspace/target/test-output/raw-comparison-local.dat");
        Path seaweedPath = new Path(getTestPath("raw-comparison-seaweed.dat"));

        // Delete if exists
        localFs.delete(localPath, false);
        seaweedFs.delete(seaweedPath, false);

        List<WriteOperation> localOps = new ArrayList<>();
        List<WriteOperation> seaweedOps = new ArrayList<>();

        System.out.println("\n1. Performing synchronized write operations...\n");

        // Open both streams
        FSDataOutputStream localStream = localFs.create(localPath, true);
        FSDataOutputStream seaweedStream = seaweedFs.create(seaweedPath, true);

        LoggingOutputStream localLogging = new LoggingOutputStream(localStream, "LOCAL", localOps);
        LoggingOutputStream seaweedLogging = new LoggingOutputStream(seaweedStream, "SEAWEED", seaweedOps);

        int opCount = 0;
        boolean mismatchFound = false;

        // Operation 1: Write 4 bytes (magic)
        opCount++;
        System.out.println("   Op " + opCount + ": write(4 bytes) - Writing magic bytes");
        byte[] magic = "PAR1".getBytes();
        localLogging.write(magic, 0, 4);
        seaweedLogging.write(magic, 0, 4);
        long localPos1 = localLogging.getPos();
        long seaweedPos1 = seaweedLogging.getPos();
        System.out.println("      LOCAL:   getPos() = " + localPos1);
        System.out.println("      SEAWEED: getPos() = " + seaweedPos1);
        if (localPos1 != seaweedPos1) {
            System.out.println("      ❌ MISMATCH!");
            mismatchFound = true;
        } else {
            System.out.println("      ✅ Match");
        }

        // Operation 2: Write 100 bytes of data
        opCount++;
        System.out.println("\n   Op " + opCount + ": write(100 bytes) - Writing data");
        byte[] data = new byte[100];
        for (int i = 0; i < 100; i++) {
            data[i] = (byte) i;
        }
        localLogging.write(data, 0, 100);
        seaweedLogging.write(data, 0, 100);
        long localPos2 = localLogging.getPos();
        long seaweedPos2 = seaweedLogging.getPos();
        System.out.println("      LOCAL:   getPos() = " + localPos2);
        System.out.println("      SEAWEED: getPos() = " + seaweedPos2);
        if (localPos2 != seaweedPos2) {
            System.out.println("      ❌ MISMATCH!");
            mismatchFound = true;
        } else {
            System.out.println("      ✅ Match");
        }

        // Operation 3: Flush
        opCount++;
        System.out.println("\n   Op " + opCount + ": flush()");
        localLogging.flush();
        seaweedLogging.flush();
        long localPos3 = localLogging.getPos();
        long seaweedPos3 = seaweedLogging.getPos();
        System.out.println("      LOCAL:   getPos() after flush = " + localPos3);
        System.out.println("      SEAWEED: getPos() after flush = " + seaweedPos3);
        if (localPos3 != seaweedPos3) {
            System.out.println("      ❌ MISMATCH!");
            mismatchFound = true;
        } else {
            System.out.println("      ✅ Match");
        }

        // Operation 4: Write more data
        opCount++;
        System.out.println("\n   Op " + opCount + ": write(50 bytes) - Writing more data");
        byte[] moreData = new byte[50];
        for (int i = 0; i < 50; i++) {
            moreData[i] = (byte) (i + 100);
        }
        localLogging.write(moreData, 0, 50);
        seaweedLogging.write(moreData, 0, 50);
        long localPos4 = localLogging.getPos();
        long seaweedPos4 = seaweedLogging.getPos();
        System.out.println("      LOCAL:   getPos() = " + localPos4);
        System.out.println("      SEAWEED: getPos() = " + seaweedPos4);
        if (localPos4 != seaweedPos4) {
            System.out.println("      ❌ MISMATCH!");
            mismatchFound = true;
        } else {
            System.out.println("      ✅ Match");
        }

        // Operation 5: Write final bytes (simulating footer)
        opCount++;
        System.out.println("\n   Op " + opCount + ": write(8 bytes) - Writing footer");
        byte[] footer = new byte[]{0x6B, 0x03, 0x00, 0x00, 0x50, 0x41, 0x52, 0x31};
        localLogging.write(footer, 0, 8);
        seaweedLogging.write(footer, 0, 8);
        long localPos5 = localLogging.getPos();
        long seaweedPos5 = seaweedLogging.getPos();
        System.out.println("      LOCAL:   getPos() = " + localPos5);
        System.out.println("      SEAWEED: getPos() = " + seaweedPos5);
        if (localPos5 != seaweedPos5) {
            System.out.println("      ❌ MISMATCH!");
            mismatchFound = true;
        } else {
            System.out.println("      ✅ Match");
        }

        // Operation 6: Close
        opCount++;
        System.out.println("\n   Op " + opCount + ": close()");
        System.out.println("      LOCAL:   closing at position " + localPos5);
        System.out.println("      SEAWEED: closing at position " + seaweedPos5);
        localLogging.close();
        seaweedLogging.close();

        // Check final file sizes
        System.out.println("\n2. Comparing final file sizes...");
        long localSize = localFs.getFileStatus(localPath).getLen();
        long seaweedSize = seaweedFs.getFileStatus(seaweedPath).getLen();
        System.out.println("   LOCAL:    " + localSize + " bytes");
        System.out.println("   SEAWEED:  " + seaweedSize + " bytes");
        
        if (localSize != seaweedSize) {
            System.out.println("   ❌ File sizes DIFFER by " + Math.abs(localSize - seaweedSize) + " bytes");
            mismatchFound = true;
        } else {
            System.out.println("   ✅ File sizes MATCH");
        }

        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  COMPARISON SUMMARY                                          ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println("   Total operations: " + opCount);
        System.out.println("   LOCAL operations:  " + localOps.size());
        System.out.println("   SEAWEED operations: " + seaweedOps.size());
        
        if (mismatchFound) {
            System.out.println("\n   ❌ MISMATCHES FOUND - Streams behave differently!");
        } else {
            System.out.println("\n   ✅ ALL OPERATIONS MATCH - Streams are identical!");
        }

        System.out.println("\n   Detailed operation log:");
        System.out.println("   ----------------------");
        int maxOps = Math.max(localOps.size(), seaweedOps.size());
        for (int i = 0; i < maxOps; i++) {
            if (i < localOps.size()) {
                System.out.println("   " + localOps.get(i));
            }
            if (i < seaweedOps.size()) {
                System.out.println("   " + seaweedOps.get(i));
            }
            if (i < localOps.size() && i < seaweedOps.size()) {
                WriteOperation localOp = localOps.get(i);
                WriteOperation seaweedOp = seaweedOps.get(i);
                if (localOp.positionAfter != seaweedOp.positionAfter) {
                    System.out.println("      ⚠️  Position mismatch: LOCAL=" + localOp.positionAfter + 
                            " SEAWEED=" + seaweedOp.positionAfter);
                }
            }
        }

        assertFalse("Streams should behave identically", mismatchFound);
    }
}

