package seaweed.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Detailed comparison of InputStream/OutputStream operations between
 * local filesystem and SeaweedFS during Parquet file writing.
 * 
 * This test intercepts and logs every read/write/getPos operation to
 * identify exactly where the behavior diverges.
 */
public class ParquetOperationComparisonTest extends SparkTestBase {

    private static final String SCHEMA_STRING = "message Employee { " +
            "  required int32 id; " +
            "  required binary name (UTF8); " +
            "  required int32 age; " +
            "}";

    private static final MessageType SCHEMA = MessageTypeParser.parseMessageType(SCHEMA_STRING);

    // Track all operations for comparison
    private static class OperationLog {
        List<String> operations = new ArrayList<>();

        void log(String op) {
            operations.add(op);
            System.out.println("  " + op);
        }

        void print(String title) {
            System.out.println("\n" + title + " (" + operations.size() + " operations):");
            for (int i = 0; i < operations.size(); i++) {
                System.out.printf("  [%3d] %s\n", i, operations.get(i));
            }
        }

        void compare(OperationLog other, String name1, String name2) {
            System.out.println("\n=== COMPARISON: " + name1 + " vs " + name2 + " ===");

            int maxLen = Math.max(operations.size(), other.operations.size());
            int differences = 0;

            for (int i = 0; i < maxLen; i++) {
                String op1 = i < operations.size() ? operations.get(i) : "<missing>";
                String op2 = i < other.operations.size() ? other.operations.get(i) : "<missing>";

                if (!op1.equals(op2)) {
                    differences++;
                    System.out.printf("[%3d] DIFF:\n", i);
                    System.out.println("  " + name1 + ": " + op1);
                    System.out.println("  " + name2 + ": " + op2);
                }
            }

            if (differences == 0) {
                System.out.println("✅ Operations are IDENTICAL!");
            } else {
                System.out.println("❌ Found " + differences + " differences");
            }
        }
    }

    // Wrapper for FSDataOutputStream that logs all operations
    private static class LoggingOutputStream extends FSDataOutputStream {
        private final FSDataOutputStream delegate;
        private final OperationLog log;
        private final String name;

        public LoggingOutputStream(FSDataOutputStream delegate, OperationLog log, String name) throws IOException {
            super(delegate.getWrappedStream(), null);
            this.delegate = delegate;
            this.log = log;
            this.name = name;
            log.log(name + " CREATED");
        }

        @Override
        public void write(int b) throws IOException {
            log.log(String.format("write(byte) pos=%d", getPos()));
            delegate.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            long posBefore = getPos();
            delegate.write(b, off, len);
            long posAfter = getPos();
            log.log(String.format("write(%d bytes) pos %d→%d", len, posBefore, posAfter));
        }

        @Override
        public long getPos() {
            long pos = delegate.getPos();
            // Don't log getPos itself to avoid infinite recursion, but track it
            return pos;
        }

        @Override
        public void flush() throws IOException {
            log.log(String.format("flush() pos=%d", getPos()));
            delegate.flush();
        }

        @Override
        public void close() throws IOException {
            log.log(String.format("close() pos=%d", getPos()));
            delegate.close();
        }

        @Override
        public void hflush() throws IOException {
            log.log(String.format("hflush() pos=%d", getPos()));
            delegate.hflush();
        }

        @Override
        public void hsync() throws IOException {
            log.log(String.format("hsync() pos=%d", getPos()));
            delegate.hsync();
        }
    }

    // Wrapper for FSDataInputStream that logs all operations
    private static class LoggingInputStream extends FSDataInputStream {
        private final OperationLog log;
        private final String name;

        public LoggingInputStream(FSDataInputStream delegate, OperationLog log, String name) throws IOException {
            super(delegate);
            this.log = log;
            this.name = name;
            log.log(name + " CREATED");
        }

        @Override
        public int read() throws IOException {
            long posBefore = getPos();
            int result = super.read();
            log.log(String.format("read() pos %d→%d result=%d", posBefore, getPos(), result));
            return result;
        }

        // Can't override read(byte[], int, int) as it's final in DataInputStream
        // The logging will happen through read(ByteBuffer) which is what Parquet uses

        @Override
        public int read(ByteBuffer buf) throws IOException {
            long posBefore = getPos();
            int result = super.read(buf);
            log.log(String.format("read(ByteBuffer %d) pos %d→%d result=%d", buf.remaining(), posBefore, getPos(),
                    result));
            return result;
        }

        @Override
        public void seek(long pos) throws IOException {
            long posBefore = getPos();
            super.seek(pos);
            log.log(String.format("seek(%d) pos %d→%d", pos, posBefore, getPos()));
        }

        @Override
        public void close() throws IOException {
            log.log(String.format("close() pos=%d", getPos()));
            super.close();
        }
    }

    @Test
    public void testCompareWriteOperations() throws Exception {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }

        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║     PARQUET WRITE OPERATION COMPARISON TEST                  ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝\n");

        // Setup filesystems
        Configuration localConf = new Configuration();
        FileSystem localFs = FileSystem.getLocal(localConf);

        Configuration seaweedConf = new Configuration();
        seaweedConf.set("fs.seaweedfs.impl", "seaweed.hdfs.SeaweedFileSystem");
        seaweedConf.set("fs.seaweed.filer.host", SEAWEEDFS_HOST);
        seaweedConf.set("fs.seaweed.filer.port", SEAWEEDFS_PORT);
        FileSystem seaweedFs = FileSystem.get(
                java.net.URI.create("seaweedfs://" + SEAWEEDFS_HOST + ":" + SEAWEEDFS_PORT),
                seaweedConf);

        Path localPath = new Path("/tmp/test-local-ops-" + System.currentTimeMillis() + ".parquet");
        Path seaweedPath = new Path("seaweedfs://" + SEAWEEDFS_HOST + ":" + SEAWEEDFS_PORT +
                "/test-spark/ops-test.parquet");

        OperationLog localLog = new OperationLog();
        OperationLog seaweedLog = new OperationLog();

        // Write to local filesystem with logging
        System.out.println("=== Writing to LOCAL filesystem ===");
        writeParquetWithLogging(localFs, localPath, localConf, localLog, "LOCAL");

        System.out.println("\n=== Writing to SEAWEEDFS ===");
        writeParquetWithLogging(seaweedFs, seaweedPath, seaweedConf, seaweedLog, "SEAWEED");

        // Print logs
        localLog.print("LOCAL OPERATIONS");
        seaweedLog.print("SEAWEEDFS OPERATIONS");

        // Compare
        localLog.compare(seaweedLog, "LOCAL", "SEAWEEDFS");

        // Cleanup
        localFs.delete(localPath, false);
        seaweedFs.delete(seaweedPath, false);

        localFs.close();
        seaweedFs.close();

        System.out.println("\n=== Test Complete ===");
    }

    @Test
    public void testCompareReadOperations() throws Exception {
        if (!TESTS_ENABLED) {
            System.out.println("Skipping test - SEAWEEDFS_TEST_ENABLED not set");
            return;
        }

        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║     PARQUET READ OPERATION COMPARISON TEST                   ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝\n");

        // Setup filesystems
        Configuration localConf = new Configuration();
        FileSystem localFs = FileSystem.getLocal(localConf);

        Configuration seaweedConf = new Configuration();
        seaweedConf.set("fs.seaweedfs.impl", "seaweed.hdfs.SeaweedFileSystem");
        seaweedConf.set("fs.seaweed.filer.host", SEAWEEDFS_HOST);
        seaweedConf.set("fs.seaweed.filer.port", SEAWEEDFS_PORT);
        FileSystem seaweedFs = FileSystem.get(
                java.net.URI.create("seaweedfs://" + SEAWEEDFS_HOST + ":" + SEAWEEDFS_PORT),
                seaweedConf);

        Path localPath = new Path("/tmp/test-local-read-" + System.currentTimeMillis() + ".parquet");
        Path seaweedPath = new Path("seaweedfs://" + SEAWEEDFS_HOST + ":" + SEAWEEDFS_PORT +
                "/test-spark/read-test.parquet");

        // First write files without logging
        System.out.println("=== Writing test files ===");
        writeParquetSimple(localFs, localPath, localConf);
        writeParquetSimple(seaweedFs, seaweedPath, seaweedConf);
        System.out.println("✅ Files written");

        OperationLog localLog = new OperationLog();
        OperationLog seaweedLog = new OperationLog();

        // Read from local filesystem with logging
        System.out.println("\n=== Reading from LOCAL filesystem ===");
        readParquetWithLogging(localFs, localPath, localLog, "LOCAL");

        System.out.println("\n=== Reading from SEAWEEDFS ===");
        readParquetWithLogging(seaweedFs, seaweedPath, seaweedLog, "SEAWEED");

        // Print logs
        localLog.print("LOCAL READ OPERATIONS");
        seaweedLog.print("SEAWEEDFS READ OPERATIONS");

        // Compare
        localLog.compare(seaweedLog, "LOCAL", "SEAWEEDFS");

        // Cleanup
        localFs.delete(localPath, false);
        seaweedFs.delete(seaweedPath, false);

        localFs.close();
        seaweedFs.close();

        System.out.println("\n=== Test Complete ===");
    }

    private void writeParquetWithLogging(FileSystem fs, Path path, Configuration conf,
            OperationLog log, String name) throws IOException {
        // We can't easily intercept ParquetWriter's internal stream usage,
        // but we can log the file operations
        log.log(name + " START WRITE");

        GroupWriteSupport.setSchema(SCHEMA, conf);

        try (ParquetWriter<Group> writer = org.apache.parquet.hadoop.example.ExampleParquetWriter.builder(path)
                .withConf(conf)
                .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE)
                .build()) {

            SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);

            log.log("WRITE ROW 1");
            Group group1 = factory.newGroup()
                    .append("id", 1)
                    .append("name", "Alice")
                    .append("age", 30);
            writer.write(group1);

            log.log("WRITE ROW 2");
            Group group2 = factory.newGroup()
                    .append("id", 2)
                    .append("name", "Bob")
                    .append("age", 25);
            writer.write(group2);

            log.log("WRITE ROW 3");
            Group group3 = factory.newGroup()
                    .append("id", 3)
                    .append("name", "Charlie")
                    .append("age", 35);
            writer.write(group3);

            log.log("CLOSE WRITER");
        }

        // Check final file size
        org.apache.hadoop.fs.FileStatus status = fs.getFileStatus(path);
        log.log(String.format("FINAL FILE SIZE: %d bytes", status.getLen()));
    }

    private void writeParquetSimple(FileSystem fs, Path path, Configuration conf) throws IOException {
        GroupWriteSupport.setSchema(SCHEMA, conf);

        try (ParquetWriter<Group> writer = org.apache.parquet.hadoop.example.ExampleParquetWriter.builder(path)
                .withConf(conf)
                .withWriteMode(org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE)
                .build()) {

            SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);

            writer.write(factory.newGroup().append("id", 1).append("name", "Alice").append("age", 30));
            writer.write(factory.newGroup().append("id", 2).append("name", "Bob").append("age", 25));
            writer.write(factory.newGroup().append("id", 3).append("name", "Charlie").append("age", 35));
        }
    }

    private void readParquetWithLogging(FileSystem fs, Path path, OperationLog log, String name) throws IOException {
        log.log(name + " START READ");

        // Read file in chunks to see the pattern
        try (FSDataInputStream in = fs.open(path)) {
            byte[] buffer = new byte[256];
            int totalRead = 0;
            int chunkNum = 0;

            while (true) {
                long posBefore = in.getPos();
                int bytesRead = in.read(buffer);

                if (bytesRead == -1) {
                    log.log(String.format("READ CHUNK %d: EOF at pos=%d", chunkNum, posBefore));
                    break;
                }

                totalRead += bytesRead;
                log.log(String.format("READ CHUNK %d: %d bytes at pos %d→%d",
                        chunkNum, bytesRead, posBefore, in.getPos()));
                chunkNum++;
            }

            log.log(String.format("TOTAL READ: %d bytes in %d chunks", totalRead, chunkNum));
        }
    }
}
