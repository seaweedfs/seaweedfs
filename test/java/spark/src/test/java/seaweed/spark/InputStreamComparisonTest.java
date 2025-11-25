package seaweed.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Compare InputStream behavior between local disk and SeaweedFS
 * to understand why Spark's ParquetFileReader fails with SeaweedFS.
 */
public class InputStreamComparisonTest extends SparkTestBase {

    private static class ReadOperation {
        String source;
        String operation;
        long position;
        int requestedBytes;
        int returnedBytes;
        boolean isEOF;
        long timestamp;

        ReadOperation(String source, String operation, long position, int requestedBytes,
                int returnedBytes, boolean isEOF) {
            this.source = source;
            this.operation = operation;
            this.position = position;
            this.requestedBytes = requestedBytes;
            this.returnedBytes = returnedBytes;
            this.isEOF = isEOF;
            this.timestamp = System.nanoTime();
        }

        @Override
        public String toString() {
            return String.format("[%s] %s: pos=%d, requested=%d, returned=%d, EOF=%b",
                    source, operation, position, requestedBytes, returnedBytes, isEOF);
        }
    }

    private static class LoggingInputStream extends InputStream {
        private final FSDataInputStream wrapped;
        private final String source;
        private final List<ReadOperation> operations;
        private long position = 0;

        LoggingInputStream(FSDataInputStream wrapped, String source, List<ReadOperation> operations) {
            this.wrapped = wrapped;
            this.source = source;
            this.operations = operations;
        }

        @Override
        public int read() throws IOException {
            int result = wrapped.read();
            operations.add(new ReadOperation(source, "read()", position, 1,
                    result == -1 ? 0 : 1, result == -1));
            if (result != -1)
                position++;
            return result;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int result = wrapped.read(b, off, len);
            operations.add(new ReadOperation(source, "read(byte[])", position, len,
                    result == -1 ? 0 : result, result == -1));
            if (result > 0)
                position += result;
            return result;
        }

        public int read(ByteBuffer buf) throws IOException {
            int requested = buf.remaining();
            long startPos = position;

            // Use reflection to call read(ByteBuffer) if available
            try {
                java.lang.reflect.Method method = wrapped.getClass().getMethod("read", ByteBuffer.class);
                int result = (int) method.invoke(wrapped, buf);
                operations.add(new ReadOperation(source, "read(ByteBuffer)", startPos, requested,
                        result == -1 ? 0 : result, result == -1));
                if (result > 0)
                    position += result;
                return result;
            } catch (Exception e) {
                // Fallback to byte array read
                byte[] temp = new byte[requested];
                int result = wrapped.read(temp, 0, requested);
                if (result > 0) {
                    buf.put(temp, 0, result);
                }
                operations.add(new ReadOperation(source, "read(ByteBuffer-fallback)", startPos, requested,
                        result == -1 ? 0 : result, result == -1));
                if (result > 0)
                    position += result;
                return result;
            }
        }

        @Override
        public long skip(long n) throws IOException {
            long result = wrapped.skip(n);
            operations.add(new ReadOperation(source, "skip()", position, (int) n, (int) result, false));
            position += result;
            return result;
        }

        @Override
        public int available() throws IOException {
            int result = wrapped.available();
            operations.add(new ReadOperation(source, "available()", position, 0, result, false));
            return result;
        }

        @Override
        public void close() throws IOException {
            operations.add(new ReadOperation(source, "close()", position, 0, 0, false));
            wrapped.close();
        }

        public void seek(long pos) throws IOException {
            wrapped.seek(pos);
            operations.add(new ReadOperation(source, "seek()", position, 0, 0, false));
            position = pos;
        }

        public long getPos() throws IOException {
            long pos = wrapped.getPos();
            operations.add(new ReadOperation(source, "getPos()", position, 0, 0, false));
            return pos;
        }
    }

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
    public void testCompareInputStreamBehavior() throws Exception {
        skipIfTestsDisabled();

        System.out.println("\n╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║  REAL-TIME INPUTSTREAM COMPARISON: LOCAL vs SEAWEEDFS       ║");
        System.out.println("╚══════════════════════════════════════════════════════════════╝");

        // Write a Parquet file to both locations
        System.out.println("\n1. Writing identical Parquet files...");

        List<SparkSQLTest.Employee> employees = java.util.Arrays.asList(
                new SparkSQLTest.Employee(1, "Alice", "Engineering", 100000),
                new SparkSQLTest.Employee(2, "Bob", "Sales", 80000),
                new SparkSQLTest.Employee(3, "Charlie", "Engineering", 120000),
                new SparkSQLTest.Employee(4, "David", "Sales", 75000));

        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> df = spark.createDataFrame(employees,
                SparkSQLTest.Employee.class);

        String localPath = "file:///workspace/target/test-output/comparison-local";
        String seaweedPath = getTestPath("comparison-seaweed");

        // Ensure directory exists
        new java.io.File("/workspace/target/test-output").mkdirs();

        df.write().mode(org.apache.spark.sql.SaveMode.Overwrite).parquet(localPath);
        df.write().mode(org.apache.spark.sql.SaveMode.Overwrite).parquet(seaweedPath);

        System.out.println("   ✅ Files written");

        // Find the actual parquet files
        Configuration conf = new Configuration();
        FileSystem localFs = FileSystem.getLocal(conf);

        conf.set("fs.seaweedfs.impl", "seaweed.hdfs.SeaweedFileSystem");
        conf.set("fs.seaweed.filer.host", SEAWEEDFS_HOST);
        conf.set("fs.seaweed.filer.port", String.valueOf(SEAWEEDFS_PORT));
        FileSystem seaweedFs = FileSystem.get(URI.create(String.format("seaweedfs://%s:%s",
                SEAWEEDFS_HOST, SEAWEEDFS_PORT)), conf);

        // Find parquet files
        Path localFile = findParquetFile(localFs, new Path(localPath));
        Path seaweedFile = findParquetFile(seaweedFs, new Path(seaweedPath));

        assertNotNull("Local parquet file not found", localFile);
        assertNotNull("SeaweedFS parquet file not found", seaweedFile);

        System.out.println("\n2. Comparing file sizes...");
        long localSize = localFs.getFileStatus(localFile).getLen();
        long seaweedSize = seaweedFs.getFileStatus(seaweedFile).getLen();
        System.out.println("   Local:    " + localSize + " bytes");
        System.out.println("   SeaweedFS: " + seaweedSize + " bytes");

        // NOW: Open both streams with logging wrappers
        List<ReadOperation> localOps = new ArrayList<>();
        List<ReadOperation> seaweedOps = new ArrayList<>();

        System.out.println("\n3. Opening streams with logging wrappers...");

        FSDataInputStream localStream = localFs.open(localFile);
        FSDataInputStream seaweedStream = seaweedFs.open(seaweedFile);

        LoggingInputStream localLogging = new LoggingInputStream(localStream, "LOCAL", localOps);
        LoggingInputStream seaweedLogging = new LoggingInputStream(seaweedStream, "SEAWEED", seaweedOps);

        System.out.println("   ✅ Streams opened");

        // Create a dual-reader that calls both and compares
        System.out.println("\n4. Performing synchronized read operations...");
        System.out.println("   (Each operation is called on BOTH streams and results are compared)\n");

        int opCount = 0;
        boolean mismatchFound = false;

        // Operation 1: Read 4 bytes (magic bytes)
        opCount++;
        System.out.println("   Op " + opCount + ": read(4 bytes) - Reading magic bytes");
        byte[] localBuf1 = new byte[4];
        byte[] seaweedBuf1 = new byte[4];
        int localRead1 = localLogging.read(localBuf1, 0, 4);
        int seaweedRead1 = seaweedLogging.read(seaweedBuf1, 0, 4);
        System.out.println("      LOCAL:    returned " + localRead1 + " bytes: " + bytesToHex(localBuf1));
        System.out.println("      SEAWEED:  returned " + seaweedRead1 + " bytes: " + bytesToHex(seaweedBuf1));
        if (localRead1 != seaweedRead1 || !java.util.Arrays.equals(localBuf1, seaweedBuf1)) {
            System.out.println("      ❌ MISMATCH!");
            mismatchFound = true;
        } else {
            System.out.println("      ✅ Match");
        }

        // Operation 2: Seek to end - 8 bytes (footer length + magic)
        opCount++;
        System.out.println("\n   Op " + opCount + ": seek(fileSize - 8) - Jump to footer");
        localLogging.seek(localSize - 8);
        seaweedLogging.seek(seaweedSize - 8);
        System.out.println("      LOCAL:    seeked to " + localLogging.getPos());
        System.out.println("      SEAWEED:  seeked to " + seaweedLogging.getPos());
        if (localLogging.getPos() != seaweedLogging.getPos()) {
            System.out.println("      ❌ MISMATCH!");
            mismatchFound = true;
        } else {
            System.out.println("      ✅ Match");
        }

        // Operation 3: Read 8 bytes (footer length + magic)
        opCount++;
        System.out.println("\n   Op " + opCount + ": read(8 bytes) - Reading footer length + magic");
        byte[] localBuf2 = new byte[8];
        byte[] seaweedBuf2 = new byte[8];
        int localRead2 = localLogging.read(localBuf2, 0, 8);
        int seaweedRead2 = seaweedLogging.read(seaweedBuf2, 0, 8);
        System.out.println("      LOCAL:    returned " + localRead2 + " bytes: " + bytesToHex(localBuf2));
        System.out.println("      SEAWEED:  returned " + seaweedRead2 + " bytes: " + bytesToHex(seaweedBuf2));
        if (localRead2 != seaweedRead2 || !java.util.Arrays.equals(localBuf2, seaweedBuf2)) {
            System.out.println("      ❌ MISMATCH!");
            mismatchFound = true;
        } else {
            System.out.println("      ✅ Match");
        }

        // Operation 4: Calculate footer offset and seek to it
        int footerLength = java.nio.ByteBuffer.wrap(localBuf2, 0, 4).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt();
        long footerOffset = localSize - 8 - footerLength;

        opCount++;
        System.out.println("\n   Op " + opCount + ": seek(" + footerOffset + ") - Jump to footer start");
        System.out.println("      Footer length: " + footerLength + " bytes");
        localLogging.seek(footerOffset);
        seaweedLogging.seek(footerOffset);
        System.out.println("      LOCAL:    seeked to " + localLogging.getPos());
        System.out.println("      SEAWEED:  seeked to " + seaweedLogging.getPos());
        if (localLogging.getPos() != seaweedLogging.getPos()) {
            System.out.println("      ❌ MISMATCH!");
            mismatchFound = true;
        } else {
            System.out.println("      ✅ Match");
        }

        // Operation 5: Read entire footer
        opCount++;
        System.out.println("\n   Op " + opCount + ": read(" + footerLength + " bytes) - Reading footer metadata");
        byte[] localFooter = new byte[footerLength];
        byte[] seaweedFooter = new byte[footerLength];
        int localRead3 = localLogging.read(localFooter, 0, footerLength);
        int seaweedRead3 = seaweedLogging.read(seaweedFooter, 0, footerLength);
        System.out.println("      LOCAL:    returned " + localRead3 + " bytes");
        System.out.println("      SEAWEED:  returned " + seaweedRead3 + " bytes");
        if (localRead3 != seaweedRead3 || !java.util.Arrays.equals(localFooter, seaweedFooter)) {
            System.out.println("      ❌ MISMATCH!");
            mismatchFound = true;
            // Show first difference
            for (int i = 0; i < Math.min(localRead3, seaweedRead3); i++) {
                if (localFooter[i] != seaweedFooter[i]) {
                    System.out.println("      First difference at byte " + i + ": LOCAL=" +
                            String.format("0x%02X", localFooter[i]) + " SEAWEED=" +
                            String.format("0x%02X", seaweedFooter[i]));
                    break;
                }
            }
        } else {
            System.out.println("      ✅ Match - Footer metadata is IDENTICAL");
        }

        // Operation 6: Try reading past EOF
        opCount++;
        System.out.println("\n   Op " + opCount + ": read(100 bytes) - Try reading past EOF");
        byte[] localBuf3 = new byte[100];
        byte[] seaweedBuf3 = new byte[100];
        int localRead4 = localLogging.read(localBuf3, 0, 100);
        int seaweedRead4 = seaweedLogging.read(seaweedBuf3, 0, 100);
        System.out.println("      LOCAL:    returned " + localRead4);
        System.out.println("      SEAWEED:  returned " + seaweedRead4);
        if (localRead4 != seaweedRead4) {
            System.out.println("      ❌ MISMATCH!");
            mismatchFound = true;
        } else {
            System.out.println("      ✅ Match - Both returned EOF");
        }

        localLogging.close();
        seaweedLogging.close();

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
        for (int i = 0; i < Math.max(localOps.size(), seaweedOps.size()); i++) {
            if (i < localOps.size()) {
                System.out.println("   " + localOps.get(i));
            }
            if (i < seaweedOps.size()) {
                System.out.println("   " + seaweedOps.get(i));
            }
        }

        assertFalse("Streams should behave identically", mismatchFound);
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X ", b));
        }
        return sb.toString().trim();
    }

    private Path findParquetFile(FileSystem fs, Path dir) throws IOException {
        org.apache.hadoop.fs.FileStatus[] files = fs.listStatus(dir);
        for (org.apache.hadoop.fs.FileStatus file : files) {
            if (file.getPath().getName().endsWith(".parquet") &&
                    !file.getPath().getName().startsWith("_")) {
                return file.getPath();
            }
        }
        return null;
    }
}
