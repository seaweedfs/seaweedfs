package seaweedfs.client;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class SeaweedWriteIntegrationTest {
    private static final byte[] BYTES = "prefix-upload\u0000payload-suffix".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PAYLOAD = Arrays.copyOfRange(BYTES, 7, BYTES.length - 7);
    private static final String PROOF = "refused-before-append";

    @Test(timeout = 30000)
    public void testProvenRefusalReassignsOnce() throws Exception {
        for (boolean cipher : new boolean[]{false, true}) {
            try (UploadServer server = new UploadServer(cipher, 1, 409, PROOF, false)) {
                FilerProto.Entry.Builder entry = server.write();
                server.assertAttempts(2);
                assertEquals(1, entry.getChunksCount());
                FilerProto.FileChunk chunk = entry.getChunks(0);
                assertEquals("7,200000000", chunk.getFileId());
                assertEquals(23, chunk.getOffset());
                assertEquals(PAYLOAD.length, chunk.getSize());
                assertEquals("uploaded", chunk.getETag());
                byte[] uploaded = server.payloads.get(1);
                if (cipher) {
                    uploaded = SeaweedCipher.decrypt(uploaded, chunk.getCipherKey().toByteArray());
                } else {
                    assertArrayEquals(PAYLOAD, server.payloads.get(0));
                }
                assertArrayEquals(PAYLOAD, uploaded);
            }
        }
    }

    @Test(timeout = 30000)
    public void testUnprovenConflictDoesNotRetry() throws Exception {
        for (String outcome : new String[]{null, "unknown", "REFUSED-BEFORE-APPEND"}) {
            try (UploadServer server = new UploadServer(false, 1, 409, outcome, false)) {
                server.assertWriteFails(1);
            }
        }
    }

    @Test(timeout = 30000)
    public void testRepeatedProvenRefusalDoesNotRetryAgain() throws Exception {
        try (UploadServer server = new UploadServer(false, 2, 409, PROOF, false)) {
            server.assertWriteFails(2);
        }
    }

    @Test(timeout = 30000)
    public void testTruncatedConflictRetainsRetryProof() throws Exception {
        try (UploadServer server = new UploadServer(false, 1, 409, null, true)) {
            assertNotNull(server.assertWriteFails(1).getCause());
        }
        try (UploadServer server = new UploadServer(false, 1, 409, PROOF, true)) {
            assertEquals(1, server.write().getChunksCount());
            server.assertAttempts(2);
        }
    }

    @Test(timeout = 30000)
    public void testOtherUploadErrorsKeepExistingRetry() throws Exception {
        try (UploadServer server = new UploadServer(false, 1, 500, PROOF, false)) {
            assertEquals(1, server.write().getChunksCount());
            server.assertAttempts(2);
        }
    }

    private static final class UploadServer implements AutoCloseable {
        private final AtomicInteger assignments = new AtomicInteger();
        private final AtomicInteger uploads = new AtomicInteger();
        private final List<byte[]> payloads = Collections.synchronizedList(new ArrayList<>());
        private final List<String> paths = Collections.synchronizedList(new ArrayList<>());
        private final List<String> auth = Collections.synchronizedList(new ArrayList<>());
        private final List<String> digests = Collections.synchronizedList(new ArrayList<>());
        private final HttpServer volume;
        private final Server filer;
        private final FilerClient client;

        private UploadServer(boolean cipher, int refusals, int status, String outcome, boolean truncated)
                throws IOException {
            volume = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            volume.createContext("/", exchange -> upload(exchange, refusals, status, outcome, truncated));
            volume.start();
            filer = ServerBuilder.forPort(0).addService(new SeaweedFilerGrpc.SeaweedFilerImplBase() {
                @Override
                public void getFilerConfiguration(FilerProto.GetFilerConfigurationRequest request,
                        StreamObserver<FilerProto.GetFilerConfigurationResponse> observer) {
                    observer.onNext(FilerProto.GetFilerConfigurationResponse.newBuilder().setCipher(cipher).build());
                    observer.onCompleted();
                }

                @Override
                public void assignVolume(FilerProto.AssignVolumeRequest request,
                        StreamObserver<FilerProto.AssignVolumeResponse> observer) {
                    int attempt = assignments.incrementAndGet();
                    observer.onNext(FilerProto.AssignVolumeResponse.newBuilder()
                            .setFileId("7," + attempt + "00000000")
                            .setAuth("token-" + attempt)
                            .setLocation(FilerProto.Location.newBuilder()
                                    .setUrl("127.0.0.1:" + volume.getAddress().getPort()))
                            .build());
                    observer.onCompleted();
                }
            }).build().start();
            client = new FilerClient("127.0.0.1", filer.getPort());
        }

        private void upload(HttpExchange exchange, int refusals, int status, String outcome, boolean truncated)
                throws IOException {
            try {
                ByteArrayOutputStream body = new ByteArrayOutputStream();
                byte[] buffer = new byte[1024];
                int count;
                while ((count = exchange.getRequestBody().read(buffer)) != -1) {
                    body.write(buffer, 0, count);
                }
                String multipart = new String(body.toByteArray(), StandardCharsets.ISO_8859_1);
                String boundary = exchange.getRequestHeaders().getFirst("Content-Type").split("boundary=")[1];
                boundary = boundary.replace("\"", "");
                int start = multipart.indexOf("\r\n\r\n") + 4;
                int end = multipart.lastIndexOf("\r\n--" + boundary);
                payloads.add(Arrays.copyOfRange(body.toByteArray(), start, end));
                paths.add(exchange.getRequestURI().getPath());
                auth.add(exchange.getRequestHeaders().getFirst("Authorization"));
                digests.add(exchange.getRequestHeaders().getFirst("Content-MD5"));
                int attempt = uploads.incrementAndGet();
                byte[] response = "{\"error\":\"read only; refused-before-append\"}"
                        .getBytes(StandardCharsets.UTF_8);
                if (attempt <= refusals) {
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    if (outcome != null) {
                        exchange.getResponseHeaders().add("X-Seaweed-Write-Outcome", outcome);
                    }
                    exchange.sendResponseHeaders(status, response.length + (truncated ? 20 : 0));
                } else {
                    exchange.getResponseHeaders().add("ETag", "\"uploaded\"");
                    exchange.sendResponseHeaders(201, response.length);
                }
                exchange.getResponseBody().write(response);
            } finally {
                exchange.close();
            }
        }

        private FilerProto.Entry.Builder write() throws IOException {
            FilerProto.Entry.Builder entry = FilerProto.Entry.newBuilder();
            SeaweedWrite.writeData(entry, "", "", client, 23, BYTES, 7, PAYLOAD.length, "/upload");
            return entry;
        }

        private IOException assertWriteFails(int attempts) throws Exception {
            FilerProto.Entry.Builder entry = FilerProto.Entry.newBuilder();
            IOException error = null;
            try {
                SeaweedWrite.writeData(entry, "", "", client, 23, BYTES, 7, PAYLOAD.length, "/upload");
                fail("expected upload conflict");
            } catch (IOException expected) {
                assertNotNull(expected.getMessage());
                error = expected;
            }
            assertEquals(0, entry.getChunksCount());
            assertAttempts(attempts);
            return error;
        }

        private void assertAttempts(int attempts) throws Exception {
            assertEquals(attempts, assignments.get());
            assertEquals(attempts, uploads.get());
            for (int i = 0; i < attempts; i++) {
                assertEquals("/7," + (i + 1) + "00000000", paths.get(i));
                assertEquals("BEARER token-" + (i + 1), auth.get(i));
                assertEquals(Base64.getEncoder().encodeToString(MessageDigest.getInstance("MD5")
                        .digest(payloads.get(i))), digests.get(i));
            }
        }

        @Override
        public void close() throws InterruptedException {
            try {
                client.shutdown();
            } finally {
                filer.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
                volume.stop(0);
            }
        }
    }
}
