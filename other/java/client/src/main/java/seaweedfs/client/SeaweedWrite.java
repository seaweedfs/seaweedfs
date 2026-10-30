package seaweedfs.client;

import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.SecureRandom;
import java.security.MessageDigest;
import java.util.List;
import java.util.Base64;

public class SeaweedWrite {

    private static final Logger LOG = LoggerFactory.getLogger(SeaweedWrite.class);

    private static final SecureRandom random = new SecureRandom();

    public static void writeData(FilerProto.Entry.Builder entry,
            final String replication,
            String collection,
            final FilerClient filerClient,
            final long offset,
            final byte[] bytes,
            final long bytesOffset, final long bytesLength,
            final String path) throws IOException {

        IOException lastException = null;
        boolean reassigned = false;
        for (long waitTime = 1000L; waitTime < 10 * 1000; waitTime += waitTime / 2) {
            try {
                FilerProto.FileChunk.Builder chunkBuilder = writeChunk(
                        replication, collection, filerClient, offset, bytes, bytesOffset, bytesLength, path);
                lastException = null;
                synchronized (entry) {
                    entry.addChunks(chunkBuilder);
                }
                break;
            } catch (IOException ioe) {
                LOG.debug("writeData:{}", ioe);
                lastException = ioe;
                if (ioe instanceof UploadException) {
                    UploadException error = (UploadException) ioe;
                    if (error.status == 409) {
                        if (!error.refusedBeforeAppend || reassigned) {
                            throw error;
                        }
                        reassigned = true;
                        continue;
                    }
                }
            }
            try {
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
            }
        }

        if (lastException != null) {
            throw lastException;
        }

    }

    public static FilerProto.FileChunk.Builder writeChunk(final String replication,
            final String collection,
            final FilerClient filerClient,
            final long offset,
            final byte[] bytes,
            final long bytesOffset,
            final long bytesLength,
            final String path) throws IOException {

        // Retry assignVolume call for transient network/server errors
        FilerProto.AssignVolumeResponse response = null;
        IOException lastException = null;
        int maxRetries = 3;

        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try {
                response = filerClient.getBlockingStub().assignVolume(
                        FilerProto.AssignVolumeRequest.newBuilder()
                                .setCollection(
                                        Strings.isNullOrEmpty(collection) ? filerClient.getCollection() : collection)
                                .setReplication(
                                        Strings.isNullOrEmpty(replication) ? filerClient.getReplication() : replication)
                                .setDataCenter("")
                                .setTtlSec(0)
                                .setPath(path)
                                .build());
                break; // Success, exit retry loop
            } catch (io.grpc.StatusRuntimeException e) {
                lastException = new IOException(
                        "assignVolume failed (attempt " + (attempt + 1) + "/" + maxRetries + "): " + e.getMessage(), e);
                if (attempt < maxRetries - 1) {
                    try {
                        Thread.sleep(100 * (attempt + 1)); // Exponential backoff: 100ms, 200ms, 300ms
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted during retry", ie);
                    }
                }
            }
        }

        if (response == null) {
            throw lastException != null ? lastException
                    : new IOException("assignVolume failed after " + maxRetries + " attempts");
        }

        if (!Strings.isNullOrEmpty(response.getError())) {
            throw new IOException(response.getError());
        }

        String fileId = response.getFileId();
        String auth = response.getAuth();

        String targetUrl = filerClient.getChunkUrl(fileId, response.getLocation().getUrl(),
                response.getLocation().getPublicUrl());

        ByteString cipherKeyString = com.google.protobuf.ByteString.EMPTY;
        byte[] cipherKey = null;
        if (filerClient.isCipher()) {
            cipherKey = genCipherKey();
            cipherKeyString = ByteString.copyFrom(cipherKey);
        }

        String etag = multipartUpload(targetUrl, auth, bytes, bytesOffset, bytesLength, cipherKey);

        return FilerProto.FileChunk.newBuilder()
                .setFileId(fileId)
                .setOffset(offset)
                .setSize(bytesLength)
                .setModifiedTsNs(System.nanoTime())
                .setETag(etag)
                .setCipherKey(cipherKeyString);
    }

    public static void writeMeta(final FilerClient filerClient,
            final String parentDirectory,
            final FilerProto.Entry.Builder entry) throws IOException {

        synchronized (entry) {
            List<FilerProto.FileChunk> chunks = FileChunkManifest.maybeManifestize(filerClient, entry.getChunksList(),
                    parentDirectory);

            entry.clearChunks();
            entry.addAllChunks(chunks);
            filerClient.getBlockingStub().createEntry(
                    FilerProto.CreateEntryRequest.newBuilder()
                            .setDirectory(parentDirectory)
                            .setEntry(entry)
                            .build());
        }
    }

    private static String multipartUpload(String targetUrl,
            String auth,
            final byte[] bytes,
            final long bytesOffset, final long bytesLength,
            byte[] cipherKey) throws IOException {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (java.security.NoSuchAlgorithmException e) {
        }

        InputStream inputStream = null;
        if (cipherKey == null || cipherKey.length == 0) {
            md.update(bytes, (int) bytesOffset, (int) bytesLength);
            inputStream = new ByteArrayInputStream(bytes, (int) bytesOffset, (int) bytesLength);
        } else {
            try {
                byte[] encryptedBytes = SeaweedCipher.encrypt(bytes, (int) bytesOffset, (int) bytesLength, cipherKey);
                md.update(encryptedBytes);
                inputStream = new ByteArrayInputStream(encryptedBytes, 0, encryptedBytes.length);
            } catch (Exception e) {
                throw new IOException("fail to encrypt data", e);
            }
        }

        HttpPost post = new HttpPost(targetUrl);
        if (auth != null && auth.length() != 0) {
            // Volume JWT and Basic Auth both use the Authorization header; the JWT wins
            // when present. Front the volume with a JWT-free path to use Basic Auth here.
            post.addHeader("Authorization", "BEARER " + auth);
        } else {
            String basicAuthHeader = FilerSecurityContext.getBasicAuthHeaderValue();
            if (basicAuthHeader != null) {
                post.addHeader("Authorization", basicAuthHeader);
            }
        }
        post.addHeader("Content-MD5", Base64.getEncoder().encodeToString(md.digest()));

        post.setEntity(MultipartEntityBuilder.create()
                .setMode(HttpMultipartMode.BROWSER_COMPATIBLE)
                .addBinaryBody("upload", inputStream)
                .build());

        try (CloseableHttpResponse response = SeaweedUtil.getClosableHttpClient().execute(post)) {
            int status = response.getStatusLine().getStatusCode();
            if (status / 100 != 2) {
                Header outcome = response.getFirstHeader("X-Seaweed-Write-Outcome");
                boolean refusedBeforeAppend = status == 409 && outcome != null
                        && "refused-before-append".equals(outcome.getValue());
                String message = response.getStatusLine().getReasonPhrase();
                IOException cause = null;
                HttpEntity entity = response.getEntity();
                try {
                    if (entity != null && entity.getContentType() != null
                            && entity.getContentType().getValue().equals("application/json")) {
                        message = EntityUtils.toString(entity, "UTF-8");
                    }
                } catch (IOException e) {
                    cause = e;
                }
                throw new UploadException(status, refusedBeforeAppend, message, cause);
            }

            String etag = response.getLastHeader("ETag").getValue();

            if (etag != null && etag.startsWith("\"") && etag.endsWith("\"")) {
                etag = etag.substring(1, etag.length() - 1);
            }

            EntityUtils.consume(response.getEntity());

            return etag;
        } finally {
            post.releaseConnection();
        }

    }

    private static final class UploadException extends IOException {
        private final int status;
        private final boolean refusedBeforeAppend;

        private UploadException(int status, boolean refusedBeforeAppend, String message, IOException cause) {
            super(message, cause);
            this.status = status;
            this.refusedBeforeAppend = refusedBeforeAppend;
        }
    }

    private static byte[] genCipherKey() {
        byte[] b = new byte[32];
        random.nextBytes(b);
        return b;
    }
}
