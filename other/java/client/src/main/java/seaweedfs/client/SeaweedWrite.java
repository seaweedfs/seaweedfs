package seaweedfs.client;

import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
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
        FilerProto.AssignVolumeResponse response = filerClient.getBlockingStub().assignVolume(
                FilerProto.AssignVolumeRequest.newBuilder()
                        .setCollection(Strings.isNullOrEmpty(collection) ? filerClient.getCollection() : collection)
                        .setReplication(Strings.isNullOrEmpty(replication) ? filerClient.getReplication() : replication)
                        .setDataCenter("")
                        .setTtlSec(0)
                        .setPath(path)
                        .build());

        if (!Strings.isNullOrEmpty(response.getError())) {
            throw new IOException(response.getError());
        }

        String fileId = response.getFileId();
        String auth = response.getAuth();

        String targetUrl = filerClient.getChunkUrl(fileId, response.getLocation().getUrl(), response.getLocation().getPublicUrl());

        ByteString cipherKeyString = com.google.protobuf.ByteString.EMPTY;
        byte[] cipherKey = null;
        if (filerClient.isCipher()) {
            cipherKey = genCipherKey();
            cipherKeyString = ByteString.copyFrom(cipherKey);
        }

        String etag = multipartUpload(targetUrl, auth, bytes, bytesOffset, bytesLength, cipherKey);

        LOG.debug("write file chunk {} size {}", targetUrl, bytesLength);

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
            List<FilerProto.FileChunk> chunks = FileChunkManifest.maybeManifestize(filerClient, entry.getChunksList(), parentDirectory);
            entry.clearChunks();
            entry.addAllChunks(chunks);
            filerClient.getBlockingStub().createEntry(
                    FilerProto.CreateEntryRequest.newBuilder()
                            .setDirectory(parentDirectory)
                            .setEntry(entry)
                            .build()
            );
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
            post.addHeader("Authorization", "BEARER " + auth);
        }
        post.addHeader("Content-MD5", Base64.getEncoder().encodeToString(md.digest()));

        post.setEntity(MultipartEntityBuilder.create()
                .setMode(HttpMultipartMode.BROWSER_COMPATIBLE)
                .addBinaryBody("upload", inputStream)
                .build());

        CloseableHttpResponse response = SeaweedUtil.getClosableHttpClient().execute(post);

        try {
            if (response.getStatusLine().getStatusCode() / 100 != 2) {
                if (response.getEntity().getContentType() != null && response.getEntity().getContentType().getValue().equals("application/json")) {
                    throw new IOException(EntityUtils.toString(response.getEntity(), "UTF-8"));
                } else {
                    throw new IOException(response.getStatusLine().getReasonPhrase());
                }
            }

            String etag = response.getLastHeader("ETag").getValue();

            if (etag != null && etag.startsWith("\"") && etag.endsWith("\"")) {
                etag = etag.substring(1, etag.length() - 1);
            }

            EntityUtils.consume(response.getEntity());

            return etag;
        } finally {
            response.close();
            post.releaseConnection();
        }

    }

    private static byte[] genCipherKey() {
        byte[] b = new byte[32];
        random.nextBytes(b);
        return b;
    }
}
