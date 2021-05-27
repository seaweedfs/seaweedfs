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
import java.util.List;

public class SeaweedWrite {

    private static final Logger LOG = LoggerFactory.getLogger(SeaweedWrite.class);

    private static final SecureRandom random = new SecureRandom();

    public static void writeData(FilerProto.Entry.Builder entry,
                                 final String replication,
                                 final FilerClient filerClient,
                                 final long offset,
                                 final byte[] bytes,
                                 final long bytesOffset, final long bytesLength,
                                 final String path) throws IOException {
        FilerProto.FileChunk.Builder chunkBuilder = writeChunk(
                replication, filerClient, offset, bytes, bytesOffset, bytesLength, path);
        synchronized (entry) {
            entry.addChunks(chunkBuilder);
        }
    }

    public static FilerProto.FileChunk.Builder writeChunk(final String replication,
                                                          final FilerClient filerClient,
                                                          final long offset,
                                                          final byte[] bytes,
                                                          final long bytesOffset,
                                                          final long bytesLength,
                                                          final String path) throws IOException {
        FilerProto.AssignVolumeResponse response = filerClient.getBlockingStub().assignVolume(
                FilerProto.AssignVolumeRequest.newBuilder()
                        .setCollection(filerClient.getCollection())
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

        String targetUrl = filerClient.getChunkUrl(fileId, response.getUrl(), response.getPublicUrl());

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
                .setMtime(System.currentTimeMillis() / 10000L)
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

        InputStream inputStream = null;
        if (cipherKey == null || cipherKey.length == 0) {
            inputStream = new ByteArrayInputStream(bytes, (int) bytesOffset, (int) bytesLength);
        } else {
            try {
                byte[] encryptedBytes = SeaweedCipher.encrypt(bytes, (int) bytesOffset, (int) bytesLength, cipherKey);
                inputStream = new ByteArrayInputStream(encryptedBytes, 0, encryptedBytes.length);
            } catch (Exception e) {
                throw new IOException("fail to encrypt data", e);
            }
        }

        HttpPost post = new HttpPost(targetUrl);
        if (auth != null && auth.length() != 0) {
            post.addHeader("Authorization", "BEARER " + auth);
        }

        post.setEntity(MultipartEntityBuilder.create()
                .setMode(HttpMultipartMode.BROWSER_COMPATIBLE)
                .addBinaryBody("upload", inputStream)
                .build());

        CloseableHttpResponse response = SeaweedUtil.getClosableHttpClient().execute(post);

        try {

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
