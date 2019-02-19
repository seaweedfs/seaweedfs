package seaweedfs.client;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class SeaweedWrite {

    public static void writeData(FilerProto.Entry.Builder entry,
                                 final String replication,
                                 final FilerGrpcClient filerGrpcClient,
                                 final long offset,
                                 final byte[] bytes,
                                 final long bytesOffset, final long bytesLength) throws IOException {
        FilerProto.AssignVolumeResponse response = filerGrpcClient.getBlockingStub().assignVolume(
                FilerProto.AssignVolumeRequest.newBuilder()
                        .setCollection("")
                        .setReplication(replication)
                        .setDataCenter("")
                        .setReplication("")
                        .setTtlSec(0)
                        .build());
        String fileId = response.getFileId();
        String url = response.getUrl();
        String auth = response.getAuth();
        String targetUrl = String.format("http://%s/%s", url, fileId);

        String etag = multipartUpload(targetUrl, auth, bytes, bytesOffset, bytesLength);

        entry.addChunks(FilerProto.FileChunk.newBuilder()
                .setFileId(fileId)
                .setOffset(offset)
                .setSize(bytesLength)
                .setMtime(System.currentTimeMillis() / 10000L)
                .setETag(etag)
        );

    }

    public static void writeMeta(final FilerGrpcClient filerGrpcClient,
                                 final String parentDirectory, final FilerProto.Entry.Builder entry) {
        filerGrpcClient.getBlockingStub().createEntry(
                FilerProto.CreateEntryRequest.newBuilder()
                        .setDirectory(parentDirectory)
                        .setEntry(entry)
                        .build()
        );
    }

    private static String multipartUpload(String targetUrl,
                                          String auth,
                                          final byte[] bytes,
                                          final long bytesOffset, final long bytesLength) throws IOException {

        CloseableHttpClient client = HttpClientBuilder.create().setUserAgent("hdfs-client").build();

        InputStream inputStream = new ByteArrayInputStream(bytes, (int) bytesOffset, (int) bytesLength);

        HttpPost post = new HttpPost(targetUrl);
        if (auth != null && auth.length() != 0) {
            post.addHeader("Authorization", "BEARER " + auth);
        }

        post.setEntity(MultipartEntityBuilder.create()
                .setMode(HttpMultipartMode.BROWSER_COMPATIBLE)
                .addBinaryBody("upload", inputStream)
                .build());

        try {
            HttpResponse response = client.execute(post);

            String etag = response.getLastHeader("ETag").getValue();

            if (etag != null && etag.startsWith("\"") && etag.endsWith("\"")) {
                etag = etag.substring(1, etag.length() - 1);
            }

            return etag;
        } finally {
            client.close();
        }

    }
}
