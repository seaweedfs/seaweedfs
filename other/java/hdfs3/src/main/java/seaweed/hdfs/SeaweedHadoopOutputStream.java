package seaweed.hdfs;

// adapted from org.apache.hadoop.fs.azurebfs.services.AbfsOutputStream

import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.Syncable;
import seaweedfs.client.FilerClient;
import seaweedfs.client.FilerProto;
import seaweedfs.client.SeaweedOutputStream;

import java.io.IOException;
import java.util.Locale;

public class SeaweedHadoopOutputStream extends SeaweedOutputStream implements Syncable, StreamCapabilities {

    public SeaweedHadoopOutputStream(FilerClient filerClient, final String path, FilerProto.Entry.Builder entry,
                                     final long position, final int bufferSize, final String replication) {
        super(filerClient, path, entry, position, bufferSize, replication);
    }

    /**
     * Similar to posix fsync, flush out the data in client's user buffer
     * all the way to the disk device (but the disk may have it in its cache).
     *
     * @throws IOException if error occurs
     */
    @Override
    public void hsync() throws IOException {
        if (supportFlush) {
            flushInternal();
        }
    }

    /**
     * Flush out the data in client's user buffer. After the return of
     * this call, new readers will see the data.
     *
     * @throws IOException if any error occurs
     */
    @Override
    public void hflush() throws IOException {
        if (supportFlush) {
            flushInternal();
        }
    }

    /**
     * Query the stream for a specific capability.
     *
     * @param capability string to query the stream support for.
     * @return true for hsync and hflush.
     */
    @Override
    public boolean hasCapability(String capability) {
        switch (capability.toLowerCase(Locale.ENGLISH)) {
            case StreamCapabilities.HSYNC:
            case StreamCapabilities.HFLUSH:
                return supportFlush;
            default:
                return false;
        }
    }

}
