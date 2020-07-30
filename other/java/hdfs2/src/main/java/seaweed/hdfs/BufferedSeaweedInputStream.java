package seaweed.hdfs;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import java.io.BufferedInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

class BufferedSeaweedInputStream extends FilterInputStream implements Seekable, PositionedReadable {

    SeaweedInputStream t;

    protected BufferedSeaweedInputStream(InputStream in, int bufferSize) {
        super(new BufferedInputStream(in, bufferSize));
        t = (SeaweedInputStream)in;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {
        return this.t.read(position,buffer,offset,length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        this.t.readFully(position,buffer,offset,length);
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        this.t.readFully(position,buffer);
    }

    @Override
    public void seek(long pos) throws IOException {
        this.t.seek(pos);
    }

    @Override
    public long getPos() throws IOException {
        return this.t.getPos();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return this.t.seekToNewSource(targetPos);
    }
}
