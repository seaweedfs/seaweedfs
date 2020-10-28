package seaweedfs.file;

import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MmapFileTest {

    static File dir = new File("/Users/chris/tmp/mm/dev");

    @Test
    public void testMmap() {
        try {
            System.out.println("starting ...");

            File f = new File(dir, "mmap_file.txt");
            RandomAccessFile raf = new RandomAccessFile(f, "rw");
            FileChannel fc = raf.getChannel();
            MappedByteBuffer mbf = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
            fc.close();
            raf.close();

            FileOutputStream fos = new FileOutputStream(f);
            fos.write("abcdefg".getBytes());
            fos.close();
            System.out.println("completed!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testBigMmap() throws IOException {
        /*

// new file
I0817 09:48:02 25175 dir.go:147] create /dev/mmap_big.txt: OpenReadWrite+OpenCreate
I0817 09:48:02 25175 wfs.go:116] AcquireHandle /dev/mmap_big.txt uid=502 gid=20
I0817 09:48:02 25175 file.go:62] file Attr /dev/mmap_big.txt, open:1, size: 0
I0817 09:48:02 25175 meta_cache_subscribe.go:32] creating /dev/mmap_big.txt

//get channel
I0817 09:48:26 25175 file.go:62] file Attr /dev/mmap_big.txt, open:1, size: 0

I0817 09:48:32 25175 file.go:62] file Attr /dev/mmap_big.txt, open:1, size: 0
I0817 09:48:32 25175 wfs.go:116] AcquireHandle /dev/mmap_big.txt uid=0 gid=0
I0817 09:48:32 25175 filehandle.go:160] Release /dev/mmap_big.txt fh 14968871991130164560

//fileChannel.map
I0817 09:49:18 25175 file.go:62] file Attr /dev/mmap_big.txt, open:1, size: 0
I0817 09:49:18 25175 file.go:112] /dev/mmap_big.txt file setattr set size=262144 chunks=0
I0817 09:49:18 25175 file.go:62] file Attr /dev/mmap_big.txt, open:1, size: 262144
I0817 09:49:18 25175 file.go:62] file Attr /dev/mmap_big.txt, open:1, size: 262144
I0817 09:49:18 25175 file.go:62] file Attr /dev/mmap_big.txt, open:1, size: 262144

// buffer.put
I0817 09:49:49 25175 filehandle.go:57] /dev/mmap_big.txt read fh 14968871991130164560: [0,32768) size 32768 resp.Data len=0 cap=32768
I0817 09:49:49 25175 reader_at.go:113] zero2 [0,32768)
I0817 09:49:50 25175 file.go:62] file Attr /dev/mmap_big.txt, open:1, size: 262144

I0817 09:49:53 25175 file.go:233] /dev/mmap_big.txt fsync file Fsync [ID=0x4 Node=0xe Uid=0 Gid=0 Pid=0] Handle 0x2 Flags 1

//close
I0817 09:50:14 25175 file.go:62] file Attr /dev/mmap_big.txt, open:1, size: 262144
I0817 09:50:14 25175 dirty_page.go:130] saveToStorage /dev/mmap_big.txt 1,315b69812039e5 [0,4096) of 262144 bytes
I0817 09:50:14 25175 file.go:274] /dev/mmap_big.txt existing 0 chunks adds 1 more
I0817 09:50:14 25175 filehandle.go:218] /dev/mmap_big.txt set chunks: 1
I0817 09:50:14 25175 filehandle.go:220] /dev/mmap_big.txt chunks 0: 1,315b69812039e5 [0,4096)
I0817 09:50:14 25175 meta_cache_subscribe.go:23] deleting /dev/mmap_big.txt
I0817 09:50:14 25175 meta_cache_subscribe.go:32] creating /dev/mmap_big.txt

// end of test
I0817 09:50:41 25175 file.go:62] file Attr /dev/mmap_big.txt, open:1, size: 262144
I0817 09:50:41 25175 filehandle.go:160] Release /dev/mmap_big.txt fh 14968871991130164560

         */
        // Create file object
        File file = new File(dir, "mmap_big.txt");

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw")) {
            // Get file channel in read-write mode
            FileChannel fileChannel = randomAccessFile.getChannel();

            // Get direct byte buffer access using channel.map() operation
            MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 4096 * 8 * 8);

            //Write the content using put methods
            buffer.put("howtodoinjava.com".getBytes());
        }

/*
> meta.cat /dev/mmap_big.txt
{
  "name": "mmap_big.txt",
  "isDirectory": false,
  "chunks": [
    {
      "fileId": "1,315b69812039e5",
      "offset": "0",
      "size": "4096",
      "mtime": "1597683014026365000",
      "eTag": "985ab0ac",
      "sourceFileId": "",
      "fid": {
        "volumeId": 1,
        "fileKey": "3234665",
        "cookie": 2166372837
      },
      "sourceFid": null,
      "cipherKey": null,
      "isCompressed": true,
      "isChunkManifest": false
    }
  ],
  "attributes": {
    "fileSize": "262144",
    "mtime": "1597683014",
    "fileMode": 420,
    "uid": 502,
    "gid": 20,
    "crtime": "1597682882",
    "mime": "application/octet-stream",
    "replication": "",
    "collection": "",
    "ttlSec": 0,
    "userName": "",
    "groupName": [
    ],
    "symlinkTarget": "",
    "md5": null
  },
  "extended": {
  }
}
 */

    }
}
