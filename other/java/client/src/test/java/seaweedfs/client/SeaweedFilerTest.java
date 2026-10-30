package seaweedfs.client;

import com.google.protobuf.ByteString;
import java.awt.image.DataBuffer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Test;
import seaweedfs.client.FilerProto.Entry;
import seaweedfs.client.FilerProto.FuseAttributes;

public class SeaweedFilerTest {
    private static FilerClient filerClient = new FilerClient("localhost", 18888);
    public static void main(String[] args){

        List<FilerProto.Entry> entries = filerClient.listEntries("/");

        for (FilerProto.Entry entry : entries) {
            System.out.println(entry.toString());
        }

        filerClient.mkdirs("/new_folder", 0755);
        filerClient.touch("/new_folder/new_empty_file", 0755);
        filerClient.touch("/new_folder/new_empty_file2", 0755);
        if(!filerClient.exists("/new_folder/new_empty_file")){
            System.out.println("/new_folder/new_empty_file should exists");
        }

        filerClient.rm("/new_folder/new_empty_file", false, true);
        filerClient.rm("/new_folder", true, true);
        if(filerClient.exists("/new_folder/new_empty_file")){
            System.out.println("/new_folder/new_empty_file should not exists");
        }
        if(!filerClient.exists("/")){
            System.out.println("/ should exists");
        }
    }

    @Test
    public void testExtendedKey() throws IOException {
        String content = "hello";
        String location = "/hello.txt";
        HashMap<String, String> metadata = new HashMap<>();
        // The storage format of key is Seaweed-mime, and the reading format is mime.
        metadata.put("mime", "text/plain");
        // The user-specified key "Seaweed-" prefix does not take effect
        // The storage format of key is Seaweed-name1, and the reading format is name1
        metadata.put("Seaweed-name1", "value1");

        long ts = System.currentTimeMillis() / 1000;
        String dir = SeaweedOutputStream.getParentDirectory(location);
        String name = SeaweedOutputStream.getFileName(location);
        FuseAttributes.Builder attributes = FuseAttributes.newBuilder()
            .setMtime(ts)
            .setCrtime(ts)
            .setFileMode(755);
        Entry.Builder entry = Entry.newBuilder()
            .setName(name)
            .setIsDirectory(false)
            .setAttributes(attributes.build());
        // put metadata
        for (Map.Entry<String, String> metadataEntry : metadata.entrySet()) {
            entry.putExtended(metadataEntry.getKey(),
                ByteString.copyFromUtf8(metadataEntry.getValue()));
        }
        try (SeaweedOutputStream outputStream = new SeaweedOutputStream(filerClient, location, entry, 0,
            1024, "")) {
            outputStream.write(content.getBytes());
        }


        Entry getEntry = filerClient.lookupEntry(dir, name);
        Assert.assertNotNull(getEntry);
        Map<String, ByteString> extendedMap = getEntry.getExtendedMap();
        HashSet<String> expectKeys = new HashSet<String>() {{
            add("mime");
            add("name1");
        }};
        Assert.assertEquals(expectKeys, extendedMap.keySet());

        filerClient.deleteEntry(dir, name, true, false, true);
    }
}
