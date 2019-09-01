package seaweedfs.client;

import java.util.List;

public class SeaweedFilerTest {
    public static void main(String[] args){

        FilerClient filerClient = new FilerClient("localhost", 18888);

        List<FilerProto.Entry> entries = filerClient.listEntries("/");

        for (FilerProto.Entry entry : entries) {
            System.out.println(entry.toString());
        }

    }
}
