package seaweedfs.client;

import java.util.List;

public class SeaweedFilerTest {
    public static void main(String[] args){

        FilerClient filerClient = new FilerClient("localhost", 18888);

        List<FilerProto.Entry> entries = filerClient.listEntries("/");

        for (FilerProto.Entry entry : entries) {
            System.out.println(entry.toString());
        }

        filerClient.mkdirs("/new_folder", 0755);
        filerClient.touch("/new_folder/new_empty_file", 0755);
        filerClient.touch("/new_folder/new_empty_file2", 0755);
        filerClient.rm("/new_folder/new_empty_file", false, true);
        filerClient.rm("/new_folder", true, true);

    }
}
