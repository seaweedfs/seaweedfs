package seaweedfs.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class FilerClient {

    private static final Logger LOG = LoggerFactory.getLogger(FilerClient.class);

    private final FilerGrpcClient filerGrpcClient;

    public FilerClient(String host, int grpcPort) {
        filerGrpcClient = new FilerGrpcClient(host, grpcPort);
    }

    public FilerClient(FilerGrpcClient filerGrpcClient) {
        this.filerGrpcClient = filerGrpcClient;
    }

    public boolean mkdirs(String path, int mode) {
        String currentUser = System.getProperty("user.name");
        return mkdirs(path, mode, 0, 0, currentUser, new String[]{});
    }

    public boolean mkdirs(String path, int mode, String userName, String[] groupNames) {
        return mkdirs(path, mode, 0, 0, userName, groupNames);
    }

    public boolean mkdirs(String path, int mode, int uid, int gid, String userName, String[] groupNames) {

        if ("/".equals(path)) {
            return true;
        }
        Path pathObject = Paths.get(path);
        String parent = pathObject.getParent().toString();
        String name = pathObject.getFileName().toString();

        mkdirs(parent, mode, uid, gid, userName, groupNames);

        FilerProto.Entry existingEntry = lookupEntry(parent, name);

        if (existingEntry != null) {
            return true;
        }

        return createEntry(
                parent,
                newDirectoryEntry(name, mode, uid, gid, userName, groupNames).build()
        );

    }

    public boolean mv(String oldPath, String newPath) {

        Path oldPathObject = Paths.get(oldPath);
        String oldParent = oldPathObject.getParent().toString();
        String oldName = oldPathObject.getFileName().toString();

        Path newPathObject = Paths.get(newPath);
        String newParent = newPathObject.getParent().toString();
        String newName = newPathObject.getFileName().toString();

        return atomicRenameEntry(oldParent, oldName, newParent, newName);

    }

    public boolean rm(String path, boolean isRecursive, boolean ignoreRecusiveError) {

        Path pathObject = Paths.get(path);
        String parent = pathObject.getParent().toString();
        String name = pathObject.getFileName().toString();

        return deleteEntry(
                parent,
                name,
                true,
                isRecursive,
                ignoreRecusiveError);
    }

    public boolean touch(String path, int mode) {
        String currentUser = System.getProperty("user.name");
        return touch(path, mode, 0, 0, currentUser, new String[]{});
    }

    public boolean touch(String path, int mode, int uid, int gid, String userName, String[] groupNames) {

        Path pathObject = Paths.get(path);
        String parent = pathObject.getParent().toString();
        String name = pathObject.getFileName().toString();

        FilerProto.Entry entry = lookupEntry(parent, name);
        if (entry == null) {
            return createEntry(
                    parent,
                    newFileEntry(name, mode, uid, gid, userName, groupNames).build()
            );
        }
        long now = System.currentTimeMillis() / 1000L;
        FilerProto.FuseAttributes.Builder attr = entry.getAttributes().toBuilder()
                .setMtime(now)
                .setUid(uid)
                .setGid(gid)
                .setUserName(userName)
                .clearGroupName()
                .addAllGroupName(Arrays.asList(groupNames));
        return updateEntry(parent, entry.toBuilder().setAttributes(attr).build());
    }

    public FilerProto.Entry.Builder newDirectoryEntry(String name, int mode,
                                                      int uid, int gid, String userName, String[] groupNames) {

        long now = System.currentTimeMillis() / 1000L;

        return FilerProto.Entry.newBuilder()
                .setName(name)
                .setIsDirectory(true)
                .setAttributes(FilerProto.FuseAttributes.newBuilder()
                        .setMtime(now)
                        .setCrtime(now)
                        .setUid(uid)
                        .setGid(gid)
                        .setFileMode(mode | 1 << 31)
                        .setUserName(userName)
                        .clearGroupName()
                        .addAllGroupName(Arrays.asList(groupNames)));
    }

    public FilerProto.Entry.Builder newFileEntry(String name, int mode,
                                                 int uid, int gid, String userName, String[] groupNames) {

        long now = System.currentTimeMillis() / 1000L;

        return FilerProto.Entry.newBuilder()
                .setName(name)
                .setIsDirectory(false)
                .setAttributes(FilerProto.FuseAttributes.newBuilder()
                        .setMtime(now)
                        .setCrtime(now)
                        .setUid(uid)
                        .setGid(gid)
                        .setFileMode(mode)
                        .setUserName(userName)
                        .clearGroupName()
                        .addAllGroupName(Arrays.asList(groupNames)));
    }

    public List<FilerProto.Entry> listEntries(String path) {
        List<FilerProto.Entry> results = new ArrayList<FilerProto.Entry>();
        String lastFileName = "";
        for (int limit = Integer.MAX_VALUE; limit > 0; ) {
            List<FilerProto.Entry> t = listEntries(path, "", lastFileName, 1024);
            if (t == null) {
                break;
            }
            int nSize = t.size();
            if (nSize > 0) {
                limit -= nSize;
                lastFileName = t.get(nSize - 1).getName();
            }
            results.addAll(t);
            if (t.size() < 1024) {
                break;
            }
        }
        return results;
    }

    public List<FilerProto.Entry> listEntries(String path, String entryPrefix, String lastEntryName, int limit) {
        Iterator<FilerProto.ListEntriesResponse> iter = filerGrpcClient.getBlockingStub().listEntries(FilerProto.ListEntriesRequest.newBuilder()
                .setDirectory(path)
                .setPrefix(entryPrefix)
                .setStartFromFileName(lastEntryName)
                .setLimit(limit)
                .build());
        List<FilerProto.Entry> entries = new ArrayList<>();
        while (iter.hasNext()) {
            FilerProto.ListEntriesResponse resp = iter.next();
            entries.add(fixEntryAfterReading(resp.getEntry()));
        }
        return entries;
    }

    public FilerProto.Entry lookupEntry(String directory, String entryName) {
        try {
            FilerProto.Entry entry = filerGrpcClient.getBlockingStub().lookupDirectoryEntry(
                    FilerProto.LookupDirectoryEntryRequest.newBuilder()
                            .setDirectory(directory)
                            .setName(entryName)
                            .build()).getEntry();
            if (entry == null) {
                return null;
            }
            return fixEntryAfterReading(entry);
        } catch (Exception e) {
            if (e.getMessage().indexOf("filer: no entry is found in filer store") > 0) {
                return null;
            }
            LOG.warn("lookupEntry {}/{}: {}", directory, entryName, e);
            return null;
        }
    }


    public boolean createEntry(String parent, FilerProto.Entry entry) {
        try {
            filerGrpcClient.getBlockingStub().createEntry(FilerProto.CreateEntryRequest.newBuilder()
                    .setDirectory(parent)
                    .setEntry(entry)
                    .build());
        } catch (Exception e) {
            LOG.warn("createEntry {}/{}: {}", parent, entry.getName(), e);
            return false;
        }
        return true;
    }

    public boolean updateEntry(String parent, FilerProto.Entry entry) {
        try {
            filerGrpcClient.getBlockingStub().updateEntry(FilerProto.UpdateEntryRequest.newBuilder()
                    .setDirectory(parent)
                    .setEntry(entry)
                    .build());
        } catch (Exception e) {
            LOG.warn("createEntry {}/{}: {}", parent, entry.getName(), e);
            return false;
        }
        return true;
    }

    public boolean deleteEntry(String parent, String entryName, boolean isDeleteFileChunk, boolean isRecursive, boolean ignoreRecusiveError) {
        try {
            filerGrpcClient.getBlockingStub().deleteEntry(FilerProto.DeleteEntryRequest.newBuilder()
                    .setDirectory(parent)
                    .setName(entryName)
                    .setIsDeleteData(isDeleteFileChunk)
                    .setIsRecursive(isRecursive)
                    .setIgnoreRecursiveError(ignoreRecusiveError)
                    .build());
        } catch (Exception e) {
            LOG.warn("deleteEntry {}/{}: {}", parent, entryName, e);
            return false;
        }
        return true;
    }

    public boolean atomicRenameEntry(String oldParent, String oldName, String newParent, String newName) {
        try {
            filerGrpcClient.getBlockingStub().atomicRenameEntry(FilerProto.AtomicRenameEntryRequest.newBuilder()
                    .setOldDirectory(oldParent)
                    .setOldName(oldName)
                    .setNewDirectory(newParent)
                    .setNewName(newName)
                    .build());
        } catch (Exception e) {
            LOG.warn("atomicRenameEntry {}/{} => {}/{}: {}", oldParent, oldName, newParent, newName, e);
            return false;
        }
        return true;
    }

    private FilerProto.Entry fixEntryAfterReading(FilerProto.Entry entry) {
        if (entry.getChunksList().size() <= 0) {
            return entry;
        }
        String fileId = entry.getChunks(0).getFileId();
        if (fileId != null && fileId.length() != 0) {
            return entry;
        }
        FilerProto.Entry.Builder entryBuilder = entry.toBuilder();
        entryBuilder.clearChunks();
        for (FilerProto.FileChunk chunk : entry.getChunksList()) {
            FilerProto.FileChunk.Builder chunkBuilder = chunk.toBuilder();
            FilerProto.FileId fid = chunk.getFid();
            fileId = String.format("%d,%d%x", fid.getVolumeId(), fid.getFileKey(), fid.getCookie());
            chunkBuilder.setFileId(fileId);
            entryBuilder.addChunks(chunkBuilder);
        }
        return entryBuilder.build();
    }

}
