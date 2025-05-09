package seaweedfs.client;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class FilerClient extends FilerGrpcClient {

    private static final Logger LOG = LoggerFactory.getLogger(FilerClient.class);

    public FilerClient(String filerHost, int filerGrpcPort) {
        this(filerHost, filerGrpcPort-10000, filerGrpcPort, "");
    }
    public FilerClient(String filerHost, int filerGrpcPort, String cn) { this(filerHost, filerGrpcPort-10000, filerGrpcPort, cn); }
    public FilerClient(String filerHost, int filerPort, int filerGrpcPort) { this(filerHost, filerPort, filerGrpcPort, ""); }

    public FilerClient(String filerHost, int filerPort, int filerGrpcPort, String cn) {
        super(filerHost, filerPort, filerGrpcPort, cn);
    }

    public static String toFileId(FilerProto.FileId fid) {
        if (fid == null) {
            return null;
        }
        return String.format("%d,%x%08x", fid.getVolumeId(), fid.getFileKey(), fid.getCookie());
    }

    public static FilerProto.FileId toFileIdObject(String fileIdStr) {
        if (fileIdStr == null || fileIdStr.length() == 0) {
            return null;
        }
        int commaIndex = fileIdStr.lastIndexOf(',');
        String volumeIdStr = fileIdStr.substring(0, commaIndex);
        String fileKeyStr = fileIdStr.substring(commaIndex + 1, fileIdStr.length() - 8);
        String cookieStr = fileIdStr.substring(fileIdStr.length() - 8);

        return FilerProto.FileId.newBuilder()
                .setVolumeId(Integer.parseInt(volumeIdStr))
                .setFileKey(Long.parseLong(fileKeyStr, 16))
                .setCookie((int) Long.parseLong(cookieStr, 16))
                .build();
    }

    public static List<FilerProto.FileChunk> beforeEntrySerialization(List<FilerProto.FileChunk> chunks) {
        List<FilerProto.FileChunk> cleanedChunks = new ArrayList<>();
        for (FilerProto.FileChunk chunk : chunks) {
            FilerProto.FileChunk.Builder chunkBuilder = chunk.toBuilder();
            chunkBuilder.clearFileId();
            chunkBuilder.clearSourceFileId();
            chunkBuilder.setFid(toFileIdObject(chunk.getFileId()));
            FilerProto.FileId sourceFid = toFileIdObject(chunk.getSourceFileId());
            if (sourceFid != null) {
                chunkBuilder.setSourceFid(sourceFid);
            }
            cleanedChunks.add(chunkBuilder.build());
        }
        return cleanedChunks;
    }

    public static FilerProto.Entry afterEntryDeserialization(FilerProto.Entry entry) {
        if (entry.getChunksList().size() <= 0) {
            if (entry.getContent().isEmpty()) {
                return entry;
            } else {
                if (entry.getAttributes().getFileSize() <= 0) {
                    FilerProto.Entry.Builder entryBuilder = entry.toBuilder();
                    FilerProto.FuseAttributes.Builder attrBuilder = entry.getAttributes().toBuilder();
                    attrBuilder.setFileSize(entry.getContent().size());
                    entryBuilder.setAttributes(attrBuilder);
                    return entryBuilder.build();
                }
            }
            return entry;
        } else {
            FilerProto.Entry.Builder entryBuilder = entry.toBuilder();
            entryBuilder.clearChunks();
            long fileSize = 0;
            for (FilerProto.FileChunk chunk : entry.getChunksList()) {
                fileSize = Math.max(fileSize, chunk.getOffset()+chunk.getSize());
                FilerProto.FileChunk.Builder chunkBuilder = chunk.toBuilder();
                chunkBuilder.setFileId(toFileId(chunk.getFid()));
                String sourceFileId = toFileId(chunk.getSourceFid());
                if (sourceFileId != null) {
                    chunkBuilder.setSourceFileId(sourceFileId);
                }
                entryBuilder.addChunks(chunkBuilder);
            }
            if (entry.getAttributes().getFileSize() <= 0) {
                FilerProto.FuseAttributes.Builder attrBuilder = entry.getAttributes().toBuilder();
                attrBuilder.setFileSize(fileSize);
                entryBuilder.setAttributes(attrBuilder);
            }
            return entryBuilder.build();
        }
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
        String[] dirAndName = SeaweedUtil.toDirAndName(path);
        String parent = dirAndName[0];
        String name = dirAndName[1];

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

        String[] oldDirAndName = SeaweedUtil.toDirAndName(oldPath);
        String oldParent = oldDirAndName[0];
        String oldName = oldDirAndName[1];

        String[] newDirAndName = SeaweedUtil.toDirAndName(newPath);
        String newParent = newDirAndName[0];
        String newName = newDirAndName[1];

        return atomicRenameEntry(oldParent, oldName, newParent, newName);

    }

    public boolean exists(String path){

        String[] dirAndName = SeaweedUtil.toDirAndName(path);
        String parent = dirAndName[0];
        String entryName = dirAndName[1];

        return lookupEntry(parent, entryName) != null;
    }

    public boolean rm(String path, boolean isRecursive, boolean ignoreRecusiveError) {

        String[] dirAndName = SeaweedUtil.toDirAndName(path);
        String parent = dirAndName[0];
        String name = dirAndName[1];

        return deleteEntry(
                parent,
                name,
                true,
                isRecursive,
                ignoreRecusiveError);
    }

    public boolean touch(String path, int mode) {

        String currentUser = System.getProperty("user.name");

        long now = System.currentTimeMillis() / 1000L;
        return touch(path, now, mode, 0, 0, currentUser, new String[]{});

    }

    public boolean touch(String path, long modifiedTimeSecond, int mode, int uid, int gid, String userName, String[] groupNames) {

        String[] dirAndName = SeaweedUtil.toDirAndName(path);
        String parent = dirAndName[0];
        String name = dirAndName[1];

        FilerProto.Entry entry = lookupEntry(parent, name);
        if (entry == null) {
            return createEntry(
                    parent,
                    newFileEntry(name, modifiedTimeSecond, mode, uid, gid, userName, groupNames).build()
            );
        }
        FilerProto.FuseAttributes.Builder attr = entry.getAttributes().toBuilder();
        if (modifiedTimeSecond>0) {
            attr.setMtime(modifiedTimeSecond);
        }
        if (uid>0) {
            attr.setUid(uid);
        }
        if (gid>0) {
            attr.setGid(gid);
        }
        if (userName!=null) {
            attr.setUserName(userName);
        }
        if (groupNames!=null) {
            attr.clearGroupName().addAllGroupName(Arrays.asList(groupNames));
        }
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

    public FilerProto.Entry.Builder newFileEntry(String name, long modifiedTimeSecond, int mode,
                                                 int uid, int gid, String userName, String[] groupNames) {

        return FilerProto.Entry.newBuilder()
                .setName(name)
                .setIsDirectory(false)
                .setAttributes(FilerProto.FuseAttributes.newBuilder()
                        .setMtime(modifiedTimeSecond)
                        .setCrtime(modifiedTimeSecond)
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
            List<FilerProto.Entry> t = listEntries(path, "", lastFileName, 1024, false);
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

    public List<FilerProto.Entry> listEntries(String path, String entryPrefix, String lastEntryName, int limit, boolean includeLastEntry) {
        Iterator<FilerProto.ListEntriesResponse> iter = this.getBlockingStub().listEntries(FilerProto.ListEntriesRequest.newBuilder()
                .setDirectory(path)
                .setPrefix(entryPrefix)
                .setStartFromFileName(lastEntryName)
                .setInclusiveStartFrom(includeLastEntry)
                .setLimit(limit)
                .build());
        List<FilerProto.Entry> entries = new ArrayList<>();
        while (iter.hasNext()) {
            FilerProto.ListEntriesResponse resp = iter.next();
            entries.add(afterEntryDeserialization(resp.getEntry()));
        }
        return entries;
    }

    public FilerProto.Entry lookupEntry(String directory, String entryName) {
        try {
            FilerProto.Entry entry = this.getBlockingStub().lookupDirectoryEntry(
                    FilerProto.LookupDirectoryEntryRequest.newBuilder()
                            .setDirectory(directory)
                            .setName(entryName)
                            .build()).getEntry();
            if (entry == null) {
                return null;
            }
            return afterEntryDeserialization(entry);
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
            FilerProto.CreateEntryResponse createEntryResponse =
                    this.getBlockingStub().createEntry(FilerProto.CreateEntryRequest.newBuilder()
                            .setDirectory(parent)
                            .setEntry(entry)
                            .build());
            if (Strings.isNullOrEmpty(createEntryResponse.getError())) {
                return true;
            }
            LOG.warn("createEntry {}/{} error: {}", parent, entry.getName(), createEntryResponse.getError());
            return false;
        } catch (Exception e) {
            LOG.warn("createEntry {}/{}: {}", parent, entry.getName(), e);
            return false;
        }
    }

    public boolean updateEntry(String parent, FilerProto.Entry entry) {
        try {
            this.getBlockingStub().updateEntry(FilerProto.UpdateEntryRequest.newBuilder()
                    .setDirectory(parent)
                    .setEntry(entry)
                    .build());
        } catch (Exception e) {
            LOG.warn("updateEntry {}/{}: {}", parent, entry.getName(), e);
            return false;
        }
        return true;
    }

    public boolean deleteEntry(String parent, String entryName, boolean isDeleteFileChunk, boolean isRecursive, boolean ignoreRecusiveError) {
        try {
            this.getBlockingStub().deleteEntry(FilerProto.DeleteEntryRequest.newBuilder()
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
            this.getBlockingStub().atomicRenameEntry(FilerProto.AtomicRenameEntryRequest.newBuilder()
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

    public Iterator<FilerProto.SubscribeMetadataResponse> watch(String prefix, String clientName, long sinceNs) {
        return this.getBlockingStub().subscribeMetadata(FilerProto.SubscribeMetadataRequest.newBuilder()
                .setPathPrefix(prefix)
                .setClientName(clientName)
                .setSinceNs(sinceNs)
                .setClientId(this.randomClientId)
                .build()
        );
    }

}
