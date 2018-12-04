package seaweed.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import seaweedfs.client.FilerGrpcClient;
import seaweedfs.client.FilerProto;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SeaweedFileSystemStore {

    private static final Logger LOG = LoggerFactory.getLogger(SeaweedFileSystemStore.class);

    private FilerGrpcClient filerGrpcClient;

    public SeaweedFileSystemStore(String host, int port) {
        int grpcPort = 10000 + port;
        filerGrpcClient = new FilerGrpcClient(host, grpcPort);
    }

    public static String getParentDirectory(Path path) {
        return path.isRoot() ? "/" : path.getParent().toUri().getPath();
    }

    static int permissionToMode(FsPermission permission, boolean isDirectory) {
        int p = permission.toShort();
        if (isDirectory) {
            p = p | 1 << 31;
        }
        return p;
    }

    public boolean createDirectory(final Path path, UserGroupInformation currentUser,
                                   final FsPermission permission, final FsPermission umask) {

        LOG.debug("createDirectory path: {} permission: {} umask: {}",
            path,
            permission,
            umask);

        long now = System.currentTimeMillis() / 1000L;

        FilerProto.CreateEntryRequest.Builder request = FilerProto.CreateEntryRequest.newBuilder()
            .setDirectory(getParentDirectory(path))
            .setEntry(FilerProto.Entry.newBuilder()
                .setName(path.getName())
                .setIsDirectory(true)
                .setAttributes(FilerProto.FuseAttributes.newBuilder()
                    .setMtime(now)
                    .setCrtime(now)
                    .setFileMode(permissionToMode(permission, true))
                    .setUserName(currentUser.getUserName())
                    .addAllGroupName(Arrays.asList(currentUser.getGroupNames())))
            );

        FilerProto.CreateEntryResponse response = filerGrpcClient.getBlockingStub().createEntry(request.build());
        return true;
    }

    public FileStatus[] listEntries(final Path path) {
        LOG.debug("listEntries path: {}", path);

        List<FileStatus> fileStatuses = new ArrayList<FileStatus>();

        List<FilerProto.Entry> entries = lookupEntries(path);

        for (FilerProto.Entry entry : entries) {

            FileStatus fileStatus = doGetFileStatus(new Path(path, entry.getName()), entry);

            fileStatuses.add(fileStatus);
        }
        return fileStatuses.toArray(new FileStatus[0]);
    }

    private List<FilerProto.Entry> lookupEntries(Path path) {

        LOG.debug("listEntries path: {}", path);

        return filerGrpcClient.getBlockingStub().listEntries(FilerProto.ListEntriesRequest.newBuilder()
            .setDirectory(path.toUri().getPath())
            .setLimit(100000)
            .build()).getEntriesList();
    }

    public FileStatus getFileStatus(final Path path) {
        LOG.debug("doGetFileStatus path: {}", path);

        FilerProto.Entry entry = lookupEntry(path);
        if (entry == null) {
            return null;
        }
        FileStatus fileStatus = doGetFileStatus(path, entry);
        return fileStatus;
    }

    public boolean deleteEntries(final Path path, boolean isDirectroy, boolean recursive) {
        LOG.debug("deleteEntries path: {} isDirectory {} recursive: {}",
            path,
            String.valueOf(isDirectroy),
            String.valueOf(recursive));

        if (path.isRoot()) {
            return true;
        }

        FilerProto.DeleteEntryResponse response =
            filerGrpcClient.getBlockingStub().deleteEntry(FilerProto.DeleteEntryRequest.newBuilder()
                .setDirectory(getParentDirectory(path))
                .setName(path.getName())
                .setIsDirectory(isDirectroy)
                .setIsDeleteData(true)
                .build());

        return true;
    }

    private FileStatus doGetFileStatus(Path path, FilerProto.Entry entry) {
        FilerProto.FuseAttributes attributes = entry.getAttributes();
        long length = SeaweedRead.totalSize(entry.getChunksList());
        boolean isDir = entry.getIsDirectory();
        int block_replication = 1;
        int blocksize = 512;
        long modification_time = attributes.getMtime();
        long access_time = 0;
        FsPermission permission = FsPermission.createImmutable((short) attributes.getFileMode());
        String owner = attributes.getUserName();
        String group = attributes.getGroupNameCount() > 0 ? attributes.getGroupName(0) : "";
        return new FileStatus(length, isDir, block_replication, blocksize,
            modification_time, access_time, permission, owner, group, null, path);
    }

    private FilerProto.Entry lookupEntry(Path path) {

        String directory = getParentDirectory(path);

        try {
            FilerProto.LookupDirectoryEntryResponse response =
                filerGrpcClient.getBlockingStub().lookupDirectoryEntry(FilerProto.LookupDirectoryEntryRequest.newBuilder()
                    .setDirectory(directory)
                    .setName(path.getName())
                    .build());
            return response.getEntry();
        } catch (io.grpc.StatusRuntimeException e) {
            return null;
        }
    }

    public void rename(Path source, Path destination) {

        LOG.debug("rename source: {} destination:{}", source, destination);

        if (source.isRoot()) {
            return;
        }
        LOG.warn("rename lookupEntry source: {}", source);
        FilerProto.Entry entry = lookupEntry(source);
        if (entry == null) {
            LOG.warn("rename non-existing source: {}", source);
            return;
        }
        LOG.warn("rename moveEntry source: {}", source);
        moveEntry(source.getParent(), entry, destination);
    }

    private boolean moveEntry(Path oldParent, FilerProto.Entry entry, Path destination) {

        LOG.debug("moveEntry: {}/{}  => {}", oldParent, entry.getName(), destination);

        if (entry.getIsDirectory()) {
            Path entryPath = new Path(oldParent, entry.getName());
            List<FilerProto.Entry> entries = lookupEntries(entryPath);
            for (FilerProto.Entry ent : entries) {
                boolean isSucess = moveEntry(entryPath, ent, new Path(destination, ent.getName()));
                if (!isSucess) {
                    return false;
                }
            }
        }

        FilerProto.Entry.Builder newEntry = entry.toBuilder().setName(destination.getName());
        filerGrpcClient.getBlockingStub().createEntry(FilerProto.CreateEntryRequest.newBuilder()
            .setDirectory(getParentDirectory(destination))
            .setEntry(newEntry)
            .build());

        filerGrpcClient.getBlockingStub().deleteEntry(FilerProto.DeleteEntryRequest.newBuilder()
            .setDirectory(oldParent.toUri().getPath())
            .setName(entry.getName())
            .setIsDirectory(entry.getIsDirectory())
            .setIsDeleteData(false)
            .build());

        return true;
    }

    public OutputStream createFile(final Path path,
                                   final boolean overwrite,
                                   FsPermission permission,
                                   int bufferSize,
                                   String replication) throws IOException {

        permission = permission == null ? FsPermission.getFileDefault() : permission;

        LOG.debug("createFile path: {} overwrite: {} permission: {}",
            path,
            overwrite,
            permission.toString());

        UserGroupInformation userGroupInformation = UserGroupInformation.getCurrentUser();
        long now = System.currentTimeMillis() / 1000L;

        FilerProto.Entry.Builder entry = null;
        long writePosition = 0;
        if (!overwrite) {
            FilerProto.Entry existingEntry = lookupEntry(path);
            if (existingEntry != null) {
                entry.mergeFrom(existingEntry);
                entry.getAttributesBuilder().setMtime(now);
            }
            writePosition = SeaweedRead.totalSize(existingEntry.getChunksList());
            replication = existingEntry.getAttributes().getReplication();
        }
        if (entry == null) {
            entry = FilerProto.Entry.newBuilder()
                .setName(path.getName())
                .setIsDirectory(false)
                .setAttributes(FilerProto.FuseAttributes.newBuilder()
                    .setFileMode(permissionToMode(permission, false))
                    .setReplication(replication)
                    .setCrtime(now)
                    .setMtime(now)
                    .setUserName(userGroupInformation.getUserName())
                    .addAllGroupName(Arrays.asList(userGroupInformation.getGroupNames()))
                );
        }

        return new SeaweedOutputStream(filerGrpcClient, path, entry, writePosition, bufferSize, replication);

    }

    public InputStream openFileForRead(final Path path, FileSystem.Statistics statistics,
                                       int bufferSize) throws IOException {

        LOG.debug("openFileForRead path:{} bufferSize:{}", path, bufferSize);

        int readAheadQueueDepth = 2;
        FilerProto.Entry entry = lookupEntry(path);

        if (entry == null) {
            throw new FileNotFoundException("read non-exist file " + path);
        }

        return new SeaweedInputStream(filerGrpcClient,
            statistics,
            path.toUri().getPath(),
            entry,
            bufferSize,
            readAheadQueueDepth);
    }
}
