package seaweed.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import seaweedfs.client.FilerGrpcClient;
import seaweedfs.client.FilerProto;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SeaweedFileSystemStore {

    private static final Logger LOG = LoggerFactory.getLogger(SeaweedFileSystemStore.class);

    private FilerGrpcClient filerGrpcClient;

    public SeaweedFileSystemStore(String host, int port) {
        filerGrpcClient = new FilerGrpcClient(host, port);
    }

    public boolean createDirectory(final Path path, UserGroupInformation currentUser,
                                   final FsPermission permission, final FsPermission umask) {

        LOG.debug("createDirectory path: {} permission: {} umask: {}",
            path,
            permission,
            umask);

        long now = System.currentTimeMillis() / 1000L;

        FilerProto.CreateEntryRequest.Builder request = FilerProto.CreateEntryRequest.newBuilder()
            .setDirectory(path.getParent().toUri().getPath())
            .setEntry(FilerProto.Entry.newBuilder()
                .setName(path.getName())
                .setIsDirectory(true)
                .setAttributes(FilerProto.FuseAttributes.newBuilder()
                    .setMtime(now)
                    .setCrtime(now)
                    .setFileMode(permission.toShort())
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

            FileStatus fileStatus = getFileStatus(new Path(path, entry.getName()), entry);

            fileStatuses.add(fileStatus);
        }
        return fileStatuses.toArray(new FileStatus[0]);
    }

    private List<FilerProto.Entry> lookupEntries(Path path) {
        return filerGrpcClient.getBlockingStub().listEntries(FilerProto.ListEntriesRequest.newBuilder()
            .setDirectory(path.toUri().getPath())
            .setLimit(100000)
            .build()).getEntriesList();
    }

    public FileStatus getFileStatus(final Path path) {
        LOG.debug("getFileStatus path: {}", path);

        FilerProto.Entry entry = lookupEntry(path);
        FileStatus fileStatus = getFileStatus(path, entry);
        return fileStatus;
    }

    public boolean deleteEntries(final Path path, boolean isDirectroy, boolean recursive) {
        LOG.debug("deleteEntries path: {} isDirectory {} recursive: {}",
            path,
            String.valueOf(isDirectroy),
            String.valueOf(recursive));

        FilerProto.DeleteEntryResponse response =
            filerGrpcClient.getBlockingStub().deleteEntry(FilerProto.DeleteEntryRequest.newBuilder()
                .setDirectory(path.getParent().toUri().getPath())
                .setName(path.getName())
                .setIsDirectory(isDirectroy)
                .setIsDeleteData(true)
                .build());

        return true;
    }


    private FileStatus getFileStatus(Path path, FilerProto.Entry entry) {
        FilerProto.FuseAttributes attributes = entry.getAttributes();
        long length = attributes.getFileSize();
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
        FilerProto.LookupDirectoryEntryResponse response =
            filerGrpcClient.getBlockingStub().lookupDirectoryEntry(FilerProto.LookupDirectoryEntryRequest.newBuilder()
                .setDirectory(path.getParent().toUri().getPath())
                .setName(path.getName())
                .build());

        return response.getEntry();
    }

    public void rename(Path source, Path destination) {
        FilerProto.Entry entry = lookupEntry(source);
        moveEntry(source.getParent(), entry, destination);
    }

    private boolean moveEntry(Path oldParent, FilerProto.Entry entry, Path destination) {
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

        filerGrpcClient.getBlockingStub().createEntry(FilerProto.CreateEntryRequest.newBuilder()
            .setDirectory(destination.getParent().toUri().getPath())
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
                                   String replication) throws IOException {

        permission = permission == null ? FsPermission.getFileDefault() : permission;

        LOG.debug("createFile path: {} overwrite: {} permission: {}",
            path,
            overwrite,
            permission.toString());

        UserGroupInformation userGroupInformation = UserGroupInformation.getCurrentUser();

        FilerProto.Entry.Builder entry = FilerProto.Entry.newBuilder();
        long writePosition = 0;
        if (!overwrite) {
            FilerProto.Entry existingEntry = lookupEntry(path);
            if (existingEntry != null) {
                entry.mergeFrom(existingEntry);
            }
            writePosition = existingEntry.getAttributes().getFileSize();
            replication = existingEntry.getAttributes().getReplication();
        }
        if (entry == null) {
            entry = FilerProto.Entry.newBuilder()
                .setAttributes(FilerProto.FuseAttributes.newBuilder()
                    .setFileMode(permission.toOctal())
                    .setReplication(replication)
                    .setCrtime(System.currentTimeMillis() / 1000L)
                    .setUserName(userGroupInformation.getUserName())
                    .addAllGroupName(Arrays.asList(userGroupInformation.getGroupNames()))
                );
        }

        return new SeaweedOutputStream(filerGrpcClient, path, entry, writePosition, 16 * 1024 * 1024, replication);

    }
}
