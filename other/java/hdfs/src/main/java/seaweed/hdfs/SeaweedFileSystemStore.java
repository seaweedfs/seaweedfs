package seaweed.hdfs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import seaweedfs.client.FilerClient;
import seaweedfs.client.FilerGrpcClient;
import seaweedfs.client.FilerProto;
import seaweedfs.client.SeaweedRead;

import javax.net.ssl.SSLException;
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
    private FilerClient filerClient;

    public SeaweedFileSystemStore(String host, int port) {
        int grpcPort = 10000 + port;
        filerGrpcClient = new FilerGrpcClient(host, grpcPort);
        filerClient = new FilerClient(filerGrpcClient);
    }

    public SeaweedFileSystemStore(String host, int port,
                                  String caFile, String clientCertFile, String clientKeyFile) throws SSLException {
        int grpcPort = 10000 + port;
        filerGrpcClient = new FilerGrpcClient(host, grpcPort, caFile, clientCertFile, clientKeyFile);
        filerClient = new FilerClient(filerGrpcClient);
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

        return filerClient.mkdirs(
            path.toUri().getPath(),
            permissionToMode(permission, true),
            currentUser.getUserName(),
            currentUser.getGroupNames()
        );
    }

    public FileStatus[] listEntries(final Path path) {
        LOG.debug("listEntries path: {}", path);

        List<FileStatus> fileStatuses = new ArrayList<FileStatus>();

        List<FilerProto.Entry> entries = filerClient.listEntries(path.toUri().getPath());

        for (FilerProto.Entry entry : entries) {

            FileStatus fileStatus = doGetFileStatus(new Path(path, entry.getName()), entry);

            fileStatuses.add(fileStatus);
        }
        return fileStatuses.toArray(new FileStatus[0]);
    }

    public FileStatus getFileStatus(final Path path) {

        FilerProto.Entry entry = lookupEntry(path);
        if (entry == null) {
            return null;
        }
        LOG.debug("doGetFileStatus path:{} entry:{}", path, entry);

        FileStatus fileStatus = doGetFileStatus(path, entry);
        return fileStatus;
    }

    public boolean deleteEntries(final Path path, boolean isDirectory, boolean recursive) {
        LOG.debug("deleteEntries path: {} isDirectory {} recursive: {}",
            path,
            String.valueOf(isDirectory),
            String.valueOf(recursive));

        if (path.isRoot()) {
            return true;
        }

        if (recursive && isDirectory) {
            List<FilerProto.Entry> entries = filerClient.listEntries(path.toUri().getPath());
            for (FilerProto.Entry entry : entries) {
                deleteEntries(new Path(path, entry.getName()), entry.getIsDirectory(), true);
            }
        }

        return filerClient.deleteEntry(getParentDirectory(path), path.getName(), true, recursive);
    }

    private FileStatus doGetFileStatus(Path path, FilerProto.Entry entry) {
        FilerProto.FuseAttributes attributes = entry.getAttributes();
        long length = SeaweedRead.totalSize(entry.getChunksList());
        boolean isDir = entry.getIsDirectory();
        int block_replication = 1;
        int blocksize = 512;
        long modification_time = attributes.getMtime() * 1000; // milliseconds
        long access_time = 0;
        FsPermission permission = FsPermission.createImmutable((short) attributes.getFileMode());
        String owner = attributes.getUserName();
        String group = attributes.getGroupNameCount() > 0 ? attributes.getGroupName(0) : "";
        return new FileStatus(length, isDir, block_replication, blocksize,
            modification_time, access_time, permission, owner, group, null, path);
    }

    private FilerProto.Entry lookupEntry(Path path) {

        return filerClient.lookupEntry(getParentDirectory(path), path.getName());

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

        FilerProto.Entry.Builder newEntry = entry.toBuilder().setName(destination.getName());
        boolean isDirectoryCreated = filerClient.createEntry(getParentDirectory(destination), newEntry.build());

        if (!isDirectoryCreated) {
            return false;
        }

        if (entry.getIsDirectory()) {
            Path entryPath = new Path(oldParent, entry.getName());
            List<FilerProto.Entry> entries = filerClient.listEntries(entryPath.toUri().getPath());
            for (FilerProto.Entry ent : entries) {
                boolean isSucess = moveEntry(entryPath, ent, new Path(destination, ent.getName()));
                if (!isSucess) {
                    return false;
                }
            }
        }

        return filerClient.deleteEntry(
            oldParent.toUri().getPath(), entry.getName(), false, false);

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
            LOG.debug("createFile merged entry path:{} existingEntry:{}", path, existingEntry);
            if (existingEntry != null) {
                entry = FilerProto.Entry.newBuilder();
                entry.mergeFrom(existingEntry);
                entry.getAttributesBuilder().setMtime(now);
            }
            LOG.debug("createFile merged entry path:{} entry:{} from:{}", path, entry, existingEntry);
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
                    .clearGroupName()
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

    public void setOwner(Path path, String owner, String group) {

        LOG.debug("setOwner path:{} owner:{} group:{}", path, owner, group);

        FilerProto.Entry entry = lookupEntry(path);
        if (entry == null) {
            LOG.debug("setOwner path:{} entry:{}", path, entry);
            return;
        }

        FilerProto.Entry.Builder entryBuilder = entry.toBuilder();
        FilerProto.FuseAttributes.Builder attributesBuilder = entry.getAttributes().toBuilder();

        if (owner != null) {
            attributesBuilder.setUserName(owner);
        }
        if (group != null) {
            attributesBuilder.clearGroupName();
            attributesBuilder.addGroupName(group);
        }

        entryBuilder.setAttributes(attributesBuilder);

        LOG.debug("setOwner path:{} entry:{}", path, entryBuilder);

        filerClient.updateEntry(getParentDirectory(path), entryBuilder.build());

    }

    public void setPermission(Path path, FsPermission permission) {

        LOG.debug("setPermission path:{} permission:{}", path, permission);

        FilerProto.Entry entry = lookupEntry(path);
        if (entry == null) {
            LOG.debug("setPermission path:{} entry:{}", path, entry);
            return;
        }

        FilerProto.Entry.Builder entryBuilder = entry.toBuilder();
        FilerProto.FuseAttributes.Builder attributesBuilder = entry.getAttributes().toBuilder();

        attributesBuilder.setFileMode(permissionToMode(permission, entry.getIsDirectory()));

        entryBuilder.setAttributes(attributesBuilder);

        LOG.debug("setPermission path:{} entry:{}", path, entryBuilder);

        filerClient.updateEntry(getParentDirectory(path), entryBuilder.build());

    }
}
