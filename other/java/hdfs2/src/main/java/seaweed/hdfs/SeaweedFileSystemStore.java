package seaweed.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import seaweedfs.client.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static seaweed.hdfs.SeaweedFileSystem.*;

public class SeaweedFileSystemStore {

    private static final Logger LOG = LoggerFactory.getLogger(SeaweedFileSystemStore.class);

    private FilerClient filerClient;
    private Configuration conf;

    public SeaweedFileSystemStore(String host, int port, int grpcPort, Configuration conf) {
        filerClient = new FilerClient(host, port, grpcPort);
        this.conf = conf;
        String volumeServerAccessMode = this.conf.get(FS_SEAWEED_VOLUME_SERVER_ACCESS, "direct");
        if (volumeServerAccessMode.equals("publicUrl")) {
            filerClient.setAccessVolumeServerByPublicUrl();
        } else if (volumeServerAccessMode.equals("filerProxy")) {
            filerClient.setAccessVolumeServerByFilerProxy();
        }

    }

    public void close() {
        try {
            this.filerClient.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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

    public FileStatus[] listEntries(final Path path) throws IOException {
        LOG.debug("listEntries path: {}", path);

        FileStatus pathStatus = getFileStatus(path);

        if (pathStatus == null) {
            return new FileStatus[0];
        }

        if (!pathStatus.isDirectory()) {
            return new FileStatus[]{pathStatus};
        }

        List<FileStatus> fileStatuses = new ArrayList<FileStatus>();

        List<FilerProto.Entry> entries = filerClient.listEntries(path.toUri().getPath());

        for (FilerProto.Entry entry : entries) {

            FileStatus fileStatus = doGetFileStatus(new Path(path, entry.getName()), entry);

            fileStatuses.add(fileStatus);
        }
        LOG.debug("listEntries path: {} size {}", fileStatuses, fileStatuses.size());
        return fileStatuses.toArray(new FileStatus[0]);

    }

    public FileStatus getFileStatus(final Path path) throws IOException {

        FilerProto.Entry entry = lookupEntry(path);
        if (entry == null) {
            throw new FileNotFoundException("File does not exist: " + path);
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

        return filerClient.deleteEntry(getParentDirectory(path), path.getName(), true, recursive, true);
    }

    private FileStatus doGetFileStatus(Path path, FilerProto.Entry entry) {
        FilerProto.FuseAttributes attributes = entry.getAttributes();
        long length = SeaweedRead.fileSize(entry);
        boolean isDir = entry.getIsDirectory();
        int block_replication = 1;
        int blocksize = this.conf.getInt(FS_SEAWEED_BUFFER_SIZE, FS_SEAWEED_DEFAULT_BUFFER_SIZE);
        long modification_time = attributes.getMtime() * 1000; // milliseconds
        long access_time = 0;
        FsPermission permission = FsPermission.createImmutable((short) attributes.getFileMode());
        String owner = attributes.getUserName();
        String group = attributes.getGroupNameCount() > 0 ? attributes.getGroupName(0) : "";
        return new FileStatus(length, isDir, block_replication, blocksize,
            modification_time, access_time, permission, owner, group, null, path);
    }

    public FilerProto.Entry lookupEntry(Path path) {

        return filerClient.lookupEntry(getParentDirectory(path), path.getName());

    }

    public void rename(Path source, Path destination) {

        LOG.debug("rename source: {} destination:{}", source, destination);

        if (source.isRoot()) {
            return;
        }
        LOG.info("rename source: {} destination:{}", source, destination);
        FilerProto.Entry entry = lookupEntry(source);
        if (entry == null) {
            LOG.warn("rename non-existing source: {}", source);
            return;
        }
        filerClient.mv(source.toUri().getPath(), destination.toUri().getPath());
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
                entry.clearContent();
                entry.getAttributesBuilder().setMtime(now);
                LOG.debug("createFile merged entry path:{} entry:{} from:{}", path, entry, existingEntry);
                writePosition = SeaweedRead.fileSize(existingEntry);
            }
        }
        if (entry == null) {
            entry = FilerProto.Entry.newBuilder()
                .setName(path.getName())
                .setIsDirectory(false)
                .setAttributes(FilerProto.FuseAttributes.newBuilder()
                    .setFileMode(permissionToMode(permission, false))
                    .setCrtime(now)
                    .setMtime(now)
                    .setUserName(userGroupInformation.getUserName())
                    .clearGroupName()
                    .addAllGroupName(Arrays.asList(userGroupInformation.getGroupNames()))
                );
            SeaweedWrite.writeMeta(filerClient, getParentDirectory(path), entry);
        }

        return new SeaweedHadoopOutputStream(filerClient, path.toString(), entry, writePosition, bufferSize, replication);

    }

    public FSInputStream openFileForRead(final Path path, FileSystem.Statistics statistics) throws IOException {

        LOG.debug("openFileForRead path:{}", path);

        FilerProto.Entry entry = lookupEntry(path);

        if (entry == null) {
            throw new FileNotFoundException("read non-exist file " + path);
        }

        return new SeaweedHadoopInputStream(filerClient,
            statistics,
            path.toUri().getPath(),
            entry);
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
