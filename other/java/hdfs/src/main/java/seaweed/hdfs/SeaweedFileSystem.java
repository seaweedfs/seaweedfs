package seaweed.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;

public class SeaweedFileSystem extends org.apache.hadoop.fs.FileSystem {

    public static final int FS_SEAWEED_DEFAULT_PORT = 8888;
    public static final String FS_SEAWEED_FILER_HOST = "fs.seaweed.filer.host";
    public static final String FS_SEAWEED_FILER_PORT = "fs.seaweed.filer.port";

    private static final Logger LOG = LoggerFactory.getLogger(SeaweedFileSystem.class);
    private static int BUFFER_SIZE = 16 * 1024 * 1024;

    private URI uri;
    private Path workingDirectory = new Path("/");
    private SeaweedFileSystemStore seaweedFileSystemStore;

    public URI getUri() {
        return uri;
    }

    public String getScheme() {
        return "seaweedfs";
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException { // get
        super.initialize(uri, conf);

        // get host information from uri (overrides info in conf)
        String host = uri.getHost();
        host = (host == null) ? conf.get(FS_SEAWEED_FILER_HOST, "localhost") : host;
        if (host == null) {
            throw new IOException("Invalid host specified");
        }
        conf.set(FS_SEAWEED_FILER_HOST, host);

        // get port information from uri, (overrides info in conf)
        int port = uri.getPort();
        port = (port == -1) ? FS_SEAWEED_DEFAULT_PORT : port;
        conf.setInt(FS_SEAWEED_FILER_PORT, port);

        conf.setInt(IO_FILE_BUFFER_SIZE_KEY, BUFFER_SIZE);

        setConf(conf);
        this.uri = uri;

        seaweedFileSystemStore = new SeaweedFileSystemStore(host, port);
    }

    public FSDataInputStream open(Path path, int bufferSize) throws IOException {

        LOG.debug("open path: {} bufferSize:{}", path, bufferSize);

        path = qualify(path);

        try {
            InputStream inputStream = seaweedFileSystemStore.openFileForRead(path, statistics, bufferSize);
            return new FSDataInputStream(inputStream);
        } catch (Exception ex) {
            return null;
        }
    }

    public FSDataOutputStream create(Path path, FsPermission permission, final boolean overwrite, final int bufferSize,
                                     final short replication, final long blockSize, final Progressable progress) throws IOException {

        LOG.debug("create path: {} bufferSize:{} blockSize:{}", path, bufferSize, blockSize);

        path = qualify(path);

        try {
            String replicaPlacement = String.format("%03d", replication - 1);
            OutputStream outputStream = seaweedFileSystemStore.createFile(path, overwrite, permission, bufferSize, replicaPlacement);
            return new FSDataOutputStream(outputStream, statistics);
        } catch (Exception ex) {
            return null;
        }
    }

    public FSDataOutputStream append(Path path, int bufferSize, Progressable progressable) throws IOException {

        LOG.debug("append path: {} bufferSize:{}", path, bufferSize);

        path = qualify(path);
        try {
            OutputStream outputStream = seaweedFileSystemStore.createFile(path, false, null, bufferSize, "");
            return new FSDataOutputStream(outputStream, statistics);
        } catch (Exception ex) {
            return null;
        }
    }

    public boolean rename(Path src, Path dst) {

        LOG.debug("rename path: {} => {}", src, dst);

        if (src.isRoot()) {
            return false;
        }

        if (src.equals(dst)) {
            return true;
        }
        FileStatus dstFileStatus = getFileStatus(dst);

        String sourceFileName = src.getName();
        Path adjustedDst = dst;

        if (dstFileStatus != null) {
            if (!dstFileStatus.isDirectory()) {
                return false;
            }
            adjustedDst = new Path(dst, sourceFileName);
        }

        Path qualifiedSrcPath = qualify(src);
        Path qualifiedDstPath = qualify(adjustedDst);

        seaweedFileSystemStore.rename(qualifiedSrcPath, qualifiedDstPath);
        return true;
    }

    public boolean delete(Path path, boolean recursive) {

        LOG.debug("delete path: {} recursive:{}", path, recursive);

        path = qualify(path);

        FileStatus fileStatus = getFileStatus(path);

        if (fileStatus == null) {
            return true;
        }

        return seaweedFileSystemStore.deleteEntries(path, fileStatus.isDirectory(), recursive);

    }

    public FileStatus[] listStatus(Path path) throws IOException {

        LOG.debug("listStatus path: {}", path);

        path = qualify(path);

        return seaweedFileSystemStore.listEntries(path);
    }

    public Path getWorkingDirectory() {
        return workingDirectory;
    }

    public void setWorkingDirectory(Path path) {
        if (path.isAbsolute()) {
            workingDirectory = path;
        } else {
            workingDirectory = new Path(workingDirectory, path);
        }
    }

    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {

        LOG.debug("mkdirs path: {}", path);

        path = qualify(path);

        FileStatus fileStatus = getFileStatus(path);

        if (fileStatus == null) {

            UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
            return seaweedFileSystemStore.createDirectory(path, currentUser,
                fsPermission == null ? FsPermission.getDirDefault() : fsPermission,
                FsPermission.getUMask(getConf()));

        }

        if (fileStatus.isDirectory()) {
            return true;
        } else {
            throw new FileAlreadyExistsException("Path is a file: " + path);
        }
    }

    public FileStatus getFileStatus(Path path) {

        LOG.debug("getFileStatus path: {}", path);

        path = qualify(path);

        return seaweedFileSystemStore.getFileStatus(path);
    }

    Path qualify(Path path) {
        return path.makeQualified(uri, workingDirectory);
    }

}
