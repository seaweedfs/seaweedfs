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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

public class SeaweedFileSystem extends org.apache.hadoop.fs.FileSystem {

    public static final int FS_SEAWEED_DEFAULT_PORT = 8333;
    public static final String FS_SEAWEED_HOST = "fs.seaweed.host";
    public static final String FS_SEAWEED_HOST_PORT = "fs.seaweed.host.port";

    private URI uri;
    private Path workingDirectory = new Path("/");
    private SeaweedFileSystemStore seaweedFileSystemStore;

    public URI getUri() {
        return uri;
    }

    public String getScheme() {
        return "seaweed";
    }

    @Override
    public void initialize(URI uri, Configuration conf) throws IOException { // get
        super.initialize(uri, conf);

        // get host information from uri (overrides info in conf)
        String host = uri.getHost();
        host = (host == null) ? conf.get(FS_SEAWEED_HOST, null) : host;
        if (host == null) {
            throw new IOException("Invalid host specified");
        }
        conf.set(FS_SEAWEED_HOST, host);

        // get port information from uri, (overrides info in conf)
        int port = uri.getPort();
        port = (port == -1) ? FS_SEAWEED_DEFAULT_PORT : port;
        conf.setInt(FS_SEAWEED_HOST_PORT, port);

        setConf(conf);
        this.uri = uri;

        seaweedFileSystemStore = new SeaweedFileSystemStore(host, port);
    }

    public FSDataInputStream open(Path path, int i) throws IOException {
        return null;
    }

    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i1, long l, Progressable progressable) throws IOException {
        return null;
    }

    public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
        return null;
    }

    public boolean rename(Path src, Path dst) throws IOException {

        Path parentFolder = src.getParent();
        if (parentFolder == null) {
            return false;
        }
        if (src.equals(dst)){
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
        return false;
    }

    public boolean delete(Path path, boolean recursive) throws IOException {

        path = qualify(path);

        FileStatus fileStatus = getFileStatus(path);
        if (fileStatus == null) {
            return true;
        }

        return seaweedFileSystemStore.deleteEntries(path, fileStatus.isDirectory(), recursive);

    }

    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {

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

        path = qualify(path);

        try {
            FileStatus fileStatus = getFileStatus(path);

            if (fileStatus.isDirectory()) {
                return true;
            } else {
                throw new FileAlreadyExistsException("Path is a file: " + path);
            }
        } catch (FileNotFoundException e) {
            UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
            return seaweedFileSystemStore.createDirectory(path, currentUser,
                fsPermission == null ? FsPermission.getDirDefault() : fsPermission,
                FsPermission.getUMask(getConf()));
        }
    }

    public FileStatus getFileStatus(Path path) throws IOException {

        path = qualify(path);

        return seaweedFileSystemStore.getFileStatus(path);
    }

    Path qualify(Path path) {
        return path.makeQualified(uri, workingDirectory);
    }

}
