package org.apache.hadoop.seaweed.ftp.service.filesystem;

import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.User;
import org.apache.hadoop.fs.Path;

import java.io.File;

/**
 * Implemented FileSystemView to use HdfsFileObject
 */
public class HdfsFileSystemView implements FileSystemView {

	private String homePath;
	private String currPath = File.separator;
	private User user;

	/**
	 * Constructor - set the user object.
	 */
	protected HdfsFileSystemView(User user) {
		if (user == null) {
			throw new IllegalArgumentException("user can not be null");
		}
		if (user.getHomeDirectory() == null) {
			throw new IllegalArgumentException(
					"User home directory can not be null");
		}

		this.homePath = user.getHomeDirectory();
		this.user = user;
	}

	public FtpFile getHomeDirectory() {
		return new HdfsFileObject(homePath, File.separator, user);
	}

	public FtpFile getWorkingDirectory() {
		FtpFile fileObj;
		if (currPath.equals(File.separator)) {
			fileObj = new HdfsFileObject(homePath, File.separator, user);
		} else {
			fileObj = new HdfsFileObject(homePath, currPath, user);

		}
		return fileObj;
	}

	public boolean changeWorkingDirectory(String dir) {

		Path path;
		if (dir.startsWith(File.separator) || new Path(currPath).equals(new Path(dir))) {
			path = new Path(dir);
		} else if (currPath.length() > 1) {
			path = new Path(currPath + File.separator + dir);
		} else {
			if(dir.startsWith("/")) {
				path = new Path(dir);
			}else {
				path = new Path(File.separator + dir);
			}
		}

		// 防止退回根目录
		if (path.getName().equals("..")) {
			path = new Path(File.separator);
		}

		HdfsFileObject file = new HdfsFileObject(homePath, path.toString(), user);
		if (file.isDirectory()) {
			currPath = path.toString();
			return true;
		} else {
			return false;
		}
	}

	public FtpFile getFile(String file) {
		String path;
		if (file.startsWith(File.separator)) {
			path = file;
		} else if (currPath.length() > 1) {
			path = currPath + File.separator + file;
		} else {
			path = File.separator + file;
		}
		return new HdfsFileObject(homePath, path, user);
	}

	/**
	 * Is the file content random accessible?
	 */
	public boolean isRandomAccessible() {
		return true;
	}

	/**
	 * Dispose file system view - does nothing.
	 */
	public void dispose() {
	}

}
