package org.apache.hadoop.seaweed.ftp.service.filesystem;

import org.apache.ftpserver.ftplet.FileSystemFactory;
import org.apache.ftpserver.ftplet.FileSystemView;
import org.apache.ftpserver.ftplet.User;

/**
 * Impelented FileSystemManager to use HdfsFileSystemView
 */
public class HdfsFileSystemManager implements FileSystemFactory {
	public FileSystemView createFileSystemView(User user) {
		return new HdfsFileSystemView(user);
	}
}
