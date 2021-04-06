package org.apache.hadoop.seaweed.ftp.service.filesystem;

import org.apache.ftpserver.ftplet.FtpFile;
import org.apache.ftpserver.ftplet.User;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.seaweed.ftp.users.HdfsUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * This class implements all actions to HDFS
 */
public class HdfsFileObject implements FtpFile {

	private final Logger log = LoggerFactory.getLogger(HdfsFileObject.class);

	private Path homePath;
	private Path path;
	private Path fullPath;
	private HdfsUser user;

	/**
	 * Constructs HdfsFileObject from path
	 *
	 * @param path path to represent object
	 * @param user accessor of the object
	 */
	public HdfsFileObject(String homePath, String path, User user) {
		this.homePath = new Path(homePath);
		this.path = new Path(path);
		this.fullPath = new Path(homePath + path);
		this.user = (HdfsUser) user;
	}

	public String getAbsolutePath() {
		// strip the last '/' if necessary
		String fullName = path.toString();
		int filelen = fullName.length();
		if ((filelen != 1) && (fullName.charAt(filelen - 1) == '/')) {
			fullName = fullName.substring(0, filelen - 1);
		}

		return fullName;
	}

	public String getName() {
		return path.getName();
	}

	/**
	 * HDFS has no hidden objects
	 *
	 * @return always false
	 */
	public boolean isHidden() {
		return false;
	}

	/**
	 * Checks if the object is a directory
	 *
	 * @return true if the object is a directory
	 */
	public boolean isDirectory() {
		try {
			log.debug("is directory? : " + fullPath);
			FileSystem dfs = HdfsOverFtpSystem.getDfs();
			FileStatus fs = dfs.getFileStatus(fullPath);
			return fs.isDir();
		} catch (IOException e) {
			log.debug(fullPath + " is not dir", e);
			return false;
		}
	}

	/**
	 * Checks if the object is a file
	 *
	 * @return true if the object is a file
	 */
	public boolean isFile() {
		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs();
			return dfs.isFile(fullPath);
		} catch (IOException e) {
			log.debug(fullPath + " is not file", e);
			return false;
		}
	}

	/**
	 * Checks if the object does exist
	 *
	 * @return true if the object does exist
	 */
	public boolean doesExist() {
		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs();
			dfs.getFileStatus(fullPath);
			return true;
		} catch (IOException e) {
			//   log.debug(path + " does not exist", e);
			return false;
		}
	}

	public boolean isReadable() {
		return true;
	}

	public boolean isWritable() {
		return true;
	}

	public boolean isRemovable() {
		return true;
	}

	/**
	 * Get owner of the object
	 *
	 * @return owner of the object
	 */
	public String getOwnerName() {
		return "root";
		/*
		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs();
			FileStatus fs = dfs.getFileStatus(fullPath);
			String owner = fs.getOwner();
			if(owner.length() == 0) {
				return "root";
			}
			return owner;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		 */
	}

	/**
	 * Get group of the object
	 *
	 * @return group of the object
	 */
	public String getGroupName() {
		return "root";
		/*
		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs();
			FileStatus fs = dfs.getFileStatus(fullPath);
			String group = fs.getGroup();
			if(group.length() == 0) {
				return "root";
			}
			return group;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		 */
	}

	/**
	 * Get link count
	 *
	 * @return 3 is for a directory and 1 is for a file
	 */
	public int getLinkCount() {
		return isDirectory() ? 3 : 1;
	}

	/**
	 * Get last modification date
	 *
	 * @return last modification date as a long
	 */
	public long getLastModified() {
		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs();
			FileStatus fs = dfs.getFileStatus(fullPath);
			return fs.getModificationTime();
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
	}

	public boolean setLastModified(long l) {
		return false;
	}

	/**
	 * Get a size of the object
	 *
	 * @return size of the object in bytes
	 */
	public long getSize() {
		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs();
			FileStatus fs = dfs.getFileStatus(fullPath);
			log.debug("getSize(): " + fullPath + " : " + fs.getLen());
			return fs.getLen();
		} catch (IOException e) {
			e.printStackTrace();
			return 0;
		}
	}

	public Object getPhysicalFile() {
		return null;
	}

	/**
	 * Create a new dir from the object
	 *
	 * @return true if dir is created
	 */
	public boolean mkdir() {
		try {
			FileSystem fs = HdfsOverFtpSystem.getDfs();
			fs.mkdirs(fullPath);
//			fs.setOwner(path, user.getName(), user.getMainGroup());
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * Delete object from the HDFS filesystem
	 *
	 * @return true if the object is deleted
	 */
	public boolean delete() {
		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs();
			dfs.delete(fullPath, true);
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}

	public boolean move(FtpFile ftpFile) {
		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs();
			dfs.rename(fullPath, new Path(fullPath.getParent() + File.separator + ftpFile.getName()));
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
	}


	/**
	 * List files of the directory
	 *
	 * @return List of files in the directory
	 */
	public List<FtpFile> listFiles() {
		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs();
			FileStatus fileStats[] = dfs.listStatus(fullPath);

			// get the virtual name of the base directory
			String virtualFileStr = getAbsolutePath();
			if (virtualFileStr.charAt(virtualFileStr.length() - 1) != '/') {
				virtualFileStr += '/';
			}

			FtpFile[] virtualFiles = new FtpFile[fileStats.length];
			for (int i = 0; i < fileStats.length; i++) {
				File fileObj = new File(fileStats[i].getPath().toString());
				String fileName = virtualFileStr + fileObj.getName();
				virtualFiles[i] = new HdfsFileObject(homePath.toString(), fileName, user);
			}
			return Collections.unmodifiableList(Arrays.asList(virtualFiles));
		} catch (IOException e) {
			log.debug("", e);
			return null;
		}
	}

	/**
	 * Creates output stream to write to the object
	 *
	 * @param l is not used here
	 * @return OutputStream
	 * @throws IOException
	 */
	public OutputStream createOutputStream(long l) {
		try {
			FileSystem fs = HdfsOverFtpSystem.getDfs();
			FSDataOutputStream out = fs.create(fullPath);
//			fs.setOwner(fullPath, user.getName(), user.getMainGroup());
			return out;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Creates input stream to read from the object
	 *
	 * @param l is not used here
	 * @return OutputStream
	 * @throws IOException
	 */
	public InputStream createInputStream(long l) {
		try {
			FileSystem dfs = HdfsOverFtpSystem.getDfs();
			FSDataInputStream in = dfs.open(fullPath);
			return in;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
}
