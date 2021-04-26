package org.apache.hadoop.seaweed.ftp.service.filesystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Class to store DFS connection
 */
public class HdfsOverFtpSystem {

	private static FileSystem fs = null;

	private static String hdfsUri;

	private static boolean seaweedFsEnable;

	private static String seaweedFsAccess;

	private static String seaweedFsReplication;

	private final static Logger log = LoggerFactory.getLogger(HdfsOverFtpSystem.class);

	private static void hdfsInit() throws IOException {
		Configuration configuration = new Configuration();

		configuration.set("fs.defaultFS", hdfsUri);
		if(seaweedFsEnable) {
			configuration.set("fs.seaweedfs.impl", "seaweed.hdfs.SeaweedFileSystem");
			configuration.set("fs.seaweed.volume.server.access", seaweedFsAccess);
			configuration.set("fs.seaweed.replication", seaweedFsReplication);
		}
		fs = FileSystem.get(configuration);
		log.info("HDFS load success");
	}

	/**
	 * Get dfs
	 *
	 * @return dfs
	 * @throws IOException
	 */
	public static FileSystem getDfs() throws IOException {
		if (fs == null) {
			hdfsInit();
		}
		return fs;
	}

	public static void setHdfsUri(String hdfsUri) {
		HdfsOverFtpSystem.hdfsUri = hdfsUri;
	}

	public static String getHdfsUri() {
		return hdfsUri;
	}

	public static void setSeaweedFsEnable(boolean seaweedFsEnable) {
		HdfsOverFtpSystem.seaweedFsEnable = seaweedFsEnable;
	}

	public static void setSeaweedFsAccess(String seaweedFsAccess) {
		HdfsOverFtpSystem.seaweedFsAccess = seaweedFsAccess;
	}

	public static void setSeaweedFsReplication(String seaweedFsReplication) {
		HdfsOverFtpSystem.seaweedFsReplication = seaweedFsReplication;
	}
}