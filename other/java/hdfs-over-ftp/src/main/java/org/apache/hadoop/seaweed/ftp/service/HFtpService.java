package org.apache.hadoop.seaweed.ftp.service;

import org.apache.ftpserver.DataConnectionConfiguration;
import org.apache.ftpserver.DataConnectionConfigurationFactory;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.command.CommandFactoryFactory;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.hadoop.seaweed.ftp.service.filesystem.HdfsFileSystemManager;
import org.apache.hadoop.seaweed.ftp.service.filesystem.HdfsOverFtpSystem;
import org.apache.hadoop.seaweed.ftp.users.HdfsUserManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;

/**
 * reference: https://github.com/AShiou/hof
 */
@Component
public class HFtpService {

	private static Logger log = Logger.getLogger(HFtpService.class);

	@Value("${ftp.port}")
	private int port = 0;

	@Value("${ftp.passive-address}")
	private String passiveAddress;

	@Value("${ftp.passive-ports}")
	private String passivePorts;

	@Value("${hdfs.uri}")
	private String hdfsUri;

	@Value("${seaweedFs.enable}")
	private boolean seaweedFsEnable;

	@Value("${seaweedFs.access}")
	private String seaweedFsAccess;

	@Value("${seaweedFs.replication}")
	private String seaweedFsReplication;

	private FtpServer ftpServer = null;

	public void startServer() throws Exception {
		log.info("Starting HDFS-Over-Ftp server. port: " + port + " passive-address: " + passiveAddress + " passive-ports: " + passivePorts + " hdfs-uri: " + hdfsUri);

		HdfsOverFtpSystem.setHdfsUri(hdfsUri);
		HdfsOverFtpSystem.setSeaweedFsEnable(seaweedFsEnable);
		HdfsOverFtpSystem.setSeaweedFsAccess(seaweedFsAccess);
		HdfsOverFtpSystem.setSeaweedFsReplication(seaweedFsReplication);

		FtpServerFactory server = new FtpServerFactory();
		server.setFileSystem(new HdfsFileSystemManager());

		ListenerFactory factory = new ListenerFactory();
		factory.setPort(port);

		DataConnectionConfigurationFactory dccFactory = new DataConnectionConfigurationFactory();
		dccFactory.setPassiveAddress("0.0.0.0");
		dccFactory.setPassivePorts(passivePorts);
		dccFactory.setPassiveExternalAddress(passiveAddress);
		DataConnectionConfiguration dcc = dccFactory.createDataConnectionConfiguration();
		factory.setDataConnectionConfiguration(dcc);

		server.addListener("default", factory.createListener());

		HdfsUserManager userManager = new HdfsUserManager();
		final File file = loadResource("/users.properties");
		userManager.setFile(file);
		server.setUserManager(userManager);

		CommandFactoryFactory cmFact = new CommandFactoryFactory();
		cmFact.setUseDefaultCommands(true);
		server.setCommandFactory(cmFact.createCommandFactory());

		// start the server
		ftpServer = server.createServer();
		ftpServer.start();
	}

	public void stopServer() {
		log.info("Stopping Hdfs-Over-Ftp server. port: " + port + " passive-address: " + passiveAddress + " passive-ports: " + passivePorts + " hdfs-uri: " + hdfsUri);
		ftpServer.stop();
	}

	public boolean statusServer() {
		try {
			return !ftpServer.isStopped();
		}catch (Exception e) {
			return false;
		}
	}

	private static File loadResource(String resourceName) {
		return new File(System.getProperty("user.dir") + resourceName);
	}
}