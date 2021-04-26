package org.apache.hadoop.seaweed.ftp.users;

import org.apache.ftpserver.FtpServerConfigurationException;
import org.apache.ftpserver.ftplet.*;
import org.apache.ftpserver.usermanager.*;
import org.apache.ftpserver.usermanager.impl.*;
import org.apache.ftpserver.util.BaseProperties;
import org.apache.ftpserver.util.IoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

public class HdfsUserManager extends AbstractUserManager {

	private final Logger LOG = LoggerFactory
			.getLogger(HdfsUserManager.class);

	private final static String DEPRECATED_PREFIX = "FtpServer.user.";

	private final static String PREFIX = "ftpserver.user.";

	private static BaseProperties userDataProp;

	private File userDataFile = new File("users.conf");

	private boolean isConfigured = false;

	private PasswordEncryptor passwordEncryptor = new Md5PasswordEncryptor();


	/**
	 * Retrieve the file used to load and store users
	 *
	 * @return The file
	 */
	public File getFile() {
		return userDataFile;
	}

	/**
	 * Set the file used to store and read users. Must be set before
	 * {@link #configure()} is called.
	 *
	 * @param propFile A file containing users
	 */
	public void setFile(File propFile) {
		if (isConfigured) {
			throw new IllegalStateException("Must be called before configure()");
		}

		this.userDataFile = propFile;
	}


	/**
	 * Retrieve the password encryptor used for this user manager
	 *
	 * @return The password encryptor. Default to {@link Md5PasswordEncryptor}
	 *         if no other has been provided
	 */
	public PasswordEncryptor getPasswordEncryptor() {
		return passwordEncryptor;
	}


	/**
	 * Set the password encryptor to use for this user manager
	 *
	 * @param passwordEncryptor The password encryptor
	 */
	public void setPasswordEncryptor(PasswordEncryptor passwordEncryptor) {
		this.passwordEncryptor = passwordEncryptor;
	}


	/**
	 * Lazy init the user manager
	 */
	private void lazyInit() {
		if (!isConfigured) {
			configure();
		}
	}

	/**
	 * Configure user manager.
	 */
	public void configure() {
		isConfigured = true;
		try {
			userDataProp = new BaseProperties();

			if (userDataFile != null && userDataFile.exists()) {
				FileInputStream fis = null;
				try {
					fis = new FileInputStream(userDataFile);
					userDataProp.load(fis);
				} finally {
					IoUtils.close(fis);
				}
			}
		} catch (IOException e) {
			throw new FtpServerConfigurationException(
					"Error loading user data file : "
							+ userDataFile.getAbsolutePath(), e);
		}

		convertDeprecatedPropertyNames();
	}

	private void convertDeprecatedPropertyNames() {
		Enumeration<?> keys = userDataProp.propertyNames();

		boolean doSave = false;

		while (keys.hasMoreElements()) {
			String key = (String) keys.nextElement();

			if (key.startsWith(DEPRECATED_PREFIX)) {
				String newKey = PREFIX
						+ key.substring(DEPRECATED_PREFIX.length());
				userDataProp.setProperty(newKey, userDataProp.getProperty(key));
				userDataProp.remove(key);

				doSave = true;
			}
		}

		if (doSave) {
			try {
				saveUserData();
			} catch (FtpException e) {
				throw new FtpServerConfigurationException(
						"Failed to save updated user data", e);
			}
		}
	}

	public synchronized void save(User usr, boolean renamePush) throws FtpException {
		lazyInit();
		userDataProp.setProperty(PREFIX + usr.getName() + ".rename.push", renamePush);
		save(usr);
	}

	/**
	 * Save user data. Store the properties.
	 */
	public synchronized void save(User usr) throws FtpException {
		lazyInit();

		// null value check
		if (usr.getName() == null) {
			throw new NullPointerException("User name is null.");
		}
		String thisPrefix = PREFIX + usr.getName() + '.';

		// set other properties
		userDataProp.setProperty(thisPrefix + ATTR_PASSWORD, getPassword(usr));

		String home = usr.getHomeDirectory();
		if (home == null) {
			home = "/";
		}
		userDataProp.setProperty(thisPrefix + ATTR_HOME, home);
		userDataProp.setProperty(thisPrefix + ATTR_ENABLE, usr.getEnabled());
		userDataProp.setProperty(thisPrefix + ATTR_WRITE_PERM, usr
				.authorize(new WriteRequest()) != null);
		userDataProp.setProperty(thisPrefix + ATTR_MAX_IDLE_TIME, usr
				.getMaxIdleTime());

		TransferRateRequest transferRateRequest = new TransferRateRequest();
		transferRateRequest = (TransferRateRequest) usr
				.authorize(transferRateRequest);

		if (transferRateRequest != null) {
			userDataProp.setProperty(thisPrefix + ATTR_MAX_UPLOAD_RATE,
					transferRateRequest.getMaxUploadRate());
			userDataProp.setProperty(thisPrefix + ATTR_MAX_DOWNLOAD_RATE,
					transferRateRequest.getMaxDownloadRate());
		} else {
			userDataProp.remove(thisPrefix + ATTR_MAX_UPLOAD_RATE);
			userDataProp.remove(thisPrefix + ATTR_MAX_DOWNLOAD_RATE);
		}

		// request that always will succeed
		ConcurrentLoginRequest concurrentLoginRequest = new ConcurrentLoginRequest(
				0, 0);
		concurrentLoginRequest = (ConcurrentLoginRequest) usr
				.authorize(concurrentLoginRequest);

		if (concurrentLoginRequest != null) {
			userDataProp.setProperty(thisPrefix + ATTR_MAX_LOGIN_NUMBER,
					concurrentLoginRequest.getMaxConcurrentLogins());
			userDataProp.setProperty(thisPrefix + ATTR_MAX_LOGIN_PER_IP,
					concurrentLoginRequest.getMaxConcurrentLoginsPerIP());
		} else {
			userDataProp.remove(thisPrefix + ATTR_MAX_LOGIN_NUMBER);
			userDataProp.remove(thisPrefix + ATTR_MAX_LOGIN_PER_IP);
		}

		saveUserData();
	}

	/**
	 * @throws FtpException
	 */
	private void saveUserData() throws FtpException {
		File dir = userDataFile.getAbsoluteFile().getParentFile();
		if (dir != null && !dir.exists() && !dir.mkdirs()) {
			String dirName = dir.getAbsolutePath();
			throw new FtpServerConfigurationException(
					"Cannot create directory for user data file : " + dirName);
		}

		// save user data
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(userDataFile);
			userDataProp.store(fos, "Generated file - don't edit (please)");
		} catch (IOException ex) {
			LOG.error("Failed saving user data", ex);
			throw new FtpException("Failed saving user data", ex);
		} finally {
			IoUtils.close(fos);
		}
	}


	public synchronized void list() throws FtpException {
		lazyInit();

		Map dataMap = new HashMap();
		Enumeration<String> propNames = (Enumeration<String>) userDataProp.propertyNames();
		ArrayList<String> a = Collections.list(propNames);
		a.remove("i18nMap");//去除i18nMap
		for(String attrName : a){
//			dataMap.put(attrName, propNames.);
		}

	}

	/**
	 * Delete an user. Removes all this user entries from the properties. After
	 * removing the corresponding from the properties, save the data.
	 */
	public synchronized void delete(String usrName) throws FtpException {
		lazyInit();

		// remove entries from properties
		String thisPrefix = PREFIX + usrName + '.';
		Enumeration<?> propNames = userDataProp.propertyNames();
		ArrayList<String> remKeys = new ArrayList<String>();
		while (propNames.hasMoreElements()) {
			String thisKey = propNames.nextElement().toString();
			if (thisKey.startsWith(thisPrefix)) {
				remKeys.add(thisKey);
			}
		}
		Iterator<String> remKeysIt = remKeys.iterator();
		while (remKeysIt.hasNext()) {
			userDataProp.remove(remKeysIt.next());
		}

		saveUserData();
	}

	/**
	 * Get user password. Returns the encrypted value.
	 * <p/>
	 * <pre>
	 * If the password value is not null
	 *    password = new password
	 * else
	 *   if user does exist
	 *     password = old password
	 *   else
	 *     password = &quot;&quot;
	 * </pre>
	 */
	private String getPassword(User usr) {
		String name = usr.getName();
		String password = usr.getPassword();

		if (password != null) {
			password = passwordEncryptor.encrypt(password);
		} else {
			String blankPassword = passwordEncryptor.encrypt("");

			if (doesExist(name)) {
				String key = PREFIX + name + '.' + ATTR_PASSWORD;
				password = userDataProp.getProperty(key, blankPassword);
			} else {
				password = blankPassword;
			}
		}
		return password;
	}

	/**
	 * Get all user names.
	 */
	public synchronized String[] getAllUserNames() {
		lazyInit();

		// get all user names
		String suffix = '.' + ATTR_HOME;
		ArrayList<String> ulst = new ArrayList<String>();
		Enumeration<?> allKeys = userDataProp.propertyNames();
		int prefixlen = PREFIX.length();
		int suffixlen = suffix.length();
		while (allKeys.hasMoreElements()) {
			String key = (String) allKeys.nextElement();
			if (key.endsWith(suffix)) {
				String name = key.substring(prefixlen);
				int endIndex = name.length() - suffixlen;
				name = name.substring(0, endIndex);
				ulst.add(name);
			}
		}

		Collections.sort(ulst);
		return ulst.toArray(new String[0]);
	}

	private ArrayList<String> parseGroups(String groupsLine) {
		String groupsArray[] = groupsLine.split(",");
		return new ArrayList(Arrays.asList(groupsArray));
	}

	public static synchronized boolean getUserRenamePush(String userName) {
		return userDataProp.getBoolean(PREFIX + userName + ".rename.push", false);
	}

	/**
	 * Load user data.
	 */
	public synchronized User getUserByName(String userName) {
		lazyInit();

		if (!doesExist(userName)) {
			return null;
		}

		String baseKey = PREFIX + userName + '.';
		HdfsUser user = new HdfsUser();
		user.setName(userName);
		user.setEnabled(userDataProp.getBoolean(baseKey + ATTR_ENABLE, true));
		user.setHomeDirectory(userDataProp
				.getProperty(baseKey + ATTR_HOME, "/"));

//		user.setGroups(parseGroups(userDataProp
//				.getProperty(baseKey + "groups")));

		List<Authority> authorities = new ArrayList<Authority>();

		if (userDataProp.getBoolean(baseKey + ATTR_WRITE_PERM, false)) {
			authorities.add(new WritePermission());
		}

		int maxLogin = userDataProp.getInteger(baseKey + ATTR_MAX_LOGIN_NUMBER,
				0);
		int maxLoginPerIP = userDataProp.getInteger(baseKey
				+ ATTR_MAX_LOGIN_PER_IP, 0);

		authorities.add(new ConcurrentLoginPermission(maxLogin, maxLoginPerIP));

		int uploadRate = userDataProp.getInteger(
				baseKey + ATTR_MAX_UPLOAD_RATE, 0);
		int downloadRate = userDataProp.getInteger(baseKey
				+ ATTR_MAX_DOWNLOAD_RATE, 0);

		authorities.add(new TransferRatePermission(downloadRate, uploadRate));

		user.setAuthorities(authorities);

		user.setMaxIdleTime(userDataProp.getInteger(baseKey
				+ ATTR_MAX_IDLE_TIME, 0));

		return user;
	}

	/**
	 * User existance check
	 */
	public synchronized boolean doesExist(String name) {
		lazyInit();

		String key = PREFIX + name + '.' + ATTR_HOME;
		return userDataProp.containsKey(key);
	}

	/**
	 * User authenticate method
	 */
	public synchronized User authenticate(Authentication authentication)
			throws AuthenticationFailedException {
		lazyInit();

		if (authentication instanceof UsernamePasswordAuthentication) {
			UsernamePasswordAuthentication upauth = (UsernamePasswordAuthentication) authentication;

			String user = upauth.getUsername();
			String password = upauth.getPassword();

			if (user == null) {
				throw new AuthenticationFailedException("Authentication failed");
			}

			if (password == null) {
				password = "";
			}

			String storedPassword = userDataProp.getProperty(PREFIX + user + '.'
					+ ATTR_PASSWORD);

			if (storedPassword == null) {
				// user does not exist
				throw new AuthenticationFailedException("Authentication failed");
			}

			if (passwordEncryptor.matches(password, storedPassword)) {
				return getUserByName(user);
			} else {
				throw new AuthenticationFailedException("Authentication failed");
			}

		} else if (authentication instanceof AnonymousAuthentication) {
			if (doesExist("anonymous")) {
				return getUserByName("anonymous");
			} else {
				throw new AuthenticationFailedException("Authentication failed");
			}
		} else {
			throw new IllegalArgumentException(
					"Authentication not supported by this user manager");
		}
	}

	/**
	 * Close the user manager - remove existing entries.
	 */
	public synchronized void dispose() {
		if (userDataProp != null) {
			userDataProp.clear();
			userDataProp = null;
		}
	}
}
