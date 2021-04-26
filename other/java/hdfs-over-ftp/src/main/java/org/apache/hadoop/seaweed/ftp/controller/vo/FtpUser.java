package org.apache.hadoop.seaweed.ftp.controller.vo;

public class FtpUser {

    private String homeDirectory;
    private String password;
    private boolean enabled;
    private String name;
    private int maxIdleTime;
    private boolean renamePush;

    public FtpUser() {
    }

    public FtpUser(String homeDirectory, String password, boolean enabled, String name, int maxIdleTime, boolean renamePush) {
        this.homeDirectory = homeDirectory;
        this.password = password;
        this.enabled = enabled;
        this.name = name;
        this.maxIdleTime = maxIdleTime;
        this.renamePush = renamePush;
    }

    public String getHomeDirectory() {
        return homeDirectory;
    }

    public void setHomeDirectory(String homeDirectory) {
        this.homeDirectory = homeDirectory;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getMaxIdleTime() {
        return maxIdleTime;
    }

    public void setMaxIdleTime(int maxIdleTime) {
        this.maxIdleTime = maxIdleTime;
    }

    public boolean isRenamePush() {
        return renamePush;
    }

    public void setRenamePush(boolean renamePush) {
        this.renamePush = renamePush;
    }
}
