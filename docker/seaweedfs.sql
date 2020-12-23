CREATE DATABASE IF NOT EXISTS seaweedfs;
CREATE USER IF NOT EXISTS 'seaweedfs'@'%' IDENTIFIED BY 'secret';
GRANT ALL PRIVILEGES ON seaweedfs_fast.* TO 'seaweedfs'@'%';
FLUSH PRIVILEGES;
USE seaweedfs;
CREATE TABLE IF NOT EXISTS filemeta (
    dirhash     BIGINT         COMMENT 'first 64 bits of MD5 hash value of directory field',
    name        VARCHAR(1000)  COMMENT 'directory or file name',
    directory   TEXT           COMMENT 'full path to parent directory',
    meta        LONGBLOB,
    PRIMARY KEY (dirhash, name)
) DEFAULT CHARSET=utf8;