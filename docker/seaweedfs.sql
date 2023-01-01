CREATE DATABASE IF NOT EXISTS seaweedfs;
CREATE USER IF NOT EXISTS 'seaweedfs'@'%' IDENTIFIED BY 'secret';
GRANT ALL PRIVILEGES ON seaweedfs.* TO 'seaweedfs'@'%';
FLUSH PRIVILEGES;
USE seaweedfs;
CREATE TABLE IF NOT EXISTS `filemeta` (
    `dirhash`   BIGINT NOT NULL       COMMENT 'first 64 bits of MD5 hash value of directory field',
    `name`      VARCHAR(766) NOT NULL COMMENT 'directory or file name',
    `directory` TEXT NOT NULL         COMMENT 'full path to parent directory',
    `meta`      LONGBLOB,
    PRIMARY KEY (`dirhash`, `name`)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;