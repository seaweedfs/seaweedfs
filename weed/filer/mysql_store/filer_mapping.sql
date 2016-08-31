CREATE TABLE IF NOT EXISTS `filer_mapping` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `uriPath` char(256) NOT NULL DEFAULT "" COMMENT 'http uriPath',
  `fid` char(36) NOT NULL DEFAULT "" COMMENT 'seaweedfs fid',
  `createTime` int(10) NOT NULL DEFAULT 0 COMMENT 'createdTime in unix timestamp',
  `updateTime` int(10) NOT NULL DEFAULT 0 COMMENT 'updatedTime in unix timestamp',
  `remark` varchar(20) NOT NULL DEFAULT "" COMMENT 'reserverd field',
  `status` tinyint(2) DEFAULT '1' COMMENT 'resource status',
  PRIMARY KEY (`id`),
  UNIQUE KEY `index_uriPath` (`uriPath`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;