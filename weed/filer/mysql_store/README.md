#MySQL filer mapping store

## Schema format


Basically, uriPath and fid are the key elements stored in MySQL. In view of the optimization and user's usage, 
adding primary key with integer type and involving createTime, updateTime, status fields should be somewhat meaningful.
Of course, you could customize the schema per your concretely circumstance freely.

<pre><code>
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
) DEFAULT CHARSET=utf8;
</code></pre>


The MySQL 's config params is not added into the weed command option as other stores(redis,cassandra). Instead,
We created a config file(json format) for them. TOML,YAML or XML also should be OK. But TOML and YAML need import thirdparty package
while XML is a little bit complex.

The sample config file's  content is below:

<pre><code>
{
    "mysql": [
        {
            "User": "root",
            "Password": "root",
            "HostName": "127.0.0.1",
            "Port": 3306,
            "DataBase": "seaweedfs"
        },
        {
            "User": "root",
            "Password": "root",
            "HostName": "127.0.0.2",
            "Port": 3306,
            "DataBase": "seaweedfs"
        }
    ],
    "IsSharding":true,
    "ShardingNum":1024
}
</code></pre>


The "mysql" field in above conf file is an array which include all mysql instances you prepared to store sharding data.
1. If one mysql instance is enough, just keep one instance in "mysql" field.
2. If table sharding at a specific mysql instance is needed , mark "IsSharding" field with true and specify total table
sharding numbers using "ShardingNum" field.
3. If the mysql service could be auto scaled transparently in your environment, just config one mysql instance(usually it's a frondend proxy or VIP),
and mark "IsSharding" with false value
4. If your prepare more than one mysql instances and have no plan to use table sharding for any instance(mark isSharding with false), instance sharding
will still be done implicitly




