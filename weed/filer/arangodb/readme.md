##arangodb

database: https://github.com/arangodb/arangodb
go driver: https://github.com/arangodb/go-driver

options:

```
[arangodb]
enabled=true
db_name="seaweedfs"
servers=["http://localhost:8529"]
#basic auth
user="root"
pass="test"

# tls settings
insecure_skip_verify=true
```

i test using this dev database:
`docker run -p 8529:8529 -e ARANGO_ROOT_PASSWORD=test arangodb/arangodb:3.9.0`



## database structure


arangodb has a few restrictions which require the use of a few tricks in order to losslessly store the data.

### filer store

arangodb does not support []byte, and will store such as a uint64 array. this would be a waste of space. to counteract this, we store the data as a length prefixed uint64 byteset.

### filer kv

same as above

### filer buckets

s3 buckets are implemented through arangodb collection. this allows us to do very fast bucket deletion by simply deleting the collection


arangodb collection name rules is character set `azAZ09_-` with a 256 character max. however the first character must be a letter.


s3 bucket name rule is the set `azAZ09.-` with a 63 characters max.

the rules for collection names is then the following:

1. if the bucket name is a valid arangodb collection name, then nothing is done.
2. if the bucket name contains a ".", the "." is replaced with "_"
3. if the bucket name now begins with a number or "_", the prefix "xN--" is prepended to the collection name

this allows for these collection names to be used.


## features i don't personally need but are missing
 [ ] provide tls cert to arango
 [ ] authentication that is not basic auth
 [ ] synchronise endpoint interval config
 [ ] automatic creation of custom index
 [ ] configure default arangodb collection sharding rules
 [ ] configure default arangodb collection replication rules


## complexity

ok, so if https://www.arangodb.com/docs/stable/indexing-index-basics.html#persistent-index is correct

O(1)
- InsertEntry
- UpdateEntry
- FindEntry
- DeleteEntry
- KvPut
- KvGet
- KvDelete

O(log(BUCKET_SIZE))
- DeleteFolderChildren

O(log(DIRECTORY_SIZE))
- ListDirectoryEntries
- ListDirectoryPrefixedEntries
