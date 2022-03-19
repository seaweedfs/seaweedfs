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
