##arangodb

database: https://github.com/arangodb/arangodb
go driver: https://github.com/arangodb/go-driver


options:

```
[arangodb]
enabled=true
db_name="seaweedfs"
servers=["http://localhost:8529"]
user="root"
pass="test"

# whether to enable fulltext index
# this allows for directory prefix query
fulltext=true

# tls settings
insecure_skip_verify=true
```

supports buckets with an extra field in document.
omitempty means extra space is not used.

i test with
`docker run -p 8529:8529 -e ARANGO_ROOT_PASSWORD=test arangodb/arangodb:3.9.0`


## todo

implement native TTL


## thoughts

should there be one collection per bucket? would make deleting a bucket instant as compared to fast


## comparison

arangodb uses rocksdb in the background, so i am assuming things run in log time

i am not sure how the prefix query scales compared to the recursive calls that some other stores do for folder deletion
might need to change that.


ok, so if https://www.arangodb.com/docs/stable/indexing-index-basics.html#persistent-index is correct
it should be log time to the number of files in the directory
and constant time if you have full directory + file
