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
