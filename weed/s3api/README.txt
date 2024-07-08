see https://blog.aqwari.net/xml-schema-go/

1. go get aqwari.net/xml/cmd/xsdgen
2. Add EncodingType element for ListBucketResult in AmazonS3.xsd
3. xsdgen -o s3api_xsd_generated.go -pkg s3api AmazonS3.xsd
4. Remove empty Grantee struct in s3api_xsd_generated.go
5. Remove xmlns: sed s'/http:\/\/s3.amazonaws.com\/doc\/2006-03-01\/\ //' s3api_xsd_generated.go
