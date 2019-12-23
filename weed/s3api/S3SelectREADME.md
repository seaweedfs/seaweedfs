# AWS S3 Select feature in Seaweedfs

## Start weed

```bash
git checkout s3-select
# Compile weed
weed server -s3
```

## Install And Configure AWSCLI

```bash
pip install awscli
aws configure // default all the way
aws configure set default.s3.signature_version s3v4
```

## Make a bucket in S3

```bash
aws --endpoint-url http://localhost:8333 s3 mb s3://newbucketgoogle
```

## Download googleplaystore.csv

Link: https://www.kaggle.com/lava18/google-play-store-apps

## Copy the file to the S3 bucket

```bash
aws --endpoint-url http://localhost:8333 s3 copy google-play-store-apps/googleplaystore.csv s3://newbucketgoogle
```

## Perform S3 Select Query

```bash
aws --endpoint-url http://localhost:8333 s3api select-object-content --bucket "newbucketgoogle"    --key
 "googleplaystore.csv"    --expression "select * from s3object limit 5"    --expression-type 'SQL'    --input-serialization '{"CSV": {"FileHeaderInfo": "USE","RecordDelimiter": "\n","FieldDelimiter": ","}, "CompressionType": "NONE"}'    --output-serialization '{"CSV": {"RecordDelimiter": "\n","FieldDelimiter": ","}}'    "sample.csv"
```

## Check out result

```bash
vim sample.csv
```

