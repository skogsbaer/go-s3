# S3-Compatible Server in Go

This project implements a custom S3-compatible server in Go using
[versitygw](https://github.com/versity/versitygw),
allowing full control over bucket and object handling with a minimal and extensible codebase.

## Features Implemented

All s3 commands as required by versitygw are implemented,
but only as dummy implementation.

## Setup Instructions

Just run the server in case of local s3 storages and local testing

```bash
go run . --local-minio \
  --s3-local-1-endpoint="https://localhost:7531" \
  --s3-local-1-region="us-east-1" \
  --s3-local-1-access="firstminio" \
  --s3-local-1-secret="firstminio" \
  --s3-local-2-endpoint="https://localhost:7532" \
  --s3-local-2-region="us-east-1" \
  --s3-local-2-access="secondminio" \
  --s3-local-2-secret="secondminio"
```

Just run the server in case of remote s3 storages and remote testing

```bash
go run . \
  --s3-local-1-endpoint="https://localhost:7531" \
  --s3-local-1-region="us-east-1" \
  --s3-local-1-access="firstminio" \
  --s3-local-1-secret="firstminio" \
  --s3-local-2-endpoint="https://localhost:7532" \
  --s3-local-2-region="us-east-1" \
  --s3-local-2-access="secondminio" \
  --s3-local-2-secret="secondminio"
```


Server starts on `http://localhost:9000`

## Testing GO-S3 Using MinIO Client (`mc`)

```bash
# Add Server Alias
mc alias set local-s3 http://localhost:9000 testkey testsecret

# Create Bucket
mc mb local-s3/mybucket

# Upload File
mc put myfile.txt local-s3/mybucket/

# Download File
mc get local-s3/mybucket/myfile.txt localfile

# List Buckets
mc ls local-s3

# List Objects in Bucket
mc ls local-s3/mybucket

# Delete Object
mc rm local-s3/mybucket/myfile.txt

# Delete Bucket
mc rb local-s3/mybucket --force
```

## Accessing the first cloud storage at play.min.io directly (`mc`)

```bash
# Create Bucket
mc mb play/mybucket

# Upload File
mc put myfile.txt play/mybucket/

# Download File
mc get play/mybucket/myfile.txt localfile

# List Buckets
mc ls play

# List Objects in Bucket
mc ls play/mybucket

# Delete Object
mc rm play/mybucket/myfile.txt

# Delete Bucket
mc rb play/mybucket --force
```

## Accessing the second cloud storage at s3.nl-ams.scw.cloud (scaleway.com storage) directly (`aws s3`)

```bash
# Create Bucket
aws s3 --endpoint-url https://s3.nl-ams.scw.cloud mb s3://mybucket
# Upload File
aws s3 --endpoint-url https://s3.nl-ams.scw.cloud cp myfile.txt s3://mybucket/

# Download File
aws s3 --endpoint-url https://s3.nl-ams.scw.cloud cp s3://mybucket/myfile.txt myfile.txt 

# List Buckets
aws s3 --endpoint-url https://s3.nl-ams.scw.cloud ls s3:// 

# List Objects in Bucket
aws s3 --endpoint-url https://s3.nl-ams.scw.cloud ls s3://mybucket/ 

# Delete Object
aws s3 --endpoint-url https://s3.nl-ams.scw.cloud rm s3://mybucket/myfile.txt

# Delete Bucket
aws s3 --endpoint-url https://s3.nl-ams.scw.cloud rb s3://mybucket 
```
