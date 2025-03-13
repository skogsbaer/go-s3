# S3-Compatible Server in Go

This project implements a custom S3-compatible server in Go using
[versitygw](https://github.com/versity/versitygw),
allowing full control over bucket and object handling with a minimal and extensible codebase.

## Features Implemented

All s3 commands as required by versitygw are implemented,
but only as dummy implementation.

## Setup Instructions

Just run the server via

```bash
go run main.go
```

Server starts on `http://localhost:9000`

## Test Using MinIO Client (`mc`)

```bash
# Add Server Alias
mc alias set local-s3 http://localhost:9000 testkey testsecret

# Create Bucket
mc mb local-s3/mybucket

# Upload File
mc cp myfile.txt local-s3/mybucket/

# Download File
mc cp local-s3/mybucket/myfile.txt ./

# List Buckets
mc ls local-s3

# List Objects in Bucket
mc ls local-s3/mybucket

# Delete Object
mc rm local-s3/mybucket/myfile.txt

# Delete Bucket
mc rb local-s3/mybucket --force
```

