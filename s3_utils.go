package main

import (
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/s3response"
)

func ConvertObjects(objs []types.Object) []s3response.Object {
	result := make([]s3response.Object, 0, len(objs))

	for _, obj := range objs {
		result = append(result, s3response.Object{
			ETag:              obj.ETag,
			Key:               obj.Key,
			LastModified:      obj.LastModified,
			Owner:             obj.Owner,
			Size:              obj.Size,
			RestoreStatus:     obj.RestoreStatus,
			StorageClass:      obj.StorageClass,
			ChecksumAlgorithm: obj.ChecksumAlgorithm,
			ChecksumType:      obj.ChecksumType,
		})
	}

	return result
}
