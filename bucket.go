package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

func (self *MyBackend) ListBuckets(ctx context.Context, input s3response.ListBucketsInput) (s3response.ListAllMyBucketsResult, error) {
	log.Printf("MyBackend.ListBuckets(%v, %v)", ctx, input)

	// Get buckets from both storage systems
	output1, err := self.client1.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return s3response.ListAllMyBucketsResult{}, handleError(err)
	}

	output2, err := self.client2.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return s3response.ListAllMyBucketsResult{}, handleError(err)
	}

	// Create maps to track buckets in each system
	buckets1 := make(map[string]time.Time)
	buckets2 := make(map[string]time.Time)

	// Populate maps with bucket names and creation dates
	for _, b := range output1.Buckets {
		buckets1[*b.Name] = *b.CreationDate
	}
	for _, b := range output2.Buckets {
		buckets2[*b.Name] = *b.CreationDate
	}

	// Find buckets that exist in both systems
	var commonBuckets []s3response.ListAllMyBucketsEntry
	for name, date1 := range buckets1 {
		if date2, exists := buckets2[name]; exists {
			// Use the earlier creation date
			creationDate := date1
			if date2.Before(date1) {
				creationDate = date2
			}
			commonBuckets = append(commonBuckets, s3response.ListAllMyBucketsEntry{
				Name:         name,
				CreationDate: creationDate,
			})
		}
	}

	return s3response.ListAllMyBucketsResult{
		Buckets: s3response.ListAllMyBucketsList{
			Bucket: commonBuckets,
		},
		Owner: s3response.CanonicalUser{
			ID:          "anonymous",
			DisplayName: "anonymous",
		},
	}, nil
}

func (self *MyBackend) CreateBucket(
	ctx context.Context, input *s3.CreateBucketInput, data []byte,
) error {
	// Check if bucket already exists in either storage system
	_, err1 := self.client1.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: input.Bucket,
	})
	if err1 == nil {
		return fmt.Errorf("bucket '%s' already exists in first storage system", *input.Bucket)
	}

	_, err2 := self.client2.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: input.Bucket,
	})
	if err2 == nil {
		return fmt.Errorf("bucket '%s' already exists in second storage system", *input.Bucket)
	}

	// Create bucket in both storage systems
	_, err1 = self.client1.CreateBucket(ctx, input)
	if err1 != nil {
		return fmt.Errorf("failed to create bucket '%s' in first storage system: %v", *input.Bucket, err1)
	}

	_, err2 = self.client2.CreateBucket(ctx, input)
	if err2 != nil {
		// If second creation fails, try to clean up the first bucket
		_, _ = self.client1.DeleteBucket(ctx, &s3.DeleteBucketInput{
			Bucket: input.Bucket,
		})
		return fmt.Errorf("failed to create bucket '%s' in second storage system: %v", *input.Bucket, err2)
	}

	return nil
}

func (self *MyBackend) DeleteBucket(ctx context.Context, bucket string) error {
	// Check if bucket exists in both storage systems
	if err := self.checkBucketAccess(ctx, bucket); err != nil {
		return err
	}

	// Delete bucket from both storage systems
	_, err1 := self.client1.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	if err1 != nil {
		return fmt.Errorf("failed to delete bucket '%s' from first storage system: %v", bucket, err1)
	}

	_, err2 := self.client2.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	if err2 != nil {
		return fmt.Errorf("failed to delete bucket '%s' from second storage system: %v", bucket, err2)
	}

	return nil
}

func (self *MyBackend) GetBucketAcl(
	ctx context.Context,
	input *s3.GetBucketAclInput,
) ([]byte, error) {
	log.Printf("MyBackend.GetBucketAcl(%v, %v)", ctx, input)
	if input.ExpectedBucketOwner != nil && *input.ExpectedBucketOwner == "" {
		input.ExpectedBucketOwner = nil
	}

	tagout, err := self.client1.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{
		Bucket: input.Bucket,
	})
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			// sdk issue workaround for missing NoSuchTagSet error type
			// https://github.com/aws/aws-sdk-go-v2/issues/2878
			if strings.Contains(ae.ErrorCode(), "NoSuchTagSet") {
				return []byte{}, nil
			}
			if strings.Contains(ae.ErrorCode(), "NotImplemented") {
				return []byte{}, nil
			}
		}
		return nil, handleError(err)
	}

	for _, tag := range tagout.TagSet {
		if *tag.Key == aclKey {
			acl, err := Base64Decode(*tag.Value)
			if err != nil {
				return nil, handleError(err)
			}
			return acl, nil
		}
	}

	return []byte{}, nil
}

func (MyBackend) ChangeBucketOwner(ctx context.Context, bucket string, acl []byte) error {
	log.Printf("MyBackend.ChangeBucketOwner(%v, %v)", ctx, bucket)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) DeleteBucketOwnershipControls(ctx context.Context, bucket string) error {
	log.Printf("MyBackend.DeleteBucketOwnershipControls(%v, %v)", ctx, bucket)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	log.Printf("MyBackend.DeleteBucketPolicy(%v, %v)", ctx, bucket)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) DeleteBucketTagging(ctx context.Context, bucket string) error {
	log.Printf("MyBackend.DeleteBucketTagging(%v, %v)", ctx, bucket)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) GetBucketPolicy(ctx context.Context, bucket string) ([]byte, error) {
	log.Printf("MyBackend.GetBucketPolicy(%v, %v)", ctx, bucket)
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) GetBucketTagging(ctx context.Context, bucket string) (map[string]string, error) {
	log.Printf("MyBackend.GetBucketTagging(%v, %v)", ctx, bucket)
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) GetBucketVersioning(ctx context.Context, bucket string) (s3response.GetBucketVersioningOutput, error) {
	log.Printf("MyBackend.GetBucketVersioning(%v, %v)", ctx, bucket)
	return s3response.GetBucketVersioningOutput{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) GetBucketOwnershipControls(ctx context.Context, bucket string) (types.ObjectOwnership, error) {
	log.Printf("MyBackend.GetBucketOwnershipControls(%v, %v)", ctx, bucket)
	return types.ObjectOwnershipBucketOwnerEnforced, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) PutBucketAcl(ctx context.Context, bucket string, data []byte) error {
	log.Printf("MyBackend.PutBucketAcl(%v, %v)", ctx, bucket)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) PutBucketVersioning(ctx context.Context, bucket string, status types.BucketVersioningStatus) error {
	log.Printf("MyBackend.PutBucketVersioning(%v, %v)", ctx, bucket)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) PutBucketPolicy(ctx context.Context, bucket string, policy []byte) error {
	log.Printf("MyBackend.PutBucketPolicy(%v, %v)", ctx, bucket)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) PutBucketOwnershipControls(ctx context.Context, bucket string, ownership types.ObjectOwnership) error {
	log.Printf("MyBackend.PutBucketOwnershipControls(%v, %v)", ctx, bucket)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) HeadBucket(ctx context.Context, input *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	log.Printf("MyBackend.HeadBucket(%v, %v)", ctx, input)
	// return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
	return &s3.HeadBucketOutput{}, nil
}

func (self *MyBackend) checkBucketAccess(ctx context.Context, bucket string) error {
	// Check first storage system
	_, err1 := self.client1.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err1 != nil {
		var ae smithy.APIError
		if errors.As(err1, &ae) {
			if ae.ErrorCode() == "NotFound" {
				return s3err.GetAPIError(s3err.ErrNoSuchBucket)
			}
			if ae.ErrorCode() == "Forbidden" {
				return s3err.GetAPIError(s3err.ErrAccessDenied)
			}
		}
		return handleError(err1)
	}

	return nil
}

func (MyBackend) PutBucketTagging(ctx context.Context, bucket string, tags map[string]string) error {
	log.Printf("MyBackend.PutBucketTagging(%v, %v)", ctx, bucket)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) ListBucketsAndOwners(ctx context.Context) ([]s3response.Bucket, error) {
	log.Printf("MyBackend.ListBucketsAndOwners(%v)", ctx)
	return []s3response.Bucket{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}
