package main

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	"bufio"
	"bytes"
	"crypto/rand"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/gofiber/fiber/v2"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/metrics"
	"github.com/versity/versitygw/s3api"
	"github.com/versity/versitygw/s3api/middlewares"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3log"
	"github.com/versity/versitygw/s3response"
	"github.com/versity/versitygw/s3select"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
)

// S3 proxy implementation:
// $HOME/go/pkg/mod/github.com/versity/versitygw@v1.0.11/backend/s3proxy/s3.go
type MyBackend struct {
	name   string
	client *s3.Client
}

const aclKey string = "pcsAclKey"

var defTime = time.Time{}

func (MyBackend) Shutdown() {
	log.Printf("MyBackend.Shutdown")
}

func handleError(err error) error {
	if err == nil {
		return nil
	}

	var ae smithy.APIError
	if errors.As(err, &ae) {
		apiErr := s3err.APIError{
			Code:        ae.ErrorCode(),
			Description: ae.ErrorMessage(),
		}
		var re *awshttp.ResponseError
		if errors.As(err, &re) {
			apiErr.HTTPStatusCode = re.Response.StatusCode
		}
		return apiErr
	}
	return err
}

func (self *MyBackend) String() string {
	return self.name
}

func (self *MyBackend) ListBuckets(ctx context.Context, input s3response.ListBucketsInput) (s3response.ListAllMyBucketsResult, error) {
	log.Printf("MyBackend.ListBuckets(%v, %v)", ctx, input)

	// Call the S3 client's ListBuckets API
	output, err := self.client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return s3response.ListAllMyBucketsResult{}, handleError(err)
	}

	// Convert the buckets to the required response format
	var bucketEntries []s3response.ListAllMyBucketsEntry
	for _, b := range output.Buckets {
		bucketEntries = append(bucketEntries, s3response.ListAllMyBucketsEntry{
			Name:         *b.Name,
			CreationDate: *b.CreationDate, // Fixed: Removed parentheses
		})
	}

	return s3response.ListAllMyBucketsResult{
		Buckets: s3response.ListAllMyBucketsList{
			Bucket: bucketEntries,
		},
		Owner: s3response.CanonicalUser{
			ID:          "anonymous",
			DisplayName: "anonymous",
		},
	}, nil
}

func (MyBackend) HeadBucket(ctx context.Context, input *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	log.Printf("MyBackend.HeadBucket(%v, %v)", ctx, input)
	// return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
	return &s3.HeadBucketOutput{}, nil
}

func (self *MyBackend) GetBucketAcl(
	ctx context.Context,
	input *s3.GetBucketAclInput,
) ([]byte, error) {
	log.Printf("MyBackend.GetBucketAcl(%v, %v)", ctx, input)
	if input.ExpectedBucketOwner != nil && *input.ExpectedBucketOwner == "" {
		input.ExpectedBucketOwner = nil
	}

	tagout, err := self.client.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{
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

func (self *MyBackend) CreateBucket(
	ctx context.Context, input *s3.CreateBucketInput, data []byte,
) error {
	log.Printf("MyBackend.CreateBucket(%v, %v)", ctx, input)
	_, err := self.client.CreateBucket(ctx, input)
	return handleError(err)
}

func (MyBackend) PutBucketAcl(ctx context.Context, bucket string, data []byte) error {
	log.Printf("MyBackend.PutBucketAcl(%v, %v)", ctx, bucket)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (self *MyBackend) DeleteBucket(ctx context.Context, bucket string) error {
	log.Printf("MyBackend.DeleteBucket(%v, %v)", ctx, bucket)

	// Attempt to delete the bucket
	_, err := self.client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		// Handle specific error cases
		var ae smithy.APIError
		if errors.As(err, &ae) {
			if ae.ErrorCode() == "NoSuchBucket" {
				return s3err.GetAPIError(s3err.ErrNoSuchBucket)
			}
			if ae.ErrorCode() == "BucketNotEmpty" {
				return s3err.GetAPIError(s3err.ErrBucketNotEmpty)
			}
		}
		return handleError(err)
	}

	return nil
}
func (MyBackend) PutBucketVersioning(ctx context.Context, bucket string, status types.BucketVersioningStatus) error {
	log.Printf("MyBackend.PutBucketVersioning(%v, %v)", ctx, bucket)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) GetBucketVersioning(ctx context.Context, bucket string) (s3response.GetBucketVersioningOutput, error) {
	log.Printf("MyBackend.GetBucketVersioning(%v, %v)", ctx, bucket)
	return s3response.GetBucketVersioningOutput{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) PutBucketPolicy(ctx context.Context, bucket string, policy []byte) error {
	log.Printf("MyBackend.PutBucketPolicy(%v, %v)", ctx, bucket)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) GetBucketPolicy(ctx context.Context, bucket string) ([]byte, error) {
	log.Printf("MyBackend.GetBucketPolicy(%v, %v)", ctx, bucket)
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	log.Printf("MyBackend.DeleteBucketPolicy(%v, %v)", ctx, bucket)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) PutBucketOwnershipControls(ctx context.Context, bucket string, ownership types.ObjectOwnership) error {
	log.Printf("MyBackend.PutBucketOwnershipControls(%v, %v)", ctx, bucket)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) GetBucketOwnershipControls(ctx context.Context, bucket string) (types.ObjectOwnership, error) {
	log.Printf("MyBackend.GetBucketOwnershipControls(%v, %v)", ctx, bucket)
	return types.ObjectOwnershipBucketOwnerEnforced, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) DeleteBucketOwnershipControls(ctx context.Context, bucket string) error {
	log.Printf("MyBackend.DeleteBucketOwnershipControls(%v, %v)", ctx, bucket)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) CreateMultipartUpload(ctx context.Context, input *s3.CreateMultipartUploadInput) (s3response.InitiateMultipartUploadResult, error) {
	log.Printf("MyBackend.CreateMultipartUpload(%v, %v)", ctx, input)
	return s3response.InitiateMultipartUploadResult{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error) {
	log.Printf("MyBackend.CompleteMultipartUpload(%v, %v)", ctx, input)
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) AbortMultipartUpload(ctx context.Context, input *s3.AbortMultipartUploadInput) error {
	log.Printf("MyBackend.AbortMultipartUpload(%v, %v)", ctx, input)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) ListMultipartUploads(ctx context.Context, input *s3.ListMultipartUploadsInput) (s3response.ListMultipartUploadsResult, error) {
	log.Printf("MyBackend.ListMultipartUploads(%v, %v)", ctx, input)
	return s3response.ListMultipartUploadsResult{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) ListParts(ctx context.Context, input *s3.ListPartsInput) (s3response.ListPartsResult, error) {
	log.Printf("MyBackend.ListParts(%v, %v)", ctx, input)
	return s3response.ListPartsResult{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) UploadPart(ctx context.Context, input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	log.Printf("MyBackend.UploadPart(%v, %v)", ctx, input)
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) UploadPartCopy(ctx context.Context, input *s3.UploadPartCopyInput) (s3response.CopyPartResult, error) {
	log.Printf("MyBackend.UploadPartCopy(%v, %v)", ctx, input)
	return s3response.CopyPartResult{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (self *MyBackend) PutObject(
	ctx context.Context, input *s3.PutObjectInput,
) (s3response.PutObjectOutput, error) {
	if input.CacheControl != nil && *input.CacheControl == "" {
		input.CacheControl = nil
	}
	if input.ChecksumCRC32 != nil && *input.ChecksumCRC32 == "" {
		input.ChecksumCRC32 = nil
	}
	if input.ChecksumCRC32C != nil && *input.ChecksumCRC32C == "" {
		input.ChecksumCRC32C = nil
	}
	if input.ChecksumCRC64NVME != nil && *input.ChecksumCRC64NVME == "" {
		input.ChecksumCRC64NVME = nil
	}
	if input.ChecksumSHA1 != nil && *input.ChecksumSHA1 == "" {
		input.ChecksumSHA1 = nil
	}
	if input.ChecksumSHA256 != nil && *input.ChecksumSHA256 == "" {
		input.ChecksumSHA256 = nil
	}
	if input.ContentDisposition != nil && *input.ContentDisposition == "" {
		input.ContentDisposition = nil
	}
	if input.ContentEncoding != nil && *input.ContentEncoding == "" {
		input.ContentEncoding = nil
	}
	if input.ContentLanguage != nil && *input.ContentLanguage == "" {
		input.ContentLanguage = nil
	}
	if input.ContentMD5 != nil && *input.ContentMD5 == "" {
		input.ContentMD5 = nil
	}
	if input.ContentType != nil && *input.ContentType == "" {
		input.ContentType = nil
	}
	if input.ExpectedBucketOwner != nil && *input.ExpectedBucketOwner == "" {
		input.ExpectedBucketOwner = nil
	}
	if input.Expires != nil && *input.Expires == defTime {
		input.Expires = nil
	}
	if input.GrantFullControl != nil && *input.GrantFullControl == "" {
		input.GrantFullControl = nil
	}
	if input.GrantRead != nil && *input.GrantRead == "" {
		input.GrantRead = nil
	}
	if input.GrantReadACP != nil && *input.GrantReadACP == "" {
		input.GrantReadACP = nil
	}
	if input.GrantWriteACP != nil && *input.GrantWriteACP == "" {
		input.GrantWriteACP = nil
	}
	if input.IfMatch != nil && *input.IfMatch == "" {
		input.IfMatch = nil
	}
	if input.IfNoneMatch != nil && *input.IfNoneMatch == "" {
		input.IfNoneMatch = nil
	}
	if input.SSECustomerAlgorithm != nil && *input.SSECustomerAlgorithm == "" {
		input.SSECustomerAlgorithm = nil
	}
	if input.SSECustomerKey != nil && *input.SSECustomerKey == "" {
		input.SSECustomerKey = nil
	}
	if input.SSECustomerKeyMD5 != nil && *input.SSECustomerKeyMD5 == "" {
		input.SSECustomerKeyMD5 = nil
	}
	if input.SSEKMSEncryptionContext != nil && *input.SSEKMSEncryptionContext == "" {
		input.SSEKMSEncryptionContext = nil
	}
	if input.SSEKMSKeyId != nil && *input.SSEKMSKeyId == "" {
		input.SSEKMSKeyId = nil
	}
	if input.Tagging != nil && *input.Tagging == "" {
		input.Tagging = nil
	}
	if input.WebsiteRedirectLocation != nil && *input.WebsiteRedirectLocation == "" {
		input.WebsiteRedirectLocation = nil
	}

	// no object lock for backend
	input.ObjectLockRetainUntilDate = nil
	input.ObjectLockMode = ""
	input.ObjectLockLegalHoldStatus = ""

	log.Printf("MyBackend.PutObject(%v, %+v)", ctx, input)

	// Read the input data into a buffer
	inputData, err := io.ReadAll(input.Body)
	if err != nil {
		return s3response.PutObjectOutput{}, handleError(err)
	}

	// Generate random noise data of the same length as the input data
	randomNoise := make([]byte, len(inputData))
	_, err = rand.Read(randomNoise)
	if err != nil {
		return s3response.PutObjectOutput{}, handleError(err)
	}

	// XOR the input data with the random noise
	xoredData := make([]byte, len(inputData))
	for i := range inputData {
		xoredData[i] = inputData[i] ^ randomNoise[i]
	}

	// Define a splitter function that splits data into two parts
	splitter := func(data []byte) [][]byte {
		mid := len(data) / 2
		return [][]byte{data[:mid], data[mid:]}
	}

	// Split the XORed data and random noise into two parts each
	xoredParts := splitter(xoredData)
	noiseParts := splitter(randomNoise)

	// Prepare the S3 objects for XORed data
	keyFirst := *input.Key + ".cypher.first"
	keySecond := *input.Key + ".cypher.second"
	inputFirst := *input
	inputSecond := *input
	inputFirst.Key = &keyFirst
	inputSecond.Key = &keySecond
	inputFirst.Body = io.NopCloser(bytes.NewReader(xoredParts[0]))
	inputSecond.Body = io.NopCloser(bytes.NewReader(xoredParts[1]))
	inputFirst.ContentLength = aws.Int64(int64(len(xoredParts[0])))
	inputSecond.ContentLength = aws.Int64(int64(len(xoredParts[1])))

	// Prepare the S3 objects for random noise
	keyRandFirst := *input.Key + ".rand.first"
	keyRandSecond := *input.Key + ".rand.second"
	randFirst := *input
	randSecond := *input
	randFirst.Key = &keyRandFirst
	randSecond.Key = &keyRandSecond
	randFirst.Body = io.NopCloser(bytes.NewReader(noiseParts[0]))
	randSecond.Body = io.NopCloser(bytes.NewReader(noiseParts[1]))
	randFirst.ContentLength = aws.Int64(int64(len(noiseParts[0])))
	randSecond.ContentLength = aws.Int64(int64(len(noiseParts[1])))

	// Perform the PutObject operations concurrently
	inputs := [...]*s3.PutObjectInput{&inputFirst, &inputSecond, &randFirst, &randSecond}
	var outputs [4]*s3.PutObjectOutput
	var errs [4]error
	var wg sync.WaitGroup
	wg.Add(4)
	for i := 0; i < 4; i++ {
		go func(i int) {
			defer wg.Done()
			output, err := self.client.PutObject(ctx, inputs[i], s3.WithAPIOptions(
				v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware,
			))
			outputs[i] = output
			errs[i] = err
		}(i)
	}
	wg.Wait()

	// Check for errors
	for i := 0; i < 4; i++ {
		if errs[i] != nil {
			log.Printf("S3 server returned error for PutObject[%v]: %v", i, errs[i])
			return s3response.PutObjectOutput{}, handleError(errs[i])
		}
	}

	// Return the result of the first XORed object
	output := outputs[0]
	var versionID string
	if output.VersionId != nil {
		versionID = *output.VersionId
	}

	return s3response.PutObjectOutput{
		ETag:              *output.ETag,
		VersionID:         versionID,
		ChecksumCRC32:     output.ChecksumCRC32,
		ChecksumCRC32C:    output.ChecksumCRC32C,
		ChecksumCRC64NVME: output.ChecksumCRC64NVME,
		ChecksumSHA1:      output.ChecksumSHA1,
		ChecksumSHA256:    output.ChecksumSHA256,
	}, nil
}

func (self *MyBackend) HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	log.Printf("MyBackend.HeadObject(%v, %v)", ctx, input)
	if input.ExpectedBucketOwner != nil && *input.ExpectedBucketOwner == "" {
		input.ExpectedBucketOwner = nil
	}
	if input.IfMatch != nil && *input.IfMatch == "" {
		input.IfMatch = nil
	}
	if input.IfModifiedSince != nil && *input.IfModifiedSince == defTime {
		input.IfModifiedSince = nil
	}
	if input.IfNoneMatch != nil && *input.IfNoneMatch == "" {
		input.IfNoneMatch = nil
	}
	if input.IfUnmodifiedSince != nil && *input.IfUnmodifiedSince == defTime {
		input.IfUnmodifiedSince = nil
	}
	if input.PartNumber != nil && *input.PartNumber == 0 {
		input.PartNumber = nil
	}
	if input.Range != nil && *input.Range == "" {
		input.Range = nil
	}
	if input.ResponseCacheControl != nil && *input.ResponseCacheControl == "" {
		input.ResponseCacheControl = nil
	}
	if input.ResponseContentDisposition != nil && *input.ResponseContentDisposition == "" {
		input.ResponseContentDisposition = nil
	}
	if input.ResponseContentEncoding != nil && *input.ResponseContentEncoding == "" {
		input.ResponseContentEncoding = nil
	}
	if input.ResponseContentLanguage != nil && *input.ResponseContentLanguage == "" {
		input.ResponseContentLanguage = nil
	}
	if input.ResponseContentType != nil && *input.ResponseContentType == "" {
		input.ResponseContentType = nil
	}
	if input.ResponseExpires != nil && *input.ResponseExpires == defTime {
		input.ResponseExpires = nil
	}
	if input.SSECustomerAlgorithm != nil && *input.SSECustomerAlgorithm == "" {
		input.SSECustomerAlgorithm = nil
	}
	if input.SSECustomerKey != nil && *input.SSECustomerKey == "" {
		input.SSECustomerKey = nil
	}
	if input.SSECustomerKeyMD5 != nil && *input.SSECustomerKeyMD5 == "" {
		input.SSECustomerKeyMD5 = nil
	}
	if input.VersionId != nil && *input.VersionId == "" {
		input.VersionId = nil
	}

	// Check if this is a request for the original file
	key := *input.Key
	if !strings.HasSuffix(key, ".cypher.first") && 
	   !strings.HasSuffix(key, ".cypher.second") && 
	   !strings.HasSuffix(key, ".rand.first") && 
	   !strings.HasSuffix(key, ".rand.second") {
		// This is a request for the original file, check if any of the related files exist
		relatedFiles := []string{
			key + ".cypher.first",
			key + ".cypher.second",
			key + ".rand.first",
			key + ".rand.second",
		}

		for _, relatedKey := range relatedFiles {
			// Create a new input with the related file key
			relatedInput := &s3.HeadObjectInput{
				Bucket: input.Bucket,
				Key:    aws.String(relatedKey),
			}

			// Try to head the related file
			out, err := self.client.HeadObject(ctx, relatedInput)
			if err == nil {
				// If any related file exists, return its metadata
				return out, nil
			}
		}

		// If none of the related files exist, return 404
		return nil, handleError(s3err.GetAPIError(s3err.ErrNoSuchKey))
	}

	// This is a request for a related file, proceed normally
	out, err := self.client.HeadObject(ctx, input)
	return out, handleError(err)
}

func (self *MyBackend) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	log.Printf("MyBackend.GetObject(%v, %v)", ctx, input)
	if input.ExpectedBucketOwner != nil && *input.ExpectedBucketOwner == "" {
		input.ExpectedBucketOwner = nil
	}
	if input.IfMatch != nil && *input.IfMatch == "" {
		input.IfMatch = nil
	}
	if input.IfModifiedSince != nil && *input.IfModifiedSince == defTime {
		input.IfModifiedSince = nil
	}
	if input.IfNoneMatch != nil && *input.IfNoneMatch == "" {
		input.IfNoneMatch = nil
	}
	if input.IfUnmodifiedSince != nil && *input.IfUnmodifiedSince == defTime {
		input.IfUnmodifiedSince = nil
	}
	if input.PartNumber != nil && *input.PartNumber == 0 {
		input.PartNumber = nil
	}
	if input.Range != nil && *input.Range == "" {
		input.Range = nil
	}
	if input.ResponseCacheControl != nil && *input.ResponseCacheControl == "" {
		input.ResponseCacheControl = nil
	}
	if input.ResponseContentDisposition != nil && *input.ResponseContentDisposition == "" {
		input.ResponseContentDisposition = nil
	}
	if input.ResponseContentEncoding != nil && *input.ResponseContentEncoding == "" {
		input.ResponseContentEncoding = nil
	}
	if input.ResponseContentLanguage != nil && *input.ResponseContentLanguage == "" {
		input.ResponseContentLanguage = nil
	}
	if input.ResponseContentType != nil && *input.ResponseContentType == "" {
		input.ResponseContentType = nil
	}
	if input.ResponseExpires != nil && *input.ResponseExpires == defTime {
		input.ResponseExpires = nil
	}
	if input.SSECustomerAlgorithm != nil && *input.SSECustomerAlgorithm == "" {
		input.SSECustomerAlgorithm = nil
	}
	if input.SSECustomerKey != nil && *input.SSECustomerKey == "" {
		input.SSECustomerKey = nil
	}
	if input.SSECustomerKeyMD5 != nil && *input.SSECustomerKeyMD5 == "" {
		input.SSECustomerKeyMD5 = nil
	}
	if input.VersionId != nil && *input.VersionId == "" {
		input.VersionId = nil
	}

	output, err := self.client.GetObject(ctx, input)
	if err != nil {
		return nil, handleError(err)
	}

	return output, nil
}

func (MyBackend) GetObjectAcl(ctx context.Context, input *s3.GetObjectAclInput) (*s3.GetObjectAclOutput, error) {
	log.Printf("MyBackend.GetObjectAcl(%v, %v)", ctx, input)
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) GetObjectAttributes(ctx context.Context, input *s3.GetObjectAttributesInput) (s3response.GetObjectAttributesResponse, error) {
	log.Printf("MyBackend.GetObjectAttributes(%v, %v)", ctx, input)
	return s3response.GetObjectAttributesResponse{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) CopyObject(ctx context.Context, input *s3.CopyObjectInput) (*s3.CopyObjectOutput, error) {
	log.Printf("MyBackend.CopyObject(%v, %v)", ctx, input)
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (self *MyBackend) ListObjects(
	ctx context.Context,
	input *s3.ListObjectsInput,
) (s3response.ListObjectsResult, error) {
	log.Printf("MyBackend.ListObjects(%v, %v)", ctx, input)

	out, err := self.client.ListObjects(ctx, input)
	if err != nil {
		return s3response.ListObjectsResult{}, handleError(err)
	}

	contents := ConvertObjects(out.Contents)

	return s3response.ListObjectsResult{
		CommonPrefixes: out.CommonPrefixes,
		Contents:       contents,
		Delimiter:      out.Delimiter,
		IsTruncated:    out.IsTruncated,
		Marker:         out.Marker,
		MaxKeys:        out.MaxKeys,
		Name:           out.Name,
		NextMarker:     out.NextMarker,
		Prefix:         out.Prefix,
	}, nil
}

func (self *MyBackend) ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input) (s3response.ListObjectsV2Result, error) {
	log.Printf("MyBackend.ListObjectsV2 called with input: %+v", input)

	// Create the input for ListObjectsV2
	listInput := &s3.ListObjectsV2Input{
		Bucket:            input.Bucket,
		Prefix:            input.Prefix,
		Delimiter:         input.Delimiter,
		StartAfter:        input.StartAfter,
	}

	// Only include ContinuationToken if it's not empty
	if input.ContinuationToken != nil && *input.ContinuationToken != "" {
		listInput.ContinuationToken = input.ContinuationToken
		log.Printf("Using ContinuationToken: %s", *input.ContinuationToken)
	} else {
		log.Printf("No ContinuationToken provided or empty")
	}

	log.Printf("Sending ListObjectsV2 request to backend: %+v", listInput)

	// Call the S3 client's ListObjectsV2 API
	output, err := self.client.ListObjectsV2(ctx, listInput)
	if err != nil {
		log.Printf("Error from ListObjectsV2: %v", err)
		return s3response.ListObjectsV2Result{}, handleError(err)
	}

	// Log the response details
	log.Printf("ListObjectsV2 response: IsTruncated=%v, KeyCount=%v", output.IsTruncated, output.KeyCount)
	if output.NextContinuationToken != nil {
		log.Printf("NextContinuationToken: %s", *output.NextContinuationToken)
	} else {
		log.Printf("No NextContinuationToken in response")
	}

	// Convert the objects to the required response format
	var contents []s3response.Object
	for _, obj := range output.Contents {
		contents = append(contents, s3response.Object{
			Key:          obj.Key,
			LastModified: obj.LastModified,
			ETag:         obj.ETag,
			Size:         obj.Size,
			StorageClass: obj.StorageClass,
		})
	}

	// Convert the common prefixes
	var commonPrefixes []types.CommonPrefix
	for _, prefix := range output.CommonPrefixes {
		commonPrefixes = append(commonPrefixes, types.CommonPrefix{
			Prefix: prefix.Prefix,
		})
	}

	// Create the response
	result := s3response.ListObjectsV2Result{
		Name:                  output.Name,
		Prefix:                output.Prefix,
		Delimiter:             output.Delimiter,
		MaxKeys:               output.MaxKeys,
		CommonPrefixes:        commonPrefixes,
		Contents:              contents,
		IsTruncated:           output.IsTruncated,
		KeyCount:              output.KeyCount,
		NextContinuationToken: output.NextContinuationToken,
		StartAfter:            output.StartAfter,
	}

	log.Printf("Returning ListObjectsV2 result: IsTruncated=%v, KeyCount=%v, Contents=%d", 
		result.IsTruncated, result.KeyCount, len(result.Contents))

	return result, nil
}
func (self *MyBackend) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	// Log all input fields
	log.Printf("DeleteObject Input Details:")
	log.Printf("  Bucket: %s", *input.Bucket)
	log.Printf("  Key: %s", *input.Key)
	if input.VersionId != nil {
		log.Printf("  VersionId: %s", *input.VersionId)
	}
	if input.ExpectedBucketOwner != nil {
		log.Printf("  ExpectedBucketOwner: %s", *input.ExpectedBucketOwner)
	}
	if input.MFA != nil {
		log.Printf("  MFA: %s", *input.MFA)
	}
	if input.BypassGovernanceRetention != nil {
		log.Printf("  BypassGovernanceRetention: %v", *input.BypassGovernanceRetention)
	}
	log.Printf("  RequestPayer: %s", input.RequestPayer)

	// Clean up empty values
	if input.ExpectedBucketOwner != nil && *input.ExpectedBucketOwner == "" {
		input.ExpectedBucketOwner = nil
	}
	if input.VersionId != nil && *input.VersionId == "" {
		input.VersionId = nil
	}
	if input.MFA != nil && *input.MFA == "" {
		input.MFA = nil
	}
	if input.BypassGovernanceRetention != nil && !*input.BypassGovernanceRetention {
		input.BypassGovernanceRetention = nil
	}

	// Create a new input with only the required fields
	deleteInput := &s3.DeleteObjectInput{
		Bucket: input.Bucket,
		Key:    input.Key,
	}

	// Only add optional fields if they have values
	if input.VersionId != nil {
		deleteInput.VersionId = input.VersionId
	}

	log.Printf("Sending DeleteObject request to backend: %+v", deleteInput)

	// Call the S3 client's DeleteObject API
	output, err := self.client.DeleteObject(ctx, deleteInput)
	if err != nil {
		log.Printf("Error from DeleteObject: %v", err)
		var ae smithy.APIError
		if errors.As(err, &ae) {
			log.Printf("API Error details - Code: %s, Message: %s", ae.ErrorCode(), ae.ErrorMessage())
		}
		return nil, handleError(err)
	}

	log.Printf("Successfully deleted object: %s from bucket: %s", *input.Key, *input.Bucket)
	if output.VersionId != nil {
		log.Printf("Deleted version: %s", *output.VersionId)
	}
	if output.DeleteMarker != nil {
		log.Printf("Delete marker: %v", *output.DeleteMarker)
	}
	return output, nil
}

func (self *MyBackend) DeleteObjects(ctx context.Context, input *s3.DeleteObjectsInput) (s3response.DeleteResult, error) {
	log.Printf("DeleteObjects Input Details:")
	log.Printf("  Bucket: %s", *input.Bucket)
	log.Printf("  Objects to delete: %d", len(input.Delete.Objects))
	for i, obj := range input.Delete.Objects {
		log.Printf("  Object[%d]: Key=%s, VersionId=%v", i, *obj.Key, obj.VersionId)
	}

	// Create a new DeleteObjectsInput with expanded objects list
	expandedObjects := make([]types.ObjectIdentifier, 0)
	for _, obj := range input.Delete.Objects {
		// Skip the original file and only process related files
		key := *obj.Key
		if strings.HasSuffix(key, ".cypher.first") || 
		   strings.HasSuffix(key, ".cypher.second") || 
		   strings.HasSuffix(key, ".rand.first") || 
		   strings.HasSuffix(key, ".rand.second") {
			// This is already a related file, add it directly
			expandedObjects = append(expandedObjects, types.ObjectIdentifier{
				Key: obj.Key,
			})
			if obj.VersionId != nil {
				expandedObjects[len(expandedObjects)-1].VersionId = obj.VersionId
			}
		} else {
			// This is the original file, add its related files
			relatedFiles := []string{
				key + ".cypher.first",
				key + ".cypher.second",
				key + ".rand.first",
				key + ".rand.second",
			}

			for _, relatedKey := range relatedFiles {
				expandedObjects = append(expandedObjects, types.ObjectIdentifier{
					Key: aws.String(relatedKey),
				})
				if obj.VersionId != nil {
					expandedObjects[len(expandedObjects)-1].VersionId = obj.VersionId
				}
			}
		}
	}

	// Create a new DeleteObjectsInput with the expanded objects list
	deleteInput := &s3.DeleteObjectsInput{
		Bucket: input.Bucket,
		Delete: &types.Delete{
			Objects: expandedObjects,
		},
	}

	log.Printf("Sending DeleteObjects request to backend with expanded objects: %+v", deleteInput)

	// Call the S3 client's DeleteObjects API
	output, err := self.client.DeleteObjects(ctx, deleteInput)
	if err != nil {
		log.Printf("Error from DeleteObjects: %v", err)
		var ae smithy.APIError
		if errors.As(err, &ae) {
			log.Printf("API Error details - Code: %s, Message: %s", ae.ErrorCode(), ae.ErrorMessage())
		}
		return s3response.DeleteResult{}, handleError(err)
	}

	// Convert the output to the required response format
	result := s3response.DeleteResult{
		Deleted: output.Deleted,
		Error:   output.Errors,
	}

	log.Printf("Successfully processed DeleteObjects request. Deleted: %d, Errors: %d", 
		len(result.Deleted), len(result.Error))
	return result, nil
}
func (MyBackend) PutObjectAcl(ctx context.Context, input *s3.PutObjectAclInput) error {
	log.Printf("MyBackend.PutObjectAcl(%v, %v)", ctx, input)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) RestoreObject(ctx context.Context, input *s3.RestoreObjectInput) error {
	log.Printf("MyBackend.RestoreObject(%v, %v)", ctx, input)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (MyBackend) SelectObjectContent(ctx context.Context, input *s3.SelectObjectContentInput) func(w *bufio.Writer) {
	log.Printf("MyBackend.SelectObjectContent(%v, %v)", ctx, input)
	return func(w *bufio.Writer) {
		var getProgress s3select.GetProgress
		progress := input.RequestProgress
		if progress != nil && *progress.Enabled {
			getProgress = func() (bytesScanned int64, bytesProcessed int64) {
				return -1, -1
			}
		}
		mh := s3select.NewMessageHandler(ctx, w, getProgress)
		apiErr := s3err.GetAPIError(s3err.ErrNotImplemented)
		mh.FinishWithError(apiErr.Code, apiErr.Description)
	}
}

func (MyBackend) ListObjectVersions(ctx context.Context, input *s3.ListObjectVersionsInput) (s3response.ListVersionsResult, error) {
	log.Printf("MyBackend.ListObjectVersions(%v, %v)", ctx, input)
	return s3response.ListVersionsResult{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) GetBucketTagging(ctx context.Context, bucket string) (map[string]string, error) {
	log.Printf("MyBackend.GetBucketTagging(%v, %v)", ctx, bucket)
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (MyBackend) PutBucketTagging(ctx context.Context, bucket string, tags map[string]string) error {
	log.Printf("MyBackend.PutBucketTagging(%v, %v)", ctx, bucket)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (MyBackend) DeleteBucketTagging(ctx context.Context, bucket string) error {
	log.Printf("MyBackend.DeleteBucketTagging(%v, %v)", ctx, bucket)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) GetObjectTagging(ctx context.Context, bucket, object string) (map[string]string, error) {
	log.Printf("MyBackend.GetObjectTagging(%v, %v, %v)", ctx, bucket, object)
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (MyBackend) PutObjectTagging(ctx context.Context, bucket, object string, tags map[string]string) error {
	log.Printf("MyBackend.PutObjectTagging(%v, %v, %v)", ctx, bucket, object)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (MyBackend) DeleteObjectTagging(ctx context.Context, bucket, object string) error {
	log.Printf("MyBackend.DeleteObjectTagging(%v, %v, %v)", ctx, bucket, object)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) PutObjectLockConfiguration(ctx context.Context, bucket string, config []byte) error {
	log.Printf("MyBackend.PutObjectLockConfiguration(%v, %v)", ctx, bucket)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (MyBackend) GetObjectLockConfiguration(ctx context.Context, bucket string) ([]byte, error) {
	log.Printf("MyBackend.GetObjectLockConfiguration(%v, %v)", ctx, bucket)
	return nil, s3err.GetAPIError(s3err.ErrObjectLockConfigurationNotFound)
}
func (MyBackend) PutObjectRetention(ctx context.Context, bucket, object, versionId string, bypass bool, retention []byte) error {
	log.Printf("MyBackend.PutObjectRetention(%v, %v, %v)", ctx, bucket, object)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (MyBackend) GetObjectRetention(ctx context.Context, bucket, object, versionId string) ([]byte, error) {
	log.Printf("MyBackend.GetObjectRetention(%v, %v, %v)", ctx, bucket, object)
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (MyBackend) PutObjectLegalHold(ctx context.Context, bucket, object, versionId string, status bool) error {
	log.Printf("MyBackend.PutObjectLegalHold(%v, %v, %v)", ctx, bucket, object)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (MyBackend) GetObjectLegalHold(ctx context.Context, bucket, object, versionId string) (*bool, error) {
	log.Printf("MyBackend.GetObjectLegalHold(%v, %v, %v)", ctx, bucket, object)
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) ChangeBucketOwner(ctx context.Context, bucket string, acl []byte) error {
	log.Printf("MyBackend.ChangeBucketOwner(%v, %v)", ctx, bucket)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (MyBackend) ListBucketsAndOwners(ctx context.Context) ([]s3response.Bucket, error) {
	log.Printf("MyBackend.ListBucketsAndOwners(%v)", ctx)
	return []s3response.Bucket{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}

// FIXME: remove deprecated API
// createS3Client creates a new AWS S3 client with the provided credentials, region, and endpoint
func createS3Client(accessKey, secretKey, region, endpoint string) (*s3.Client, error) {
	// Create a custom credentials provider
	credProvider := credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")

	// Create custom endpoint resolver
	customResolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
		if service == s3.ServiceID {
			return aws.Endpoint{
				URL:               endpoint,
				SigningRegion:     region,
				HostnameImmutable: true,
			}, nil
		}
		// Fallback to default endpoint resolution
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	// Load AWS configuration with custom credentials and endpoint resolver
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithCredentialsProvider(credProvider),
		config.WithEndpointResolver(customResolver),
	)
	if err != nil {
		return nil, err
	}

	// Create and return the S3 client with custom options
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true // Use path-style addressing
	}), nil
}

func main() {
	app := fiber.New(fiber.Config{
		AppName:               "go-s3",
		ServerHeader:          "GO_S3",
		StreamRequestBody:     true,
		DisableKeepalive:      true,
		Network:               fiber.NetworkTCP,
		DisableStartupMessage: false,
	})

	key := "testkey"
	secret := "testsecret"
	region := "us-east-1"
	s3Key := "Q3AM3UQ867SPQQA43P2F"
	s3Secret := "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
	// Specify the S3 server endpoint
	s3Endpoint := "https://play.min.io"

	// Create the S3 client with the custom endpoint
	s3Client, err := createS3Client(s3Key, s3Secret, region, s3Endpoint)
	if err != nil {
		log.Fatalf("Failed to create S3 client: %v", err)
	}

	// Initialize backend with the S3 client
	backend := &MyBackend{
		name:   "aws-s3-backend",
		client: s3Client,
	}

	iam, err := auth.New(&auth.Opts{
		RootAccount: auth.Account{
			Access: key,
			Secret: secret,
			Role:   auth.RoleAdmin,
		}})
	if err != nil {
		log.Fatalf("setup iam failed: %v", err)
	}
	loggers, err := s3log.InitLogger(&s3log.LogConfig{
		LogFile:      "access.log",
		WebhookURL:   "",
		AdminLogFile: "admin.log",
	})
	_, err = s3api.New(
		app,
		backend,
		middlewares.RootUserConfig{Access: key, Secret: secret},
		"9000",
		"us-east-1",
		iam,
		loggers.S3Logger,
		loggers.AdminLogger,
		nil, //s3event.NewNoopSender(),
		func() *metrics.Manager {
			mgr, _ := metrics.NewManager(context.Background(), metrics.Config{})
			return mgr
		}(),
	)
	if err != nil {
		log.Fatalf("s3api init failed: %v", err)
	}
	log.Println("S3-compatible server running on http://localhost:9000")
	log.Fatal(app.Listen(":9000"))
}
