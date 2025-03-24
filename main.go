package main

import (
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	"bufio"

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
func (MyBackend) ListBuckets(ctx context.Context, input s3response.ListBucketsInput) (s3response.ListAllMyBucketsResult, error) {
	log.Printf("MyBackend.ListBuckets(%v, %v)", ctx, input)
	return s3response.ListAllMyBucketsResult{}, s3err.GetAPIError(s3err.ErrNotImplemented)
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
func (MyBackend) DeleteBucket(ctx context.Context, bucket string) error {
	log.Printf("MyBackend.DeleteBucket(%v, %v)", ctx, bucket)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
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
	// Define a splitter function that splits each chunk into outputs parts
	splitter := func(chunk []byte) [][]byte {
		res := make([][]byte, 2)
		for i := 0; i < len(chunk); i++ {
			if i%2 == 0 {
				res[0] = append(res[0], chunk[i])
			} else {
				res[1] = append(res[1], chunk[i])
			}
		}
		log.Printf("res: %v", res)
		return res
	}

	// Create a MultiSplitter
	ms, readers, err := NewMultiSplitter(input.Body, 1024, 2, splitter)
	if err != nil {
		log.Fatalf("Failed to create MultiSplitter: %v", err)
		return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrInternalError)
	}
	// Duplicate the inputs
	input.ContentMD5 = nil
	origLen := *input.ContentLength
	inputCopy := *input
	input.Body = readers[0]
	inputCopy.Body = readers[1]
	keyCopy := *input.Key + "-copy"
	inputCopy.Key = &keyCopy
	newLen1 := origLen / 2
	if origLen%2 != 0 {
		newLen1++
	}
	newLen2 := origLen / 2
	input.ContentLength = &newLen1
	inputCopy.ContentLength = &newLen2
	inputs := [...]*s3.PutObjectInput{input, &inputCopy}
	var outputs [2]*s3.PutObjectOutput
	var errs [2]error
	var wg sync.WaitGroup
	// Perform two PutObject operations concurrently
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			output, err := self.client.PutObject(ctx, inputs[i], s3.WithAPIOptions(
				v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware,
			))
			outputs[i] = output
			errs[i] = err
		}()
	}
	wg.Wait()
	ms.Close()
	for i := 0; i < 2; i++ {
		if errs[i] != nil {
			log.Printf("S3 server returned error for PutObject[%v]: %v", i, errs[i])
			return s3response.PutObjectOutput{}, handleError(errs[i])
		}
	}
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

	origBody := output.Body
	output.Body = UpperCaseReader{r: origBody}
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

func (MyBackend) ListObjectsV2(ctx context.Context, input *s3.ListObjectsV2Input) (s3response.ListObjectsV2Result, error) {
	log.Printf("MyBackend.ListObjectsV2(%v, %v)", ctx, input)
	return s3response.ListObjectsV2Result{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (MyBackend) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	log.Printf("MyBackend.DeleteObject(%v, %v)", ctx, input)
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (MyBackend) DeleteObjects(ctx context.Context, input *s3.DeleteObjectsInput) (s3response.DeleteResult, error) {
	log.Printf("MyBackend.DeleteObjects(%v, %v)", ctx, input)
	return s3response.DeleteResult{}, s3err.GetAPIError(s3err.ErrNotImplemented)
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
