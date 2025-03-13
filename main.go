package main

import (
	"context"
	"log"

	"bufio"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/gofiber/fiber/v2"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/metrics"
	"github.com/versity/versitygw/s3api"
	"github.com/versity/versitygw/s3api/middlewares"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3log"
	"github.com/versity/versitygw/s3response"
	"github.com/versity/versitygw/s3select"
)

type MyBackend struct {
	name string
}

func (MyBackend) Shutdown() {
	log.Printf("MyBackend.Shutdown")
}
func (self MyBackend) String() string {
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
func (MyBackend) GetBucketAcl(ctx context.Context, input *s3.GetBucketAclInput) ([]byte, error) {
	log.Printf("MyBackend.GetBucketAcl(%v, %v)", ctx, input)
	// return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
	return []byte{}, nil
}
func (MyBackend) CreateBucket(ctx context.Context, input *s3.CreateBucketInput, data []byte) error {
	log.Printf("MyBackend.CreateBucket(%v, %v)", ctx, input)
	return s3err.GetAPIError(s3err.ErrNotImplemented)
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

func (MyBackend) PutObject(ctx context.Context, input *s3.PutObjectInput) (s3response.PutObjectOutput, error) {
	log.Printf("MyBackend.PutObject(%v, %v)", ctx, input)
	return s3response.PutObjectOutput{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}
func (MyBackend) HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	log.Printf("MyBackend.HeadObject(%v, %v)", ctx, input)
	// return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
	return &s3.HeadObjectOutput{}, nil
}
func (MyBackend) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	log.Printf("MyBackend.GetObject(%v, %v)", ctx, input)
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
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
func (MyBackend) ListObjects(ctx context.Context, input *s3.ListObjectsInput) (s3response.ListObjectsResult, error) {
	log.Printf("MyBackend.ListObjects(%v, %v)", ctx, input)
	// return s3response.ListObjectsResult{}, s3err.GetAPIError(s3err.ErrNotImplemented)
	return s3response.ListObjectsResult{}, nil
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
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
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

func main() {
	app := fiber.New(fiber.Config{
		AppName:               "go-s3",
		ServerHeader:          "GO_S3",
		StreamRequestBody:     true,
		DisableKeepalive:      true,
		Network:               fiber.NetworkTCP,
		DisableStartupMessage: false,
	})

	backend := &MyBackend{}
	key := "testkey"
	secret := "testsecret"
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
