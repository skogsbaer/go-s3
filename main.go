package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	configAws "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/gofiber/fiber/v2"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/metrics"
	"github.com/versity/versitygw/s3api"
	"github.com/versity/versitygw/s3api/middlewares"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3log"
	"github.com/versity/versitygw/s3response"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
)

// S3 proxy implementation:
// $HOME/go/pkg/mod/github.com/versity/versitygw@v1.0.11/backend/s3proxy/s3.go
type MyBackend struct {
	name    string
	client1 *s3.Client // First S3 client for .cypher.first and .rand.second
	client2 *s3.Client // Second S3 client for .cypher.second and .rand.first
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

// createS3Client creates two AWS S3 clients with different endpoints and credentials
func createS3Client(client1Config, client2Config S3ClientConfig) (*s3.Client, *s3.Client, error) {
	credProvider1 := credentials.NewStaticCredentialsProvider(client1Config.AccessKey, client1Config.SecretKey, "")
	cfg1, err := configAws.LoadDefaultConfig(context.TODO(),
		configAws.WithRegion(client1Config.Region),
		configAws.WithCredentialsProvider(credProvider1),
	)
	if err != nil {
		return nil, nil, err
	}
	client1 := s3.NewFromConfig(cfg1, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(client1Config.Endpoint)
		o.UsePathStyle = true
	})
	log.Printf("Created client1 with endpoint: %s", client1Config.Endpoint)

	credProvider2 := credentials.NewStaticCredentialsProvider(client2Config.AccessKey, client2Config.SecretKey, "")
	cfg2, err := configAws.LoadDefaultConfig(context.TODO(),
		configAws.WithRegion(client2Config.Region),
		configAws.WithCredentialsProvider(credProvider2),
	)
	if err != nil {
		return nil, nil, err
	}
	client2 := s3.NewFromConfig(cfg2, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(client2Config.Endpoint)
		o.UsePathStyle = true
	})
	log.Printf("Created client2 with endpoint: %s", client2Config.Endpoint)

	return client1, client2, nil
}

func main() {
	// Parse command line flags
	localMinio := flag.Bool("local-minio", false, "Use local MinIO server")
	flag.Parse()

	app := fiber.New(fiber.Config{
		AppName:               "go-s3",
		ServerHeader:          "GO_S3",
		StreamRequestBody:     true,
		DisableKeepalive:      true,
		Network:               fiber.NetworkTCP,
		DisableStartupMessage: false,
	})

	// Load S3 client configs
	client1Config, client2Config := LoadDefaultConfigs(*localMinio)

	log.Printf("Initializing S3 clients...")
	log.Printf("Client1 config - Endpoint: %s, Region: %s", client1Config.Endpoint, client1Config.Region)
	log.Printf("Client2 config - Endpoint: %s, Region: %s", client2Config.Endpoint, client2Config.Region)

	// Create the S3 clients with different endpoints
	client1, client2, err := createS3Client(client1Config, client2Config)
	if err != nil {
		log.Fatalf("Failed to create S3 clients: %v", err)
	}

	// Test both clients with a simple operation
	ctx := context.Background()

	// Test client1
	log.Printf("Testing client1 connection...")
	_, err = client1.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		log.Printf("Warning: client1 test failed: %v", err)
	} else {
		log.Printf("client1 test successful")
	}

	// Test client2 with more detailed logging
	log.Printf("Testing client2 connection...")
	_, err = client2.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		log.Printf("Warning: client2 test failed: %v", err)
	} else {
		log.Printf("client2 test successful")
	}

	// Additional test for client2 - try to list objects in the bucket
	log.Printf("Testing client2 bucket access...")
	_, err = client2.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String("freiburg-bucket"),
	})
	if err != nil {
		log.Printf("Warning: client2 bucket test failed: %v", err)
	} else {
		log.Printf("client2 bucket test successful")
	}

	// Initialize backend with the S3 clients
	backend := &MyBackend{
		name:    "aws-s3-backend",
		client1: client1,
		client2: client2,
	}

	iam, err := auth.New(&auth.Opts{
		RootAccount: auth.Account{
			Access: "testkey",
			Secret: "testsecret",
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
		middlewares.RootUserConfig{Access: "testkey", Secret: "testsecret"},
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
