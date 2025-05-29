package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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

	"crypto/tls"
	"crypto/x509"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
)

// Command line flags
var localMinio = flag.Bool("local-minio", false, "Use local MinIO server")

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
	// Create custom HTTP client with TLS config
	var tlsConfig *tls.Config
	if strings.HasPrefix(client1Config.Endpoint, "https://") {
		// Get the system's root certificate pool
		systemRoots, err := x509.SystemCertPool()
		if err != nil {
			systemRoots = x509.NewCertPool()
		}

		// Load the certificate file from the certs directory
		certFile := filepath.Join("certs", "cert.pem")
		cert, err := os.ReadFile(certFile)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read certificate file: %v", err)
		}

		// Add our certificate to the system's root pool
		if !systemRoots.AppendCertsFromPEM(cert) {
			return nil, nil, fmt.Errorf("failed to append certificate to pool")
		}

		tlsConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    systemRoots,
			ServerName: "localhost",
			// For development only - remove in production
			InsecureSkipVerify: true,
		}
	} else {
		tlsConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	tr := &http.Transport{
		TLSClientConfig:     tlsConfig,
		MaxIdleConns:        100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	httpClient := &http.Client{
		Transport: tr,
		Timeout:   30 * time.Second,
	}

	// Create custom endpoint resolver
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL:               client1Config.Endpoint,
			HostnameImmutable: true,
			SigningRegion:     client1Config.Region,
		}, nil
	})

	// Load AWS config for client1
	cfg1, err := configAws.LoadDefaultConfig(context.TODO(),
		configAws.WithRegion(client1Config.Region),
		configAws.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(client1Config.AccessKey, client1Config.SecretKey, "")),
		configAws.WithHTTPClient(httpClient),
		configAws.WithEndpointResolverWithOptions(customResolver),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load AWS config for client1: %v", err)
	}

	// Create client1 with custom options
	client1 := s3.NewFromConfig(cfg1, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	// Create custom endpoint resolver for client2
	customResolver2 := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL:               client2Config.Endpoint,
			HostnameImmutable: true,
			SigningRegion:     client2Config.Region,
		}, nil
	})

	// Load AWS config for client2
	cfg2, err := configAws.LoadDefaultConfig(context.TODO(),
		configAws.WithRegion(client2Config.Region),
		configAws.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(client2Config.AccessKey, client2Config.SecretKey, "")),
		configAws.WithHTTPClient(httpClient),
		configAws.WithEndpointResolverWithOptions(customResolver2),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load AWS config for client2: %v", err)
	}

	// Create client2 with custom options
	client2 := s3.NewFromConfig(cfg2, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	return client1, client2, nil
}

func main() {
	// Parse command line flags
	flag.Parse()

	// Create standard log directory if it doesn't exist
	logDir := "/var/log/go-s3"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Fatalf("Failed to create log directory: %v", err)
	}

	app := fiber.New(fiber.Config{
		AppName:               "go-s3",
		ServerHeader:          "GO_S3",
		StreamRequestBody:     true,
		DisableKeepalive:      true,
		Network:               fiber.NetworkTCP,
		DisableStartupMessage: false,
	})

	// Load S3 client configs
	client1Config, client2Config, err := LoadDefaultConfigs(*localMinio)
	if err != nil {
		log.Fatalf("Failed to load configurations: %v", err)
	}

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

	// Use standard log file paths
	accessLogPath := filepath.Join(logDir, "access.log")
	adminLogPath := filepath.Join(logDir, "admin.log")

	loggers, err := s3log.InitLogger(&s3log.LogConfig{
		LogFile:      accessLogPath,
		WebhookURL:   "",
		AdminLogFile: adminLogPath,
	})
	if err != nil {
		log.Fatalf("Failed to initialize loggers: %v", err)
	}

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
	log.Printf("S3-compatible server running on http://localhost:9000")
	log.Printf("Log files are located in: %s", logDir)
	log.Fatal(app.Listen(":9000"))
}
