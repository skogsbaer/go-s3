package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// CreateS3Client creates an S3 client with the given configuration.
func CreateS3Client(cfg S3ClientConfig, localMinio bool) *s3.Client {
	log.Printf("DEBUG: Starting CreateS3Client with localMinio=%v", localMinio)
	log.Printf("DEBUG: Endpoint=%s, Region=%s", cfg.Endpoint, cfg.Region)

	// Create custom HTTP client with TLS config
	var tlsConfig *tls.Config
	if localMinio {
		log.Printf("DEBUG: Setting up TLS for local MinIO")
		// Get the system's root certificate pool
		systemRoots, err := x509.SystemCertPool()
		if err != nil {
			log.Printf("DEBUG: Failed to load system cert pool: %v", err)
			systemRoots = x509.NewCertPool()
		}

		// Load the certificate file from the certs directory
		certFile := filepath.Join("certs", "cert.pem")
		log.Printf("DEBUG: Attempting to load certificate from: %s", certFile)

		cert, err := os.ReadFile(certFile)
		if err != nil {
			log.Printf("DEBUG: Failed to read certificate file: %v", err)
			log.Fatalf("Failed to read certificate file: %v", err)
		}
		log.Printf("DEBUG: Successfully read certificate file, size: %d bytes", len(cert))

		// Add our certificate to the system's root pool
		if !systemRoots.AppendCertsFromPEM(cert) {
			log.Printf("DEBUG: Failed to append certificate to pool")
			log.Fatalf("Failed to append certificate to pool")
		}
		log.Printf("DEBUG: Successfully added certificate to pool")

		// Parse the certificate to get more information
		block, _ := pem.Decode(cert)
		if block == nil {
			log.Printf("DEBUG: Failed to decode certificate PEM")
			log.Fatalf("Failed to decode certificate PEM")
		}
		certObj, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			log.Printf("DEBUG: Failed to parse certificate: %v", err)
			log.Fatalf("Failed to parse certificate: %v", err)
		}
		log.Printf("DEBUG: Certificate details:")
		log.Printf("DEBUG:   Subject: %s", certObj.Subject)
		log.Printf("DEBUG:   Issuer: %s", certObj.Issuer)
		log.Printf("DEBUG:   DNS Names: %v", certObj.DNSNames)
		log.Printf("DEBUG:   Valid until: %s", certObj.NotAfter)

		tlsConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    systemRoots,
			ServerName: "localhost",
			// For development only - remove in production
			InsecureSkipVerify: true,
		}
		log.Printf("DEBUG: Created TLS config with server name: localhost")
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
		log.Printf("DEBUG: Resolving endpoint for service=%s, region=%s", service, region)
		return aws.Endpoint{
			URL:               cfg.Endpoint,
			HostnameImmutable: true,
			SigningRegion:     cfg.Region,
		}, nil
	})

	// Load AWS config with our custom HTTP client and endpoint resolver
	awsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")),
		config.WithHTTPClient(httpClient),
		config.WithEndpointResolverWithOptions(customResolver),
	)
	if err != nil {
		log.Printf("DEBUG: Failed to load AWS config: %v", err)
		log.Fatalf("unable to load SDK config, %v", err)
	}

	// Create S3 client with custom options
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
	log.Printf("DEBUG: Created client with endpoint: %s", cfg.Endpoint)

	return client
}
