package main

import (
	"flag"
	"fmt"
)

// S3ClientConfig holds configuration for an S3 client.
type S3ClientConfig struct {
	AccessKey string
	SecretKey string
	Region    string
	Endpoint  string
}

// Define command line flags for all storage configurations
var (
	// Local MinIO configurations
	local1Endpoint = flag.String("s3-local-1-endpoint", "", "Endpoint for first local MinIO server")
	local1Region   = flag.String("s3-local-1-region", "", "Region for first local MinIO server")
	local1Access   = flag.String("s3-local-1-access", "", "Access key for first local MinIO server")
	local1Secret   = flag.String("s3-local-1-secret", "", "Secret key for first local MinIO server")

	local2Endpoint = flag.String("s3-local-2-endpoint", "", "Endpoint for second local MinIO server")
	local2Region   = flag.String("s3-local-2-region", "", "Region for second local MinIO server")
	local2Access   = flag.String("s3-local-2-access", "", "Access key for second local MinIO server")
	local2Secret   = flag.String("s3-local-2-secret", "", "Secret key for second local MinIO server")

	// Remote storage configurations
	remote1Endpoint = flag.String("s3-remote-1-endpoint", "", "Endpoint for first remote storage")
	remote1Region   = flag.String("s3-remote-1-region", "", "Region for first remote storage")
	remote1Access   = flag.String("s3-remote-1-access", "", "Access key for first remote storage")
	remote1Secret   = flag.String("s3-remote-1-secret", "", "Secret key for first remote storage")

	remote2Endpoint = flag.String("s3-remote-2-endpoint", "", "Endpoint for second remote storage")
	remote2Region   = flag.String("s3-remote-2-region", "", "Region for second remote storage")
	remote2Access   = flag.String("s3-remote-2-access", "", "Access key for second remote storage")
	remote2Secret   = flag.String("s3-remote-2-secret", "", "Secret key for second remote storage")
)

// LoadDefaultConfigs returns the configs for client1 and client2 based on localMinio flag
func LoadDefaultConfigs(localMinio bool) (client1, client2 S3ClientConfig, err error) {
	if localMinio {
		// Validate local MinIO configurations
		if err := validateConfig(*local1Endpoint, *local1Region, *local1Access, *local1Secret); err != nil {
			return S3ClientConfig{}, S3ClientConfig{}, fmt.Errorf("invalid local1 configuration: %v", err)
		}
		if err := validateConfig(*local2Endpoint, *local2Region, *local2Access, *local2Secret); err != nil {
			return S3ClientConfig{}, S3ClientConfig{}, fmt.Errorf("invalid local2 configuration: %v", err)
		}

		client1 = S3ClientConfig{
			AccessKey: *local1Access,
			SecretKey: *local1Secret,
			Region:    *local1Region,
			Endpoint:  *local1Endpoint,
		}
		client2 = S3ClientConfig{
			AccessKey: *local2Access,
			SecretKey: *local2Secret,
			Region:    *local2Region,
			Endpoint:  *local2Endpoint,
		}
	} else {
		// Validate remote storage configurations
		if err := validateConfig(*remote1Endpoint, *remote1Region, *remote1Access, *remote1Secret); err != nil {
			return S3ClientConfig{}, S3ClientConfig{}, fmt.Errorf("invalid remote1 configuration: %v", err)
		}
		if err := validateConfig(*remote2Endpoint, *remote2Region, *remote2Access, *remote2Secret); err != nil {
			return S3ClientConfig{}, S3ClientConfig{}, fmt.Errorf("invalid remote2 configuration: %v", err)
		}

		client1 = S3ClientConfig{
			AccessKey: *remote1Access,
			SecretKey: *remote1Secret,
			Region:    *remote1Region,
			Endpoint:  *remote1Endpoint,
		}
		client2 = S3ClientConfig{
			AccessKey: *remote2Access,
			SecretKey: *remote2Secret,
			Region:    *remote2Region,
			Endpoint:  *remote2Endpoint,
		}
	}
	return client1, client2, nil
}

// validateConfig checks if all required configuration values are provided
func validateConfig(endpoint, region, access, secret string) error {
	if endpoint == "" {
		return fmt.Errorf("endpoint is required")
	}
	if region == "" {
		return fmt.Errorf("region is required")
	}
	if access == "" {
		return fmt.Errorf("access key is required")
	}
	if secret == "" {
		return fmt.Errorf("secret key is required")
	}
	return nil
}
