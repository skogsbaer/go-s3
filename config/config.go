package config

// S3ClientConfig holds configuration for an S3 client.
type S3ClientConfig struct {
	AccessKey string
	SecretKey string
	Region    string
	Endpoint  string
}

// LoadDefaultConfigs returns the default configs for client1 and client2.
func LoadDefaultConfigs() (client1, client2 S3ClientConfig) {
	client1 = S3ClientConfig{
		AccessKey: "Q3AM3UQ867SPQQA43P2F",
		SecretKey: "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
		Region:    "us-east-1",
		Endpoint:  "https://play.min.io",
	}
	client2 = S3ClientConfig{
		AccessKey: "SCWMAKHJNSFN5EX7ASDF",
		SecretKey: "6ec7f541-f1a8-42f8-a72c-e1e3b85d615b",
		Region:    "nl-ams",
		Endpoint:  "https://s3.nl-ams.scw.cloud",
	}
	return
}
