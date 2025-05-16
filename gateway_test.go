package main

import (
	"os/exec"
	"testing"
)

func TestCreateBucket(t *testing.T) {
	// Test bucket name
	bucketName := "freiburg-test-bucket"

	// 1. Check preconditions
	// Check if bucket exists in first storage (MinIO)
	cmd := exec.Command("mc", "ls", "play/"+bucketName)
	if err := cmd.Run(); err == nil {
		// Bucket exists, remove it
		cmd = exec.Command("mc", "rb", "play/"+bucketName)
		if err := cmd.Run(); err != nil {
			t.Fatalf("Failed to remove bucket from first storage: %v", err)
		}
	}

	// Check if bucket exists in second storage (Scaleway)
	cmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "ls", "s3://"+bucketName)
	if err := cmd.Run(); err == nil {
		// Bucket exists, remove it
		cmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "rb", "s3://"+bucketName)
		if err := cmd.Run(); err != nil {
			t.Fatalf("Failed to remove bucket from second storage: %v", err)
		}
	}

	// 2. Create bucket through our gateway
	cmd = exec.Command("mc", "mb", "local-s3/"+bucketName)
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to create bucket through gateway: %v", err)
	}

	// 3. Verify bucket creation
	// Check first storage
	cmd = exec.Command("mc", "ls", "play/"+bucketName)
	if err := cmd.Run(); err != nil {
		t.Errorf("Bucket not found in first storage: %v", err)
	}

	// Check second storage
	cmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "ls", "s3://"+bucketName)
	if err := cmd.Run(); err != nil {
		t.Errorf("Bucket not found in second storage: %v", err)
	}

	// Cleanup
	cmd = exec.Command("mc", "rb", "play/"+bucketName)
	cmd.Run() // Ignore error as bucket might not exist
	cmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "rb", "s3://"+bucketName)
	cmd.Run() // Ignore error as bucket might not exist
}

func TestDeleteBucket(t *testing.T) {
	// Test bucket name
	bucketName := "freiburg-test-bucket"

	// 1. Ensure bucket exists in both storages
	// Check and create in first storage (MinIO)
	cmd := exec.Command("mc", "ls", "play/"+bucketName)
	if err := cmd.Run(); err != nil {
		// Bucket doesn't exist, create it
		cmd = exec.Command("mc", "mb", "play/"+bucketName)
		if err := cmd.Run(); err != nil {
			t.Fatalf("Failed to create bucket in first storage: %v", err)
		}
	}

	// Check and create in second storage (Scaleway)
	cmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "ls", "s3://"+bucketName)
	if err := cmd.Run(); err != nil {
		// Bucket doesn't exist, create it
		cmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "mb", "s3://"+bucketName)
		if err := cmd.Run(); err != nil {
			t.Fatalf("Failed to create bucket in second storage: %v", err)
		}
	}

	// 2. Delete bucket through our gateway
	cmd = exec.Command("mc", "rb", "local-s3/"+bucketName)
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to delete bucket through gateway: %v", err)
	}

	// 3. Verify bucket deletion
	// Check first storage
	cmd = exec.Command("mc", "ls", "play/"+bucketName)
	if err := cmd.Run(); err == nil {
		t.Error("Bucket still exists in first storage")
	}

	// Check second storage
	cmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "ls", "s3://"+bucketName)
	if err := cmd.Run(); err == nil {
		t.Error("Bucket still exists in second storage")
	}

	// 4. Cleanup (in case test failed)
	cmd = exec.Command("mc", "rb", "play/"+bucketName)
	cmd.Run() // Ignore error as bucket might not exist
	cmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "rb", "s3://"+bucketName)
	cmd.Run() // Ignore error as bucket might not exist
}
