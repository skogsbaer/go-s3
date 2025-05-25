package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"testing"
)

// Test flags
var localMinioForTesting = flag.Bool("test-local-minio", false, "Use local MinIO for testing")
var noCleanup = flag.Bool("test-no-cleanup", false, "Skip cleanup after tests")

const (
	testBucket = "freiburg-test-bucket"
	testFile   = "myfile.txt"
)

// StorageType represents which storage system to verify
type StorageType int

const (
	FirstStorage StorageType = iota
	SecondStorage
)

// verifyBucketExistsDirectly verifies that a bucket exists in the specified storage system
// storageType: FirstStorage or SecondStorage
// local: true for local MinIO, false for cloud storage
// bucketName: name of the bucket to verify
func verifyBucketExistsDirectly(t *testing.T, storageType StorageType, local bool, bucketName string) error {
	var cmd *exec.Cmd

	if local {
		// Local MinIO verification
		storage := "firstminio"
		if storageType == SecondStorage {
			storage = "secondminio"
		}
		cmd = exec.Command("mc", "--insecure", "ls", storage+"/"+bucketName)
	} else {
		// Cloud storage verification
		if storageType == FirstStorage {
			// MinIO Play
			cmd = exec.Command("mc", "ls", "play/"+bucketName)
		} else {
			// Scaleway
			cmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "ls", "s3://"+bucketName)
		}
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		storageName := "second"
		if storageType == FirstStorage {
			storageName = "first"
		}
		return fmt.Errorf("bucket not found in %s storage: %v\nDebug output: %s",
			storageName,
			err,
			string(output))
	}
	return nil
}

// verifyBucketDoesNotExistDirectly verifies that a bucket does not exist in the specified storage system
func verifyBucketDoesNotExistDirectly(t *testing.T, storageType StorageType, local bool, bucketName string) error {
	var cmd *exec.Cmd

	if local {
		// Local MinIO verification
		storage := "firstminio"
		if storageType == SecondStorage {
			storage = "secondminio"
		}
		cmd = exec.Command("mc", "--insecure", "ls", storage+"/"+bucketName)
	} else {
		// Cloud storage verification
		if storageType == FirstStorage {
			// MinIO Play
			cmd = exec.Command("mc", "ls", "play/"+bucketName)
		} else {
			// Scaleway
			cmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "ls", "s3://"+bucketName)
		}
	}

	output, err := cmd.CombinedOutput()
	if err == nil {
		storageName := "second"
		if storageType == FirstStorage {
			storageName = "first"
		}
		return fmt.Errorf("bucket still exists in %s storage\nDebug output: %s",
			storageName,
			string(output))
	}
	return nil
}

// cleanupTestEnvironment removes test buckets from both storage systems
func cleanupTestEnvironment(t *testing.T) {
	t.Log("Cleaning up test environment...")

	if *localMinioForTesting {
		// Clean local MinIO buckets
		cmd := exec.Command("mc", "rb", "--insecure", "--force", "firstminio/"+testBucket)
		cmd.Run() // Ignore error as bucket might not exist

		cmd = exec.Command("mc", "rb", "--insecure", "--force", "secondminio/"+testBucket)
		cmd.Run() // Ignore error as bucket might not exist
	} else {
		// Clean cloud buckets
		cmd := exec.Command("mc", "rb", "--force", "play/"+testBucket)
		cmd.Run() // Ignore error as bucket might not exist

		cmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "rb", "--force", "s3://"+testBucket)
		cmd.Run() // Ignore error as bucket might not exist
	}
}

// setupTestEnvironment ensures test buckets are clean before starting
func setupTestEnvironment(t *testing.T) error {

	if *localMinioForTesting {
		t.Log("Using local MinIO storage for testing")

		// Check if bucket exists in first storage (MinIO)
		cmd := exec.Command("mc", "--insecure", "ls", "firstminio/"+testBucket)
		if err := cmd.Run(); err == nil {
			// Bucket exists, remove it
			cmd = exec.Command("mc", "--insecure", "rb", "--force", "firstminio/"+testBucket)
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("failed to remove bucket from first storage: %v", err)
			}
		}

		// Check if bucket exists in second storage (MinIO)
		cmd = exec.Command("mc", "--insecure", "ls", "secondminio/"+testBucket)
		if err := cmd.Run(); err == nil {
			// Bucket exists, remove it
			cmd = exec.Command("mc", "--insecure", "rb", "--force", "secondminio/"+testBucket)
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("failed to remove bucket from second storage: %v", err)
			}
		}

	} else {
		t.Log("Using cloud storage for testing")

		// Check if bucket exists in first storage (MinIO)
		cmd := exec.Command("mc", "ls", "play/"+testBucket)
		if err := cmd.Run(); err == nil {
			// Bucket exists, remove it
			cmd = exec.Command("mc", "rb", "--force", "play/"+testBucket)
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("failed to remove bucket from first storage: %v", err)
			}
		}

		// Check if bucket exists in second storage (Scaleway)
		cmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "ls", "s3://"+testBucket)
		if err := cmd.Run(); err == nil {
			// Bucket exists, remove it
			cmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "rb", "--force", "s3://"+testBucket)
			if err := cmd.Run(); err != nil {
				return fmt.Errorf("failed to remove bucket from second storage: %v", err)
			}
		}
	}

	return nil
}

// cleanupTestFile removes a test file from the filesystem
func cleanupTestFile(t *testing.T, filename string) {
	if err := os.Remove(filename); err != nil {
		t.Logf("Failed to remove test file: %v", err)
	}
}

// createTestFile creates a test file with the given name and content
func createTestFile(t *testing.T, filename string, content string) error {
	if err := os.WriteFile(filename, []byte(content), 0644); err != nil {
		return fmt.Errorf("failed to create test file: %v", err)
	}
	return nil
}
