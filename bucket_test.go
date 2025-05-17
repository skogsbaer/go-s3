package main

import (
	"os/exec"
	"testing"
)

func TestCreateBucket(t *testing.T) {
	// 1. Check preconditions
	t.Log("Setting up test environment...")
	if err := setupTestEnvironment(t); err != nil {
		t.Fatalf("Failed to setup test environment: %v", err)
	}
	defer cleanupTestEnvironment(t)

	// 2. Create bucket through our gateway
	t.Log("Creating bucket through gateway...")
	cmd := exec.Command("mc", "mb", "local-s3/"+testBucket)
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to create bucket through gateway: %v", err)
	}

	// 3. Verify bucket creation
	t.Log("Verifying bucket in first storage...")
	cmd = exec.Command("mc", "ls", "play/"+testBucket)
	if err := cmd.Run(); err != nil {
		t.Errorf("Bucket not found in first storage: %v", err)
	}

	t.Log("Verifying bucket in second storage...")
	cmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "ls", "s3://"+testBucket)
	if err := cmd.Run(); err != nil {
		t.Errorf("Bucket not found in second storage: %v", err)
	}
}

func TestDeleteBucket(t *testing.T) {
	// 1. Ensure bucket exists in both storages
	t.Log("Setting up test environment...")
	if err := setupTestEnvironment(t); err != nil {
		t.Fatalf("Failed to setup test environment: %v", err)
	}
	defer cleanupTestEnvironment(t)

	// Create bucket through our gateway
	t.Log("Creating bucket through gateway...")
	cmd := exec.Command("mc", "mb", "local-s3/"+testBucket)
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to create bucket through gateway: %v", err)
	}

	// 2. Delete bucket through our gateway
	t.Log("Deleting bucket through gateway...")
	cmd = exec.Command("mc", "rb", "local-s3/"+testBucket)
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to delete bucket through gateway: %v", err)
	}

	// 3. Verify bucket deletion
	t.Log("Verifying bucket deletion in first storage...")
	cmd = exec.Command("mc", "ls", "play/"+testBucket)
	if err := cmd.Run(); err == nil {
		t.Error("Bucket still exists in first storage")
	}

	t.Log("Verifying bucket deletion in second storage...")
	cmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "ls", "s3://"+testBucket)
	if err := cmd.Run(); err == nil {
		t.Error("Bucket still exists in second storage")
	}
}
