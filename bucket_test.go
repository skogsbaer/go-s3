package main

import (
	"os/exec"
	"strings"
	"testing"
)

/*
	func TestBucketDummy(t *testing.T) {
		t.Log("Verifying bucket in first MinIO storage for debugging only ...")
		cmd := exec.Command("mc", "ls", "firstminio/"+testBucket)
		output1, err := cmd.CombinedOutput()
		if err != nil {
			t.Logf("Debug output from first MinIO command: %s", string(output1))
			t.Fatalf("Failed to verify bucket in first MinIO storage: %v", err)
		}
	}
*/
func TestCreateBucket(t *testing.T) {
	// 1. Check preconditions
	t.Logf("Setting up test environment... (Using local MinIO: %v)", *localMinioForTesting)

	if err := setupTestEnvironment(t); err != nil {
		t.Fatalf("Failed to setup test environment: %v", err)
	}
	if !*noCleanup {
		defer cleanupTestEnvironment(t)
	}

	// 2. Create bucket through our gateway
	t.Log("Creating bucket through gateway...")
	cmd := exec.Command("mc", "mb", "local-s3/"+testBucket)
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to create bucket through gateway: %v", err)
	}

	// 3. Verify bucket creation
	t.Log("Verifying bucket in both storage systems...")
	firstErr, secondErr := verifyBucketExistsDirectly(t, *localMinioForTesting, testBucket)
	if firstErr != nil {
		t.Errorf("%v", firstErr)
	}
	if secondErr != nil {
		t.Errorf("%v", secondErr)
	}
}

func TestDeleteBucket(t *testing.T) {
	// 1. Ensure bucket exists in both storages
	t.Log("Setting up test environment and creating bucket...")
	if err := setupTestEnvironmentWithBucket(t, testBucket); err != nil {
		t.Fatalf("Failed to setup test environment and create bucket: %v", err)
	}
	if !*noCleanup {
		defer cleanupTestEnvironment(t)
	}

	// 2. Delete bucket through our gateway
	t.Log("Deleting bucket through gateway...")
	cmd := exec.Command("mc", "rb", "local-s3/"+testBucket)
	if err := cmd.Run(); err != nil {
		t.Fatalf("Failed to delete bucket through gateway: %v", err)
	}

	// 3. Verify bucket deletion
	t.Log("Verifying bucket deletion in both storage systems...")
	firstErr, secondErr := verifyBucketDoesNotExistDirectly(t, *localMinioForTesting, testBucket)
	if firstErr != nil {
		t.Errorf("%v", firstErr)
	}
	if secondErr != nil {
		t.Errorf("%v", secondErr)
	}
}

func TestListEmptyBucket(t *testing.T) {
	// 1. Setup test environment and create bucket
	t.Log("Setting up test environment and creating bucket...")
	if err := setupTestEnvironmentWithBucket(t, testBucket); err != nil {
		t.Fatalf("Failed to setup test environment and create bucket: %v", err)
	}
	if !*noCleanup {
		defer cleanupTestEnvironment(t)
	}

	// 2. List bucket content through gateway
	t.Log("Listing bucket content through gateway...")
	cmd := exec.Command("mc", "ls", "local-s3/"+testBucket)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to list bucket through gateway: %v", err)
	}
	if string(output) != "" {
		t.Errorf("Expected empty bucket listing through gateway, got: %s", string(output))
	}

	// 3. Verify bucket is empty in both storage systems
	t.Log("Verifying empty buckets in both storage systems...")
	firstErr, secondErr := verifyBucketIsEmptyDirectly(t, *localMinioForTesting, testBucket)
	if firstErr != nil {
		t.Errorf("%v", firstErr)
	}
	if secondErr != nil {
		t.Errorf("%v", secondErr)
	}
}

func TestListBucketWithObjects(t *testing.T) {
	// 1. Setup test environment and create bucket
	t.Log("Setting up test environment and creating bucket...")
	if err := setupTestEnvironmentWithBucket(t, testBucket); err != nil {
		t.Fatalf("Failed to setup test environment and create bucket: %v", err)
	}
	if !*noCleanup {
		defer cleanupTestEnvironment(t)
	}

	// 2. Create and upload test files
	shortName := "a.txt"
	longName := "thisisaveryveryelongobjectnameinordertotestthelengthoftheobjectname.txt"

	// Create test files
	if err := createTestFile(t, shortName, "test content for short file"); err != nil {
		t.Fatalf("Failed to create short test file: %v", err)
	}
	defer cleanupTestFile(t, shortName)

	if err := createTestFile(t, longName, "test content for long file"); err != nil {
		t.Fatalf("Failed to create long test file: %v", err)
	}
	defer cleanupTestFile(t, longName)

	// Upload files to storage systems
	t.Log("Uploading files to both storage systems...")

	// Upload short file
	firstErr, secondErr := uploadObjectDirectly(t, *localMinioForTesting, testBucket, shortName, shortName)
	if firstErr != nil {
		t.Fatalf("Failed to upload short file: %v", firstErr)
	}
	if secondErr != nil {
		t.Fatalf("Failed to upload short file: %v", secondErr)
	}

	// Upload long file
	firstErr, secondErr = uploadObjectDirectly(t, *localMinioForTesting, testBucket, longName, longName)
	if firstErr != nil {
		t.Fatalf("Failed to upload long file: %v", firstErr)
	}
	if secondErr != nil {
		t.Fatalf("Failed to upload long file: %v", secondErr)
	}

	// 3. List bucket content through gateway
	t.Log("Listing bucket content through gateway...")
	listCmd := exec.Command("mc", "ls", "local-s3/"+testBucket)
	output, err := listCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to list bucket through gateway: %v", err)
	}

	// 4. Verify the listing contains exactly the two objects
	outputStr := string(output)
	if !strings.Contains(outputStr, shortName) {
		t.Errorf("Expected to find %s in gateway listing, got:\n%s", shortName, outputStr)
	}
	if !strings.Contains(outputStr, longName) {
		t.Errorf("Expected to find %s in gateway listing, got:\n%s", longName, outputStr)
	}

	// Count the number of lines in the output (each object should be on its own line)
	lines := strings.Split(strings.TrimSpace(outputStr), "\n")
	if len(lines) != 2 {
		t.Errorf("Expected exactly 2 objects in gateway listing, got %d:\n%s", len(lines), outputStr)
	}
}

func TestListBucketWithIncompleteObject(t *testing.T) {
	// 1. Setup test environment with bucket
	t.Log("Setting up test environment with bucket...")
	if err := setupTestEnvironmentWithBucket(t, testBucket); err != nil {
		t.Fatalf("Failed to setup test environment with bucket: %v", err)
	}
	if !*noCleanup {
		defer cleanupTestEnvironment(t)
	}

	// 2. Create and upload test files
	shortName := "a.txt"
	longName := "thisisaveryveryelongobjectnameinordertotestthelengthoftheobjectname.txt"

	// Create test files
	if err := createTestFile(t, shortName, "test content for short file"); err != nil {
		t.Fatalf("Failed to create short test file: %v", err)
	}
	defer cleanupTestFile(t, shortName)

	if err := createTestFile(t, longName, "test content for long file"); err != nil {
		t.Fatalf("Failed to create long test file: %v", err)
	}
	defer cleanupTestFile(t, longName)

	// Upload short file with all suffixes using uploadObjectDirectly
	t.Log("Uploading short file to both storage systems...")
	firstErr, secondErr := uploadObjectDirectly(t, *localMinioForTesting, testBucket, shortName, shortName)
	if firstErr != nil {
		t.Fatalf("Failed to upload short file: %v", firstErr)
	}
	if secondErr != nil {
		t.Fatalf("Failed to upload short file: %v", secondErr)
	}

	// Upload long file with incomplete set of suffixes
	if *localMinioForTesting {
		// Upload to first MinIO storage
		cmd := exec.Command("mc", "--insecure", "cp", longName, "firstminio/"+testBucket+"/"+longName+".cypher.first")
		if err := cmd.Run(); err != nil {
			t.Fatalf("Failed to upload long file to first MinIO storage: %v", err)
		}
		cmd = exec.Command("mc", "--insecure", "cp", longName, "firstminio/"+testBucket+"/"+longName+".rand.second")
		if err := cmd.Run(); err != nil {
			t.Fatalf("Failed to upload long file to first MinIO storage: %v", err)
		}

		// Upload to second MinIO storage (missing a.txt.rand.first)
		cmd = exec.Command("mc", "--insecure", "cp", longName, "secondminio/"+testBucket+"/"+longName+".cypher.second")
		if err := cmd.Run(); err != nil {
			t.Fatalf("Failed to upload long file to second MinIO storage: %v", err)
		}
	} else {
		// Upload to first cloud storage (MinIO Play)
		cmd := exec.Command("mc", "cp", longName, "play/"+testBucket+"/"+longName+".cypher.first")
		if err := cmd.Run(); err != nil {
			t.Fatalf("Failed to upload long file to first cloud storage: %v", err)
		}
		cmd = exec.Command("mc", "cp", longName, "play/"+testBucket+"/"+longName+".rand.second")
		if err := cmd.Run(); err != nil {
			t.Fatalf("Failed to upload long file to first cloud storage: %v", err)
		}

		// Upload to second cloud storage (Scaleway) - missing a.txt.rand.first
		cmd = exec.Command("aws", "s3", "--endpoint-url", "https://s3.nl-ams.scw.cloud", "cp", longName, "s3://"+testBucket+"/"+longName+".cypher.second")
		if err := cmd.Run(); err != nil {
			t.Fatalf("Failed to upload long file to second cloud storage: %v", err)
		}

	}

	// 3. List bucket content through gateway
	t.Log("Listing bucket content through gateway...")
	listCmd := exec.Command("mc", "ls", "local-s3/"+testBucket)
	output, err := listCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to list bucket through gateway: %v", err)
	}

	// 4. Verify the listing contains only the short object
	outputStr := string(output)
	if !strings.Contains(outputStr, shortName) {
		t.Errorf("Expected to find %s in gateway listing, got:\n%s", shortName, outputStr)
	}
	if strings.Contains(outputStr, longName) {
		t.Errorf("Expected not to find %s in gateway listing, got:\n%s", longName, outputStr)
	}

	// Count the number of lines in the output (each object should be on its own line)
	lines := strings.Split(strings.TrimSpace(outputStr), "\n")
	if len(lines) != 1 {
		t.Errorf("Expected exactly 1 object in gateway listing, got %d:\n%s", len(lines), outputStr)
	}
}
