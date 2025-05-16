package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
	"github.com/versity/versitygw/s3select"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

func (self *MyBackend) HeadObject(ctx context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	log.Printf("MyBackend.HeadObject(%v, %v)", ctx, input)
	if input.ExpectedBucketOwner != nil && *input.ExpectedBucketOwner == "" {
		input.ExpectedBucketOwner = nil
	}

	// Check if bucket exists and is accessible
	if err := self.checkBucketAccess(ctx, *input.Bucket); err != nil {
		return nil, err
	}

	// Try to get object from first storage system
	output, err := self.client1.HeadObject(ctx, input)
	if err == nil {
		return output, nil
	}

	// If not found in first storage, try second storage
	output, err = self.client2.HeadObject(ctx, input)
	if err != nil {
		return nil, handleError(err)
	}

	return output, nil
}

func (self *MyBackend) GetObject(ctx context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	// Check bucket access first
	if err := self.checkBucketAccess(ctx, *input.Bucket); err != nil {
		return nil, handleError(err)
	}

	log.Printf("MyBackend.GetObject(%v, %v)", ctx, input)
	if input.ExpectedBucketOwner != nil && *input.ExpectedBucketOwner == "" {
		input.ExpectedBucketOwner = nil
	}
	if input.IfMatch != nil && *input.IfMatch == "" {
		input.IfMatch = nil
	}
	if input.IfModifiedSince != nil && *input.IfModifiedSince == defTime {
		input.IfModifiedSince = nil
	}
	if input.IfNoneMatch != nil && *input.IfNoneMatch == "" {
		input.IfNoneMatch = nil
	}
	if input.IfUnmodifiedSince != nil && *input.IfUnmodifiedSince == defTime {
		input.IfUnmodifiedSince = nil
	}
	if input.PartNumber != nil && *input.PartNumber == 0 {
		input.PartNumber = nil
	}
	if input.Range != nil && *input.Range == "" {
		input.Range = nil
	}
	if input.ResponseCacheControl != nil && *input.ResponseCacheControl == "" {
		input.ResponseCacheControl = nil
	}
	if input.ResponseContentDisposition != nil && *input.ResponseContentDisposition == "" {
		input.ResponseContentDisposition = nil
	}
	if input.ResponseContentEncoding != nil && *input.ResponseContentEncoding == "" {
		input.ResponseContentEncoding = nil
	}
	if input.ResponseContentLanguage != nil && *input.ResponseContentLanguage == "" {
		input.ResponseContentLanguage = nil
	}
	if input.ResponseContentType != nil && *input.ResponseContentType == "" {
		input.ResponseContentType = nil
	}
	if input.ResponseExpires != nil && *input.ResponseExpires == defTime {
		input.ResponseExpires = nil
	}
	if input.SSECustomerAlgorithm != nil && *input.SSECustomerAlgorithm == "" {
		input.SSECustomerAlgorithm = nil
	}
	if input.SSECustomerKey != nil && *input.SSECustomerKey == "" {
		input.SSECustomerKey = nil
	}
	if input.SSECustomerKeyMD5 != nil && *input.SSECustomerKeyMD5 == "" {
		input.SSECustomerKeyMD5 = nil
	}
	if input.VersionId != nil && *input.VersionId == "" {
		input.VersionId = nil
	}

	// Check if this is a request for the original file
	key := *input.Key
	if !strings.HasSuffix(key, ".cypher.first") &&
		!strings.HasSuffix(key, ".cypher.second") &&
		!strings.HasSuffix(key, ".rand.first") &&
		!strings.HasSuffix(key, ".rand.second") {
		// This is a request for the original file, we need to reconstruct it
		// from its four parts across both storage engines

		// Define the related files and their corresponding clients
		type fileInfo struct {
			key    string
			client *s3.Client
		}
		files := []fileInfo{
			{key + ".cypher.first", self.client1},  // client1
			{key + ".cypher.second", self.client2}, // client2
			{key + ".rand.first", self.client2},    // client2
			{key + ".rand.second", self.client1},   // client1
		}

		// Download all four parts concurrently
		var parts [4][]byte
		var errs [4]error
		var wg sync.WaitGroup
		wg.Add(4)

		for i, file := range files {
			go func(i int, file fileInfo) {
				defer wg.Done()
				// Create a new input for the related file
				relatedInput := &s3.GetObjectInput{
					Bucket: input.Bucket,
					Key:    aws.String(file.key),
				}

				// Get the related file using the appropriate client
				output, err := file.client.GetObject(ctx, relatedInput)
				if err != nil {
					errs[i] = err
					return
				}

				// Read the data
				data, err := io.ReadAll(output.Body)
				if err != nil {
					errs[i] = err
					return
				}
				parts[i] = data
			}(i, file)
		}
		wg.Wait()

		// Check for errors
		for i := 0; i < 4; i++ {
			if errs[i] != nil {
				log.Printf("Error downloading part %d: %v", i, errs[i])
				return nil, handleError(errs[i])
			}
		}

		// Define a joiner function that combines two parts
		joiner := func(part1, part2 []byte) []byte {
			return append(part1, part2...)
		}

		// Join the parts
		cypherData := joiner(parts[0], parts[1]) // .cypher.first + .cypher.second
		randData := joiner(parts[2], parts[3])   // .rand.first + .rand.second

		// XOR the joined data to reconstruct the original
		secretData := make([]byte, len(cypherData))
		for i := range cypherData {
			secretData[i] = cypherData[i] ^ randData[i]
		}

		// Create a new output with the reconstructed data
		return &s3.GetObjectOutput{
			Body:          io.NopCloser(bytes.NewReader(secretData)),
			ContentLength: aws.Int64(int64(len(secretData))),
			LastModified:  aws.Time(time.Now()),
		}, nil
	}

	// This is a request for a related file, determine which client to use
	var client *s3.Client
	if strings.HasSuffix(key, ".cypher.first") || strings.HasSuffix(key, ".rand.second") {
		client = self.client1
	} else {
		client = self.client2
	}

	// Get the object using the appropriate client
	output, err := client.GetObject(ctx, input)
	if err != nil {
		return nil, handleError(err)
	}

	return output, nil
}

func (MyBackend) GetObjectAcl(ctx context.Context, input *s3.GetObjectAclInput) (*s3.GetObjectAclOutput, error) {
	log.Printf("MyBackend.GetObjectAcl(%v, %v)", ctx, input)
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (self *MyBackend) PutObject(
	ctx context.Context, input *s3.PutObjectInput,
) (s3response.PutObjectOutput, error) {
	// Check bucket access first
	if err := self.checkBucketAccess(ctx, *input.Bucket); err != nil {
		return s3response.PutObjectOutput{}, handleError(err)
	}

	if input.CacheControl != nil && *input.CacheControl == "" {
		input.CacheControl = nil
	}
	if input.ChecksumCRC32 != nil && *input.ChecksumCRC32 == "" {
		input.ChecksumCRC32 = nil
	}
	if input.ChecksumCRC32C != nil && *input.ChecksumCRC32C == "" {
		input.ChecksumCRC32C = nil
	}
	if input.ChecksumCRC64NVME != nil && *input.ChecksumCRC64NVME == "" {
		input.ChecksumCRC64NVME = nil
	}
	if input.ChecksumSHA1 != nil && *input.ChecksumSHA1 == "" {
		input.ChecksumSHA1 = nil
	}
	if input.ChecksumSHA256 != nil && *input.ChecksumSHA256 == "" {
		input.ChecksumSHA256 = nil
	}
	if input.ContentDisposition != nil && *input.ContentDisposition == "" {
		input.ContentDisposition = nil
	}
	if input.ContentEncoding != nil && *input.ContentEncoding == "" {
		input.ContentEncoding = nil
	}
	if input.ContentLanguage != nil && *input.ContentLanguage == "" {
		input.ContentLanguage = nil
	}
	if input.ContentMD5 != nil && *input.ContentMD5 == "" {
		input.ContentMD5 = nil
	}
	if input.ContentType != nil && *input.ContentType == "" {
		input.ContentType = nil
	}
	if input.ExpectedBucketOwner != nil && *input.ExpectedBucketOwner == "" {
		input.ExpectedBucketOwner = nil
	}
	if input.Expires != nil && *input.Expires == defTime {
		input.Expires = nil
	}
	if input.GrantFullControl != nil && *input.GrantFullControl == "" {
		input.GrantFullControl = nil
	}
	if input.GrantRead != nil && *input.GrantRead == "" {
		input.GrantRead = nil
	}
	if input.GrantReadACP != nil && *input.GrantReadACP == "" {
		input.GrantReadACP = nil
	}
	if input.GrantWriteACP != nil && *input.GrantWriteACP == "" {
		input.GrantWriteACP = nil
	}
	if input.IfMatch != nil && *input.IfMatch == "" {
		input.IfMatch = nil
	}
	if input.IfNoneMatch != nil && *input.IfNoneMatch == "" {
		input.IfNoneMatch = nil
	}
	if input.SSECustomerAlgorithm != nil && *input.SSECustomerAlgorithm == "" {
		input.SSECustomerAlgorithm = nil
	}
	if input.SSECustomerKey != nil && *input.SSECustomerKey == "" {
		input.SSECustomerKey = nil
	}
	if input.SSECustomerKeyMD5 != nil && *input.SSECustomerKeyMD5 == "" {
		input.SSECustomerKeyMD5 = nil
	}
	if input.SSEKMSEncryptionContext != nil && *input.SSEKMSEncryptionContext == "" {
		input.SSEKMSEncryptionContext = nil
	}
	if input.SSEKMSKeyId != nil && *input.SSEKMSKeyId == "" {
		input.SSEKMSKeyId = nil
	}
	if input.Tagging != nil && *input.Tagging == "" {
		input.Tagging = nil
	}
	if input.WebsiteRedirectLocation != nil && *input.WebsiteRedirectLocation == "" {
		input.WebsiteRedirectLocation = nil
	}

	// no object lock for backend
	input.ObjectLockRetainUntilDate = nil
	input.ObjectLockMode = ""
	input.ObjectLockLegalHoldStatus = ""

	log.Printf("MyBackend.PutObject(%v, %+v)", ctx, input)

	// Read the input data into a buffer
	inputData, err := io.ReadAll(input.Body)
	if err != nil {
		return s3response.PutObjectOutput{}, handleError(err)
	}

	// Generate random noise data of the same length as the input data
	randomNoise := make([]byte, len(inputData))
	_, err = rand.Read(randomNoise)
	if err != nil {
		return s3response.PutObjectOutput{}, handleError(err)
	}

	// XOR the input data with the random noise
	xoredData := make([]byte, len(inputData))
	for i := range inputData {
		xoredData[i] = inputData[i] ^ randomNoise[i]
	}

	// Define a splitter function that splits data into two parts
	splitter := func(data []byte) [][]byte {
		mid := len(data) / 2
		return [][]byte{data[:mid], data[mid:]}
	}

	// Split the XORed data and random noise into two parts each
	xoredParts := splitter(xoredData)
	noiseParts := splitter(randomNoise)

	// Prepare the S3 objects for XORed data
	keyFirst := *input.Key + ".cypher.first"
	keySecond := *input.Key + ".cypher.second"
	inputFirst := *input
	inputSecond := *input
	inputFirst.Key = &keyFirst
	inputSecond.Key = &keySecond
	inputFirst.Body = io.NopCloser(bytes.NewReader(xoredParts[0]))
	inputSecond.Body = io.NopCloser(bytes.NewReader(xoredParts[1]))
	inputFirst.ContentLength = aws.Int64(int64(len(xoredParts[0])))
	inputSecond.ContentLength = aws.Int64(int64(len(xoredParts[1])))

	// Prepare the S3 objects for random noise
	keyRandFirst := *input.Key + ".rand.first"
	keyRandSecond := *input.Key + ".rand.second"
	randFirst := *input
	randSecond := *input
	randFirst.Key = &keyRandFirst
	randSecond.Key = &keyRandSecond
	randFirst.Body = io.NopCloser(bytes.NewReader(noiseParts[0]))
	randSecond.Body = io.NopCloser(bytes.NewReader(noiseParts[1]))
	randFirst.ContentLength = aws.Int64(int64(len(noiseParts[0])))
	randSecond.ContentLength = aws.Int64(int64(len(noiseParts[1])))

	// Perform the PutObject operations concurrently using both clients
	var wg sync.WaitGroup
	wg.Add(4)

	// Channel to collect results
	type result struct {
		output *s3.PutObjectOutput
		err    error
		index  int
	}
	results := make(chan result, 4)

	// Store .cypher.first and .rand.second in client1
	go func() {
		defer wg.Done()
		output, err := self.client1.PutObject(ctx, &inputFirst, s3.WithAPIOptions(
			v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware,
		))
		results <- result{output: output, err: err, index: 0}
	}()

	go func() {
		defer wg.Done()
		output, err := self.client1.PutObject(ctx, &randSecond, s3.WithAPIOptions(
			v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware,
		))
		results <- result{output: output, err: err, index: 3}
	}()

	// Store .cypher.second and .rand.first in client2
	go func() {
		defer wg.Done()
		output, err := self.client2.PutObject(ctx, &inputSecond, s3.WithAPIOptions(
			v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware,
		))
		results <- result{output: output, err: err, index: 1}
	}()

	go func() {
		defer wg.Done()
		output, err := self.client2.PutObject(ctx, &randFirst, s3.WithAPIOptions(
			v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware,
		))
		results <- result{output: output, err: err, index: 2}
	}()

	// Wait for all operations to complete
	wg.Wait()
	close(results)

	// Check for errors
	var outputs [4]*s3.PutObjectOutput
	for r := range results {
		if r.err != nil {
			log.Printf("S3 server returned error for PutObject[%v]: %v", r.index, r.err)
			return s3response.PutObjectOutput{}, handleError(r.err)
		}
		outputs[r.index] = r.output
	}

	// Return the result of the first XORed object
	output := outputs[0]
	var versionID string
	if output.VersionId != nil {
		versionID = *output.VersionId
	}

	return s3response.PutObjectOutput{
		ETag:              *output.ETag,
		VersionID:         versionID,
		ChecksumCRC32:     output.ChecksumCRC32,
		ChecksumCRC32C:    output.ChecksumCRC32C,
		ChecksumCRC64NVME: output.ChecksumCRC64NVME,
		ChecksumSHA1:      output.ChecksumSHA1,
		ChecksumSHA256:    output.ChecksumSHA256,
	}, nil
}

func (MyBackend) GetObjectAttributes(ctx context.Context, input *s3.GetObjectAttributesInput) (s3response.GetObjectAttributesResponse, error) {
	log.Printf("MyBackend.GetObjectAttributes(%v, %v)", ctx, input)
	return s3response.GetObjectAttributesResponse{}, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (MyBackend) CopyObject(ctx context.Context, input *s3.CopyObjectInput) (*s3.CopyObjectOutput, error) {
	log.Printf("MyBackend.CopyObject(%v, %v)", ctx, input)
	return nil, s3err.GetAPIError(s3err.ErrNotImplemented)
}

func (self *MyBackend) ListObjects(
	ctx context.Context,
	input *s3.ListObjectsInput,
) (s3response.ListObjectsResult, error) {
	// Check bucket access first
	if err := self.checkBucketAccess(ctx, *input.Bucket); err != nil {
		return s3response.ListObjectsResult{}, handleError(err)
	}

	log.Printf("MyBackend.ListObjects(%v, %v)", ctx, input)

	// Get objects from both storage systems
	out1, err := self.client1.ListObjects(ctx, input)
	if err != nil {
		return s3response.ListObjectsResult{}, handleError(err)
	}

	out2, err := self.client2.ListObjects(ctx, input)
	if err != nil {
		return s3response.ListObjectsResult{}, handleError(err)
	}

	// Create maps to track objects in each system
	objects1 := make(map[string]types.Object)
	objects2 := make(map[string]types.Object)

	// Populate maps with objects from each system
	for _, obj := range out1.Contents {
		key := *obj.Key
		// Only track objects with correct postfixes for client1
		if strings.HasSuffix(key, ".cypher.first") || strings.HasSuffix(key, ".rand.second") {
			objects1[key] = obj
		}
	}

	for _, obj := range out2.Contents {
		key := *obj.Key
		// Only track objects with correct postfixes for client2
		if strings.HasSuffix(key, ".cypher.second") || strings.HasSuffix(key, ".rand.first") {
			objects2[key] = obj
		}
	}

	// Create a map to track which base files have all four parts
	completeFiles := make(map[string]bool)

	// Check for complete sets across both storage systems
	for key1 := range objects1 {
		// Extract base name from client1 objects
		baseName := strings.TrimSuffix(key1, ".cypher.first")
		baseName = strings.TrimSuffix(baseName, ".rand.second")

		// Check if all required parts exist
		requiredParts := []string{
			baseName + ".cypher.first",  // client1
			baseName + ".cypher.second", // client2
			baseName + ".rand.first",    // client2
			baseName + ".rand.second",   // client1
		}

		// Verify all parts exist in the correct storage systems
		allPartsExist := true
		for _, part := range requiredParts {
			if strings.HasSuffix(part, ".cypher.first") || strings.HasSuffix(part, ".rand.second") {
				if _, exists := objects1[part]; !exists {
					allPartsExist = false
					break
				}
			} else {
				if _, exists := objects2[part]; !exists {
					allPartsExist = false
					break
				}
			}
		}

		if allPartsExist {
			completeFiles[baseName] = true
		}
	}

	// Create a new list of objects containing only complete sets
	var filteredContents []types.Object
	for baseName := range completeFiles {
		// Use the .cypher.first object as the base object
		if obj, exists := objects1[baseName+".cypher.first"]; exists {
			// Create a new object with the base name
			newObj := obj
			newObj.Key = aws.String(baseName)
			filteredContents = append(filteredContents, newObj)
		}
	}

	// Update the output with filtered contents
	out1.Contents = filteredContents

	contents := ConvertObjects(out1.Contents)

	return s3response.ListObjectsResult{
		CommonPrefixes: out1.CommonPrefixes,
		Contents:       contents,
		Delimiter:      out1.Delimiter,
		IsTruncated:    out1.IsTruncated,
		Marker:         out1.Marker,
		MaxKeys:        out1.MaxKeys,
		Name:           out1.Name,
		NextMarker:     out1.NextMarker,
		Prefix:         out1.Prefix,
	}, nil
}

func (self *MyBackend) ListObjectsV2(
	ctx context.Context,
	input *s3.ListObjectsV2Input,
) (s3response.ListObjectsV2Result, error) {
	// Check bucket access first
	if err := self.checkBucketAccess(ctx, *input.Bucket); err != nil {
		return s3response.ListObjectsV2Result{}, handleError(err)
	}

	log.Printf("MyBackend.ListObjectsV2(%v, %v)", ctx, input)

	// If we have a prefix that doesn't end with a delimiter and is not empty, we should check if it's a file
	if input.Prefix != nil && *input.Prefix != "" && !strings.HasSuffix(*input.Prefix, "/") {
		// Check if the file exists in either storage system
		key := *input.Prefix
		relatedFiles := []string{
			key + ".cypher.first",
			key + ".cypher.second",
			key + ".rand.first",
			key + ".rand.second",
		}

		log.Printf("Checking for complete file set: %s", key)

		// Check if all parts exist
		allPartsExist := true
		for _, part := range relatedFiles {
			var client *s3.Client
			if strings.HasSuffix(part, ".cypher.first") || strings.HasSuffix(part, ".rand.second") {
				client = self.client1
			} else {
				client = self.client2
			}

			_, err := client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: input.Bucket,
				Key:    aws.String(part),
			})
			if err != nil {
				log.Printf("Missing part: %s", part)
				allPartsExist = false
				break
			}
			log.Printf("Found part: %s", part)
		}

		if allPartsExist {
			// If all parts exist, return a single object
			obj, err := self.client1.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: input.Bucket,
				Key:    aws.String(key + ".cypher.first"),
			})
			if err != nil {
				return s3response.ListObjectsV2Result{}, handleError(err)
			}

			// Create a single object entry
			entry := s3response.Object{
				Key:          aws.String(key),
				LastModified: obj.LastModified,
				ETag:         obj.ETag,
				Size:         obj.ContentLength,
				StorageClass: types.ObjectStorageClassStandard,
			}

			log.Printf("Returning single object: %s", key)
			return s3response.ListObjectsV2Result{
				Contents:              []s3response.Object{entry},
				KeyCount:              aws.Int32(1),
				MaxKeys:               input.MaxKeys,
				Name:                  input.Bucket,
				Prefix:                input.Prefix,
				IsTruncated:           aws.Bool(false),
				NextContinuationToken: nil,
			}, nil
		} else {
			// If not all parts exist, return an empty result
			log.Printf("File not found: %s", key)
			return s3response.ListObjectsV2Result{
				Contents:              []s3response.Object{},
				KeyCount:              aws.Int32(0),
				MaxKeys:               input.MaxKeys,
				Name:                  input.Bucket,
				Prefix:                input.Prefix,
				IsTruncated:           aws.Bool(false),
				NextContinuationToken: nil,
			}, nil
		}
	}

	// Create a copy of the input for client1 and client2
	input1 := *input
	input2 := *input

	// Clear continuation token for both inputs
	input1.ContinuationToken = nil
	input2.ContinuationToken = nil

	// Get objects from client1
	out1, err := self.client1.ListObjectsV2(ctx, &input1)
	if err != nil {
		return s3response.ListObjectsV2Result{}, handleError(err)
	}

	// Get objects from client2
	out2, err := self.client2.ListObjectsV2(ctx, &input2)
	if err != nil {
		return s3response.ListObjectsV2Result{}, handleError(err)
	}

	log.Printf("Found %d objects in client1 and %d objects in client2", len(out1.Contents), len(out2.Contents))

	// Create maps to track objects in each system
	objects1 := make(map[string]types.Object)
	objects2 := make(map[string]types.Object)

	// Populate maps with objects from each system
	for _, obj := range out1.Contents {
		key := *obj.Key
		// Only track objects with correct postfixes for client1
		if strings.HasSuffix(key, ".cypher.first") || strings.HasSuffix(key, ".rand.second") {
			objects1[key] = obj
			log.Printf("Found object in client1: %s", key)
		}
	}

	for _, obj := range out2.Contents {
		key := *obj.Key
		// Only track objects with correct postfixes for client2
		if strings.HasSuffix(key, ".cypher.second") || strings.HasSuffix(key, ".rand.first") {
			objects2[key] = obj
			log.Printf("Found object in client2: %s", key)
		}
	}

	// Create a map to track which base files have all four parts
	completeFiles := make(map[string]bool)

	// Check for complete sets across both storage systems
	for key1 := range objects1 {
		// Extract base name from client1 objects
		baseName := strings.TrimSuffix(key1, ".cypher.first")
		baseName = strings.TrimSuffix(baseName, ".rand.second")

		// Skip if this is not a base name (i.e., it still has a postfix)
		if baseName == key1 {
			continue
		}

		log.Printf("Checking complete set for base name: %s", baseName)

		// Check if all required parts exist
		requiredParts := []string{
			baseName + ".cypher.first",  // client1
			baseName + ".cypher.second", // client2
			baseName + ".rand.first",    // client2
			baseName + ".rand.second",   // client1
		}

		// Verify all parts exist in the correct storage systems
		allPartsExist := true
		for _, part := range requiredParts {
			if strings.HasSuffix(part, ".cypher.first") || strings.HasSuffix(part, ".rand.second") {
				if _, exists := objects1[part]; !exists {
					log.Printf("Missing part in client1: %s", part)
					allPartsExist = false
					break
				}
			} else {
				if _, exists := objects2[part]; !exists {
					log.Printf("Missing part in client2: %s", part)
					allPartsExist = false
					break
				}
			}
		}

		if allPartsExist {
			log.Printf("Found complete set for: %s", baseName)
			completeFiles[baseName] = true
		}
	}

	// Create a new list of objects containing only complete sets
	var filteredContents []types.Object
	for baseName := range completeFiles {
		// Use the .cypher.first object as the base object
		if obj, exists := objects1[baseName+".cypher.first"]; exists {
			// Create a new object with the base name
			newObj := obj
			newObj.Key = aws.String(baseName)
			filteredContents = append(filteredContents, newObj)
			log.Printf("Added complete file to results: %s", baseName)
		}
	}

	// Calculate the new key count based on filtered contents
	keyCount := int32(len(filteredContents))

	// Create the response
	result := s3response.ListObjectsV2Result{
		CommonPrefixes:        out1.CommonPrefixes,
		Contents:              ConvertObjects(filteredContents),
		Delimiter:             out1.Delimiter,
		IsTruncated:           aws.Bool(false),
		KeyCount:              &keyCount,
		MaxKeys:               out1.MaxKeys,
		Name:                  out1.Name,
		NextContinuationToken: nil,
		Prefix:                out1.Prefix,
		StartAfter:            out1.StartAfter,
	}

	log.Printf("Returning %d objects in result", len(result.Contents))
	return result, nil
}

func (self *MyBackend) DeleteObjects(ctx context.Context, input *s3.DeleteObjectsInput) (s3response.DeleteResult, error) {
	log.Printf("DeleteObjects Input Details:")
	log.Printf("  Bucket: %s", *input.Bucket)
	log.Printf("  Objects to delete: %d", len(input.Delete.Objects))
	for i, obj := range input.Delete.Objects {
		log.Printf("  Object[%d]: Key=%s, VersionId=%v", i, *obj.Key, obj.VersionId)
	}

	// Create separate delete requests for each storage system
	type deleteRequest struct {
		client *s3.Client
		keys   []string
	}
	deleteRequests := []deleteRequest{
		{client: self.client1, keys: make([]string, 0)},
		{client: self.client2, keys: make([]string, 0)},
	}

	// Distribute objects to their respective storage systems
	for _, obj := range input.Delete.Objects {
		key := *obj.Key
		// Check if this is one of our special files
		if strings.HasSuffix(key, ".cypher.first") || strings.HasSuffix(key, ".rand.second") {
			// These go to client1
			log.Printf("Adding %s to client1 deletion list", key)
			deleteRequests[0].keys = append(deleteRequests[0].keys, key)
		} else if strings.HasSuffix(key, ".cypher.second") || strings.HasSuffix(key, ".rand.first") {
			// These go to client2
			log.Printf("Adding %s to client2 deletion list", key)
			deleteRequests[1].keys = append(deleteRequests[1].keys, key)
		} else {
			// This is the original file, add its related files to both storage systems
			log.Printf("Original file %s detected, adding all related files", key)
			relatedFiles := []struct {
				key    string
				client int
			}{
				{key + ".cypher.first", 0},  // client1
				{key + ".cypher.second", 1}, // client2
				{key + ".rand.first", 1},    // client2
				{key + ".rand.second", 0},   // client1
			}

			for _, file := range relatedFiles {
				log.Printf("Adding %s to client%d deletion list", file.key, file.client+1)
				deleteRequests[file.client].keys = append(deleteRequests[file.client].keys, file.key)
			}
		}
	}

	// Perform deletions for each storage system
	var allDeleted []types.DeletedObject
	var allErrors []types.Error

	for i, req := range deleteRequests {
		if len(req.keys) == 0 {
			log.Printf("No objects to delete for client%d", i+1)
			continue
		}

		log.Printf("Processing %d objects for client%d", len(req.keys), i+1)

		// Delete each object individually
		for _, key := range req.keys {
			log.Printf("Attempting to delete %s from client%d", key, i+1)

			// First verify the object exists before deletion
			_, err := req.client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: input.Bucket,
				Key:    aws.String(key),
			})
			if err != nil {
				var ae smithy.APIError
				if errors.As(err, &ae) && ae.ErrorCode() == "NotFound" {
					log.Printf("Object %s does not exist in client%d before deletion", key, i+1)
					continue
				}
				log.Printf("Error checking existence of %s in client%d: %v", key, i+1, err)
			} else {
				log.Printf("Object %s exists in client%d before deletion", key, i+1)
			}

			deleteInput := &s3.DeleteObjectInput{
				Bucket: input.Bucket,
				Key:    aws.String(key),
			}

			output, err := req.client.DeleteObject(ctx, deleteInput)
			if err != nil {
				log.Printf("Error deleting %s from client%d: %v", key, i+1, err)
				var ae smithy.APIError
				if errors.As(err, &ae) {
					log.Printf("API Error details - Code: %s, Message: %s", ae.ErrorCode(), ae.ErrorMessage())
					allErrors = append(allErrors, types.Error{
						Key:     aws.String(key),
						Code:    aws.String(ae.ErrorCode()),
						Message: aws.String(ae.ErrorMessage()),
					})
				} else {
					allErrors = append(allErrors, types.Error{
						Key:     aws.String(key),
						Code:    aws.String("InternalError"),
						Message: aws.String(err.Error()),
					})
				}
				continue
			}

			// Wait a short time to allow for eventual consistency
			time.Sleep(100 * time.Millisecond)

			// Verify deletion by attempting to head the object
			_, err = req.client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: input.Bucket,
				Key:    aws.String(key),
			})
			if err != nil {
				var ae smithy.APIError
				if errors.As(err, &ae) && ae.ErrorCode() == "NotFound" {
					log.Printf("Successfully deleted %s from client%d (verified)", key, i+1)
					allDeleted = append(allDeleted, types.DeletedObject{
						Key:       aws.String(key),
						VersionId: output.VersionId,
					})
				} else {
					log.Printf("Warning: Unexpected error verifying deletion of %s from client%d: %v", key, i+1, err)
					allErrors = append(allErrors, types.Error{
						Key:     aws.String(key),
						Code:    aws.String("DeletionVerificationFailed"),
						Message: aws.String(fmt.Sprintf("Unexpected error verifying deletion: %v", err)),
					})
				}
			} else {
				log.Printf("Warning: %s still exists in client%d after deletion attempt", key, i+1)
				allErrors = append(allErrors, types.Error{
					Key:     aws.String(key),
					Code:    aws.String("DeletionVerificationFailed"),
					Message: aws.String("Object still exists after deletion"),
				})
			}
		}
	}

	// Create the final result
	result := s3response.DeleteResult{
		Deleted: allDeleted,
		Error:   allErrors,
	}

	log.Printf("Final deletion summary - Total deleted: %d, Total errors: %d",
		len(result.Deleted), len(result.Error))
	return result, nil
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
	return nil, s3err.GetAPIError(s3err.ErrObjectLockConfigurationNotFound)
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

func (self *MyBackend) DeleteObject(ctx context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	// Check bucket access first
	if err := self.checkBucketAccess(ctx, *input.Bucket); err != nil {
		return nil, handleError(err)
	}

	// Log all input fields
	log.Printf("DeleteObject Input Details:")
	log.Printf("  Bucket: %s", *input.Bucket)
	log.Printf("  Key: %s", *input.Key)
	if input.VersionId != nil {
		log.Printf("  VersionId: %s", *input.VersionId)
	}
	if input.ExpectedBucketOwner != nil {
		log.Printf("  ExpectedBucketOwner: %s", *input.ExpectedBucketOwner)
	}
	if input.MFA != nil {
		log.Printf("  MFA: %s", *input.MFA)
	}
	if input.BypassGovernanceRetention != nil {
		log.Printf("  BypassGovernanceRetention: %v", *input.BypassGovernanceRetention)
	}
	log.Printf("  RequestPayer: %s", input.RequestPayer)

	// Clean up empty values
	if input.ExpectedBucketOwner != nil && *input.ExpectedBucketOwner == "" {
		input.ExpectedBucketOwner = nil
	}
	if input.VersionId != nil && *input.VersionId == "" {
		input.VersionId = nil
	}
	if input.MFA != nil && *input.MFA == "" {
		input.MFA = nil
	}
	if input.BypassGovernanceRetention != nil && !*input.BypassGovernanceRetention {
		input.BypassGovernanceRetention = nil
	}

	key := *input.Key
	// Check if this is one of our special files
	if strings.HasSuffix(key, ".cypher.first") || strings.HasSuffix(key, ".rand.second") {
		// These go to client1
		log.Printf("Deleting %s from client1", key)
		deleteInput := &s3.DeleteObjectInput{
			Bucket: input.Bucket,
			Key:    input.Key,
		}
		if input.VersionId != nil {
			deleteInput.VersionId = input.VersionId
		}
		output, err := self.client1.DeleteObject(ctx, deleteInput)
		if err != nil {
			log.Printf("Error deleting %s from client1: %v", key, err)
			return nil, handleError(err)
		}
		return output, nil
	} else if strings.HasSuffix(key, ".cypher.second") || strings.HasSuffix(key, ".rand.first") {
		// These go to client2
		log.Printf("Deleting %s from client2", key)
		deleteInput := &s3.DeleteObjectInput{
			Bucket: input.Bucket,
			Key:    input.Key,
		}
		if input.VersionId != nil {
			deleteInput.VersionId = input.VersionId
		}
		output, err := self.client2.DeleteObject(ctx, deleteInput)
		if err != nil {
			log.Printf("Error deleting %s from client2: %v", key, err)
			return nil, handleError(err)
		}
		return output, nil
	} else {
		// This is the original file, delete all related files
		log.Printf("Original file %s detected, deleting all related files", key)
		relatedFiles := []struct {
			key    string
			client *s3.Client
		}{
			{key + ".cypher.first", self.client1},  // client1
			{key + ".cypher.second", self.client2}, // client2
			{key + ".rand.first", self.client2},    // client2
			{key + ".rand.second", self.client1},   // client1
		}

		var lastOutput *s3.DeleteObjectOutput
		var lastErr error

		for _, file := range relatedFiles {
			log.Printf("Deleting %s", file.key)
			deleteInput := &s3.DeleteObjectInput{
				Bucket: input.Bucket,
				Key:    aws.String(file.key),
			}
			if input.VersionId != nil {
				deleteInput.VersionId = input.VersionId
			}
			output, err := file.client.DeleteObject(ctx, deleteInput)
			if err != nil {
				log.Printf("Error deleting %s: %v", file.key, err)
				lastErr = err
			} else {
				lastOutput = output
			}
		}

		if lastErr != nil {
			return nil, handleError(lastErr)
		}
		return lastOutput, nil
	}
}
