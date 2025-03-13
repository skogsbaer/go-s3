package main

import (
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ==== XML Response Structures ====

type ErrorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestID string   `xml:"RequestId"`
}

type ListAllMyBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Owner   Owner    `xml:"Owner"`
	Buckets Buckets  `xml:"Buckets"`
}

type Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type Buckets struct {
	Bucket []BucketInfo `xml:"Bucket"`
}

type BucketInfo struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type ListBucketResult struct {
	XMLName xml.Name `xml:"ListBucketResult"`
	Name    string   `xml:"Name"`
	Objects []Object `xml:"Contents"`
}

type Object struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	Size         int64  `xml:"Size"`
}

// ==== Helper Functions ====

func writeXMLError(w http.ResponseWriter, code int, msg, resource string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(code)
	resp := ErrorResponse{
		Code:      http.StatusText(code),
		Message:   msg,
		Resource:  resource,
		RequestID: "req-12345",
	}
	xml.NewEncoder(w).Encode(resp)
}

// ==== Handlers ====

func main() {
	http.HandleFunc("/", router)
	log.Println("[Startup] Listening on :9000")
	log.Fatal(http.ListenAndServe(":9000", nil))
}

func router(w http.ResponseWriter, r *http.Request) {
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
	if len(parts) == 1 && r.Method == "GET" {
		listBuckets(w, r)
		return
	} else if len(parts) < 2 {
		writeXMLError(w, http.StatusBadRequest, "Invalid path", r.URL.Path)
		return
	}
	bucket, object := parts[0], parts[1]

	// Handle ?location (GetBucketLocation)
	if r.Method == "GET" && r.URL.Query().Has("location") && object == "" {
		getBucketLocation(w, r, bucket)
		return
	}

	switch r.Method {
	case "PUT":
		if r.Header.Get("X-Amz-Copy-Source") != "" {
			copyObject(w, r, bucket, object)
		} else {
			putObject(w, r, bucket, object)
		}
	case "GET":
		if r.URL.Query().Has("list-type") || strings.HasSuffix(r.URL.Path, "/") {
			listObjects(w, r, bucket)
		} else {
			getObject(w, r, bucket, object)
		}
	case "HEAD":
		headObject(w, r, bucket, object)
	case "DELETE":
		if object == "" {
			deleteBucket(w, r, bucket)
		} else {
			deleteObject(w, r, bucket, object)
		}
	case "POST":
		createBucket(w, r, bucket)
	default:
		writeXMLError(w, http.StatusMethodNotAllowed, "Unsupported method", r.URL.Path)
	}
}

func listBuckets(w http.ResponseWriter, r *http.Request) {
	dirs, err := os.ReadDir("data")
	if err != nil {
		writeXMLError(w, 500, "Unable to list buckets", "/")
		return
	}
	var buckets []BucketInfo
	for _, d := range dirs {
		if d.IsDir() {
			buckets = append(buckets, BucketInfo{
				Name:         d.Name(),
				CreationDate: time.Now().UTC().Format(time.RFC3339),
			})
		}
	}
	resp := ListAllMyBucketsResult{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
		Owner: Owner{
			ID:          "1234567890",
			DisplayName: "localadmin",
		},
		Buckets: Buckets{Bucket: buckets},
	}
	w.Header().Set("Content-Type", "application/xml")
	xml.NewEncoder(w).Encode(resp)
}

func getBucketLocation(w http.ResponseWriter, r *http.Request, bucket string) {
	path := filepath.Join("data", bucket)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		type BucketError struct {
			XMLName    xml.Name `xml:"Error"`
			Code       string   `xml:"Code"`
			Message    string   `xml:"Message"`
			BucketName string   `xml:"BucketName"`
			RequestID  string   `xml:"RequestId"`
			HostID     string   `xml:"HostId"`
		}
		xml.NewEncoder(w).Encode(BucketError{
			Code:       "NoSuchBucket",
			Message:    "The specified bucket does not exist",
			BucketName: bucket,
			RequestID:  "req-12345",
			HostID:     "s3-custom-server",
		})
		return
	}
	type LocationConstraint struct {
		XMLName xml.Name `xml:"LocationConstraint"`
		Xmlns   string   `xml:"xmlns,attr"`
	}
	resp := LocationConstraint{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
	}
	w.Header().Set("Content-Type", "application/xml")
	xml.NewEncoder(w).Encode(resp)
}

func createBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	path := filepath.Join("data", bucket)
	err := os.MkdirAll(path, 0755)
	if err != nil {
		writeXMLError(w, 500, "Could not create bucket", r.URL.Path)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func deleteBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	path := filepath.Join("data", bucket)
	err := os.RemoveAll(path)
	if err != nil {
		writeXMLError(w, 500, "Could not delete bucket", r.URL.Path)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func putObject(w http.ResponseWriter, r *http.Request, bucket, object string) {
	path := filepath.Join("data", bucket, object)
	log.Printf("Trying to create file: %s\n", path)
	os.MkdirAll(filepath.Dir(path), 0755)
	f, err := os.Create(path)
	if err != nil {
		writeXMLError(w, 500, "Failed to create file", r.URL.Path)
		return
	}
	defer f.Close()
	_, err = io.Copy(f, r.Body)
	if err != nil {
		writeXMLError(w, 500, "Failed to write data", r.URL.Path)
		return
	}
	metaPath := path + ".meta"
	mf, _ := os.Create(metaPath)
	defer mf.Close()
	for key, vals := range r.Header {
		if strings.HasPrefix(strings.ToLower(key), "x-amz-meta-") {
			fmt.Fprintf(mf, "%s=%s\n", key, vals[0])
		}
	}
	w.WriteHeader(http.StatusOK)
}

func getObject(w http.ResponseWriter, r *http.Request, bucket, object string) {
	path := filepath.Join("data", bucket, object)
	f, err := os.Open(path)
	if err != nil {
		writeXMLError(w, 404, "Object not found", r.URL.Path)
		return
	}
	defer f.Close()
	http.ServeContent(w, r, object, time.Now(), f)
}

func headObject(w http.ResponseWriter, r *http.Request, bucket, object string) {
	path := filepath.Join("data", bucket, object)
	info, err := os.Stat(path)
	if err != nil {
		writeXMLError(w, 404, "Object not found", r.URL.Path)
		return
	}
	w.Header().Set("Content-Length", fmt.Sprintf("%d", info.Size()))
	w.Header().Set("Last-Modified", info.ModTime().UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
}

func deleteObject(w http.ResponseWriter, r *http.Request, bucket, object string) {
	path := filepath.Join("data", bucket, object)
	os.Remove(path + ".meta")
	err := os.Remove(path)
	if err != nil {
		writeXMLError(w, 404, "Object not found", r.URL.Path)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func copyObject(w http.ResponseWriter, r *http.Request, bucket, object string) {
	src := r.Header.Get("X-Amz-Copy-Source")
	src = strings.TrimPrefix(src, "/")
	parts := strings.SplitN(src, "/", 2)
	if len(parts) != 2 {
		writeXMLError(w, 400, "Invalid copy source", r.URL.Path)
		return
	}
	srcBucket, srcObject := parts[0], parts[1]
	srcPath := filepath.Join("data", srcBucket, srcObject)
	dstPath := filepath.Join("data", bucket, object)
	os.MkdirAll(filepath.Dir(dstPath), 0755)
	srcFile, err := os.Open(srcPath)
	if err != nil {
		writeXMLError(w, 404, "Source object not found", r.URL.Path)
		return
	}
	defer srcFile.Close()
	dstFile, err := os.Create(dstPath)
	if err != nil {
		writeXMLError(w, 500, "Failed to create destination object", r.URL.Path)
		return
	}
	defer dstFile.Close()
	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		writeXMLError(w, 500, "Copy failed", r.URL.Path)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func listObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	bucketPath := filepath.Join("data", bucket)
	entries, err := os.ReadDir(bucketPath)
	if err != nil {
		writeXMLError(w, 500, "Could not read bucket", r.URL.Path)
		return
	}
	var objects []Object
	for _, entry := range entries {
		if entry.IsDir() || strings.HasSuffix(entry.Name(), ".meta") {
			continue
		}
		info, err := os.Stat(filepath.Join(bucketPath, entry.Name()))
		if err != nil {
			continue
		}
		objects = append(objects, Object{
			Key:          entry.Name(),
			LastModified: info.ModTime().UTC().Format(time.RFC3339),
			Size:         info.Size(),
		})
	}
	res := ListBucketResult{
		Name:    bucket,
		Objects: objects,
	}
	w.Header().Set("Content-Type", "application/xml")
	xml.NewEncoder(w).Encode(res)
}
