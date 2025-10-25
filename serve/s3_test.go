package serve

import (
	"bytes"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestS3ErrorResponse(t *testing.T) {
	// Create a simple S3 handler with nil SSS to test error responses
	handler := &S3Serve{
		sss:    nil,
		bucket: "test-bucket",
	}

	// Test wrong bucket name
	req := httptest.NewRequest(http.MethodGet, "/wrong-bucket/test-key", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("Expected status 404 for wrong bucket, got %d", rec.Code)
	}

	// Parse error response
	var errResp Error
	if err := xml.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("Failed to parse error XML: %v", err)
	}

	if errResp.Code != "NoSuchBucket" {
		t.Errorf("Expected error code 'NoSuchBucket', got %q", errResp.Code)
	}
}

func TestS3ListBucketXMLFormat(t *testing.T) {
	// Test that the ListBucketResult struct properly marshals to XML
	result := ListBucketResult{
		Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:        "test-bucket",
		Prefix:      "test/",
		Marker:      "",
		MaxKeys:     1000,
		IsTruncated: false,
		Contents: []Object{
			{
				Key:          "test/file1.txt",
				Size:         123,
				StorageClass: "STANDARD",
			},
		},
		CommonPrefixes: []CommonPrefix{
			{Prefix: "test/subdir/"},
		},
	}

	data, err := xml.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal XML: %v", err)
	}

	// Verify the XML contains expected elements
	if !bytes.Contains(data, []byte("ListBucketResult")) {
		t.Error("XML should contain ListBucketResult element")
	}
	if !bytes.Contains(data, []byte("test-bucket")) {
		t.Error("XML should contain bucket name")
	}
	if !bytes.Contains(data, []byte("test/file1.txt")) {
		t.Error("XML should contain object key")
	}
	if !bytes.Contains(data, []byte("test/subdir/")) {
		t.Error("XML should contain common prefix")
	}

	// Verify we can unmarshal it back
	var parsed ListBucketResult
	if err := xml.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Failed to unmarshal XML: %v", err)
	}

	if parsed.Name != result.Name {
		t.Errorf("Expected bucket name %q, got %q", result.Name, parsed.Name)
	}
}

func TestS3ErrorXMLFormat(t *testing.T) {
	// Test that the Error struct properly marshals to XML
	errResp := Error{
		Code:      "NoSuchKey",
		Message:   "The specified key does not exist",
		Resource:  "/test-bucket/test-key",
		RequestID: "12345",
	}

	data, err := xml.MarshalIndent(errResp, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal XML: %v", err)
	}

	// Verify the XML contains expected elements
	if !bytes.Contains(data, []byte("Error")) {
		t.Error("XML should contain Error element")
	}
	if !bytes.Contains(data, []byte("NoSuchKey")) {
		t.Error("XML should contain error code")
	}
	if !bytes.Contains(data, []byte("The specified key does not exist")) {
		t.Error("XML should contain error message")
	}

	// Verify we can unmarshal it back
	var parsed Error
	if err := xml.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Failed to unmarshal XML: %v", err)
	}

	if parsed.Code != errResp.Code {
		t.Errorf("Expected error code %q, got %q", errResp.Code, parsed.Code)
	}
}

func TestS3WriteError(t *testing.T) {
	handler := &S3Serve{
		bucket: "test-bucket",
	}

	rec := httptest.NewRecorder()
	handler.writeError(rec, "TestError", "Test message", "/test-resource", http.StatusBadRequest)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}

	contentType := rec.Header().Get("Content-Type")
	if contentType != "application/xml" {
		t.Errorf("Expected Content-Type application/xml, got %q", contentType)
	}

	var errResp Error
	if err := xml.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("Failed to parse error XML: %v", err)
	}

	if errResp.Code != "TestError" {
		t.Errorf("Expected error code 'TestError', got %q", errResp.Code)
	}

	if errResp.Message != "Test message" {
		t.Errorf("Expected message 'Test message', got %q", errResp.Message)
	}
}
