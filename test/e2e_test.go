package sss_test

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

var (
	sssBinary = "../sss"
	testURL   = `sss://minioadmin:minioadmin@sss-test-bucket.region?forcepathstyle=true&secure=false&chunksize=5242880&regionendpoint=http://127.0.0.1:9000`
)

// TestE2EPutGet tests basic put and get operations via CLI
func TestE2EPutGet(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")
	downloadFile := filepath.Join(tmpDir, "downloaded.txt")
	testContent := []byte("Hello, SSS E2E Test!")
	remoteKey := "e2e-test-put-get"

	// Create test file
	err := os.WriteFile(testFile, testContent, 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Put file to S3
	cmd := exec.Command(sssBinary, "put", "--url", testURL, remoteKey, testFile)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("put command failed: %v, output: %s", err, output)
	}

	// Get file from S3
	cmd = exec.Command(sssBinary, "get", "--url", testURL, remoteKey, downloadFile)
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("get command failed: %v, output: %s", err, output)
	}

	// Verify content
	downloadedContent, err := os.ReadFile(downloadFile)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}

	if !bytes.Equal(testContent, downloadedContent) {
		t.Fatalf("content mismatch: expected %s, got %s", testContent, downloadedContent)
	}

	// Clean up
	cmd = exec.Command(sssBinary, "rm", "--url", testURL, remoteKey)
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("rm command failed: %v, output: %s", err, output)
	}
}

// TestE2EPutGetStdin tests put and get operations via stdin/stdout
func TestE2EPutGetStdin(t *testing.T) {
	testContent := []byte("Hello from stdin!")
	remoteKey := "e2e-test-stdin"

	// Put from stdin
	cmd := exec.Command(sssBinary, "put", "--url", testURL, remoteKey)
	cmd.Stdin = bytes.NewReader(testContent)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("put from stdin failed: %v, output: %s", err, output)
	}

	// Get to stdout
	cmd = exec.Command(sssBinary, "get", "--url", testURL, remoteKey)
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("get to stdout failed: %v, output: %s", err, output)
	}

	if !bytes.Equal(testContent, output) {
		t.Fatalf("content mismatch: expected %s, got %s", testContent, output)
	}

	// Clean up
	cmd = exec.Command(sssBinary, "rm", "--url", testURL, remoteKey)
	_, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("rm command failed: %v", err)
	}
}

// TestE2EList tests ls command
func TestE2EList(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")
	testContent := []byte("test")
	
	keys := []string{
		"e2e-ls-test/a.txt",
		"e2e-ls-test/b.txt",
		"e2e-ls-test/subdir/c.txt",
	}

	// Create test file
	err := os.WriteFile(testFile, testContent, 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Upload test files
	for _, key := range keys {
		cmd := exec.Command(sssBinary, "put", "--url", testURL, key, testFile)
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("put command failed for %s: %v, output: %s", key, err, output)
		}
	}

	// List files
	cmd := exec.Command(sssBinary, "ls", "--url", testURL, "e2e-ls-test")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("ls command failed: %v, output: %s", err, output)
	}

	outputStr := string(output)
	if !strings.Contains(outputStr, "a.txt") || !strings.Contains(outputStr, "b.txt") {
		t.Fatalf("ls output missing expected files: %s", outputStr)
	}

	// Clean up
	for _, key := range keys {
		cmd := exec.Command(sssBinary, "rm", "--url", testURL, key)
		_, err := cmd.CombinedOutput()
		if err != nil {
			t.Logf("cleanup failed for %s: %v", key, err)
		}
	}
}

// TestE2ECopy tests cp command
func TestE2ECopy(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")
	testContent := []byte("Copy test content")
	sourceKey := "e2e-test-copy-src"
	destKey := "e2e-test-copy-dest"
	downloadFile := filepath.Join(tmpDir, "downloaded.txt")

	// Create test file
	err := os.WriteFile(testFile, testContent, 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Put source file
	cmd := exec.Command(sssBinary, "put", "--url", testURL, sourceKey, testFile)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("put command failed: %v, output: %s", err, output)
	}

	// Copy within S3 (cp <destination> <source>)
	cmd = exec.Command(sssBinary, "cp", "--url", testURL, destKey, sourceKey)
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("cp command failed: %v, output: %s", err, output)
	}

	// Verify destination exists
	cmd = exec.Command(sssBinary, "get", "--url", testURL, destKey, downloadFile)
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("get command failed: %v, output: %s", err, output)
	}

	downloadedContent, err := os.ReadFile(downloadFile)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}

	if !bytes.Equal(testContent, downloadedContent) {
		t.Fatalf("content mismatch: expected %s, got %s", testContent, downloadedContent)
	}

	// Clean up
	cmd = exec.Command(sssBinary, "rm", "--url", testURL, sourceKey)
	cmd.Run()
	cmd = exec.Command(sssBinary, "rm", "--url", testURL, destKey)
	cmd.Run()
}

// TestE2EStat tests stat command
func TestE2EStat(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")
	testContent := []byte("Stat test content")
	remoteKey := "e2e-test-stat"

	// Create test file
	err := os.WriteFile(testFile, testContent, 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Put file
	cmd := exec.Command(sssBinary, "put", "--url", testURL, remoteKey, testFile)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("put command failed: %v, output: %s", err, output)
	}

	// Stat file
	cmd = exec.Command(sssBinary, "stat", "--url", testURL, remoteKey)
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("stat command failed: %v, output: %s", err, output)
	}

	outputStr := string(output)
	if !strings.Contains(outputStr, remoteKey) {
		t.Fatalf("stat output missing key name: %s", outputStr)
	}

	// Clean up
	cmd = exec.Command(sssBinary, "rm", "--url", testURL, remoteKey)
	cmd.Run()
}

// TestE2EFind tests find command
func TestE2EFind(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")
	testContent := []byte("test")

	keys := []string{
		"e2e-find-test/file1.txt",
		"e2e-find-test/file2.log",
		"e2e-find-test/subdir/file3.txt",
	}

	// Create test file
	err := os.WriteFile(testFile, testContent, 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Upload test files
	for _, key := range keys {
		cmd := exec.Command(sssBinary, "put", "--url", testURL, key, testFile)
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("put command failed for %s: %v, output: %s", key, err, output)
		}
	}

	// Find files
	cmd := exec.Command(sssBinary, "find", "--url", testURL, "e2e-find-test")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("find command failed: %v, output: %s", err, output)
	}

	outputStr := string(output)
	for _, key := range keys {
		if !strings.Contains(outputStr, key) {
			t.Fatalf("find output missing key %s: %s", key, outputStr)
		}
	}

	// Clean up
	for _, key := range keys {
		cmd := exec.Command(sssBinary, "rm", "--url", testURL, key)
		cmd.Run()
	}
}

// TestE2ELargeFile tests handling of larger files
func TestE2ELargeFile(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "large.bin")
	downloadFile := filepath.Join(tmpDir, "downloaded.bin")
	remoteKey := "e2e-test-large"

	// Create a 10MB test file with random data
	fileSize := 10 * 1024 * 1024
	testData := make([]byte, fileSize)
	_, err := rand.Read(testData)
	if err != nil {
		t.Fatalf("failed to generate random data: %v", err)
	}

	err = os.WriteFile(testFile, testData, 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Calculate checksum
	hash := sha256.New()
	hash.Write(testData)
	expectedHash := hex.EncodeToString(hash.Sum(nil))

	// Put large file
	cmd := exec.Command(sssBinary, "put", "--url", testURL, remoteKey, testFile)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("put command failed: %v, output: %s", err, output)
	}

	// Get large file
	cmd = exec.Command(sssBinary, "get", "--url", testURL, remoteKey, downloadFile)
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("get command failed: %v, output: %s", err, output)
	}

	// Verify checksum
	downloadedData, err := os.ReadFile(downloadFile)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}

	hash = sha256.New()
	hash.Write(downloadedData)
	actualHash := hex.EncodeToString(hash.Sum(nil))

	if expectedHash != actualHash {
		t.Fatalf("checksum mismatch: expected %s, got %s", expectedHash, actualHash)
	}

	// Clean up
	cmd = exec.Command(sssBinary, "rm", "--url", testURL, remoteKey)
	cmd.Run()
}

// TestE2EContinuePut tests resumable upload
func TestE2EContinuePut(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "continue.txt")
	downloadFile := filepath.Join(tmpDir, "downloaded.txt")
	testContent := []byte("First part")
	remoteKey := "e2e-test-continue"

	// Create test file with first part
	err := os.WriteFile(testFile, testContent, 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Put file (first part, without commit)
	cmd := exec.Command(sssBinary, "put", "--url", testURL, "--commit=false", remoteKey, testFile)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("put command failed: %v, output: %s", err, output)
	}

	// Append more content to test file
	additionalContent := []byte("Second part")
	fullContent := append(testContent, additionalContent...)
	err = os.WriteFile(testFile, fullContent, 0644)
	if err != nil {
		t.Fatalf("failed to update test file: %v", err)
	}

	// Continue put and commit
	cmd = exec.Command(sssBinary, "put", "--url", testURL, "--continue", remoteKey, testFile)
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("continue put command failed: %v, output: %s", err, output)
	}

	// Get file
	cmd = exec.Command(sssBinary, "get", "--url", testURL, remoteKey, downloadFile)
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("get command failed: %v, output: %s", err, output)
	}

	// Verify content
	downloadedContent, err := os.ReadFile(downloadFile)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}

	if !bytes.Equal(fullContent, downloadedContent) {
		t.Fatalf("content mismatch: expected %s, got %s", fullContent, downloadedContent)
	}

	// Clean up
	cmd = exec.Command(sssBinary, "rm", "--url", testURL, remoteKey)
	cmd.Run()
}

// TestE2EContinueGet tests resumable download
func TestE2EContinueGet(t *testing.T) {
	tmpDir := t.TempDir()
	testFile := filepath.Join(tmpDir, "test.txt")
	downloadFile := filepath.Join(tmpDir, "downloaded.txt")
	testContent := []byte("Full content for resume test")
	remoteKey := "e2e-test-continue-get"

	// Create and upload test file
	err := os.WriteFile(testFile, testContent, 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	cmd := exec.Command(sssBinary, "put", "--url", testURL, remoteKey, testFile)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("put command failed: %v, output: %s", err, output)
	}

	// Download first part (simulate partial download)
	partialContent := testContent[:10]
	err = os.WriteFile(downloadFile, partialContent, 0644)
	if err != nil {
		t.Fatalf("failed to create partial file: %v", err)
	}

	// Continue download
	cmd = exec.Command(sssBinary, "get", "--url", testURL, "--continue", remoteKey, downloadFile)
	output, err = cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("continue get command failed: %v, output: %s", err, output)
	}

	// Verify content
	downloadedContent, err := os.ReadFile(downloadFile)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}

	if !bytes.Equal(testContent, downloadedContent) {
		t.Fatalf("content mismatch: expected %s, got %s", testContent, downloadedContent)
	}

	// Clean up
	cmd = exec.Command(sssBinary, "rm", "--url", testURL, remoteKey)
	cmd.Run()
}
