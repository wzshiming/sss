# SSS Tests

This directory contains unit tests, integration tests, and end-to-end (e2e) tests for the SSS project.

## Test Types

### Unit/Integration Tests (`sss_test.go`)
- Tests for the SSS library API
- Covers basic operations (put, get, delete)
- Tests list and walk operations
- Tests file writer and multipart upload functionality

### E2E Tests (`e2e_test.go`)
- End-to-end tests for the SSS CLI commands
- Tests all major CLI operations
- Tests stdin/stdout operations
- Tests resumable uploads and downloads
- Tests large file handling

## Prerequisites

- Go 1.24 or later
- Docker and Docker Compose (for running MinIO)

## Running Tests

### Setup

The tests use Docker Compose to run a local MinIO instance. The setup is handled automatically by `TestMain` in `main_test.go`, which:
1. Starts MinIO using `docker compose up -d`
2. Waits for MinIO to be ready
3. Creates the test bucket if needed
4. Runs the tests
5. Cleans up by running `docker compose down`

### Run All Tests

```bash
cd test
go test -v
```

### Run Only E2E Tests

```bash
cd test
go test -v -run TestE2E
```

### Run Specific Test

```bash
cd test
go test -v -run TestE2EPutGet
```

## E2E Test Coverage

The e2e tests cover the following CLI commands and scenarios:

- **TestE2EPutGet**: Basic file upload and download
- **TestE2EPutGetStdin**: Upload from stdin and download to stdout
- **TestE2EList**: List objects in a bucket/prefix
- **TestE2ECopy**: Copy objects within S3
- **TestE2EStat**: Get object metadata
- **TestE2EFind**: Find objects recursively
- **TestE2ELargeFile**: Handle large files (10MB+)
- **TestE2EContinuePut**: Resumable upload functionality
- **TestE2EContinueGet**: Resumable download functionality

## Test Configuration

The tests use the following configuration:
- MinIO endpoint: `http://127.0.0.1:9000`
- Access key: `minioadmin`
- Secret key: `minioadmin`
- Test bucket: `sss-test-bucket`
- Chunk size: 5MB

## Notes

- The e2e tests build the `sss` CLI binary before running tests
- The binary is excluded from git via `.gitignore`
- Tests clean up after themselves by deleting created objects
- Some tests may take longer due to large file operations
