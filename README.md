# SSS - S3 Simple Storage

SSS is a lightweight Go library and CLI tool that provides a simple, intuitive interface for Amazon S3 and S3-compatible storage services. It wraps the AWS SDK for Go to offer both programmatic access and command-line utilities for common S3 operations.

## Features

- **CLI Tool**: Command-line interface for S3 operations (get, put, ls, rm, cp, find, stat)
- **HTTP Server**: Serve S3 content over HTTP with optional presigned URL redirection
- **Go Library**: Clean API for S3 operations in your Go applications
- **Multipart Upload**: Support for resumable and parallel multipart uploads
- **Presigned URLs**: Generate temporary signed URLs for secure access
- **S3-Compatible**: Works with AWS S3, MinIO, and other S3-compatible services

## Installation

### Using Go Install

```bash
go install github.com/wzshiming/sss/cmd/sss@latest
```

### Using Go Get (for library)

```bash
go get github.com/wzshiming/sss
```

### Building from Source

```bash
git clone https://github.com/wzshiming/sss.git
cd sss
go build ./cmd/sss
```

## Quick Start

### CLI Usage

The `sss` command-line tool uses a URL format to configure S3 connections:

```
sss://[access_key]:[secret_key]@[bucket].[region]?[options]
```

#### URL Format and Options

**Basic URL Structure:**
```
sss://ACCESS_KEY:SECRET_KEY@BUCKET.REGION?param1=value1&param2=value2
```

**Common Query Parameters:**
- `regionendpoint` - Custom S3 endpoint (e.g., for MinIO: `http://localhost:9000`)
- `forcepathstyle` - Use path-style addressing (true/false, default: false)
- `secure` - Use HTTPS (true/false, default: true)
- `chunksize` - Chunk size for multipart uploads (default: 10485760 bytes = 10MB)
- `encrypt` - Enable server-side encryption (true/false)
- `keyid` - KMS key ID for encryption
- `storageclass` - S3 storage class (STANDARD, REDUCED_REDUNDANCY, etc.)
- `objectacl` - Object ACL (private, public-read, etc.)
- `sessiontoken` - AWS session token for temporary credentials
- `usedualstack` - Use dual-stack endpoint (true/false)
- `accelerate` - Use S3 Transfer Acceleration (true/false)
- `signendpoint` - Endpoint for presigned URLs
- `signendpointmethods` - Comma-separated HTTP methods to sign (GET,PUT,DELETE)

**Example URLs:**

```bash
# AWS S3 with standard configuration
export SSS_URL="sss://AKIAIOSFODNN7EXAMPLE:wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY@my-bucket.us-west-2"

# MinIO local instance
export SSS_URL="sss://minioadmin:minioadmin@my-bucket.region?regionendpoint=http://localhost:9000&forcepathstyle=true&secure=false"

# With custom chunk size (5MB) and encryption
export SSS_URL="sss://ACCESS_KEY:SECRET_KEY@my-bucket.us-east-1?chunksize=5242880&encrypt=true"
```

### CLI Commands

#### Get - Download files from S3

```bash
# Download to stdout
sss get --url="$SSS_URL" /path/to/file.txt

# Download to local file
sss get --url="$SSS_URL" /path/to/file.txt ./local-file.txt

# Download with offset
sss get --url="$SSS_URL" --offset=1024 /path/to/file.txt

# Continue interrupted download
sss get --url="$SSS_URL" --continue /path/to/file.txt ./local-file.txt
```

#### Put - Upload files to S3

```bash
# Upload from stdin
cat file.txt | sss put --url="$SSS_URL" /path/to/file.txt

# Upload from local file
sss put --url="$SSS_URL" /path/to/file.txt ./local-file.txt

# Upload with SHA256 verification
sss put --url="$SSS_URL" --sha256="..." /path/to/file.txt ./local-file.txt

# Resume upload (continue)
sss put --url="$SSS_URL" --continue /path/to/file.txt ./local-file.txt
```

#### List - List files and directories

```bash
# List root directory
sss ls --url="$SSS_URL" /

# List specific directory
sss ls --url="$SSS_URL" /my-folder/

# Limit number of results
sss ls --url="$SSS_URL" --limit=100 /
```

#### Copy - Copy files within S3

```bash
# Copy file
sss cp --url="$SSS_URL" /source/file.txt /destination/file.txt

# Copy directory recursively
sss cp --url="$SSS_URL" --recursive /source-dir/ /dest-dir/
```

#### Remove - Delete files from S3

```bash
# Delete single file
sss rm --url="$SSS_URL" /path/to/file.txt

# Delete recursively
sss rm --url="$SSS_URL" --recursive /path/to/directory/
```

#### Find - Search for files

```bash
# Find files in directory
sss find --url="$SSS_URL" /path/to/search/
```

#### Stat - Get file information

```bash
# Get file metadata
sss stat --url="$SSS_URL" /path/to/file.txt
```

#### Sign - Generate presigned URLs

```bash
# Generate presigned GET URL (valid for 1 hour)
sss sign get --url="$SSS_URL" --expires=1h /path/to/file.txt

# Generate presigned PUT URL
sss sign put --url="$SSS_URL" --expires=30m /path/to/file.txt

# Generate presigned DELETE URL
sss sign rm --url="$SSS_URL" --expires=15m /path/to/file.txt

# List with presigned URL
sss sign ls --url="$SSS_URL" --expires=1h /path/to/directory/
```

#### Serve - HTTP server for S3 content

```bash
# Basic HTTP server (port 8080)
sss serve --url="$SSS_URL"

# Custom port
sss serve --url="$SSS_URL" --address=":3000"

# Enable redirects to presigned URLs
sss serve --url="$SSS_URL" --redirect --expires=5m

# Allow specific operations
sss serve --url="$SSS_URL" --allow-list --allow-put --allow-delete
```

**Server endpoints:**
- `GET /path/to/file` - Download file
- `GET /path/to/dir/` - List directory (if `--allow-list` is enabled)
- `PUT /path/to/file` - Upload file (if `--allow-put` is enabled)
- `DELETE /path/to/file` - Delete file (if `--allow-delete` is enabled)
- `HEAD /path/to/file` - Get file metadata

#### Part - Multipart upload management

```bash
# List ongoing multipart uploads
sss part ls --url="$SSS_URL" /path/to/file.txt

# Commit a multipart upload
sss part commit --url="$SSS_URL" --upload-id="..." /path/to/file.txt
```

## Library Usage

### Basic Example

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/wzshiming/sss"
)

func main() {
    // Initialize SSS client
    s, err := sss.NewSSS(
        sss.WithAccessKey("YOUR_ACCESS_KEY"),
        sss.WithSecretKey("YOUR_SECRET_KEY"),
        sss.WithBucket("my-bucket"),
        sss.WithRegion("us-west-2"),
    )
    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()

    // Upload content
    content := []byte("Hello, SSS!")
    err = s.PutContent(ctx, "/hello.txt", content)
    if err != nil {
        log.Fatal(err)
    }

    // Download content
    data, err := s.GetContent(ctx, "/hello.txt")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Content: %s\n", string(data))

    // Delete file
    err = s.Delete(ctx, "/hello.txt")
    if err != nil {
        log.Fatal(err)
    }
}
```

### Using URL Configuration

```go
s, err := sss.NewSSS(
    sss.WithURL("sss://ACCESS_KEY:SECRET_KEY@bucket.region?secure=true"),
)
```

### MinIO Example

```go
s, err := sss.NewSSS(
    sss.WithURL("sss://minioadmin:minioadmin@my-bucket.region?regionendpoint=http://localhost:9000&forcepathstyle=true&secure=false"),
)
```

### List Files

```go
err := s.List(ctx, "/my-folder/", func(fileInfo sss.FileInfo) bool {
    if fileInfo.IsDir() {
        fmt.Printf("Directory: %s\n", fileInfo.Path())
    } else {
        fmt.Printf("File: %s (size: %d, modified: %s)\n",
            fileInfo.Path(),
            fileInfo.Size(),
            fileInfo.ModTime(),
        )
    }
    return true // continue listing
})
```

### Walk Directory Tree

```go
err := s.Walk(ctx, "/", func(fileInfo sss.FileInfo) error {
    fmt.Printf("%s\n", fileInfo.Path())
    return nil // return error to stop walking
})
```

### File Upload with Writer

```go
// Create writer
w, err := s.Writer(ctx, "/large-file.bin")
if err != nil {
    log.Fatal(err)
}
defer w.Close()

// Write data
_, err = io.Copy(w, largeFileReader)
if err != nil {
    w.Cancel(ctx) // cancel upload on error
    log.Fatal(err)
}

// Commit upload
err = w.Commit(ctx)
if err != nil {
    log.Fatal(err)
}
```

### Resume Upload

```go
// Create writer with append
w, err := s.WriterWithAppend(ctx, "/large-file.bin")
if err != nil {
    log.Fatal(err)
}

// w.Size() returns current uploaded size
fmt.Printf("Resuming from byte: %d\n", w.Size())

// Continue writing from offset
_, err = io.Copy(w, remainingDataReader)
// ... commit as above
```

### Multipart Upload (Parallel)

```go
// Create multipart upload
m, err := s.NewMultipart(ctx, "/huge-file.bin")
if err != nil {
    log.Fatal(err)
}

// Upload parts in parallel
var wg sync.WaitGroup
chunkSize := s.ChunkSize() // default 10MB

for partNum := 1; partNum <= totalParts; partNum++ {
    wg.Add(1)
    go func(pn int) {
        defer wg.Done()
        
        partData := getPartData(pn, chunkSize)
        err := m.UploadPart(ctx, int64(pn), bytes.NewReader(partData))
        if err != nil {
            log.Printf("Part %d failed: %v", pn, err)
        }
    }(partNum)
}

wg.Wait()

// Complete multipart upload
err = m.Commit(ctx)
if err != nil {
    log.Fatal(err)
}
```

### Download with Offset

```go
// Read from specific offset
reader, err := s.ReaderWithOffset(ctx, "/large-file.bin", 1024)
if err != nil {
    log.Fatal(err)
}
defer reader.Close()

io.Copy(os.Stdout, reader)
```

### Generate Presigned URLs

```go
// Presigned GET URL (valid for 1 hour)
url, err := s.SignGet("/file.txt", 1*time.Hour)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Download URL: %s\n", url)

// Presigned PUT URL
url, err = s.SignPut("/upload.txt", 30*time.Minute)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Upload URL: %s\n", url)
```

### Advanced Configuration

```go
s, err := sss.NewSSS(
    sss.WithAccessKey("ACCESS_KEY"),
    sss.WithSecretKey("SECRET_KEY"),
    sss.WithBucket("my-bucket"),
    sss.WithRegion("us-west-2"),
    sss.WithChunkSize(5*1024*1024), // 5MB chunks
    sss.WithEncryption(true),        // Enable encryption
    sss.WithStorageClass("STANDARD_IA"),
    sss.WithObjectACL("public-read"),
    sss.WithForcePathStyle(true),
    sss.WithSecure(true),
)
```

## Configuration Options

All configuration options available as functions:

- `WithURL(url string)` - Configure from URL string
- `WithHTTPClient(client *http.Client)` - Custom HTTP client
- `WithDriverName(name string)` - Set driver name
- `WithAccessKey(key string)` - AWS access key
- `WithSecretKey(key string)` - AWS secret key
- `WithBucket(bucket string)` - S3 bucket name
- `WithRegion(region string)` - AWS region
- `WithRegionEndpoint(endpoint string)` - Custom S3 endpoint
- `WithSignEndpoint(endpoint string, methods ...string)` - Endpoint for presigned URLs
- `WithForcePathStyle(enable bool)` - Use path-style addressing
- `WithEncryption(enable bool)` - Enable server-side encryption
- `WithKMSKeyID(id string)` - KMS key ID for encryption
- `WithSecure(enable bool)` - Use HTTPS
- `WithChunkSize(size int)` - Chunk size for multipart uploads
- `WithRootDirectory(dir string)` - Root directory prefix
- `WithStorageClass(class string)` - S3 storage class
- `WithUserAgent(ua string)` - Custom user agent
- `WithObjectACL(acl string)` - Object ACL
- `WithSessionToken(token string)` - AWS session token
- `WithDualStack(enable bool)` - Use dual-stack endpoints
- `WithAccelerate(enable bool)` - Use S3 Transfer Acceleration
- `WithLogLevel(level aws.LogLevelType)` - AWS SDK log level

## Testing

The project includes comprehensive tests. To run tests, you'll need Docker and Docker Compose installed:

```bash
# Run tests (starts MinIO container automatically)
cd test
go test -v
```

## License

Licensed under the MIT License. See [LICENSE](https://github.com/wzshiming/sss/blob/master/LICENSE) for the full license text.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Author

Shiming Zhang ([@wzshiming](https://github.com/wzshiming))
