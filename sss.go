// Package sss provides a simple interface for Amazon S3 and S3-compatible storage services.
//
// SSS (S3 Simple Storage) wraps the AWS SDK for Go to offer both a programmatic API
// and command-line utilities for common S3 operations including:
//   - File upload/download with resume support
//   - Multipart uploads with parallel processing
//   - Directory listing and walking
//   - Presigned URL generation
//   - HTTP server for S3 content
//
// Basic usage:
//
//	s, err := sss.NewSSS(
//		sss.WithAccessKey("YOUR_ACCESS_KEY"),
//		sss.WithSecretKey("YOUR_SECRET_KEY"),
//		sss.WithBucket("my-bucket"),
//		sss.WithRegion("us-west-2"),
//	)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Upload content
//	err = s.PutContent(ctx, "/hello.txt", []byte("Hello, World!"))
//
//	// Download content
//	data, err := s.GetContent(ctx, "/hello.txt")
//
// For MinIO or other S3-compatible services:
//
//	s, err := sss.NewSSS(
//		sss.WithURL("sss://minioadmin:minioadmin@bucket.region?regionendpoint=http://localhost:9000&forcepathstyle=true&secure=false"),
//	)
package sss

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	defaultChunkSize = 10 * 1024 * 1024

	// listMax is the largest amount of objects you can request from S3 in a list call
	listMax = 1000

	// noStorageClass defines the value to be used if storage class is not supported by the S3 endpoint
	noStorageClass = "NONE"
)

type sssOption struct {
	HTTPClient          *http.Client
	DriverName          string
	AccessKey           string
	SecretKey           string
	Bucket              string
	Region              string
	RegionEndpoint      string
	SignEndpoint        string
	SignEndpointMethods []string
	ForcePathStyle      bool
	Encrypt             bool
	KeyID               string
	Secure              bool
	ChunkSize           int
	RootDirectory       string
	StorageClass        string
	UserAgent           string
	ObjectACL           string
	SessionToken        string
	UseDualStack        bool
	Accelerate          bool
	LogLevel            aws.LogLevelType
}

// Option is a function that configures an SSS instance.
type Option func(*sssOption) error

// WithHTTPClient sets a custom HTTP client for S3 requests.
func WithHTTPClient(client *http.Client) Option {
	return func(p *sssOption) error {
		p.HTTPClient = client
		return nil
	}
}

// WithDriverName sets the driver name identifier.
func WithDriverName(name string) Option {
	return func(p *sssOption) error {
		p.DriverName = name
		return nil
	}
}

// WithAccessKey sets the AWS access key ID for authentication.
func WithAccessKey(key string) Option {
	return func(p *sssOption) error {
		p.AccessKey = key
		return nil
	}
}

// WithSecretKey sets the AWS secret access key for authentication.
func WithSecretKey(key string) Option {
	return func(p *sssOption) error {
		p.SecretKey = key
		return nil
	}
}

// WithBucket sets the S3 bucket name.
func WithBucket(bucket string) Option {
	return func(p *sssOption) error {
		p.Bucket = bucket
		return nil
	}
}

// WithRegion sets the AWS region (e.g., "us-west-2").
func WithRegion(region string) Option {
	return func(p *sssOption) error {
		p.Region = region
		return nil
	}
}

// WithRegionEndpoint sets a custom S3 endpoint URL.
// Use this for S3-compatible services like MinIO.
func WithRegionEndpoint(endpoint string) Option {
	return func(p *sssOption) error {
		p.RegionEndpoint = endpoint
		return nil
	}
}

// WithSignEndpoint sets a custom endpoint for generating presigned URLs.
// The optional methods parameter specifies which HTTP methods to use this endpoint for.
func WithSignEndpoint(endpoint string, methods ...string) Option {
	return func(p *sssOption) error {
		p.SignEndpoint = endpoint
		p.SignEndpointMethods = methods
		return nil
	}
}

// WithForcePathStyle enables path-style S3 addressing (bucket.name/key instead of bucket-name.s3.amazonaws.com/key).
// Required for some S3-compatible services like MinIO.
func WithForcePathStyle(enable bool) Option {
	return func(p *sssOption) error {
		p.ForcePathStyle = enable
		return nil
	}
}

// WithEncryption enables server-side encryption for uploaded objects.
func WithEncryption(enable bool) Option {
	return func(p *sssOption) error {
		p.Encrypt = enable
		return nil
	}
}

// WithKMSKeyID sets the AWS KMS key ID for server-side encryption.
// When set, objects will be encrypted using AWS KMS instead of AES256.
func WithKMSKeyID(id string) Option {
	return func(p *sssOption) error {
		p.KeyID = id
		return nil
	}
}

// WithSecure enables HTTPS for S3 connections.
// Set to false for local development with MinIO.
func WithSecure(enable bool) Option {
	return func(p *sssOption) error {
		p.Secure = enable
		return nil
	}
}

// WithChunkSize sets the chunk size in bytes for multipart uploads.
// Default is 10MB (10485760 bytes).
func WithChunkSize(size int) Option {
	return func(p *sssOption) error {
		p.ChunkSize = size
		return nil
	}
}

// WithRootDirectory sets a root directory prefix for all S3 operations.
// All paths will be relative to this directory.
func WithRootDirectory(dir string) Option {
	return func(p *sssOption) error {
		p.RootDirectory = dir
		return nil
	}
}

// WithStorageClass sets the S3 storage class for uploaded objects.
// Common values: STANDARD, REDUCED_REDUNDANCY, STANDARD_IA, ONEZONE_IA, GLACIER, DEEP_ARCHIVE.
func WithStorageClass(class string) Option {
	return func(p *sssOption) error {
		p.StorageClass = class
		return nil
	}
}

// WithUserAgent sets a custom user agent string for S3 requests.
func WithUserAgent(ua string) Option {
	return func(p *sssOption) error {
		p.UserAgent = ua
		return nil
	}
}

// WithObjectACL sets the access control list (ACL) for uploaded objects.
// Common values: private, public-read, public-read-write, authenticated-read.
func WithObjectACL(acl string) Option {
	return func(p *sssOption) error {
		p.ObjectACL = acl
		return nil
	}
}

// WithSessionToken sets the AWS session token for temporary credentials.
func WithSessionToken(token string) Option {
	return func(p *sssOption) error {
		p.SessionToken = token
		return nil
	}
}

// WithDualStack enables IPv4/IPv6 dual-stack endpoints.
func WithDualStack(enable bool) Option {
	return func(p *sssOption) error {
		p.UseDualStack = enable
		return nil
	}
}

// WithAccelerate enables S3 Transfer Acceleration for faster uploads/downloads.
func WithAccelerate(enable bool) Option {
	return func(p *sssOption) error {
		p.Accelerate = enable
		return nil
	}
}

// WithLogLevel sets the AWS SDK log level for debugging.
// Use aws.LogDebug to enable detailed logging.
func WithLogLevel(level aws.LogLevelType) Option {
	return func(p *sssOption) error {
		p.LogLevel = level
		return nil
	}
}

// WithURL configures the SSS client from a URL string.
// URL format: sss://[access_key]:[secret_key]@[bucket].[region]?[options]
//
// Example:
//
//	sss://AKIAIOSFODNN7EXAMPLE:wJalrXUt...@my-bucket.us-west-2
//	sss://minioadmin:minioadmin@bucket.region?regionendpoint=http://localhost:9000&forcepathstyle=true&secure=false
//
// Supported query parameters:
//   - regionendpoint: Custom S3 endpoint URL
//   - forcepathstyle: Use path-style addressing (true/false)
//   - secure: Use HTTPS (true/false)
//   - chunksize: Chunk size for multipart uploads in bytes
//   - encrypt: Enable server-side encryption (true/false)
//   - keyid: KMS key ID for encryption
//   - storageclass: S3 storage class
//   - objectacl: Object ACL
//   - sessiontoken: AWS session token
//   - usedualstack: Use dual-stack endpoints (true/false)
//   - accelerate: Use S3 Transfer Acceleration (true/false)
//   - signendpoint: Endpoint for presigned URLs
//   - signendpointmethods: Comma-separated HTTP methods for presigned URLs
//   - loglevel: AWS SDK log level (debug)
func WithURL(uri string) Option {
	return func(p *sssOption) error {
		u, err := url.Parse(uri)
		if err != nil {
			return err
		}

		query := u.Query()

		accessKey := u.User.Username()
		secretKey, _ := u.User.Password()

		var bucket string
		var region string
		if u.Host != "" {
			part := strings.SplitN(u.Host, ".", 2)
			if len(part) != 2 {
				return fmt.Errorf("invalid host %q", u.Host)
			}

			bucket = part[0]
			region = part[1]
		}

		signEndpoint := query.Get("signendpoint")

		signendpointmethods := query.Get("signendpointmethods")
		var signEndpointMethodsStrings []string
		if signendpointmethods != "" {
			signEndpointMethodsStrings = strings.Split(signendpointmethods, ",")
		}

		regionEndpoint := query.Get("regionendpoint")

		forcePathStyleBool, _ := strconv.ParseBool(query.Get("forcepathstyle"))

		if regionEndpoint == "" {
			if region == "" {
				return fmt.Errorf("no region parameter provided")
			}
		}

		encryptBool, _ := strconv.ParseBool(query.Get("encrypt"))

		secureBool, _ := strconv.ParseBool(query.Get("secure"))

		keyID := query.Get("keyid")

		chunkSize := defaultChunkSize
		chunkSizeInt, err := strconv.Atoi(query.Get("chunksize"))
		if err == nil && chunkSizeInt > 0 {
			chunkSize = chunkSizeInt
		}

		rootDirectory := u.Path
		rootDirectoryStr := query.Get("rootdirectory")
		if rootDirectoryStr != "" {
			rootDirectory = rootDirectoryStr
		}

		storageClass := s3.StorageClassStandard
		storageClassString := query.Get("storageclass")
		if storageClassString != "" {
			storageClass = storageClassString
		}

		userAgent := query.Get("useragent")

		objectACL := s3.ObjectCannedACLPrivate
		objectACLString := query.Get("objectacl")
		if objectACLString != "" {
			objectACL = objectACLString
		}

		useDualStackBool, _ := strconv.ParseBool(query.Get("usedualstack"))

		sessionToken := query.Get("sessiontoken")

		accelerateBool, _ := strconv.ParseBool(query.Get("accelerate"))

		var logLevel = aws.LogOff
		switch query.Get("loglevel") {
		case "debug":
			logLevel = aws.LogDebug
		}

		p.DriverName = u.Scheme
		p.AccessKey = accessKey
		p.SecretKey = secretKey
		p.Bucket = bucket
		p.Region = region
		p.SignEndpoint = signEndpoint
		p.RegionEndpoint = regionEndpoint
		p.ForcePathStyle = forcePathStyleBool
		p.Encrypt = encryptBool
		p.KeyID = keyID
		p.Secure = secureBool
		p.ChunkSize = chunkSize
		p.RootDirectory = rootDirectory
		p.StorageClass = storageClass
		p.UserAgent = userAgent
		p.ObjectACL = objectACL
		p.SessionToken = sessionToken
		p.UseDualStack = useDualStackBool
		p.Accelerate = accelerateBool
		p.LogLevel = logLevel
		p.SignEndpointMethods = signEndpointMethodsStrings
		return nil
	}
}

// SSS is the main client for interacting with S3 storage.
// It provides methods for uploading, downloading, listing, and managing S3 objects.
type SSS struct {
	s3            *s3.S3
	signS3        *s3.S3
	signMethods   map[string]struct{}
	Name          string
	bucket        string
	chunkSize     int
	encrypt       bool
	keyID         string
	rootDirectory string
	storageClass  string
	objectACL     string
	pool          *sync.Pool
}

// NewSSS creates a new SSS client with the provided options.
//
// Example:
//
//	s, err := sss.NewSSS(
//		sss.WithAccessKey("YOUR_ACCESS_KEY"),
//		sss.WithSecretKey("YOUR_SECRET_KEY"),
//		sss.WithBucket("my-bucket"),
//		sss.WithRegion("us-west-2"),
//	)
//
// For MinIO or other S3-compatible services:
//
//	s, err := sss.NewSSS(
//		sss.WithURL("sss://minioadmin:minioadmin@bucket.region?regionendpoint=http://localhost:9000&forcepathstyle=true&secure=false"),
//	)
func NewSSS(opts ...Option) (*SSS, error) {
	params := sssOption{
		StorageClass: s3.StorageClassStandard,
		ObjectACL:    s3.ObjectCannedACLPrivate,
		ChunkSize:    defaultChunkSize,
	}

	for _, opt := range opts {
		err := opt(&params)
		if err != nil {
			return nil, err
		}
	}

	awsConfig := aws.NewConfig()
	if params.AccessKey != "" && params.SecretKey != "" {
		creds := credentials.NewStaticCredentials(
			params.AccessKey,
			params.SecretKey,
			params.SessionToken,
		)
		awsConfig.WithCredentials(creds)
	} else {
		awsConfig.WithCredentials(credentials.AnonymousCredentials)
	}

	if params.RegionEndpoint != "" {
		awsConfig.WithEndpoint(params.RegionEndpoint)
	}

	awsConfig.WithRegion(params.Region)
	awsConfig.WithS3ForcePathStyle(params.ForcePathStyle)
	awsConfig.WithS3UseAccelerate(params.Accelerate)
	awsConfig.WithDisableSSL(!params.Secure)
	awsConfig.WithHTTPClient(params.HTTPClient)
	awsConfig.WithLogLevel(params.LogLevel)

	if params.UseDualStack {
		awsConfig.UseDualStackEndpoint = endpoints.DualStackEndpointStateEnabled
	}

	sess, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create new session with aws config: %v", err)
	}

	if params.UserAgent != "" {
		sess.Handlers.Build.PushBack(request.MakeAddToUserAgentFreeFormHandler(params.UserAgent))
	}

	s := &SSS{
		s3:            s3.New(sess),
		Name:          params.DriverName,
		bucket:        params.Bucket,
		chunkSize:     params.ChunkSize,
		encrypt:       params.Encrypt,
		keyID:         params.KeyID,
		rootDirectory: params.RootDirectory,
		storageClass:  params.StorageClass,
		objectACL:     params.ObjectACL,
		pool: &sync.Pool{
			New: func() any { return &bytes.Buffer{} },
		},
	}

	if params.SignEndpoint != "" {
		sess.Config.Endpoint = &params.SignEndpoint
		sess.Config.S3ForcePathStyle = aws.Bool(true)
		s.signS3 = s3.New(sess)
		if len(params.SignEndpointMethods) != 0 {
			s.signMethods = make(map[string]struct{})
			for _, method := range params.SignEndpointMethods {
				s.signMethods[strings.ToUpper(method)] = struct{}{}
			}
		} else {
			s.signMethods = nil
		}
	}
	return s, nil
}

func (s *SSS) presign(expires time.Duration, fun func(s3 *s3.S3) *request.Request) (string, error) {
	if s.signS3 == nil {
		return fun(s.s3).Presign(expires)
	}
	if s.signMethods != nil {
		req := fun(s.s3)
		if _, ok := s.signMethods[req.HTTPRequest.Method]; !ok {
			return req.Presign(expires)
		}
	}
	req := fun(s.signS3)
	req.HTTPRequest.URL.Path = strings.TrimPrefix(req.HTTPRequest.URL.Path, "/{Bucket}")
	return req.Presign(expires)
}

func (s *SSS) s3Path(path string) string {
	return strings.TrimLeft(strings.TrimRight(s.rootDirectory, "/")+path, "/")
}

func parseError(path string, err error) error {
	if s3Err, ok := err.(awserr.Error); ok && s3Err.Code() == "NoSuchKey" {
		return fmt.Errorf("path not found: %s", path)
	}

	return err
}

func (s *SSS) getEncryptionMode() *string {
	if !s.encrypt {
		return nil
	}
	if s.keyID == "" {
		return aws.String("AES256")
	}
	return aws.String("aws:kms")
}

func (s *SSS) getSSEKMSKeyID() *string {
	if s.keyID != "" {
		return aws.String(s.keyID)
	}
	return nil
}

func (s *SSS) getContentType() *string {
	return aws.String("application/octet-stream")
}

func (s *SSS) getACL() *string {
	return aws.String(s.objectACL)
}

func (s *SSS) getStorageClass() *string {
	if s.storageClass == noStorageClass {
		return nil
	}
	return aws.String(s.storageClass)
}

func (s *SSS) getBucket() *string {
	return aws.String(s.bucket)
}

// ChunkSize returns the configured chunk size for multipart uploads.
func (s *SSS) ChunkSize() int {
	return s.chunkSize
}

// S3 returns the underlying AWS S3 client for advanced operations.
func (s *SSS) S3() *s3.S3 {
	return s.s3
}

type s3completedParts []*s3.CompletedPart

func (a s3completedParts) Len() int           { return len(a) }
func (a s3completedParts) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a s3completedParts) Less(i, j int) bool { return *a[i].PartNumber < *a[j].PartNumber }

type s3parts []*s3.Part

func (a s3parts) Len() int           { return len(a) }
func (a s3parts) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a s3parts) Less(i, j int) bool { return *a[i].PartNumber < *a[j].PartNumber }
