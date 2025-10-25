package sss

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/logging"
)

const (
	defaultChunkSize = 10 * 1024 * 1024

	// listMax is the largest amount of objects you can request from S3 in a list call
	listMax = 1000

	// noStorageClass defines the value to be used if storage class is not supported by the S3 endpoint
	noStorageClass = "NONE"
)

type sssOption struct {
	HTTPClient     *http.Client
	DriverName     string
	AccessKey      string
	SecretKey      string
	Bucket         string
	Region         string
	RegionEndpoint string
	SignEndpoint   string
	ForcePathStyle bool
	Encrypt        bool
	KeyID          string
	Secure         bool
	ChunkSize      int
	RootDirectory  string
	StorageClass   string
	UserAgent      string
	ObjectACL      string
	SessionToken   string
	UseDualStack   bool
	Accelerate     bool
	LogLevel       logging.Classification
}

type Option func(*sssOption) error

func WithHTTPClient(client *http.Client) Option {
	return func(p *sssOption) error {
		p.HTTPClient = client
		return nil
	}
}

func WithDriverName(name string) Option {
	return func(p *sssOption) error {
		p.DriverName = name
		return nil
	}
}

func WithAccessKey(key string) Option {
	return func(p *sssOption) error {
		p.AccessKey = key
		return nil
	}
}

func WithSecretKey(key string) Option {
	return func(p *sssOption) error {
		p.SecretKey = key
		return nil
	}
}

func WithBucket(bucket string) Option {
	return func(p *sssOption) error {
		p.Bucket = bucket
		return nil
	}
}

func WithRegion(region string) Option {
	return func(p *sssOption) error {
		p.Region = region
		return nil
	}
}

func WithRegionEndpoint(endpoint string) Option {
	return func(p *sssOption) error {
		p.RegionEndpoint = endpoint
		return nil
	}
}

func WithForcePathStyle(enable bool) Option {
	return func(p *sssOption) error {
		p.ForcePathStyle = enable
		return nil
	}
}

func WithEncryption(enable bool) Option {
	return func(p *sssOption) error {
		p.Encrypt = enable
		return nil
	}
}

func WithKMSKeyID(id string) Option {
	return func(p *sssOption) error {
		p.KeyID = id
		return nil
	}
}

func WithSecure(enable bool) Option {
	return func(p *sssOption) error {
		p.Secure = enable
		return nil
	}
}

func WithChunkSize(size int) Option {
	return func(p *sssOption) error {
		p.ChunkSize = size
		return nil
	}
}

func WithRootDirectory(dir string) Option {
	return func(p *sssOption) error {
		p.RootDirectory = dir
		return nil
	}
}

func WithStorageClass(class string) Option {
	return func(p *sssOption) error {
		p.StorageClass = class
		return nil
	}
}

func WithUserAgent(ua string) Option {
	return func(p *sssOption) error {
		p.UserAgent = ua
		return nil
	}
}

func WithObjectACL(acl string) Option {
	return func(p *sssOption) error {
		p.ObjectACL = acl
		return nil
	}
}

func WithSessionToken(token string) Option {
	return func(p *sssOption) error {
		p.SessionToken = token
		return nil
	}
}

func WithDualStack(enable bool) Option {
	return func(p *sssOption) error {
		p.UseDualStack = enable
		return nil
	}
}

func WithAccelerate(enable bool) Option {
	return func(p *sssOption) error {
		p.Accelerate = enable
		return nil
	}
}

func WithLogLevel(level logging.Classification) Option {
	return func(p *sssOption) error {
		p.LogLevel = level
		return nil
	}
}

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

		storageClass := string(s3types.StorageClassStandard)
		storageClassString := query.Get("storageclass")
		if storageClassString != "" {
			storageClass = storageClassString
		}

		userAgent := query.Get("useragent")

		objectACL := string(s3types.ObjectCannedACLPrivate)
		objectACLString := query.Get("objectacl")
		if objectACLString != "" {
			objectACL = objectACLString
		}

		useDualStackBool, _ := strconv.ParseBool(query.Get("usedualstack"))

		sessionToken := query.Get("sessiontoken")

		accelerateBool, _ := strconv.ParseBool(query.Get("accelerate"))

		var logLevel logging.Classification
		switch query.Get("loglevel") {
		case "debug":
			logLevel = logging.Debug
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
		return nil
	}
}

type SSS struct {
	s3            *s3.Client
	signS3        *s3.Client
	presignClient *s3.PresignClient
	signPresignClient *s3.PresignClient
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

func NewSSS(opts ...Option) (*SSS, error) {
	params := sssOption{
		StorageClass: string(s3types.StorageClassStandard),
		ObjectACL:    string(s3types.ObjectCannedACLPrivate),
		ChunkSize:    defaultChunkSize,
	}

	for _, opt := range opts {
		err := opt(&params)
		if err != nil {
			return nil, err
		}
	}

	// Build AWS config options
	configOpts := []func(*config.LoadOptions) error{}

	if params.Region != "" {
		configOpts = append(configOpts, config.WithRegion(params.Region))
	}

	if params.AccessKey != "" && params.SecretKey != "" {
		configOpts = append(configOpts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				params.AccessKey,
				params.SecretKey,
				params.SessionToken,
			),
		))
	} else {
		configOpts = append(configOpts, config.WithCredentialsProvider(
			aws.AnonymousCredentials{},
		))
	}

	if params.HTTPClient != nil {
		configOpts = append(configOpts, config.WithHTTPClient(params.HTTPClient))
	}

	cfg, err := config.LoadDefaultConfig(context.Background(), configOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %v", err)
	}

	// Build S3 client options
	s3Opts := []func(*s3.Options){}

	if params.RegionEndpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(params.RegionEndpoint)
		})
	}

	s3Opts = append(s3Opts, func(o *s3.Options) {
		o.UsePathStyle = params.ForcePathStyle
	})

	s3Opts = append(s3Opts, func(o *s3.Options) {
		o.UseAccelerate = params.Accelerate
	})

	if params.UseDualStack {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.EndpointOptions.UseDualStackEndpoint = aws.DualStackEndpointStateEnabled
		})
	}

	s3Client := s3.NewFromConfig(cfg, s3Opts...)
	presignClient := s3.NewPresignClient(s3Client)

	var signS3Client *s3.Client
	var signPresignClient *s3.PresignClient
	if params.SignEndpoint != "" {
		signS3Opts := append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(params.SignEndpoint)
			o.UsePathStyle = true
		})
		signS3Client = s3.NewFromConfig(cfg, signS3Opts...)
		signPresignClient = s3.NewPresignClient(signS3Client)
	}

	sss := &SSS{
		s3:                s3Client,
		signS3:            signS3Client,
		presignClient:     presignClient,
		signPresignClient: signPresignClient,
		Name:              params.DriverName,
		bucket:            params.Bucket,
		chunkSize:         params.ChunkSize,
		encrypt:           params.Encrypt,
		keyID:             params.KeyID,
		rootDirectory:     params.RootDirectory,
		storageClass:      params.StorageClass,
		objectACL:         params.ObjectACL,
		pool: &sync.Pool{
			New: func() any { return &bytes.Buffer{} },
		},
	}

	return sss, nil
}

func (s *SSS) presign(expires time.Duration, fun func(presignClient *s3.PresignClient) (*v4.PresignedHTTPRequest, error)) (string, error) {
	if s.signPresignClient == nil {
		req, err := fun(s.presignClient)
		if err != nil {
			return "", err
		}
		return req.URL, nil
	}
	req, err := fun(s.signPresignClient)
	if err != nil {
		return "", err
	}
	// Trim the /{Bucket} prefix if present
	parsedURL, err := url.Parse(req.URL)
	if err != nil {
		return req.URL, nil
	}
	parsedURL.Path = strings.TrimPrefix(parsedURL.Path, "/"+s.bucket)
	return parsedURL.String(), nil
}

func (s *SSS) s3Path(path string) string {
	return strings.TrimLeft(strings.TrimRight(s.rootDirectory, "/")+path, "/")
}

func parseError(path string, err error) error {
	if err == nil {
		return nil
	}
	
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if apiErr.ErrorCode() == "NoSuchKey" || apiErr.ErrorCode() == "NotFound" {
			return fmt.Errorf("path not found: %s", path)
		}
	}

	return err
}

func (s *SSS) getEncryptionMode() s3types.ServerSideEncryption {
	if !s.encrypt {
		return ""
	}
	if s.keyID == "" {
		return s3types.ServerSideEncryptionAes256
	}
	return s3types.ServerSideEncryptionAwsKms
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

func (s *SSS) getACL() s3types.ObjectCannedACL {
	return s3types.ObjectCannedACL(s.objectACL)
}

func (s *SSS) getStorageClass() s3types.StorageClass {
	if s.storageClass == noStorageClass {
		return ""
	}
	return s3types.StorageClass(s.storageClass)
}

func (s *SSS) getBucket() *string {
	return aws.String(s.bucket)
}

func (s *SSS) ChunkSize() int {
	return s.chunkSize
}

func (s *SSS) S3() *s3.Client {
	return s.s3
}

type s3completedParts []s3types.CompletedPart

func (a s3completedParts) Len() int           { return len(a) }
func (a s3completedParts) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a s3completedParts) Less(i, j int) bool { return *a[i].PartNumber < *a[j].PartNumber }

type s3parts []s3types.Part

func (a s3parts) Len() int           { return len(a) }
func (a s3parts) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a s3parts) Less(i, j int) bool { return *a[i].PartNumber < *a[j].PartNumber }
