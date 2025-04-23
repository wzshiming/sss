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
	LogLevel       aws.LogLevelType
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

func WithLogLevel(level aws.LogLevelType) Option {
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
		return nil
	}
}

type SSS struct {
	s3            *s3.S3
	signS3        *s3.S3
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
	}
	return s, nil
}

func (s *SSS) presign(expires time.Duration, fun func(s3 *s3.S3) *request.Request) (string, error) {
	if s.signS3 == nil {
		return fun(s.s3).Presign(expires)
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

func (s *SSS) ChunkSize() int {
	return s.chunkSize
}

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
