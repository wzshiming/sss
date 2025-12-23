package sss

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
)

// TestOptionFunctions tests all the option functions
func TestOptionFunctions(t *testing.T) {
	tests := []struct {
		name   string
		option Option
		verify func(*sssOption) bool
	}{
		{
			name:   "WithAccessKey",
			option: WithAccessKey("test-access-key"),
			verify: func(o *sssOption) bool { return o.AccessKey == "test-access-key" },
		},
		{
			name:   "WithSecretKey",
			option: WithSecretKey("test-secret-key"),
			verify: func(o *sssOption) bool { return o.SecretKey == "test-secret-key" },
		},
		{
			name:   "WithBucket",
			option: WithBucket("test-bucket"),
			verify: func(o *sssOption) bool { return o.Bucket == "test-bucket" },
		},
		{
			name:   "WithRegion",
			option: WithRegion("us-west-2"),
			verify: func(o *sssOption) bool { return o.Region == "us-west-2" },
		},
		{
			name:   "WithRegionEndpoint",
			option: WithRegionEndpoint("http://localhost:9000"),
			verify: func(o *sssOption) bool { return o.RegionEndpoint == "http://localhost:9000" },
		},
		{
			name:   "WithSignEndpoint",
			option: WithSignEndpoint("http://sign.example.com", "GET", "POST"),
			verify: func(o *sssOption) bool {
				return o.SignEndpoint == "http://sign.example.com" &&
					len(o.SignEndpointMethods) == 2 &&
					o.SignEndpointMethods[0] == "GET" &&
					o.SignEndpointMethods[1] == "POST"
			},
		},
		{
			name:   "WithForcePathStyle",
			option: WithForcePathStyle(true),
			verify: func(o *sssOption) bool { return o.ForcePathStyle == true },
		},
		{
			name:   "WithEncryption",
			option: WithEncryption(true),
			verify: func(o *sssOption) bool { return o.Encrypt == true },
		},
		{
			name:   "WithKMSKeyID",
			option: WithKMSKeyID("test-key-id"),
			verify: func(o *sssOption) bool { return o.KeyID == "test-key-id" },
		},
		{
			name:   "WithSecure",
			option: WithSecure(true),
			verify: func(o *sssOption) bool { return o.Secure == true },
		},
		{
			name:   "WithChunkSize",
			option: WithChunkSize(5 * 1024 * 1024),
			verify: func(o *sssOption) bool { return o.ChunkSize == 5*1024*1024 },
		},
		{
			name:   "WithRootDirectory",
			option: WithRootDirectory("/test/dir"),
			verify: func(o *sssOption) bool { return o.RootDirectory == "/test/dir" },
		},
		{
			name:   "WithStorageClass",
			option: WithStorageClass("STANDARD_IA"),
			verify: func(o *sssOption) bool { return o.StorageClass == "STANDARD_IA" },
		},
		{
			name:   "WithUserAgent",
			option: WithUserAgent("test-agent/1.0"),
			verify: func(o *sssOption) bool { return o.UserAgent == "test-agent/1.0" },
		},
		{
			name:   "WithObjectACL",
			option: WithObjectACL("public-read"),
			verify: func(o *sssOption) bool { return o.ObjectACL == "public-read" },
		},
		{
			name:   "WithSessionToken",
			option: WithSessionToken("test-token"),
			verify: func(o *sssOption) bool { return o.SessionToken == "test-token" },
		},
		{
			name:   "WithDualStack",
			option: WithDualStack(true),
			verify: func(o *sssOption) bool { return o.UseDualStack == true },
		},
		{
			name:   "WithAccelerate",
			option: WithAccelerate(true),
			verify: func(o *sssOption) bool { return o.Accelerate == true },
		},
		{
			name:   "WithLogLevel",
			option: WithLogLevel(aws.LogDebug),
			verify: func(o *sssOption) bool { return o.LogLevel == aws.LogDebug },
		},
		{
			name:   "WithDriverName",
			option: WithDriverName("s3"),
			verify: func(o *sssOption) bool { return o.DriverName == "s3" },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opt := &sssOption{}
			err := tt.option(opt)
			if err != nil {
				t.Fatalf("option returned error: %v", err)
			}
			if !tt.verify(opt) {
				t.Fatalf("option verification failed")
			}
		})
	}
}

func TestWithURL(t *testing.T) {
	tests := []struct {
		name      string
		url       string
		wantErr   bool
		verifyOpt func(*sssOption) bool
	}{
		{
			name: "valid URL with all parameters",
			url:  "sss://accesskey:secretkey@bucket.region?forcepathstyle=true&secure=false&chunksize=5242880&regionendpoint=http://127.0.0.1:9000",
			verifyOpt: func(o *sssOption) bool {
				return o.DriverName == "sss" &&
					o.AccessKey == "accesskey" &&
					o.SecretKey == "secretkey" &&
					o.Bucket == "bucket" &&
					o.Region == "region" &&
					o.ForcePathStyle == true &&
					o.Secure == false &&
					o.ChunkSize == 5242880 &&
					o.RegionEndpoint == "http://127.0.0.1:9000"
			},
		},
		{
			name: "URL with encryption",
			url:  "sss://key:secret@bucket.region?encrypt=true&keyid=test-key",
			verifyOpt: func(o *sssOption) bool {
				return o.Encrypt == true && o.KeyID == "test-key"
			},
		},
		{
			name:    "invalid URL - missing region",
			url:     "sss://accesskey:secretkey@bucket",
			wantErr: true,
		},
		{
			name:    "invalid URL - malformed",
			url:     "://invalid",
			wantErr: true,
		},
		{
			name: "URL with storage class",
			url:  "sss://key:secret@bucket.region?storageclass=GLACIER",
			verifyOpt: func(o *sssOption) bool {
				return o.StorageClass == "GLACIER"
			},
		},
		{
			name: "URL with user agent",
			url:  "sss://key:secret@bucket.region?useragent=myagent",
			verifyOpt: func(o *sssOption) bool {
				return o.UserAgent == "myagent"
			},
		},
		{
			name: "URL with object ACL",
			url:  "sss://key:secret@bucket.region?objectacl=public-read",
			verifyOpt: func(o *sssOption) bool {
				return o.ObjectACL == "public-read"
			},
		},
		{
			name: "URL with dual stack",
			url:  "sss://key:secret@bucket.region?usedualstack=true",
			verifyOpt: func(o *sssOption) bool {
				return o.UseDualStack == true
			},
		},
		{
			name: "URL with session token",
			url:  "sss://key:secret@bucket.region?sessiontoken=mytoken",
			verifyOpt: func(o *sssOption) bool {
				return o.SessionToken == "mytoken"
			},
		},
		{
			name: "URL with accelerate",
			url:  "sss://key:secret@bucket.region?accelerate=true",
			verifyOpt: func(o *sssOption) bool {
				return o.Accelerate == true
			},
		},
		{
			name: "URL with log level debug",
			url:  "sss://key:secret@bucket.region?loglevel=debug",
			verifyOpt: func(o *sssOption) bool {
				return o.LogLevel == aws.LogDebug
			},
		},
		{
			name: "URL with sign endpoint",
			url:  "sss://key:secret@bucket.region?signendpoint=http://sign.example.com&signendpointmethods=GET,POST",
			verifyOpt: func(o *sssOption) bool {
				return o.SignEndpoint == "http://sign.example.com" &&
					len(o.SignEndpointMethods) == 2 &&
					o.SignEndpointMethods[0] == "GET" &&
					o.SignEndpointMethods[1] == "POST"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opt := &sssOption{}
			err := WithURL(tt.url)(opt)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.verifyOpt != nil && !tt.verifyOpt(opt) {
				t.Fatalf("option verification failed")
			}
		})
	}
}

func TestS3Path(t *testing.T) {
	tests := []struct {
		name          string
		rootDirectory string
		path          string
		expected      string
	}{
		{
			name:          "empty root directory",
			rootDirectory: "",
			path:          "/path/to/file",
			expected:      "path/to/file",
		},
		{
			name:          "root directory with slash",
			rootDirectory: "/root",
			path:          "/path/to/file",
			expected:      "root/path/to/file",
		},
		{
			name:          "root directory without slash",
			rootDirectory: "root",
			path:          "/path/to/file",
			expected:      "root/path/to/file",
		},
		{
			name:          "root directory with trailing slash",
			rootDirectory: "/root/",
			path:          "/path/to/file",
			expected:      "root/path/to/file",
		},
		{
			name:          "empty path",
			rootDirectory: "/root",
			path:          "",
			expected:      "root",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SSS{rootDirectory: tt.rootDirectory}
			result := s.s3Path(tt.path)
			if result != tt.expected {
				t.Errorf("s3Path() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestGetEncryptionMode(t *testing.T) {
	tests := []struct {
		name    string
		encrypt bool
		keyID   string
		want    *string
	}{
		{
			name:    "no encryption",
			encrypt: false,
			keyID:   "",
			want:    nil,
		},
		{
			name:    "AES256 encryption",
			encrypt: true,
			keyID:   "",
			want:    aws.String("AES256"),
		},
		{
			name:    "KMS encryption",
			encrypt: true,
			keyID:   "test-key-id",
			want:    aws.String("aws:kms"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SSS{encrypt: tt.encrypt, keyID: tt.keyID}
			got := s.getEncryptionMode()
			if tt.want == nil {
				if got != nil {
					t.Errorf("getEncryptionMode() = %v, want nil", *got)
				}
			} else if got == nil {
				t.Errorf("getEncryptionMode() = nil, want %v", *tt.want)
			} else if *got != *tt.want {
				t.Errorf("getEncryptionMode() = %v, want %v", *got, *tt.want)
			}
		})
	}
}

func TestGetSSEKMSKeyID(t *testing.T) {
	tests := []struct {
		name  string
		keyID string
		want  *string
	}{
		{
			name:  "no key ID",
			keyID: "",
			want:  nil,
		},
		{
			name:  "with key ID",
			keyID: "test-key-id",
			want:  aws.String("test-key-id"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SSS{keyID: tt.keyID}
			got := s.getSSEKMSKeyID()
			if tt.want == nil {
				if got != nil {
					t.Errorf("getSSEKMSKeyID() = %v, want nil", *got)
				}
			} else if got == nil {
				t.Errorf("getSSEKMSKeyID() = nil, want %v", *tt.want)
			} else if *got != *tt.want {
				t.Errorf("getSSEKMSKeyID() = %v, want %v", *got, *tt.want)
			}
		})
	}
}

func TestGetStorageClass(t *testing.T) {
	tests := []struct {
		name         string
		storageClass string
		want         *string
	}{
		{
			name:         "standard storage class",
			storageClass: "STANDARD",
			want:         aws.String("STANDARD"),
		},
		{
			name:         "no storage class",
			storageClass: "NONE", // noStorageClass constant value
			want:         nil,
		},
		{
			name:         "glacier storage class",
			storageClass: "GLACIER",
			want:         aws.String("GLACIER"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SSS{storageClass: tt.storageClass}
			got := s.getStorageClass()
			if tt.want == nil {
				if got != nil {
					t.Errorf("getStorageClass() = %v, want nil", *got)
				}
			} else if got == nil {
				t.Errorf("getStorageClass() = nil, want %v", *tt.want)
			} else if *got != *tt.want {
				t.Errorf("getStorageClass() = %v, want %v", *got, *tt.want)
			}
		})
	}
}

func TestGetBucket(t *testing.T) {
	s := &SSS{bucket: "test-bucket"}
	got := s.getBucket()
	if got == nil {
		t.Fatalf("getBucket() returned nil")
	}
	if *got != "test-bucket" {
		t.Errorf("getBucket() = %v, want %v", *got, "test-bucket")
	}
}

func TestGetContentType(t *testing.T) {
	s := &SSS{}
	got := s.getContentType()
	if got == nil {
		t.Fatalf("getContentType() returned nil")
	}
	if *got != "application/octet-stream" {
		t.Errorf("getContentType() = %v, want %v", *got, "application/octet-stream")
	}
}

func TestGetACL(t *testing.T) {
	s := &SSS{objectACL: "private"}
	got := s.getACL()
	if got == nil {
		t.Fatalf("getACL() returned nil")
	}
	if *got != "private" {
		t.Errorf("getACL() = %v, want %v", *got, "private")
	}
}

func TestChunkSize(t *testing.T) {
	s := &SSS{chunkSize: 1024}
	got := s.ChunkSize()
	if got != 1024 {
		t.Errorf("ChunkSize() = %v, want %v", got, 1024)
	}
}
