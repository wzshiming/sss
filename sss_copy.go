package sss

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func (s *SSS) SignCopy(ctx context.Context, sourcePath, destPath string, expires time.Duration) (string, error) {
	// Note: CopyObject presigning is not directly supported in AWS SDK v2
	// This would require manual URL signing
	return "", fmt.Errorf("SignCopy is not supported in AWS SDK v2")
}

func (s *SSS) Copy(ctx context.Context, sourcePath, destPath string) error {
	encryptMode := s.getEncryptionMode()
	storageClass := s.getStorageClass()
	
	input := &s3.CopyObjectInput{
		Bucket:      s.getBucket(),
		Key:         aws.String(s.s3Path(destPath)),
		ContentType: s.getContentType(),
		ACL:         s.getACL(),
		CopySource:  aws.String(s.bucket + "/" + s.s3Path(sourcePath)),
	}
	
	if encryptMode != "" {
		input.ServerSideEncryption = encryptMode
	}
	if s.getSSEKMSKeyID() != nil {
		input.SSEKMSKeyId = s.getSSEKMSKeyID()
	}
	if storageClass != "" {
		input.StorageClass = storageClass
	}
	
	_, err := s.s3.CopyObject(ctx, input)
	if err != nil {
		return parseError(sourcePath, err)
	}
	return nil

}
