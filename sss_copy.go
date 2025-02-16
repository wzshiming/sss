package sss

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

func (s *SSS) SignCopy(ctx context.Context, sourcePath, destPath string, expires time.Duration) (string, error) {
	req, _ := s.s3.CopyObjectRequest(&s3.CopyObjectInput{
		Bucket:               s.getBucket(),
		Key:                  aws.String(s.s3Path(destPath)),
		ContentType:          s.getContentType(),
		ACL:                  s.getACL(),
		ServerSideEncryption: s.getEncryptionMode(),
		SSEKMSKeyId:          s.getSSEKMSKeyID(),
		StorageClass:         s.getStorageClass(),
		CopySource:           aws.String(s.bucket + "/" + s.s3Path(sourcePath)),
	})
	return req.Presign(expires)
}

func (s *SSS) Copy(ctx context.Context, sourcePath, destPath string) error {
	_, err := s.s3.CopyObjectWithContext(ctx, &s3.CopyObjectInput{
		Bucket:               s.getBucket(),
		Key:                  aws.String(s.s3Path(destPath)),
		ContentType:          s.getContentType(),
		ACL:                  s.getACL(),
		ServerSideEncryption: s.getEncryptionMode(),
		SSEKMSKeyId:          s.getSSEKMSKeyID(),
		StorageClass:         s.getStorageClass(),
		CopySource:           aws.String(s.bucket + "/" + s.s3Path(sourcePath)),
	})
	if err != nil {
		return parseError(sourcePath, err)
	}
	return nil

}
