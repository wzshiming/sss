package sss

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
)

func (s *SSS) SignCopy(ctx context.Context, sourcePath, destPath string, expires time.Duration) (string, error) {
	return s.presign(expires,
		func(c *s3.S3) *request.Request {
			req, _ := c.CopyObjectRequest(&s3.CopyObjectInput{
				Bucket:               s.getBucket(),
				Key:                  aws.String(s.s3Path(destPath)),
				ContentType:          s.getContentType(),
				ACL:                  s.getACL(),
				ServerSideEncryption: s.getEncryptionMode(),
				SSEKMSKeyId:          s.getSSEKMSKeyID(),
				StorageClass:         s.getStorageClass(),
				CopySource:           aws.String(s.bucket + "/" + s.s3Path(sourcePath)),
			})
			return req
		})
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
