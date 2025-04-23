package sss

import (
	"context"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
)

func (s *SSS) SignList(path string, expires time.Duration) (string, error) {
	return s.presign(expires,
		func(c *s3.S3) *request.Request {
			req, _ := c.ListObjectsRequest(&s3.ListObjectsInput{
				Bucket: s.getBucket(),
				Prefix: aws.String(s.s3Path(path)),
			})
			return req
		})
}

func (s *SSS) List(ctx context.Context, opath string, fun func(fileInfo FileInfo) bool) error {
	path := opath
	if path != "" && path != "/" && path[len(path)-1] != '/' {
		path = path + "/"
	}

	s3Path := s.s3Path("")

	// This is to cover for the cases when the rootDirectory of the driver is either "" or "/".
	// In those cases, there is no root prefix to replace and we must actually add a "/" to all
	// results in order to keep them as valid paths as recognized by PathRegexp
	prefix := ""
	if s3Path == "" {
		prefix = "/"
	}

	err := s.s3.ListObjectsV2PagesWithContext(ctx, &s3.ListObjectsV2Input{
		Bucket:    s.getBucket(),
		Prefix:    aws.String(s.s3Path(path)),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int64(listMax),
	}, func(resp *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, key := range resp.Contents {
			fileInfo := &fileInfo{
				path:    strings.Replace(*key.Key, s3Path, prefix, 1),
				isDir:   *key.Size == 0,
				size:    *key.Size,
				modTime: *key.LastModified,
			}
			if !fun(fileInfo) {
				return false
			}
		}

		for _, commonPrefix := range resp.CommonPrefixes {
			commonPrefix := *commonPrefix.Prefix
			if !fun(&fileInfo{
				path:    strings.Replace(commonPrefix[0:len(commonPrefix)-1], s3Path, prefix, 1),
				isDir:   true,
				modTime: time.Time{},
			}) {
				return false
			}
		}
		return !lastPage
	})
	if err != nil {
		return parseError(opath, err)
	}
	return nil
}
