package sss

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func (s *SSS) SignList(path string, expires time.Duration) (string, error) {
	// Note: ListObjectsV2 presigning is not directly supported in AWS SDK v2
	// This would require manual URL signing or using GetObject presigning as a workaround
	return "", fmt.Errorf("SignList is not supported in AWS SDK v2")
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

	paginator := s3.NewListObjectsV2Paginator(s.s3, &s3.ListObjectsV2Input{
		Bucket:    s.getBucket(),
		Prefix:    aws.String(s.s3Path(path)),
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int32(listMax),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return parseError(opath, err)
		}

		for _, key := range page.Contents {
			if *key.Size == 0 {
				fileInfo := &fileInfo{
					path:    strings.Replace(*key.Key, s3Path, prefix, 1),
					isDir:   true,
					size:    0,
					modTime: *key.LastModified,
				}
				if !fun(fileInfo) {
					return nil
				}
			} else {
				fileInfo := &fileInfo{
					path:    strings.Replace(*key.Key, s3Path, prefix, 1),
					isDir:   false,
					size:    *key.Size,
					modTime: *key.LastModified,
				}
				if !fun(fileInfo) {
					return nil
				}
			}
		}

		for _, commonPrefix := range page.CommonPrefixes {
			commonPrefix := *commonPrefix.Prefix
			if !fun(&fileInfo{
				path:    strings.Replace(commonPrefix[0:len(commonPrefix)-1], s3Path, prefix, 1),
				isDir:   true,
				modTime: time.Time{},
			}) {
				return nil
			}
		}
	}
	return nil
}
