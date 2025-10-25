package sss

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

func (s *SSS) SignHead(path string, expires time.Duration) (string, error) {
	return s.presign(expires,
		func(presignClient *s3.PresignClient) (*v4.PresignedHTTPRequest, error) {
			return presignClient.PresignHeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket: s.getBucket(),
				Key:    aws.String(s.s3Path(path)),
			}, s3.WithPresignExpires(expires))
		})
}

func (s *SSS) StatHead(ctx context.Context, path string) (FileInfo, error) {
	resp, err := s.s3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: s.getBucket(),
		Key:    aws.String(s.s3Path(path)),
	})
	if err != nil {
		return nil, err
	}
	return &fileInfo{
		path:    path,
		isDir:   false,
		size:    *resp.ContentLength,
		modTime: *resp.LastModified,
		sys: FileInfoExpansion{
			ContentType:  resp.ContentType,
			AcceptRanges: resp.AcceptRanges,
			ETag:         resp.ETag,
			Expires:      resp.Expires,
		},
	}, nil
}

func (s *SSS) StatHeadList(ctx context.Context, path string) (FileInfo, error) {
	s3Path := s.s3Path(path)
	resp, err := s.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  s.getBucket(),
		Prefix:  aws.String(s3Path),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return nil, err
	}
	if len(resp.Contents) == 1 {
		if *resp.Contents[0].Key != s3Path {
			return &fileInfo{
				path:  path,
				isDir: true,
			}, nil
		}
		return &fileInfo{
			path:    path,
			size:    *resp.Contents[0].Size,
			modTime: *resp.Contents[0].LastModified,
		}, nil
	}
	if len(resp.CommonPrefixes) == 1 {
		return &fileInfo{
			path:  path,
			isDir: true,
		}, nil
	}
	return nil, fmt.Errorf("path not found: %s", path)
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (s *SSS) Stat(ctx context.Context, path string) (FileInfo, error) {
	fi, err := s.StatHead(ctx, path)
	if err != nil {
		// For AWS errors, we fail over to ListObjects:
		// Though the official docs https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_Errors
		// are slightly outdated, the HeadObject actually returns NotFound error
		// if querying a key which doesn't exist or a key which has nested keys
		// and Forbidden if IAM/ACL permissions do not allow Head but allow List.
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			fi, err := s.StatHeadList(ctx, path)
			if err != nil {
				return nil, parseError(path, err)
			}
			return fi, nil
		}
		// For non-AWS errors, return the error directly
		return nil, err
	}
	return fi, nil
}
