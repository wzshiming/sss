package sss

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
)

func (s *SSS) SignDelete(path string, expires time.Duration) (string, error) {
	return s.presign(expires,
		func(c *s3.S3) *request.Request {
			req, _ := c.DeleteObjectRequest(&s3.DeleteObjectInput{
				Bucket: s.getBucket(),
				Key:    aws.String(s.s3Path(path)),
			})
			return req
		})
}

// Delete deletes the object stored at the given paths
func (s *SSS) Delete(ctx context.Context, path string) error {
	_, err := s.s3.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: s.getBucket(),
		Key:    aws.String(s.s3Path(path)),
	})
	if err != nil {
		return err
	}
	return nil
}

// DeleteBatch deletes multiple objects stored at the given paths
func (s *SSS) DeleteBatch(ctx context.Context, paths []string) error {
	var s3Objects []*s3.ObjectIdentifier
	for i := 0; i < len(paths); i += listMax {
		end := i + listMax
		if end > len(paths) {
			end = len(paths)
		}

		for _, path := range paths[i:end] {
			s3Objects = append(s3Objects, &s3.ObjectIdentifier{
				Key: aws.String(s.s3Path(path)),
			})
		}

		resp, err := s.s3.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
			Bucket: s.getBucket(),
			Delete: &s3.Delete{
				Objects: s3Objects,
				Quiet:   aws.Bool(false),
			},
		})
		if err != nil {
			return err
		}

		s3Objects = s3Objects[:0]

		if len(resp.Errors) > 0 {
			errs := make([]error, 0, len(resp.Errors))
			for _, err := range resp.Errors {
				errs = append(errs, errors.New(err.String()))
			}
			return errors.Join(errs...)
		}
	}
	return nil
}

// DeleteAll recursively deletes all objects stored at path and its subpaths.
// We must be careful since S3 does not guarantee read after delete consistency
func (s *SSS) DeleteAll(ctx context.Context, path string) error {
	var batch []string
	err := s.Walk(ctx, path, func(fileInfo FileInfo) error {
		if fileInfo.IsDir() {
			return nil
		}

		batch = append(batch, fileInfo.Path())
		if len(batch) == listMax {
			if err := s.DeleteBatch(ctx, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
		return nil
	})

	if err != nil {
		return err
	}

	if len(batch) > 0 {
		if err := s.DeleteBatch(ctx, batch); err != nil {
			return err
		}
	}

	return nil
}
