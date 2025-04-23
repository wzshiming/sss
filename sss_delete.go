package sss

import (
	"context"
	"errors"
	"fmt"
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

func (s *SSS) Delete(ctx context.Context, path string) error {
	s3Path := s.s3Path(path)
	_, err := s.s3.DeleteObjectsWithContext(ctx, &s3.DeleteObjectsInput{
		Bucket: s.getBucket(),
		Delete: &s3.Delete{
			Objects: []*s3.ObjectIdentifier{
				{
					Key: aws.String(s3Path),
				},
			},
			Quiet: aws.Bool(false),
		},
	})
	if err != nil {
		return err
	}
	return nil
}

// DeleteAll recursively deletes all objects stored at "path" and its subpaths.
// We must be careful since S3 does not guarantee read after delete consistency
func (s *SSS) DeleteAll(ctx context.Context, path string) error {
	s3Objects := make([]*s3.ObjectIdentifier, 0, listMax)
	s3Path := s.s3Path(path)
	listObjectsInput := &s3.ListObjectsV2Input{
		Bucket: s.getBucket(),
		Prefix: aws.String(s3Path),
	}

	for {
		// list all the objects
		resp, err := s.s3.ListObjectsV2WithContext(ctx, listObjectsInput)

		// resp.Contents can only be empty on the first call
		// if there were no more results to return after the first call, resp.IsTruncated would have been false
		// and the loop would exit without recalling ListObjects
		if err != nil || len(resp.Contents) == 0 {
			return fmt.Errorf("path not found: %s", path)
		}

		for _, key := range resp.Contents {
			// Skip if we encounter a key that is not a subpath (so that deleting "/a" does not delete "/ab").
			if len(*key.Key) > len(s3Path) && (*key.Key)[len(s3Path)] != '/' {
				continue
			}
			s3Objects = append(s3Objects, &s3.ObjectIdentifier{
				Key: key.Key,
			})
		}

		// Delete objects only if the list is not empty, otherwise S3 API returns a cryptic error
		if len(s3Objects) > 0 {
			// NOTE: according to AWS docs https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
			// by default the response returns up to 1,000 key names. The response _might_ contain fewer keys but it will never contain more.
			// 10000 keys is coincidentally (?) also the max number of keys that can be deleted in a single Delete operation, so we'll just smack
			// Delete here straight away and reset the object slice when successful.
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

			if len(resp.Errors) > 0 {
				// NOTE: AWS SDK s3.Error does not implement error interface which
				// is pretty intensely sad, so we have to do away with this for now.
				errs := make([]error, 0, len(resp.Errors))
				for _, err := range resp.Errors {
					errs = append(errs, errors.New(err.String()))
				}
				return errors.Join(errs...)
			}
		}
		// NOTE: we don't want to reallocate
		// the slice so we simply "reset" it
		s3Objects = s3Objects[:0]

		// resp.Contents must have at least one element or we would have returned not found
		listObjectsInput.StartAfter = resp.Contents[len(resp.Contents)-1].Key

		// from the s3 api docs, IsTruncated "specifies whether (true) or not (false) all of the results were returned"
		// if everything has been returned, break
		if resp.IsTruncated == nil || !*resp.IsTruncated {
			break
		}
	}

	return nil
}
