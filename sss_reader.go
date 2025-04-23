package sss

import (
	"bytes"
	"context"
	"io"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
)

func (s *SSS) SignGet(path string, expires time.Duration) (string, error) {
	return s.presign(expires,
		func(c *s3.S3) *request.Request {
			req, _ := c.GetObjectRequest(&s3.GetObjectInput{
				Bucket: s.getBucket(),
				Key:    aws.String(s.s3Path(path)),
			})
			return req
		})
}

func (s *SSS) GetContent(ctx context.Context, path string) ([]byte, error) {
	reader, err := s.Reader(ctx, path)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(reader)
}

func (s *SSS) Reader(ctx context.Context, path string) (io.ReadCloser, error) {
	return s.ReaderWithOffset(ctx, path, 0)
}

func (s *SSS) ReaderWithOffset(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	getObjectInput := &s3.GetObjectInput{
		Bucket: s.getBucket(),
		Key:    aws.String(s.s3Path(path)),
	}
	if offset > 0 {
		getObjectInput.Range = aws.String("bytes=" + strconv.FormatInt(offset, 10) + "-")
	}
	resp, err := s.s3.GetObjectWithContext(ctx, getObjectInput)
	if err != nil {
		return nil, parseError(path, err)
	}
	return resp.Body, nil
}

func (s *SSS) ReaderWithOffsetAndLimit(ctx context.Context, path string, offset, limit int64) (io.ReadCloser, error) {
	if limit <= 0 {
		return io.NopCloser(bytes.NewBuffer(nil)), nil
	}
	getObjectInput := &s3.GetObjectInput{
		Bucket: s.getBucket(),
		Key:    aws.String(s.s3Path(path)),
	}
	if offset > 0 {
		getObjectInput.Range = aws.String("bytes=" + strconv.FormatInt(offset, 10) + "-" + strconv.FormatInt(offset+limit-1, 10))
	}
	resp, err := s.s3.GetObjectWithContext(ctx, getObjectInput)
	if err != nil {
		return nil, parseError(path, err)
	}
	return resp.Body, nil
}
