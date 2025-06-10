package sss

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
)

func (s *SSS) SignPut(path string, expires time.Duration) (string, error) {
	return s.presign(expires,
		func(c *s3.S3) *request.Request {
			req, _ := c.PutObjectRequest(&s3.PutObjectInput{
				Bucket: s.getBucket(),
				Key:    aws.String(s.s3Path(path)),
			})
			return req
		})
}

type writerOption struct {
	SHA256 string
}

type WriterOptions func(*writerOption)

func WithSHA256(sha256 string) WriterOptions {
	return func(o *writerOption) {
		_, err := base64.URLEncoding.DecodeString(sha256)
		if err == nil {
			o.SHA256 = sha256
			return
		}
		data, err := hex.DecodeString(sha256)
		if err == nil {
			o.SHA256 = base64.URLEncoding.EncodeToString(data)
			return
		}

		log.Printf("unknown checksum sha256 %q, ignore it", sha256)
	}
}

func (s *SSS) PutContent(ctx context.Context, path string, contents []byte, opts ...WriterOptions) error {
	putObjectInput := &s3.PutObjectInput{
		Bucket:               s.getBucket(),
		Key:                  aws.String(s.s3Path(path)),
		ContentType:          s.getContentType(),
		ACL:                  s.getACL(),
		ServerSideEncryption: s.getEncryptionMode(),
		SSEKMSKeyId:          s.getSSEKMSKeyID(),
		StorageClass:         s.getStorageClass(),
		Body:                 bytes.NewReader(contents),
	}

	var o writerOption
	for _, opt := range opts {
		opt(&o)
	}
	if o.SHA256 != "" {
		putObjectInput.ChecksumSHA256 = aws.String(o.SHA256)
	}
	_, err := s.s3.PutObjectWithContext(ctx, putObjectInput)
	return parseError(path, err)
}

func (s *SSS) Writer(ctx context.Context, path string, opts ...WriterOptions) (FileWriter, error) {
	key := s.s3Path(path)

	var o writerOption
	for _, opt := range opts {
		opt(&o)
	}

	resp, err := s.s3.CreateMultipartUploadWithContext(ctx, &s3.CreateMultipartUploadInput{
		Bucket:               s.getBucket(),
		Key:                  aws.String(key),
		ContentType:          s.getContentType(),
		ACL:                  s.getACL(),
		ServerSideEncryption: s.getEncryptionMode(),
		SSEKMSKeyId:          s.getSSEKMSKeyID(),
		StorageClass:         s.getStorageClass(),
	})
	if err != nil {
		return nil, err
	}
	return s.newWriter(ctx, key, *resp.UploadId, nil, o), nil
}

func (s *SSS) WriterWithAppend(ctx context.Context, path string, opts ...WriterOptions) (FileWriter, error) {
	key := s.s3Path(path)

	var o writerOption
	for _, opt := range opts {
		opt(&o)
	}

	m, err := s.GetMultipart(ctx, path)
	if err != nil {
		return nil, err
	}

	if m.UploadID() == "" {
		return nil, fmt.Errorf("multipart upload not found: %s", path)
	}

	parts, err := m.OrderParts(ctx)
	if err != nil {
		return nil, err
	}
	return s.newWriter(ctx, key, m.UploadID(), parts.Items(), o), nil
}

func (s *SSS) WriterWithAppendByUploadID(ctx context.Context, path, uploadID string, opts ...WriterOptions) (FileWriter, error) {
	key := s.s3Path(path)

	var o writerOption
	for _, opt := range opts {
		opt(&o)
	}

	m := s.GetMultipartWithUploadID(path, uploadID)
	parts, err := m.OrderParts(ctx)
	if err != nil {
		return nil, err
	}
	return s.newWriter(ctx, key, uploadID, parts.Items(), o), nil
}

type FileWriter interface {
	io.WriteCloser
	Size() int64
	Cancel(ctx context.Context) error
	Commit(ctx context.Context) error
}

type writer struct {
	ctx       context.Context
	driver    *SSS
	key       string
	uploadID  string
	parts     []*s3.Part
	size      int64
	buf       *bytes.Buffer
	chunkSize int
	closed    bool
	committed bool
	cancelled bool
	opt       writerOption
}

func (s *SSS) newWriter(ctx context.Context, key, uploadID string, parts []*s3.Part, opt writerOption) FileWriter {
	var chunkSize = s.chunkSize
	var size int64
	if len(parts) > 0 {
		sort.Sort(s3parts(parts))
		chunkSize = int(*parts[0].Size)
		for i := 0; i < len(parts); i++ {
			part := parts[i]
			if *part.PartNumber != int64(i+1) {
				parts = parts[:i]
				break
			}
			if *part.Size != int64(chunkSize) {
				parts = parts[:i]
				break
			}
			size += *part.Size
		}
	}

	return &writer{
		ctx:       ctx,
		driver:    s,
		key:       key,
		uploadID:  uploadID,
		parts:     parts,
		size:      size,
		chunkSize: chunkSize,
		opt:       opt,
		buf:       s.pool.Get().(*bytes.Buffer),
	}
}

func (w *writer) Write(p []byte) (int, error) {
	if err := w.done(); err != nil {
		return 0, err
	}

	n, _ := w.buf.Write(p)
	for w.buf.Len() >= w.chunkSize {
		if err := w.flush(); err != nil {
			return 0, fmt.Errorf("flush: %w", err)
		}
	}
	return n, nil
}

func (w *writer) Size() int64 {
	return w.size
}

func (w *writer) Close() error {
	if w.closed {
		return fmt.Errorf("already closed")
	}

	w.closed = true

	defer w.releaseBuffer()

	return nil
}

// releaseBuffer resets the buffer and returns it to the pool.
func (w *writer) releaseBuffer() {
	w.buf.Reset()
	w.driver.pool.Put(w.buf)
}

// Cancel aborts the multipart upload and closes the writer.
func (w *writer) Cancel(ctx context.Context) error {
	if err := w.done(); err != nil {
		return err
	}

	w.cancelled = true
	_, err := w.driver.s3.AbortMultipartUploadWithContext(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(w.driver.bucket),
		Key:      aws.String(w.key),
		UploadId: aws.String(w.uploadID),
	})
	return err
}

// Commit flushes any remaining data in the buffer and completes the multipart upload.
func (w *writer) Commit(ctx context.Context) error {
	if err := w.done(); err != nil {
		return err
	}

	if err := w.flush(); err != nil {
		return err
	}

	w.committed = true

	if len(w.parts) == 0 {
		return fmt.Errorf("no parts to commit")
	}

	completedUploadedParts := make(s3completedParts, len(w.parts))
	for i, part := range w.parts {
		completedUploadedParts[i] = &s3.CompletedPart{
			ETag:       part.ETag,
			PartNumber: part.PartNumber,
		}
	}
	sort.Sort(completedUploadedParts)

	completeMultipartUploadInput := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(w.driver.bucket),
		Key:      aws.String(w.key),
		UploadId: aws.String(w.uploadID),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedUploadedParts,
		},
	}

	if w.opt.SHA256 != "" {
		completeMultipartUploadInput.ChecksumSHA256 = aws.String(w.opt.SHA256)
	}

	_, err := w.driver.s3.CompleteMultipartUploadWithContext(ctx, completeMultipartUploadInput)
	if err != nil {
		return err
	}
	return nil
}

func (w *writer) flush() error {
	if w.buf.Len() == 0 {
		return nil
	}

	r := bytes.NewReader(w.buf.Next(w.chunkSize))

	partSize := r.Len()
	partNumber := aws.Int64(int64(len(w.parts)) + 1)

	resp, err := w.driver.s3.UploadPartWithContext(w.ctx, &s3.UploadPartInput{
		Bucket:     aws.String(w.driver.bucket),
		Key:        aws.String(w.key),
		PartNumber: partNumber,
		UploadId:   aws.String(w.uploadID),
		Body:       r,
	})
	if err != nil {
		return fmt.Errorf("upload part: %w", err)
	}

	w.parts = append(w.parts, &s3.Part{
		ETag:       resp.ETag,
		PartNumber: partNumber,
		Size:       aws.Int64(int64(partSize)),
	})

	w.size += int64(partSize)

	return nil
}

func (w *writer) done() error {
	switch {
	case w.closed:
		return fmt.Errorf("already closed")
	case w.committed:
		return fmt.Errorf("already committed")
	case w.cancelled:
		return fmt.Errorf("already cancelled")
	}
	return nil
}
