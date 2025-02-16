package sss

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Multipart struct {
	driver   *SSS
	path     string
	uploadId string

	size  int64
	count int

	lastModified time.Time
	parts        []*s3.Part
}

func (m *Multipart) Path() string {
	return m.path
}

func (m *Multipart) UploadID() string {
	return m.uploadId
}

func (m *Multipart) init(ctx context.Context) error {
	if m.parts != nil {
		return nil
	}
	parts := make([]*s3.Part, 0, 16)
	listPartsInput := &s3.ListPartsInput{
		Bucket:   m.driver.getBucket(),
		Key:      aws.String(m.driver.s3Path(m.path)),
		UploadId: aws.String(m.uploadId),
	}

	err := m.driver.s3.ListPartsPagesWithContext(ctx, listPartsInput, func(partsList *s3.ListPartsOutput, lastPage bool) bool {
		parts = append(parts, partsList.Parts...)
		return !lastPage
	})
	if err != nil {
		return err
	}

	var lastModified = time.Now()
	var chunkSize = m.driver.chunkSize
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

			if part.LastModified.Before(lastModified) {
				lastModified = *part.LastModified
			}
			size += *part.Size
		}
	}
	m.parts = parts
	m.size = size
	m.count = len(parts)
	m.lastModified = lastModified
	return nil
}

func (m *Multipart) Parts(ctx context.Context) ([]*s3.Part, error) {
	err := m.init(ctx)
	if err != nil {
		return nil, err
	}
	return m.parts, nil
}

func (m *Multipart) Size(ctx context.Context) (int64, error) {
	err := m.init(ctx)
	if err != nil {
		return 0, err
	}
	return m.size, nil
}

func (m *Multipart) Count(ctx context.Context) (int, error) {
	err := m.init(ctx)
	if err != nil {
		return 0, err
	}
	return m.count, nil
}

func (m *Multipart) LastModified(ctx context.Context) (time.Time, error) {
	err := m.init(ctx)
	if err != nil {
		return time.Time{}, err
	}
	return m.lastModified, nil
}

func (m *Multipart) Cancel(ctx context.Context) error {
	_, err := m.driver.s3.AbortMultipartUploadWithContext(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(m.driver.bucket),
		Key:      aws.String(m.path),
		UploadId: aws.String(m.uploadId),
	})
	return err
}

func (m *Multipart) Commit(ctx context.Context) error {
	parts, err := m.Parts(ctx)
	if err != nil {
		return err
	}
	completedUploadedParts := make(s3completedParts, len(parts))
	for i, part := range parts {
		completedUploadedParts[i] = &s3.CompletedPart{
			ETag:       part.ETag,
			PartNumber: part.PartNumber,
		}
	}
	sort.Sort(completedUploadedParts)

	completeMultipartUploadInput := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(m.driver.bucket),
		Key:      aws.String(m.path),
		UploadId: aws.String(m.uploadId),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedUploadedParts,
		},
	}

	_, err = m.driver.s3.CompleteMultipartUploadWithContext(ctx, completeMultipartUploadInput)
	if err != nil {
		return err
	}
	return nil
}

func (s *SSS) ListMultipart(ctx context.Context, path string, fun func(mp *Multipart) bool) error {
	key := s.s3Path(path)

	listMultipartUploadsInput := &s3.ListMultipartUploadsInput{
		Bucket: s.getBucket(),
		Prefix: aws.String(key),
	}

	err := s.s3.ListMultipartUploadsPagesWithContext(ctx, listMultipartUploadsInput, func(resp *s3.ListMultipartUploadsOutput, lastPage bool) bool {
		for _, multi := range resp.Uploads {
			if !fun(&Multipart{
				uploadId: *multi.UploadId,
				path:     *multi.Key,
				driver:   s,
			}) {
				return false
			}
		}
		return !lastPage
	})
	if err != nil {
		return parseError(path, err)
	}

	return nil
}

func (s *SSS) GetMultipart(ctx context.Context, path string) (*Multipart, error) {
	key := s.s3Path(path)

	var mps []*Multipart
	err := s.ListMultipart(ctx, key, func(mp *Multipart) bool {
		if mp.Path() == key {
			mps = append(mps, mp)
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	switch len(mps) {
	case 0:
		return nil, fmt.Errorf("not found part: %s", path)
	case 1:
		return mps[0], nil
	}

	sort.Slice(mps, func(i, j int) bool {
		a, _ := mps[i].Size(ctx)
		b, _ := mps[j].Size(ctx)
		return a > b
	})
	return mps[0], nil
}

func (s *SSS) GetMultipartByUploadID(ctx context.Context, path, uploadID string) (*Multipart, error) {
	key := s.s3Path(path)

	var mps *Multipart
	err := s.ListMultipart(ctx, key, func(mp *Multipart) bool {
		if mp.Path() == path && mp.UploadID() == uploadID {
			mps = mp
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	if mps == nil {
		return nil, fmt.Errorf("not found part with upload id: %s, %s", path, uploadID)
	}

	return mps, nil
}
