package sss

import (
	"context"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Parts struct {
	size         int64
	lastModified time.Time
	parts        []*s3.Part
}

func (m *Parts) init() error {

	var lastModified = time.Now()

	var size int64
	if len(m.parts) > 0 {
		sort.Sort(s3parts(m.parts))
		chunkSize := int(*m.parts[0].Size)
		for i := 0; i < len(m.parts); i++ {
			part := m.parts[i]
			if *part.PartNumber != int64(i+1) {
				m.parts = m.parts[:i]
				break
			}
			if *part.Size != int64(chunkSize) {
				m.parts = m.parts[:i]
				break
			}

			if part.LastModified.Before(lastModified) {
				lastModified = *part.LastModified
			}
			size += *part.Size
		}
	}
	m.size = size
	m.lastModified = lastModified
	return nil
}

func (m *Parts) Items() []*s3.Part {
	return m.parts
}

func (m *Parts) Size() int64 {
	return m.size
}

func (m *Parts) Count() int {
	return len(m.parts)
}

func (p *Parts) LastModified() time.Time {
	return p.lastModified
}

type Multipart struct {
	driver   *SSS
	key      string
	uploadID string

	parts []*s3.Part
}

func (m *Multipart) Key() string {
	return m.key
}

func (m *Multipart) UploadID() string {
	return m.uploadID
}

func (m *Multipart) Resume(ctx context.Context) error {
	parts := make([]*s3.Part, 0, 16)
	listPartsInput := &s3.ListPartsInput{
		Bucket:   m.driver.getBucket(),
		Key:      aws.String(m.driver.s3Path(m.key)),
		UploadId: aws.String(m.uploadID),
	}

	err := m.driver.s3.ListPartsPagesWithContext(ctx, listPartsInput, func(partsList *s3.ListPartsOutput, lastPage bool) bool {
		parts = append(parts, partsList.Parts...)
		return !lastPage
	})
	if err != nil {
		return err
	}

	partMap := map[int64]*s3.Part{}
	ignore := &s3.Part{}

	for _, part := range parts {
		if existingPart, exists := partMap[*part.PartNumber]; exists {
			if existingPart == ignore {
				continue
			}
			if *part.Size != *existingPart.Size || *part.ETag != *existingPart.ETag {
				partMap[*part.PartNumber] = ignore
			}
		} else {
			partMap[*part.PartNumber] = part
		}
	}

	uniqueParts := make([]*s3.Part, 0, len(partMap))
	for _, part := range partMap {
		uniqueParts = append(uniqueParts, part)
	}

	sort.Sort(s3parts(uniqueParts))
	m.parts = uniqueParts

	return nil
}

func (m *Multipart) AllParts(ctx context.Context) (*Parts, error) {
	if len(m.parts) == 0 {
		err := m.Resume(ctx)
		if err != nil {
			return nil, err
		}
	}

	var size int64
	var lastModified = time.Now()
	for i := 0; i < len(m.parts); i++ {
		part := m.parts[i]
		if part.LastModified.Before(lastModified) {
			lastModified = *part.LastModified
		}
		size += *part.Size
	}
	return &Parts{
		size:         size,
		parts:        m.parts,
		lastModified: lastModified,
	}, nil
}

func (m *Multipart) OrderParts(ctx context.Context) (*Parts, error) {
	if len(m.parts) == 0 {
		err := m.Resume(ctx)
		if err != nil {
			return nil, err
		}
	}

	if len(m.parts) == 0 {
		return &Parts{}, nil
	}
	parts := make([]*s3.Part, 0, 16)
	var size int64
	var lastModified = time.Now()
	chunkSize := int(*m.parts[0].Size)
	for i := 0; i < len(m.parts); i++ {
		part := m.parts[i]
		if *part.PartNumber != int64(i+1) {
			break
		}
		if *part.Size != int64(chunkSize) {
			break
		}

		if part.LastModified.Before(lastModified) {
			lastModified = *part.LastModified
		}
		parts = append(parts, part)
		size += *part.Size
	}
	return &Parts{
		size:         size,
		parts:        parts,
		lastModified: lastModified,
	}, nil
}

func (m *Multipart) Cancel(ctx context.Context) error {
	_, err := m.driver.s3.AbortMultipartUploadWithContext(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(m.driver.bucket),
		Key:      aws.String(m.key),
		UploadId: aws.String(m.uploadID),
	})
	return err
}

func (m *Multipart) SignUploadPart(partNumber int64, expires time.Duration) (string, error) {
	req, _ := m.driver.s3.UploadPartRequest(&s3.UploadPartInput{
		Bucket:     aws.String(m.driver.bucket),
		Key:        aws.String(m.key),
		PartNumber: &partNumber,
		UploadId:   aws.String(m.uploadID),
	})
	return req.Presign(expires)
}

func (m *Multipart) UploadPart(ctx context.Context, partNumber int64, body io.ReadSeeker) error {
	_, err := m.driver.s3.UploadPartWithContext(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(m.driver.bucket),
		Key:        aws.String(m.key),
		PartNumber: &partNumber,
		UploadId:   aws.String(m.uploadID),
		Body:       body,
	})
	if err != nil {
		return fmt.Errorf("upload part: %w", err)
	}
	return nil
}

func (m *Multipart) Commit(ctx context.Context) error {
	if len(m.parts) == 0 {
		err := m.Resume(ctx)
		if err != nil {
			return err
		}
	}
	if len(m.parts) == 0 {
		return fmt.Errorf("no parts commit")
	}
	parts := m.parts
	completedUploadedParts := make(s3completedParts, 0, len(parts))
	for _, part := range parts {
		completedUploadedParts = append(completedUploadedParts, &s3.CompletedPart{
			ETag:       part.ETag,
			PartNumber: part.PartNumber,
		})
	}
	sort.Sort(completedUploadedParts)

	completeMultipartUploadInput := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(m.driver.bucket),
		Key:      aws.String(m.key),
		UploadId: aws.String(m.uploadID),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedUploadedParts,
		},
	}

	_, err := m.driver.s3.CompleteMultipartUploadWithContext(ctx, completeMultipartUploadInput)
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
				uploadID: *multi.UploadId,
				key:      *multi.Key,
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
		if mp.Key() == key {
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
		var a, b int64
		p, err := mps[i].OrderParts(ctx)
		if err == nil {
			a = p.Size()
		}
		p, err = mps[j].OrderParts(ctx)
		if err == nil {
			b = p.Size()
		}
		return a > b
	})
	return mps[0], nil
}

func (s *SSS) GetMultipartByUploadID(ctx context.Context, path, uploadID string) (*Multipart, error) {
	key := s.s3Path(path)

	var mps *Multipart
	err := s.ListMultipart(ctx, key, func(mp *Multipart) bool {
		if mp.Key() == path && mp.UploadID() == uploadID {
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

func (s *SSS) NewMultipart(ctx context.Context, path string) (*Multipart, error) {
	key := s.s3Path(path)
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

	return &Multipart{
		uploadID: *resp.UploadId,
		key:      *resp.Key,
		driver:   s,
	}, nil
}
