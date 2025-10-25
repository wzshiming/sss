package sss

import (
	"context"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type Parts struct {
	size         int64
	lastModified time.Time
	parts        []s3types.Part
}

func (m *Parts) Items() []s3types.Part {
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

	parts []s3types.Part
}

func (m *Multipart) Key() string {
	return m.key
}

func (m *Multipart) UploadID() string {
	return m.uploadID
}

func (m *Multipart) SetParts(parts []s3types.Part) {
	m.parts = parts
}

func (m *Multipart) Resume(ctx context.Context) error {
	parts := make([]s3types.Part, 0, 16)
	listPartsInput := &s3.ListPartsInput{
		Bucket:   m.driver.getBucket(),
		Key:      aws.String(m.driver.s3Path(m.key)),
		UploadId: aws.String(m.uploadID),
	}

	paginator := s3.NewListPartsPaginator(m.driver.s3, listPartsInput)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return err
		}
		parts = append(parts, page.Parts...)
	}

	partMap := map[int32]s3types.Part{}
	var ignore s3types.Part

	for _, part := range parts {
		if existingPart, exists := partMap[*part.PartNumber]; exists {
			// Check if this is the ignore marker
			if existingPart.PartNumber == ignore.PartNumber && existingPart.ETag == ignore.ETag {
				continue
			}
			if *part.Size != *existingPart.Size || *part.ETag != *existingPart.ETag {
				partMap[*part.PartNumber] = ignore
			}
		} else {
			partMap[*part.PartNumber] = part
		}
	}

	uniqueParts := make([]s3types.Part, 0, len(partMap))
	for _, part := range partMap {
		// Skip ignore markers
		if part.PartNumber != ignore.PartNumber || part.ETag != ignore.ETag {
			uniqueParts = append(uniqueParts, part)
		}
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
	parts := make([]s3types.Part, 0, 16)
	var size int64
	var lastModified = time.Now()
	chunkSize := int(*m.parts[0].Size)
	for i := 0; i < len(m.parts); i++ {
		part := m.parts[i]
		if *part.PartNumber != int32(i+1) {
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
	_, err := m.driver.s3.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(m.driver.bucket),
		Key:      aws.String(m.key),
		UploadId: aws.String(m.uploadID),
	})
	return err
}

func (m *Multipart) SignUploadPart(partNumber int64, expires time.Duration) (string, error) {
	pn := aws.Int32(int32(partNumber))
	return m.driver.presign(expires,
		func(presignClient *s3.PresignClient) (*v4.PresignedHTTPRequest, error) {
			return presignClient.PresignUploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:     aws.String(m.driver.bucket),
				Key:        aws.String(m.key),
				PartNumber: pn,
				UploadId:   aws.String(m.uploadID),
			}, s3.WithPresignExpires(expires))
		})
}

func (m *Multipart) UploadPart(ctx context.Context, partNumber int64, body io.ReadSeeker) error {
	pn := aws.Int32(int32(partNumber))
	_, err := m.driver.s3.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     aws.String(m.driver.bucket),
		Key:        aws.String(m.key),
		PartNumber: pn,
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
		completedUploadedParts = append(completedUploadedParts, s3types.CompletedPart{
			ETag:       part.ETag,
			PartNumber: part.PartNumber,
		})
	}
	sort.Sort(completedUploadedParts)

	completeMultipartUploadInput := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(m.driver.bucket),
		Key:      aws.String(m.key),
		UploadId: aws.String(m.uploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: completedUploadedParts,
		},
	}

	_, err := m.driver.s3.CompleteMultipartUpload(ctx, completeMultipartUploadInput)
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

	paginator := s3.NewListMultipartUploadsPaginator(s.s3, listMultipartUploadsInput)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return parseError(path, err)
		}
		for _, multi := range page.Uploads {
			if !fun(&Multipart{
				uploadID: *multi.UploadId,
				key:      *multi.Key,
				driver:   s,
			}) {
				return nil
			}
		}
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

func (s *SSS) GetMultipartWithUploadID(path, uploadID string) *Multipart {
	key := s.s3Path(path)
	mps := &Multipart{
		driver:   s,
		key:      key,
		uploadID: uploadID,
	}
	return mps
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
	
	encryptMode := s.getEncryptionMode()
	storageClass := s.getStorageClass()
	
	input := &s3.CreateMultipartUploadInput{
		Bucket:      s.getBucket(),
		Key:         aws.String(key),
		ContentType: s.getContentType(),
		ACL:         s.getACL(),
	}
	
	if encryptMode != "" {
		input.ServerSideEncryption = encryptMode
	}
	if s.getSSEKMSKeyID() != nil {
		input.SSEKMSKeyId = s.getSSEKMSKeyID()
	}
	if storageClass != "" {
		input.StorageClass = storageClass
	}
	
	resp, err := s.s3.CreateMultipartUpload(ctx, input)
	if err != nil {
		return nil, err
	}

	return &Multipart{
		uploadID: *resp.UploadId,
		key:      *resp.Key,
		driver:   s,
	}, nil
}
