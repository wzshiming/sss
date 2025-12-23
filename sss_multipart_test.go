package sss

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
)

func TestParts_Items(t *testing.T) {
	parts := []*s3.Part{
		{PartNumber: int64Ptr(1), Size: int64Ptr(100)},
		{PartNumber: int64Ptr(2), Size: int64Ptr(200)},
	}
	p := &Parts{parts: parts}
	got := p.Items()
	if len(got) != 2 {
		t.Errorf("Items() length = %v, want %v", len(got), 2)
	}
	if *got[0].PartNumber != 1 {
		t.Errorf("Items()[0].PartNumber = %v, want %v", *got[0].PartNumber, 1)
	}
}

func TestParts_Size(t *testing.T) {
	p := &Parts{size: 12345}
	got := p.Size()
	if got != 12345 {
		t.Errorf("Size() = %v, want %v", got, 12345)
	}
}

func TestParts_Count(t *testing.T) {
	parts := []*s3.Part{
		{PartNumber: int64Ptr(1)},
		{PartNumber: int64Ptr(2)},
		{PartNumber: int64Ptr(3)},
	}
	p := &Parts{parts: parts}
	got := p.Count()
	if got != 3 {
		t.Errorf("Count() = %v, want %v", got, 3)
	}
}

func TestParts_LastModified(t *testing.T) {
	now := time.Now()
	p := &Parts{lastModified: now}
	got := p.LastModified()
	if !got.Equal(now) {
		t.Errorf("LastModified() = %v, want %v", got, now)
	}
}

func TestMultipart_Key(t *testing.T) {
	m := &Multipart{key: "test/key"}
	got := m.Key()
	if got != "test/key" {
		t.Errorf("Key() = %v, want %v", got, "test/key")
	}
}

func TestMultipart_UploadID(t *testing.T) {
	m := &Multipart{uploadID: "test-upload-id"}
	got := m.UploadID()
	if got != "test-upload-id" {
		t.Errorf("UploadID() = %v, want %v", got, "test-upload-id")
	}
}

func TestMultipart_SetParts(t *testing.T) {
	parts := []*s3.Part{
		{PartNumber: int64Ptr(1), Size: int64Ptr(100)},
		{PartNumber: int64Ptr(2), Size: int64Ptr(200)},
	}
	m := &Multipart{}
	m.SetParts(parts)
	if len(m.parts) != 2 {
		t.Errorf("SetParts() parts length = %v, want %v", len(m.parts), 2)
	}
	if *m.parts[0].PartNumber != 1 {
		t.Errorf("SetParts() parts[0].PartNumber = %v, want %v", *m.parts[0].PartNumber, 1)
	}
}

func TestS3CompletedParts(t *testing.T) {
	parts := s3completedParts{
		{PartNumber: int64Ptr(3)},
		{PartNumber: int64Ptr(1)},
		{PartNumber: int64Ptr(2)},
	}

	if parts.Len() != 3 {
		t.Errorf("Len() = %v, want %v", parts.Len(), 3)
	}

	if !parts.Less(1, 0) { // part 1 (number 1) < part 0 (number 3)
		t.Errorf("Less(1, 0) should be true")
	}

	parts.Swap(0, 1)
	if *parts[0].PartNumber != 1 {
		t.Errorf("After Swap, parts[0].PartNumber = %v, want %v", *parts[0].PartNumber, 1)
	}
	if *parts[1].PartNumber != 3 {
		t.Errorf("After Swap, parts[1].PartNumber = %v, want %v", *parts[1].PartNumber, 3)
	}
}

func TestS3Parts(t *testing.T) {
	parts := s3parts{
		{PartNumber: int64Ptr(3)},
		{PartNumber: int64Ptr(1)},
		{PartNumber: int64Ptr(2)},
	}

	if parts.Len() != 3 {
		t.Errorf("Len() = %v, want %v", parts.Len(), 3)
	}

	if !parts.Less(1, 0) { // part 1 (number 1) < part 0 (number 3)
		t.Errorf("Less(1, 0) should be true")
	}

	parts.Swap(0, 1)
	if *parts[0].PartNumber != 1 {
		t.Errorf("After Swap, parts[0].PartNumber = %v, want %v", *parts[0].PartNumber, 1)
	}
	if *parts[1].PartNumber != 3 {
		t.Errorf("After Swap, parts[1].PartNumber = %v, want %v", *parts[1].PartNumber, 3)
	}
}

// Helper function for creating int64 pointers
func int64Ptr(i int64) *int64 {
	return &i
}
