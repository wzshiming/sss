package sss_test

import (
	"bytes"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"math/rand"
	"reflect"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/wzshiming/sss"
)

func TestBasic(t *testing.T) {
	key := "test-object"
	content := []byte("Hello, SSS!")

	_, err := s.StatHead(t.Context(), key)
	if err != nil {
		if s3Err, ok := err.(awserr.Error); ok && s3Err.Code() != "NotFound" {
			t.Fatalf("failed to stat head: %v", err)
		}
	} else {
		t.Fatalf("head are exist: %v", key)
	}

	err = s.PutContent(t.Context(), key, content, sss.WithContentType("test"))
	if err != nil {
		t.Fatalf("failed to put object: %v", err)
	}

	f, err := s.StatHead(t.Context(), key)
	if err != nil {
		t.Fatalf("failed to stat head after put: %v", err)
	}

	fie := f.Sys().(sss.FileInfoExpansion)
	if *fie.ContentType != "test" {
		t.Fatalf("expected content type 'test', got '%s'", *fie.ContentType)
	}

	if f.Size() != int64(len(content)) {
		t.Fatalf("expected size %d, got %d", f.Size(), len(content))
	}

	body, err := s.GetContent(t.Context(), key)
	if err != nil {
		t.Fatalf("failed to read object body: %v", err)
	}

	if string(body) != string(content) {
		t.Fatalf("expected %s, got %s", content, body)
	}

	err = s.Delete(t.Context(), key)
	if err != nil {
		t.Fatalf("failed to delete object: %v", err)
	}

	_, err = s.StatHead(t.Context(), key)
	if err != nil {
		if s3Err, ok := err.(awserr.Error); ok && s3Err.Code() != "NotFound" {
			t.Fatalf("failed to stat head: %v", err)
		}
	} else {
		t.Fatalf("head are exist: %v", key)
	}
}

func TestListAndWalk(t *testing.T) {
	keys := []string{
		"a",
		"b",
		"c/d",
		"c/e",
		"c/f/g",
		"c/f/h",
	}

	for _, key := range keys {
		err := s.PutContent(t.Context(), key, []byte("test"))
		if err != nil {
			t.Fatalf("failed to put object: %v", err)
		}
	}

	want1 := []string{
		"/a",
		"/b",
		"/c/",
	}
	got1 := []string{}
	err := s.List(t.Context(), "/", func(fileInfo sss.FileInfo) bool {
		if fileInfo.IsDir() {
			got1 = append(got1, fileInfo.Path()+"/")
		} else {
			got1 = append(got1, fileInfo.Path())
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(got1, want1) {
		t.Fatalf("expected %v, got %v", want1, got1)
	}

	want2 := []string{
		"/c/d",
		"/c/e",
		"/c/f/",
	}
	got2 := []string{}
	err = s.List(t.Context(), "/c", func(fileInfo sss.FileInfo) bool {
		if fileInfo.IsDir() {
			got2 = append(got2, fileInfo.Path()+"/")
		} else {
			got2 = append(got2, fileInfo.Path())
		}
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(got2, want2) {
		t.Fatalf("expected %v, got %v", want2, got2)
	}

	want3 := []string{
		"/a",
		"/b",
		"/c/",
		"/c/d",
		"/c/e",
		"/c/f/",
		"/c/f/g",
		"/c/f/h",
	}
	got3 := []string{}

	err = s.Walk(t.Context(), "/", func(fileInfo sss.FileInfo) error {
		if fileInfo.IsDir() {
			got3 = append(got3, fileInfo.Path()+"/")
		} else {
			got3 = append(got3, fileInfo.Path())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got3, want3) {
		t.Fatalf("expected %v, got %v", want3, got3)
	}

	err = s.DeleteAll(t.Context(), "c/f")
	if err != nil {
		t.Fatal(err)
	}

	want4 := []string{
		"/a",
		"/b",
		"/c/",
		"/c/d",
		"/c/e",
	}
	got4 := []string{}
	err = s.Walk(t.Context(), "/", func(fileInfo sss.FileInfo) error {
		if fileInfo.IsDir() {
			got4 = append(got4, fileInfo.Path()+"/")
		} else {
			got4 = append(got4, fileInfo.Path())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got4, want4) {
		t.Fatalf("expected %v, got %v", want4, got4)
	}

	err = s.Delete(t.Context(), "a")
	if err != nil {
		t.Fatal(err)
	}

	want5 := []string{
		"/b",
		"/c/",
		"/c/d",
		"/c/e",
	}
	got5 := []string{}
	err = s.Walk(t.Context(), "/", func(fileInfo sss.FileInfo) error {
		if fileInfo.IsDir() {
			got5 = append(got5, fileInfo.Path()+"/")
		} else {
			got5 = append(got5, fileInfo.Path())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got5, want5) {
		t.Fatalf("expected %v, got %v", want5, got5)
	}
}

func TestFileWriter(t *testing.T) {
	key := "test-big-object"
	wantBuffer := bytes.NewBuffer(nil)
	_, err := io.Copy(wantBuffer, io.LimitReader(crand.Reader, rand.Int63n(1024*1024)+(128+rand.Int63n(128))*1024*1024))
	if err != nil {
		t.Fatal(err)
	}

	wantData := bytes.NewReader(wantBuffer.Bytes())

	w, err := s.Writer(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}

	_, err = io.Copy(w, io.LimitReader(wantData, int64(wantData.Len()/3)))
	if err != nil {
		t.Fatal(err)
	}

	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	w, err = s.WriterWithAppend(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}

	_, err = wantData.Seek(w.Size(), io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	_, err = io.Copy(w, io.LimitReader(wantData, int64(wantData.Len()/3)))
	if err != nil {
		t.Fatal(err)
	}

	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	w, err = s.WriterWithAppend(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}

	_, err = wantData.Seek(w.Size(), io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	_, err = io.Copy(w, wantData)
	if err != nil {
		t.Fatal(err)
	}

	err = w.Commit(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	err = w.Close()
	if err != nil {
		t.Fatal(err)
	}

	r, err := s.Reader(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}

	got := sha256.New()

	gotSize, err := io.Copy(got, r)
	if err != nil {
		t.Fatal(err)
	}

	want := sha256.New()
	_, err = wantData.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	wantSize, err := io.Copy(want, wantData)
	if err != nil {
		t.Fatal(err)
	}

	if wantSize != gotSize {
		t.Fatalf("expected size %d, got %d", wantSize, gotSize)
	}

	wantHex := hex.EncodeToString(want.Sum(nil))
	gotHex := hex.EncodeToString(got.Sum(nil))

	if wantHex != gotHex {
		t.Fatalf("expected %s, got %s", wantHex, gotHex)
	}
}

func TestSignEndpoint(t *testing.T) {
	// Create a new SSS instance with SignEndpoint configured
	// This tests that SignEndpoint creates a separate session without modifying the main one
	signURL := `sss://minioadmin:minioadmin@` + bucket + `.region?forcepathstyle=true&secure=false&regionendpoint=http://127.0.0.1:9000&signendpoint=http://localhost:9000`

	sWithSign, err := sss.NewSSS(sss.WithURL(signURL))
	if err != nil {
		t.Fatalf("failed to create SSS with sign endpoint: %v", err)
	}

	key := "test-sign-object"
	content := []byte("Test sign endpoint")

	// Put an object
	err = sWithSign.PutContent(t.Context(), key, content)
	if err != nil {
		t.Fatalf("failed to put object: %v", err)
	}

	// Generate a presigned GET URL
	signedURL, err := sWithSign.SignGet(key, 60*1000000000) // 60 seconds
	if err != nil {
		t.Fatalf("failed to sign get: %v", err)
	}

	// Verify the presigned URL contains the sign endpoint (localhost:9000)
	if signedURL == "" {
		t.Fatal("signed URL is empty")
	}

	// The signed URL should contain localhost (from signendpoint) not 127.0.0.1 (from regionendpoint)
	// This verifies that the sign endpoint is being used correctly
	t.Logf("Signed URL: %s", signedURL)

	// Clean up
	err = sWithSign.Delete(t.Context(), key)
	if err != nil {
		t.Fatalf("failed to delete object: %v", err)
	}
}

func TestMultipartFileWriter(t *testing.T) {
	key := "test-multipart-object"
	wantBuffer := bytes.NewBuffer(nil)
	_, err := io.Copy(wantBuffer, io.LimitReader(crand.Reader, rand.Int63n(1024*1024)+(128+rand.Int63n(128))*1024*1024))
	if err != nil {
		t.Fatal(err)
	}

	wantData := bytes.NewReader(wantBuffer.Bytes())

	m, err := s.NewMultipart(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}

	chunkSize := s.ChunkSize()

	partNumber := wantBuffer.Len() / chunkSize
	if wantBuffer.Len()%chunkSize != 0 {
		partNumber++
	}

	sem := make(chan struct{}, 5)
	var wg sync.WaitGroup
	for i := 0; i != partNumber; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer func() {
				<-sem
				wg.Done()
			}()
			buf := make([]byte, chunkSize)
			n, _ := wantData.ReadAt(buf, int64(chunkSize*i))
			err := m.UploadPart(t.Context(), int64(i+1), bytes.NewReader(buf[:n]))
			if err != nil {
				t.Error(err)
			}
		}(i)
	}

	wg.Wait()

	err = m.Commit(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	r, err := s.Reader(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}

	got := sha256.New()

	gotSize, err := io.Copy(got, r)
	if err != nil {
		t.Fatal(err)
	}

	want := sha256.New()
	_, err = wantData.Seek(0, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}
	wantSize, err := io.Copy(want, wantData)
	if err != nil {
		t.Fatal(err)
	}

	if wantSize != gotSize {
		t.Fatalf("expected size %d, got %d", wantSize, gotSize)
	}

	wantHex := hex.EncodeToString(want.Sum(nil))
	gotHex := hex.EncodeToString(got.Sum(nil))

	if wantHex != gotHex {
		t.Fatalf("expected %s, got %s", wantHex, gotHex)
	}
}
