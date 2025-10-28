package fs

import (
	"context"
	"testing"

	"github.com/wzshiming/sss"
)

func TestNewFS(t *testing.T) {
	// Create a minimal SSS instance for testing
	// We can't fully initialize it without AWS credentials, but we can test the FS wrapper
	ctx := context.Background()
	s := &sss.SSS{} // Minimal instance
	dir := "/test/dir"

	fs := NewFS(ctx, s, dir)
	if fs == nil {
		t.Fatal("NewFS() returned nil")
	}

	// Verify it implements the expected interfaces
	_, ok := fs.(FS)
	if !ok {
		t.Error("NewFS() does not implement FS interface")
	}

	_, ok = fs.(ReadDirFS)
	if !ok {
		t.Error("NewFS() does not implement ReadDirFS interface")
	}

	_, ok = fs.(ReadFileFS)
	if !ok {
		t.Error("NewFS() does not implement ReadFileFS interface")
	}

	_, ok = fs.(StatFS)
	if !ok {
		t.Error("NewFS() does not implement StatFS interface")
	}

	_, ok = fs.(SubFS)
	if !ok {
		t.Error("NewFS() does not implement SubFS interface")
	}
}

func TestFileSystem_Open(t *testing.T) {
	ctx := context.Background()
	s := &sss.SSS{}
	dir := "/test/dir"

	fsys := &fileSystem{
		ctx: ctx,
		s:   s,
		dir: dir,
	}

	// Test opening a file
	file, err := fsys.Open("testfile.txt")
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if file == nil {
		t.Fatal("Open() returned nil file")
	}
}

func TestFileSystem_Sub(t *testing.T) {
	ctx := context.Background()
	s := &sss.SSS{}
	dir := "/test/dir"

	fsys := &fileSystem{
		ctx: ctx,
		s:   s,
		dir: dir,
	}

	// Test creating a sub filesystem
	subFS, err := fsys.Sub("subdir")
	if err != nil {
		t.Fatalf("Sub() error = %v", err)
	}
	if subFS == nil {
		t.Fatal("Sub() returned nil")
	}

	// Verify the sub filesystem has the correct directory
	subFsys, ok := subFS.(*fileSystem)
	if !ok {
		t.Fatal("Sub() returned wrong type")
	}
	if subFsys.dir != "/test/dir/subdir" {
		t.Errorf("Sub() dir = %v, want %v", subFsys.dir, "/test/dir/subdir")
	}
}
