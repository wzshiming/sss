package sss

import (
	"io/fs"
	"testing"
	"time"
)

func TestFileInfo_Name(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected string
	}{
		{
			name:     "simple file",
			path:     "/path/to/file.txt",
			expected: "file.txt",
		},
		{
			name:     "root file",
			path:     "/file.txt",
			expected: "file.txt",
		},
		{
			name:     "no extension",
			path:     "/path/to/file",
			expected: "file",
		},
		{
			name:     "just filename",
			path:     "file.txt",
			expected: "file.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fi := fileInfo{path: tt.path}
			got := fi.Name()
			if got != tt.expected {
				t.Errorf("Name() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestFileInfo_Path(t *testing.T) {
	fi := fileInfo{path: "/path/to/file.txt"}
	got := fi.Path()
	if got != "/path/to/file.txt" {
		t.Errorf("Path() = %v, want %v", got, "/path/to/file.txt")
	}
}

func TestFileInfo_Size(t *testing.T) {
	fi := fileInfo{size: 1024}
	got := fi.Size()
	if got != 1024 {
		t.Errorf("Size() = %v, want %v", got, 1024)
	}
}

func TestFileInfo_ModTime(t *testing.T) {
	now := time.Now()
	fi := fileInfo{modTime: now}
	got := fi.ModTime()
	if !got.Equal(now) {
		t.Errorf("ModTime() = %v, want %v", got, now)
	}
}

func TestFileInfo_IsDir(t *testing.T) {
	tests := []struct {
		name  string
		isDir bool
	}{
		{
			name:  "is directory",
			isDir: true,
		},
		{
			name:  "is file",
			isDir: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fi := fileInfo{isDir: tt.isDir}
			got := fi.IsDir()
			if got != tt.isDir {
				t.Errorf("IsDir() = %v, want %v", got, tt.isDir)
			}
		})
	}
}

func TestFileInfo_Mode(t *testing.T) {
	tests := []struct {
		name     string
		isDir    bool
		expected fs.FileMode
	}{
		{
			name:     "directory mode",
			isDir:    true,
			expected: fs.ModeDir | 0755,
		},
		{
			name:     "file mode",
			isDir:    false,
			expected: 0644,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fi := fileInfo{isDir: tt.isDir}
			got := fi.Mode()
			if got != tt.expected {
				t.Errorf("Mode() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestFileInfo_Sys(t *testing.T) {
	expansion := FileInfoExpansion{
		ContentType: strPtr("text/plain"),
		ETag:        strPtr("test-etag"),
	}
	fi := fileInfo{sys: expansion}
	got := fi.Sys()
	
	sysExpansion, ok := got.(FileInfoExpansion)
	if !ok {
		t.Fatalf("Sys() returned wrong type")
	}
	
	if sysExpansion.ContentType == nil || *sysExpansion.ContentType != "text/plain" {
		t.Errorf("Sys() ContentType mismatch")
	}
	if sysExpansion.ETag == nil || *sysExpansion.ETag != "test-etag" {
		t.Errorf("Sys() ETag mismatch")
	}
}

// Helper function for creating string pointers
func strPtr(s string) *string {
	return &s
}
