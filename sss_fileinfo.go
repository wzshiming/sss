package sss

import (
	"time"
)

// FileInfo returns information about a given path.
type FileInfo interface {
	// Path provides the full path of the target of this file info.
	Path() string

	// Size returns current length in bytes of the file.
	Size() int64

	// ModTime returns the modification time for the file.
	ModTime() time.Time

	// IsDir returns true if the path is a directory.
	IsDir() bool
}

type fileInfo struct {
	path    string
	size    int64
	modTime time.Time
	isDir   bool
}

func (fi fileInfo) Path() string {
	return fi.path
}

func (fi fileInfo) Size() int64 {
	return fi.size
}

func (fi fileInfo) ModTime() time.Time {
	return fi.modTime
}

func (fi fileInfo) IsDir() bool {
	return fi.isDir
}
