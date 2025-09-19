package sss

import (
	"io/fs"
	"path"
	"time"
)

// FileInfo returns information about a given path.
type FileInfo interface {
	// Path provides the full path of the target of this file info.
	Path() string

	fs.FileInfo
}

type FileInfoExpansion struct {
	ContentType  *string
	AcceptRanges *string
	ETag         *string
	Expires      *string
}

type fileInfo struct {
	path    string
	size    int64
	modTime time.Time
	isDir   bool

	sys FileInfoExpansion
}

func (fi fileInfo) Name() string {
	_, file := path.Split(fi.path)
	return file
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

func (fi fileInfo) Mode() fs.FileMode {
	if fi.isDir {
		return fs.ModeDir | 0755
	}
	return 0644
}

func (fi fileInfo) Sys() any {
	return fi.sys
}
