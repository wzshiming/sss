package fs

import (
	"context"
	"io"
	"io/fs"
	"log/slog"
	"path"
	"time"

	"github.com/wzshiming/sss"
)

type (
	File        = fs.File
	FileInfo    = fs.FileInfo
	FileMode    = fs.FileMode
	DirEntry    = fs.DirEntry
	ReadDirFile = fs.ReadDirFile
)

var (
	_ File        = (*file)(nil)
	_ FileInfo    = (*file)(nil)
	_ DirEntry    = (*file)(nil)
	_ ReadDirFile = (*file)(nil)
)

type file struct {
	ctx  context.Context
	s    *sss.SSS
	path string

	stat sss.FileInfo

	readSeekCloser io.ReadSeekCloser
}

func (s *file) Stat() (FileInfo, error) {
	return s, nil
}

func (s *file) init() {
	if s.readSeekCloser == nil {
		s.readSeekCloser = NewReadSeekCloser(func(start int64) (io.ReadCloser, error) {
			return s.s.ReaderWithOffset(s.ctx, s.path, start)
		}, s.Size())
	}
}

func (s *file) Seek(offset int64, whence int) (int64, error) {
	s.init()
	return s.readSeekCloser.Seek(offset, whence)
}

func (s *file) Read(p []byte) (int, error) {
	s.init()
	return s.readSeekCloser.Read(p)
}

func (s *file) Close() error {
	if s.readSeekCloser == nil {
		return nil
	}
	s.init()
	return s.readSeekCloser.Close()
}

func (s *file) Name() string {
	return path.Base(s.path)
}

func (s *file) Mode() FileMode {
	return 0666
}

func (s *file) initStat() {
	if s.stat == nil {
		var err error
		s.stat, err = s.s.Stat(s.ctx, s.path)
		if err != nil {
			slog.Error("stat file", "path", s.path, "error", err)
		}
	}
}

func (s *file) Size() int64 {
	s.initStat()
	return s.stat.Size()
}

func (s *file) ModTime() time.Time {
	s.initStat()
	return s.stat.ModTime()
}

func (s *file) IsDir() bool {
	s.initStat()
	return s.stat.IsDir()
}

func (s *file) Sys() any {
	return nil
}

func (s *file) Type() FileMode {
	if s.IsDir() {
		return fs.ModeDir
	}
	return 0
}

func (s *file) Info() (FileInfo, error) {
	return s, nil
}

func (s *file) ReadDir(n int) ([]DirEntry, error) {
	var list []DirEntry
	err := s.s.List(s.ctx, s.path, func(fileInfo sss.FileInfo) bool {
		if fileInfo.IsDir() {
			list = append(list, &file{
				ctx:  s.ctx,
				s:    s.s,
				path: fileInfo.Path(),
			})
		}
		if n > 0 && len(list) >= n {
			return false
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	return list, nil
}
