package fs

import (
	"context"
	"io/fs"
	"path"

	"github.com/wzshiming/sss"
)

type (
	FS         = fs.FS
	ReadDirFS  = fs.ReadDirFS
	ReadFileFS = fs.ReadFileFS
	StatFS     = fs.StatFS
	SubFS      = fs.SubFS
)

var (
	_ FS         = (*fileSystem)(nil)
	_ ReadDirFS  = (*fileSystem)(nil)
	_ ReadFileFS = (*fileSystem)(nil)
	_ StatFS     = (*fileSystem)(nil)
	_ SubFS      = (*fileSystem)(nil)
)

func NewFS(ctx context.Context, s *sss.SSS, dir string) FS {
	return &fileSystem{
		ctx: ctx,
		s:   s,
		dir: dir,
	}
}

type fileSystem struct {
	ctx context.Context
	s   *sss.SSS
	dir string
}

func (s *fileSystem) Open(name string) (File, error) {
	path := path.Join(s.dir, name)
	return &file{
		ctx:  s.ctx,
		s:    s.s,
		path: path,
	}, nil
}

func (s *fileSystem) ReadDir(name string) ([]DirEntry, error) {
	p := path.Join(s.dir, name)
	var list []string
	err := s.s.List(s.ctx, p, func(fileInfo sss.FileInfo) bool {
		if fileInfo.IsDir() {
			list = append(list, fileInfo.Path())
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	des := make([]DirEntry, 0, len(list))
	for _, item := range list {
		des = append(des, fs.FileInfoToDirEntry(&file{
			ctx:  s.ctx,
			s:    s.s,
			path: item,
		}))
	}
	return des, nil
}

func (s *fileSystem) ReadFile(name string) ([]byte, error) {
	p := path.Join(s.dir, name)
	return s.s.GetContent(s.ctx, p)
}

func (s *fileSystem) Stat(name string) (FileInfo, error) {
	p := path.Join(s.dir, name)

	return &file{
		ctx:  s.ctx,
		s:    s.s,
		path: p,
	}, nil
}

func (s *fileSystem) Sub(dir string) (FS, error) {
	p := path.Join(s.dir, dir)
	return &fileSystem{
		ctx: s.ctx,
		s:   s.s,
		dir: p,
	}, nil
}
