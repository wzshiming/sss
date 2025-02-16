package sss

import (
	"context"
	"errors"
	"path"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// ErrSkipDir is used as a return value from onFileFunc to indicate that
// the directory named in the call is to be skipped. It is not returned
// as an error by any function.
var ErrSkipDir = errors.New("skip this directory")

// ErrFilledBuffer is used as a return value from onFileFunc to indicate
// that the requested number of entries has been reached and the walk can
// stop.
var ErrFilledBuffer = errors.New("we have enough entries")

// WalkFn is called once per file by Walk
type WalkFn func(fileInfo FileInfo) error

// walkOptions provides options to the walk function that may adjust its behaviour
type walkOptions struct {
	// If StartAfterHint is set, the walk may start with the first item lexographically
	// after the hint, but it is not guaranteed and drivers may start the walk from the path.
	StartAfterHint string
}

func WithStartAfterHint(startAfterHint string) func(*walkOptions) {
	return func(s *walkOptions) {
		s.StartAfterHint = startAfterHint
	}
}

// Walk traverses a filesystem defined within driver, starting
// from the given path, calling f on each file
func (s *SSS) Walk(ctx context.Context, from string, f WalkFn, options ...func(*walkOptions)) error {
	walkOptions := &walkOptions{}
	for _, o := range options {
		o(walkOptions)
	}

	var objectCount int64
	if err := s.doWalk(ctx, &objectCount, from, walkOptions.StartAfterHint, f); err != nil {
		return err
	}

	return nil
}

func (s *SSS) doWalk(ctx context.Context, objectCount *int64, from, startAfter string, f WalkFn) error {
	var (
		retError error
		// the most recent directory walked for de-duping
		prevDir string
		// the most recent skip directory to avoid walking over undesirable files
		prevSkipDir string
	)
	prevDir = from

	path := from
	if !strings.HasSuffix(path, "/") {
		path = path + "/"
	}

	prefix := ""
	if s.s3Path("") == "" {
		prefix = "/"
	}

	listObjectsInput := &s3.ListObjectsV2Input{
		Bucket:     s.getBucket(),
		Prefix:     aws.String(s.s3Path(path)),
		MaxKeys:    aws.Int64(listMax),
		StartAfter: aws.String(s.s3Path(startAfter)),
	}

	// When the "delimiter" argument is omitted, the S3 list API will list all objects in the bucket
	// recursively, omitting directory paths. Objects are listed in sorted, depth-first order so we
	// can infer all the directories by comparing each object path to the last one we saw.
	// See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/ListingKeysUsingAPIs.html

	// With files returned in sorted depth-first order, directories are inferred in the same order.
	// ErrSkipDir is handled by explicitly skipping over any files under the skipped directory. This may be sub-optimal
	// for extreme edge cases but for the general use case in a registry, this is orders of magnitude
	// faster than a more explicit recursive implementation.
	listObjectErr := s.s3.ListObjectsV2PagesWithContext(ctx, listObjectsInput, func(objects *s3.ListObjectsV2Output, lastPage bool) bool {
		walkInfos := make([]fileInfo, 0, len(objects.Contents))

		for _, file := range objects.Contents {
			filePath := strings.Replace(*file.Key, s.s3Path(""), prefix, 1)

			// get a list of all inferred directories between the previous directory and this file
			dirs := directoryDiff(prevDir, filePath)
			for _, dir := range dirs {
				walkInfos = append(walkInfos, fileInfo{
					isDir: true,
					path:  dir,
				})
				prevDir = dir
			}

			if strings.HasSuffix(filePath, "/") {
				continue
			}

			walkInfos = append(walkInfos, fileInfo{
				isDir:   false,
				size:    *file.Size,
				modTime: *file.LastModified,
				path:    filePath,
			})
		}

		for _, walkInfo := range walkInfos {
			// skip any results under the last skip directory
			if prevSkipDir != "" && strings.HasPrefix(walkInfo.Path(), prevSkipDir) {
				continue
			}

			err := f(walkInfo)
			*objectCount++

			if err != nil {
				if err == ErrSkipDir {
					prevSkipDir = walkInfo.Path()
					continue
				}
				if err == ErrFilledBuffer {
					return false
				}
				retError = err
				return false
			}
		}
		return !lastPage
	})

	if retError != nil {
		return retError
	}

	if listObjectErr != nil {
		return listObjectErr
	}

	return nil
}

// directoryDiff finds all directories that are not in common between
// the previous and current paths in sorted order.
//
// # Examples
//
//	directoryDiff("/path/to/folder", "/path/to/folder/folder/file")
//	// => [ "/path/to/folder/folder" ]
//
//	directoryDiff("/path/to/folder/folder1", "/path/to/folder/folder2/file")
//	// => [ "/path/to/folder/folder2" ]
//
//	directoryDiff("/path/to/folder/folder1/file", "/path/to/folder/folder2/file")
//	// => [ "/path/to/folder/folder2" ]
//
//	directoryDiff("/path/to/folder/folder1/file", "/path/to/folder/folder2/folder1/file")
//	// => [ "/path/to/folder/folder2", "/path/to/folder/folder2/folder1" ]
//
//	directoryDiff("/", "/path/to/folder/folder/file")
//	// => [ "/path", "/path/to", "/path/to/folder", "/path/to/folder/folder" ]
func directoryDiff(prev, current string) []string {
	var paths []string

	if prev == "" || current == "" {
		return paths
	}

	parent := current
	for {
		parent = path.Dir(parent)
		if parent == "/" || parent == prev || strings.HasPrefix(prev+"/", parent+"/") {
			break
		}
		paths = append(paths, parent)
	}
	slices.Reverse(paths)
	return paths
}
