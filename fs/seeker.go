package fs

import (
	"fmt"
	"io"
)

type readSeekCloser struct {
	getReader func(start int64) (io.ReadCloser, error)
	size      int64
	offset    int64
	current   io.ReadCloser
}

func NewReadSeekCloser(getReadCloser func(int64) (io.ReadCloser, error), size int64) io.ReadSeekCloser {
	return &readSeekCloser{
		getReader: getReadCloser,
		size:      size,
		offset:    0,
		current:   nil,
	}
}

func (rs *readSeekCloser) Read(p []byte) (int, error) {
	if rs.offset >= rs.size {
		return 0, io.EOF
	}

	if rs.current == nil {
		reader, err := rs.getReader(rs.offset)
		if err != nil {
			return 0, err
		}
		rs.current = reader
	}

	n, err := rs.current.Read(p)
	rs.offset += int64(n)

	if err == io.EOF {
		rs.current.Close()
		rs.current = nil
		if rs.offset >= rs.size {
			err = io.EOF
		} else {
			err = nil
		}
	}

	return n, err
}

func (rs *readSeekCloser) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = rs.offset + offset
	case io.SeekEnd:
		newOffset = rs.size + offset
	default:
		return 0, fmt.Errorf("seek: invalid whence")
	}

	if newOffset < 0 {
		return 0, fmt.Errorf("seek: negative position")
	}
	if newOffset > rs.size {
		newOffset = rs.size
	}

	if rs.current != nil && newOffset != rs.offset {
		rs.current.Close()
		rs.current = nil
	}

	rs.offset = newOffset
	return newOffset, nil
}

func (rs *readSeekCloser) Close() error {
	if rs.current != nil {
		err := rs.current.Close()
		rs.current = nil
		return err
	}
	return nil
}
