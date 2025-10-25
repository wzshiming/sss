package serve

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/wzshiming/sss"
)

// S3Serve implements S3-compatible API endpoints
type S3Serve struct {
	sss    *sss.SSS
	bucket string
}

// NewS3Serve creates a new S3-compatible server handler
func NewS3Serve(s *sss.SSS, bucket string) http.Handler {
	return &S3Serve{
		sss:    s,
		bucket: bucket,
	}
}

// ListBucketResult represents the XML response for ListBucket operation
type ListBucketResult struct {
	XMLName        xml.Name       `xml:"ListBucketResult"`
	Xmlns          string         `xml:"xmlns,attr"`
	Name           string         `xml:"Name"`
	Prefix         string         `xml:"Prefix"`
	Marker         string         `xml:"Marker"`
	MaxKeys        int            `xml:"MaxKeys"`
	IsTruncated    bool           `xml:"IsTruncated"`
	Contents       []Object       `xml:"Contents"`
	CommonPrefixes []CommonPrefix `xml:"CommonPrefixes"`
}

// Object represents an S3 object in the list response
type Object struct {
	Key          string    `xml:"Key"`
	LastModified time.Time `xml:"LastModified"`
	ETag         string    `xml:"ETag"`
	Size         int64     `xml:"Size"`
	StorageClass string    `xml:"StorageClass"`
}

// CommonPrefix represents a common prefix (directory) in the list response
type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

// Error represents an S3 error response
type Error struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestID string   `xml:"RequestId"`
}

func (s *S3Serve) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	// Parse the path to extract bucket and key
	pathParts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)

	var bucket, key string
	if len(pathParts) > 0 && pathParts[0] != "" {
		bucket = pathParts[0]
	}
	if len(pathParts) > 1 {
		key = pathParts[1]
	}

	// Validate bucket name matches
	if bucket != "" && bucket != s.bucket {
		s.writeError(rw, "NoSuchBucket", "The specified bucket does not exist", r.URL.Path, http.StatusNotFound)
		return
	}

	// Route based on operation
	switch r.Method {
	case http.MethodGet:
		if key == "" {
			// ListBucket operation
			s.listBucket(rw, r)
		} else {
			// GetObject operation
			s.getObject(rw, r, key)
		}
	case http.MethodHead:
		if key != "" {
			// HeadObject operation
			s.headObject(rw, r, key)
		} else {
			s.writeError(rw, "MethodNotAllowed", "The specified method is not allowed against this resource", r.URL.Path, http.StatusMethodNotAllowed)
		}
	case http.MethodPut:
		if key != "" {
			// PutObject operation
			s.putObject(rw, r, key)
		} else {
			s.writeError(rw, "MethodNotAllowed", "The specified method is not allowed against this resource", r.URL.Path, http.StatusMethodNotAllowed)
		}
	case http.MethodDelete:
		if key != "" {
			// DeleteObject operation
			s.deleteObject(rw, r, key)
		} else {
			s.writeError(rw, "MethodNotAllowed", "The specified method is not allowed against this resource", r.URL.Path, http.StatusMethodNotAllowed)
		}
	default:
		s.writeError(rw, "MethodNotAllowed", "The specified method is not allowed", r.URL.Path, http.StatusMethodNotAllowed)
	}
}

func (s *S3Serve) listBucket(rw http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	prefix := query.Get("prefix")
	delimiter := query.Get("delimiter")
	marker := query.Get("marker")
	maxKeysStr := query.Get("max-keys")

	maxKeys := 1000
	if maxKeysStr != "" {
		if mk, err := strconv.Atoi(maxKeysStr); err == nil && mk > 0 {
			maxKeys = mk
		}
	}

	// Normalize prefix to have leading slash
	if prefix != "" && !strings.HasPrefix(prefix, "/") {
		prefix = "/" + prefix
	}

	result := ListBucketResult{
		Xmlns:          "http://s3.amazonaws.com/doc/2006-03-01/",
		Name:           s.bucket,
		Prefix:         strings.TrimPrefix(prefix, "/"),
		Marker:         marker,
		MaxKeys:        maxKeys,
		IsTruncated:    false,
		Contents:       []Object{},
		CommonPrefixes: []CommonPrefix{},
	}

	count := 0
	skipUntilMarker := marker != ""
	seenPrefixes := make(map[string]bool)

	err := s.sss.Walk(r.Context(), prefix, func(fileInfo sss.FileInfo) error {
		if count >= maxKeys {
			result.IsTruncated = true
			return io.EOF
		}

		filePath := fileInfo.Path()
		// Remove leading slash for S3 compatibility
		key := strings.TrimPrefix(filePath, "/")

		// Skip until we pass the marker
		if skipUntilMarker {
			if key <= marker {
				return nil
			}
			skipUntilMarker = false
		}

		// Handle delimiter (common prefixes)
		if delimiter != "" && strings.Contains(strings.TrimPrefix(key, strings.TrimPrefix(prefix, "/")), delimiter) {
			// Extract the common prefix
			relPath := strings.TrimPrefix(key, strings.TrimPrefix(prefix, "/"))
			parts := strings.SplitN(relPath, delimiter, 2)
			commonPrefix := strings.TrimPrefix(prefix, "/") + parts[0] + delimiter

			if !seenPrefixes[commonPrefix] {
				seenPrefixes[commonPrefix] = true
				result.CommonPrefixes = append(result.CommonPrefixes, CommonPrefix{Prefix: commonPrefix})
				count++
			}
			return nil
		}

		if !fileInfo.IsDir() {
			result.Contents = append(result.Contents, Object{
				Key:          key,
				LastModified: fileInfo.ModTime(),
				ETag:         fmt.Sprintf(`"%s"`, ""),
				Size:         fileInfo.Size(),
				StorageClass: "STANDARD",
			})
			count++
		}

		return nil
	})

	if err != nil && err != io.EOF {
		s.writeError(rw, "InternalError", err.Error(), r.URL.Path, http.StatusInternalServerError)
		return
	}

	rw.Header().Set("Content-Type", "application/xml")
	rw.WriteHeader(http.StatusOK)

	encoder := xml.NewEncoder(rw)
	encoder.Indent("", "  ")
	if err := encoder.Encode(result); err != nil {
		// Error already started writing response, can't send error response
		return
	}
}

func (s *S3Serve) getObject(rw http.ResponseWriter, r *http.Request, key string) {
	// Normalize key to have leading slash
	if !strings.HasPrefix(key, "/") {
		key = "/" + key
	}

	info, err := s.sss.StatHead(r.Context(), key)
	if err != nil {
		s.writeError(rw, "NoSuchKey", "The specified key does not exist", key, http.StatusNotFound)
		return
	}

	// Set headers
	rw.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
	rw.Header().Set("Content-Type", "application/octet-stream")
	rw.Header().Set("Last-Modified", info.ModTime().UTC().Format(http.TimeFormat))
	rw.Header().Set("ETag", fmt.Sprintf(`"%s"`, ""))

	reader, err := s.sss.Reader(r.Context(), key)
	if err != nil {
		s.writeError(rw, "InternalError", err.Error(), key, http.StatusInternalServerError)
		return
	}
	defer reader.Close()

	rw.WriteHeader(http.StatusOK)
	io.Copy(rw, reader)
}

func (s *S3Serve) headObject(rw http.ResponseWriter, r *http.Request, key string) {
	// Normalize key to have leading slash
	if !strings.HasPrefix(key, "/") {
		key = "/" + key
	}

	info, err := s.sss.StatHead(r.Context(), key)
	if err != nil {
		s.writeError(rw, "NoSuchKey", "The specified key does not exist", key, http.StatusNotFound)
		return
	}

	// Set headers
	rw.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
	rw.Header().Set("Content-Type", "application/octet-stream")
	rw.Header().Set("Last-Modified", info.ModTime().UTC().Format(http.TimeFormat))
	rw.Header().Set("ETag", fmt.Sprintf(`"%s"`, ""))

	rw.WriteHeader(http.StatusOK)
}

func (s *S3Serve) putObject(rw http.ResponseWriter, r *http.Request, key string) {
	// Normalize key to have leading slash
	if !strings.HasPrefix(key, "/") {
		key = "/" + key
	}

	w, err := s.sss.Writer(r.Context(), key)
	if err != nil {
		s.writeError(rw, "InternalError", err.Error(), key, http.StatusInternalServerError)
		return
	}
	defer w.Close()

	n, err := io.Copy(w, r.Body)
	if err != nil {
		w.Cancel(r.Context())
		s.writeError(rw, "InternalError", err.Error(), key, http.StatusInternalServerError)
		return
	}

	if r.ContentLength > 0 && r.ContentLength != n {
		w.Cancel(r.Context())
		s.writeError(rw, "IncompleteBody", "Content length mismatch", key, http.StatusBadRequest)
		return
	}

	err = w.Commit(r.Context())
	if err != nil {
		w.Cancel(r.Context())
		s.writeError(rw, "InternalError", err.Error(), key, http.StatusInternalServerError)
		return
	}

	rw.Header().Set("ETag", fmt.Sprintf(`"%s"`, ""))
	rw.WriteHeader(http.StatusOK)
}

func (s *S3Serve) deleteObject(rw http.ResponseWriter, r *http.Request, key string) {
	// Normalize key to have leading slash
	if !strings.HasPrefix(key, "/") {
		key = "/" + key
	}

	err := s.sss.Delete(r.Context(), key)
	if err != nil {
		// Check if it's a "not found" error - S3 returns 204 even if object doesn't exist
		errStr := err.Error()
		if strings.Contains(errStr, "not found") || strings.Contains(errStr, "NoSuchKey") {
			rw.WriteHeader(http.StatusNoContent)
			return
		}
		// Return error for other actual errors
		s.writeError(rw, "InternalError", err.Error(), key, http.StatusInternalServerError)
		return
	}

	rw.WriteHeader(http.StatusNoContent)
}

func (s *S3Serve) writeError(rw http.ResponseWriter, code, message, resource string, status int) {
	errorResp := Error{
		Code:      code,
		Message:   message,
		Resource:  resource,
		RequestID: "",
	}

	rw.Header().Set("Content-Type", "application/xml")
	rw.WriteHeader(status)

	encoder := xml.NewEncoder(rw)
	encoder.Indent("", "  ")
	encoder.Encode(errorResp)
}

// WithS3Compatibility returns an Option to enable S3 compatibility mode
func WithS3Compatibility(bucket string) Option {
	return func(s *Serve) {
		s.s3Compatible = true
		s.s3Bucket = bucket
	}
}
