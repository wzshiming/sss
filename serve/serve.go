package serve

import (
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/wzshiming/sss"
	"github.com/wzshiming/sss/fs"
)

type Option func(s *Serve)

func WithSSS(sss *sss.SSS) Option {
	return func(s *Serve) {
		s.sss = sss
	}
}

func WithRedirect(r bool, expires time.Duration) Option {
	return func(s *Serve) {
		s.redirect = r
		s.expires = expires
	}
}

func WithAllowList(b bool) Option {
	return func(s *Serve) {
		s.allowList = b
	}
}

func WithAllowPut(b bool) Option {
	return func(s *Serve) {
		s.allowPut = b
	}
}

func WithAllowDelete(b bool) Option {
	return func(s *Serve) {
		s.allowDelete = b
	}
}

type Serve struct {
	sss          *sss.SSS
	expires      time.Duration
	redirect     bool
	allowList    bool
	allowPut     bool
	allowDelete  bool
	s3Compatible bool
	s3Bucket     string
}

func NewServe(opts ...Option) http.Handler {
	s := &Serve{}
	for _, opt := range opts {
		opt(s)
	}

	// If S3 compatibility mode is enabled, return S3 handler
	if s.s3Compatible {
		return NewS3Serve(s.sss, s.s3Bucket)
	}

	return s
}

func (s *Serve) notAllowed(rw http.ResponseWriter) {
	http.Error(rw, "Method Not Allowed", http.StatusMethodNotAllowed)
}

func (s *Serve) forbidden(rw http.ResponseWriter) {
	http.Error(rw, "Forbidden", http.StatusForbidden)
}

func (s *Serve) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	switch r.Method {
	default:
		s.notAllowed(rw)
	case http.MethodPut:
		if !s.allowPut {
			s.notAllowed(rw)
			return
		}
		if s.redirect {
			s.putRedirect(rw, r)
		} else {
			s.put(rw, r)
		}
	case http.MethodDelete:
		if !s.allowDelete {
			s.notAllowed(rw)
			return
		}
		if s.redirect {
			s.deleteRedirect(rw, r)
		} else {
			s.delete(rw, r)
		}
	case http.MethodGet:
		if strings.HasSuffix(r.URL.Path, "/") {
			if !s.allowList {
				s.forbidden(rw)
				return
			}
			s.list(rw, r)
		} else {
			if s.redirect {
				s.getRedirect(rw, r)
			} else {
				s.get(rw, r)
			}
		}
	case http.MethodHead:
		if strings.HasSuffix(r.URL.Path, "/") {
			if !s.allowList {
				s.forbidden(rw)
				return
			}
			s.list(rw, r)
		} else {
			if s.redirect {
				s.headRedirect(rw, r)
			} else {
				s.get(rw, r)
			}
		}
	}
}

func (s *Serve) delete(rw http.ResponseWriter, r *http.Request) {
	err := s.sss.Delete(r.Context(), r.URL.Path)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	rw.WriteHeader(http.StatusOK)
}

func (s *Serve) put(rw http.ResponseWriter, r *http.Request) {
	w, err := s.sss.Writer(r.Context(), r.URL.Path)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	defer w.Close()
	n, err := io.Copy(w, r.Body)
	if err != nil {
		w.Cancel(r.Context())
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	if r.ContentLength != n {
		w.Cancel(r.Context())
		http.Error(rw, "content length mismatch", http.StatusInternalServerError)
		return
	}
	err = w.Commit(r.Context())
	if err != nil {
		w.Cancel(r.Context())
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	rw.WriteHeader(http.StatusCreated)
}

func (s *Serve) get(rw http.ResponseWriter, r *http.Request) {
	info, err := s.sss.StatHead(r.Context(), r.URL.Path)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusNotFound)
		return
	}
	http.ServeContent(rw, r, r.URL.Path, info.ModTime(), fs.NewReadSeekCloser(func(start int64) (io.ReadCloser, error) {
		return s.sss.ReaderWithOffset(r.Context(), r.URL.Path, start)
	}, info.Size()))
}

func (s *Serve) list(rw http.ResponseWriter, r *http.Request) {
	_, err := s.sss.StatHeadList(r.Context(), r.URL.Path)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusNotFound)
		return
	}

	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	if r.Method == http.MethodHead {
		return
	}

	fmt.Fprintln(rw, `<!doctype html>`)
	fmt.Fprintln(rw, `<meta name="viewport" content="width=device-width">`)
	fmt.Fprintf(rw, `<pre>`)
	if r.URL.Path != "/" && r.URL.Path != "" {
		fmt.Fprintf(rw, `<a href="%s">..</a>
`, path.Dir(strings.TrimSuffix(r.URL.Path, "/")))
	}
	err = s.sss.List(r.Context(), r.URL.Path, func(fileInfo sss.FileInfo) bool {
		if fileInfo.IsDir() {
			fmt.Fprintf(rw, `<a href="%s/">%s/</a>
`, fileInfo.Path(), path.Base(fileInfo.Path()))
		} else {
			fmt.Fprintf(rw, `<a href="%s">%s</a> %d %s
`, fileInfo.Path(), path.Base(fileInfo.Path()), fileInfo.Size(), fileInfo.ModTime().Format(time.RFC3339))
		}
		return true
	})
	if err != nil {
		fmt.Fprintf(rw, `<span style="color: red;">%s</span>`, err.Error())
	}
	fmt.Fprintf(rw, `</pre>`)
}

func (s *Serve) headRedirect(rw http.ResponseWriter, r *http.Request) {
	url, err := s.sss.SignHead(r.URL.Path, s.expires)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(rw, r, url, http.StatusTemporaryRedirect)
}

func (s *Serve) getRedirect(rw http.ResponseWriter, r *http.Request) {
	url, err := s.sss.SignGet(r.URL.Path, s.expires)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(rw, r, url, http.StatusTemporaryRedirect)
}

func (s *Serve) putRedirect(rw http.ResponseWriter, r *http.Request) {
	url, err := s.sss.SignPut(r.URL.Path, s.expires)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(rw, r, url, http.StatusTemporaryRedirect)
}

func (s *Serve) deleteRedirect(rw http.ResponseWriter, r *http.Request) {
	url, err := s.sss.SignDelete(r.URL.Path, s.expires)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(rw, r, url, http.StatusTemporaryRedirect)
}
