package serve

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/wzshiming/sss"
	"github.com/wzshiming/sss/fs"
)

type flagpole struct {
	URL     string
	Address string
}

// NewCommand returns a new cobra.Command for serve
func NewCommand(ctx context.Context) *cobra.Command {
	flags := &flagpole{
		Address: ":8080",
	}

	cmd := &cobra.Command{
		Args: cobra.NoArgs,
		Use:  "serve",
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := sss.NewSSS(sss.WithURL(flags.URL))
			if err != nil {
				return err
			}

			// fileSystem := fs.NewFS(cmd.Context(), s, "/")

			// http.Handle("/", http.FileServerFS(fileSystem))

			http.Handle("/", http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				info, err := s.Stat(cmd.Context(), r.URL.Path)
				if err != nil {
					http.Error(rw, err.Error(), http.StatusInternalServerError)
					return
				}

				if info.IsDir() {

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
					err = s.List(cmd.Context(), r.URL.Path, func(fileInfo sss.FileInfo) bool {
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
					return
				}
				http.ServeContent(rw, r, r.URL.Path, info.ModTime(), fs.NewReadSeekCloser(func(start int64) (io.ReadCloser, error) {
					return s.ReaderWithOffset(cmd.Context(), r.URL.Path, start)
				}, info.Size()))
			}))

			http.Handle("DELETE /", http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				err := s.Delete(cmd.Context(), r.URL.Path)
				if err != nil {
					http.Error(rw, err.Error(), http.StatusInternalServerError)
					return
				}
				rw.WriteHeader(http.StatusOK)
			}))

			http.Handle("PUT /", http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
				w, err := s.Writer(cmd.Context(), r.URL.Path)
				if err != nil {
					http.Error(rw, err.Error(), http.StatusInternalServerError)
					return
				}
				defer w.Close()
				n, err := io.Copy(w, r.Body)
				if err != nil {
					w.Cancel(cmd.Context())
					http.Error(rw, err.Error(), http.StatusInternalServerError)
					return
				}
				if r.ContentLength != n {
					w.Cancel(cmd.Context())
					http.Error(rw, "content length mismatch", http.StatusInternalServerError)
					return
				}
				err = w.Commit(cmd.Context())
				if err != nil {
					w.Cancel(cmd.Context())
					http.Error(rw, err.Error(), http.StatusInternalServerError)
					return
				}
				rw.WriteHeader(http.StatusCreated)
			}))

			return http.ListenAndServe(flags.Address, nil)
		},
	}
	cmd.Flags().StringVar(&flags.URL, "url", flags.URL, "config url")
	cmd.Flags().StringVar(&flags.Address, "address", flags.Address, "address")

	return cmd
}
