package find

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/spf13/cobra"

	"github.com/wzshiming/sss"
)

type flagpole struct {
	URL      string
	Name     string
	FromDate string
	ToDate   string
	Limit    int
}

// NewCommand returns a new cobra.Command for find
func NewCommand(ctx context.Context) *cobra.Command {
	flags := &flagpole{
		Limit: 1000,
	}

	cmd := &cobra.Command{
		Args: cobra.RangeArgs(0, 1),
		Use:  "find <remote>",
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := sss.NewSSS(sss.WithURL(flags.URL))
			if err != nil {
				return err
			}

			var remote string = "/"
			if len(args) != 0 {
				remote = args[0]
			}

			fromDate, err := time.Parse(time.RFC3339, flags.FromDate)
			if err != nil && flags.FromDate != "" {
				return fmt.Errorf("invalid from-date format: %v", err)
			}

			toDate, err := time.Parse(time.RFC3339, flags.ToDate)
			if err != nil && flags.ToDate != "" {
				return fmt.Errorf("invalid to-date format: %v", err)
			}

			var count int
			return s.Walk(cmd.Context(), remote, func(fileInfo sss.FileInfo) error {
				if fileInfo.IsDir() {
					return nil
				}

				if flags.Name != "" {
					matched, err := path.Match(flags.Name, path.Base(fileInfo.Path()))
					if err != nil {
						return fmt.Errorf("invalid name pattern: %v", err)
					}
					if !matched {
						return nil
					}
				}

				modTime := fileInfo.ModTime()
				if flags.FromDate != "" && modTime.Before(fromDate) {
					return nil
				}
				if flags.ToDate != "" && modTime.After(toDate) {
					return nil
				}

				fmt.Println(fileInfo.Path(), fileInfo.Size(), fileInfo.ModTime().Format(time.RFC3339))
				count++
				if count < 0 || count < flags.Limit {
					return nil
				}
				return sss.ErrFilledBuffer
			})
		},
	}
	cmd.Flags().StringVar(&flags.URL, "url", flags.URL, "config url")
	cmd.Flags().StringVar(&flags.Name, "name", "", "filter by file name (supports glob patterns)")
	cmd.Flags().StringVar(&flags.FromDate, "from-date", "", "filter files modified after this date (RFC3339 format)")
	cmd.Flags().StringVar(&flags.ToDate, "to-date", "", "filter files modified before this date (RFC3339 format)")
	cmd.Flags().IntVar(&flags.Limit, "limit", flags.Limit, "maximum number to return")
	return cmd
}
