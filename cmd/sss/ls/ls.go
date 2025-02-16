package ls

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/wzshiming/sss"
)

type flagpole struct {
	URL   string
	Limit int
}

// NewCommand returns a new cobra.Command for ls
func NewCommand(ctx context.Context) *cobra.Command {
	flags := &flagpole{
		Limit: 1000,
	}

	cmd := &cobra.Command{
		Args: cobra.RangeArgs(0, 1),
		Use:  "ls <remote>",
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := sss.NewSSS(sss.WithURL(flags.URL))
			if err != nil {
				return err
			}

			var remote string = "/"
			if len(args) != 0 {
				remote = args[0]
			}

			var count int
			err = s.List(ctx, remote, func(fileInfo sss.FileInfo) bool {
				count++
				if fileInfo.IsDir() {
					fmt.Println(fileInfo.Path())
					return true
				}
				fmt.Println(fileInfo.Path(), fileInfo.Size(), fileInfo.ModTime().Format(time.RFC3339))
				return flags.Limit < 0 || count < flags.Limit
			})
			if err != nil {
				return err
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&flags.URL, "url", flags.URL, "config url")
	cmd.Flags().IntVar(&flags.Limit, "limit", flags.Limit, "maximum number to return")
	return cmd
}
