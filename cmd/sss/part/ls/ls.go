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
	Wide  bool
	Limit int
}

// NewCommand returns a new cobra.Command for ls
func NewCommand(ctx context.Context) *cobra.Command {
	flags := &flagpole{
		Limit: 1000,
	}

	cmd := &cobra.Command{
		Args:  cobra.RangeArgs(0, 1),
		Use:   "ls <remote>",
		Short: "List ongoing multipart uploads",
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
			err = s.ListMultipart(cmd.Context(), remote, func(mp *sss.Multipart) bool {
				if flags.Wide {
					p, err := mp.OrderParts(cmd.Context())
					if err == nil {
						fmt.Println(mp.Key(), p.Size(), p.Count(), p.LastModified().Format(time.RFC3339), mp.UploadID())
					} else {
						fmt.Println(mp.Key(), err)
					}
				} else {
					fmt.Println(mp.Key())
				}
				count++
				return flags.Limit < 0 || count < flags.Limit
			})
			if err != nil {
				return err
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&flags.URL, "url", flags.URL, "config url")
	cmd.Flags().BoolVar(&flags.Wide, "wide", flags.Wide, "wide")
	cmd.Flags().IntVar(&flags.Limit, "limit", flags.Limit, "maximum number to return")
	return cmd
}
