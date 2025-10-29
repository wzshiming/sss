package get

import (
	"context"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/wzshiming/sss"
)

type flagpole struct {
	URL      string
	Offset   int64
	Continue bool
}

// NewCommand returns a new cobra.Command for get
func NewCommand(ctx context.Context) *cobra.Command {
	flags := &flagpole{}

	cmd := &cobra.Command{
		Args:  cobra.RangeArgs(1, 2),
		Use:   "get <remote> [local]",
		Short: "Download files from S3",
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := sss.NewSSS(sss.WithURL(flags.URL))
			if err != nil {
				return err
			}

			remote := args[0]
			if len(args) == 1 {
				rc, err := s.ReaderWithOffset(cmd.Context(), remote, flags.Offset)
				if err != nil {
					return err
				}
				defer rc.Close()
				_, err = io.Copy(os.Stdout, rc)
				return err
			}

			local := args[1]

			if !flags.Continue {
				f, err := os.OpenFile(local, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
				if err != nil {
					return err
				}
				defer f.Close()

				rc, err := s.ReaderWithOffset(cmd.Context(), remote, flags.Offset)
				if err != nil {
					return err
				}
				defer rc.Close()
				_, err = io.Copy(f, rc)
				return err
			}

			offset := flags.Offset
			stat, err := os.Stat(local)
			if err == nil {
				offset = stat.Size()
			}

			f, err := os.OpenFile(local, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
			if err != nil {
				return err
			}
			defer f.Close()

			rc, err := s.ReaderWithOffset(cmd.Context(), remote, offset)
			if err != nil {
				return err
			}
			defer rc.Close()

			_, err = io.Copy(f, rc)
			return err
		},
	}
	cmd.Flags().StringVar(&flags.URL, "url", flags.URL, "config url")
	cmd.Flags().Int64Var(&flags.Offset, "offset", flags.Offset, "offset")
	cmd.Flags().BoolVar(&flags.Continue, "continue", flags.Continue, "continue")

	return cmd
}
