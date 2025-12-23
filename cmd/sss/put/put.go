package put

import (
	"context"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/wzshiming/sss"
)

type flagpole struct {
	URL      string
	Continue bool
	Commit   bool
	SHA256   string
}

// NewCommand returns a new cobra.Command for put
func NewCommand(ctx context.Context) *cobra.Command {
	flags := &flagpole{
		Commit: true,
	}

	cmd := &cobra.Command{
		Args:  cobra.RangeArgs(1, 2),
		Use:   "put <remote> [local]",
		Short: "Upload files to S3",
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := sss.NewSSS(sss.WithURL(flags.URL))
			if err != nil {
				return err
			}

			opts := []sss.WriterOptions{}
			if flags.SHA256 != "" {
				opts = append(opts, sss.WithSHA256(flags.SHA256))
			}

			remote := args[0]
			if len(args) == 1 {
				if !flags.Continue {
					rc, err := s.Writer(cmd.Context(), remote, opts...)
					if err != nil {
						return err
					}

					_, err = io.Copy(rc, os.Stdin)
					if err != nil {
						rc.Close()
						return err
					}

					if flags.Commit {
						err := rc.Commit(ctx)
						if err != nil {
							return err
						}
					}
					return err
				}

				rc, err := s.WriterWithAppend(cmd.Context(), remote, opts...)
				if err != nil {
					return err
				}

				_, err = io.Copy(rc, os.Stdin)
				if err != nil {
					rc.Close()
					return err
				}

				if flags.Commit {
					err := rc.Commit(ctx)
					if err != nil {
						return err
					}
				}
				return err
			}

			local := args[1]
			if !flags.Continue {
				f, err := os.Open(local)
				if err != nil {
					return err
				}
				defer f.Close()

				rc, err := s.Writer(cmd.Context(), remote, opts...)
				if err != nil {
					return err
				}

				_, err = io.Copy(rc, f)
				if err != nil {
					rc.Close()
					return err
				}

				if flags.Commit {
					err := rc.Commit(ctx)
					if err != nil {
						return err
					}
				}
				return err
			}

			rc, err := s.WriterWithAppend(cmd.Context(), remote, opts...)
			if err != nil {
				return err
			}

			f, err := os.Open(local)
			if err != nil {
				rc.Close()
				return err
			}
			defer f.Close()

			_, err = f.Seek(rc.Size(), io.SeekStart)
			if err != nil {
				rc.Close()
				return err
			}

			_, err = io.Copy(rc, f)
			if err != nil {
				rc.Close()
				return err
			}

			if flags.Commit {
				err := rc.Commit(ctx)
				if err != nil {
					return err
				}
			}
			return err
		},
	}
	cmd.Flags().StringVar(&flags.URL, "url", flags.URL, "config url")
	cmd.Flags().BoolVar(&flags.Continue, "continue", flags.Continue, "continue")
	cmd.Flags().BoolVar(&flags.Commit, "commit", flags.Commit, "commit")
	cmd.Flags().StringVar(&flags.SHA256, "sha256", flags.SHA256, "sha256")

	return cmd
}
