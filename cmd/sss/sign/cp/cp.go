package cp

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/wzshiming/sss"
)

type flagpole struct {
	URL     string
	Expires time.Duration
}

// NewCommand returns a new cobra.Command for cp
func NewCommand(ctx context.Context) *cobra.Command {
	flags := &flagpole{
		Expires: 1 * time.Hour,
	}

	cmd := &cobra.Command{
		Args: cobra.ExactArgs(2),
		Use:  "cp <remote> <remote-old>",
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := sss.NewSSS(sss.WithURL(flags.URL))
			if err != nil {
				return err
			}

			remote := args[0]
			remoteOld := args[1]

			u, err := s.SignCopy(cmd.Context(), remoteOld, remote, flags.Expires)
			if err != nil {
				return err
			}

			fmt.Println(u)
			return nil
		},
	}
	cmd.Flags().StringVar(&flags.URL, "url", flags.URL, "config url")
	cmd.Flags().DurationVar(&flags.Expires, "expires", flags.Expires, "expires")

	return cmd
}
