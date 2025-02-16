package cp

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/wzshiming/sss"
)

type flagpole struct {
	URL string
}

// NewCommand returns a new cobra.Command for cp
func NewCommand(ctx context.Context) *cobra.Command {
	flags := &flagpole{}

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

			return s.Copy(cmd.Context(), remoteOld, remote)
		},
	}
	cmd.Flags().StringVar(&flags.URL, "url", flags.URL, "config url")

	return cmd
}
