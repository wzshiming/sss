package rm

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/wzshiming/sss"
)

type flagpole struct {
	URL       string
	Recursive bool
}

// NewCommand returns a new cobra.Command for rm
func NewCommand(ctx context.Context) *cobra.Command {
	flags := &flagpole{}

	cmd := &cobra.Command{
		Args:  cobra.ExactArgs(1),
		Use:   "rm <remote>",
		Short: "Delete files from S3",
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := sss.NewSSS(sss.WithURL(flags.URL))
			if err != nil {
				return err
			}

			remote := args[0]

			if flags.Recursive {
				return s.DeleteAll(cmd.Context(), remote)
			}
			return s.Delete(cmd.Context(), remote)
		},
	}
	cmd.Flags().StringVar(&flags.URL, "url", flags.URL, "config url")
	cmd.Flags().BoolVar(&flags.Recursive, "recursive", flags.Recursive, "recursive delete")
	return cmd
}
