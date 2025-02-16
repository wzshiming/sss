package rm

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/wzshiming/sss"
)

type flagpole struct {
	URL string
	ID  string
}

// NewCommand returns a new cobra.Command for rm
func NewCommand(ctx context.Context) *cobra.Command {
	flags := &flagpole{}

	cmd := &cobra.Command{
		Args: cobra.ExactArgs(1),
		Use:  "rm <remote>",
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := sss.NewSSS(sss.WithURL(flags.URL))
			if err != nil {
				return err
			}

			remote := args[0]

			var mp *sss.Multipart
			if flags.ID == "" {
				mp, err = s.GetMultipart(cmd.Context(), remote)
			} else {
				mp, err = s.GetMultipartByUploadID(cmd.Context(), remote, flags.ID)
			}
			if err != nil {
				return err
			}

			return mp.Cancel(cmd.Context())
		},
	}
	cmd.Flags().StringVar(&flags.URL, "url", flags.URL, "config url")

	return cmd
}
