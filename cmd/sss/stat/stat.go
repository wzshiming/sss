package stat

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/wzshiming/sss"
)

type flagpole struct {
	URL string
}

// NewCommand returns a new cobra.Command for stat
func NewCommand(ctx context.Context) *cobra.Command {
	flags := &flagpole{}

	cmd := &cobra.Command{
		Args: cobra.RangeArgs(1, 2),
		Use:  "stat <remote>",
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := sss.NewSSS(sss.WithURL(flags.URL))
			if err != nil {
				return err
			}

			remote := args[0]
			stat, err := s.Stat(cmd.Context(), remote)
			if err != nil {
				return err
			}

			fmt.Println(stat.Path(), stat.Size(), stat.ModTime().Format(time.RFC3339))
			return err
		},
	}
	cmd.Flags().StringVar(&flags.URL, "url", flags.URL, "config url")

	return cmd
}
