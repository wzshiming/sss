package part

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/wzshiming/sss/cmd/sss/part/commit"
	"github.com/wzshiming/sss/cmd/sss/part/ls"
	"github.com/wzshiming/sss/cmd/sss/part/rm"
)

// NewCommand returns a new cobra.Command for part
func NewCommand(ctx context.Context) *cobra.Command {
	cmd := &cobra.Command{
		Args:  cobra.NoArgs,
		Use:   "part",
		Short: "Manage multipart uploads",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(ls.NewCommand(ctx))
	cmd.AddCommand(rm.NewCommand(ctx))
	cmd.AddCommand(commit.NewCommand(ctx))
	return cmd
}
