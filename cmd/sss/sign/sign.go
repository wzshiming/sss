package sign

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/wzshiming/sss/cmd/sss/sign/cp"
	"github.com/wzshiming/sss/cmd/sss/sign/get"
	"github.com/wzshiming/sss/cmd/sss/sign/head"
	"github.com/wzshiming/sss/cmd/sss/sign/ls"
	"github.com/wzshiming/sss/cmd/sss/sign/put"
	"github.com/wzshiming/sss/cmd/sss/sign/rm"
)

// NewCommand returns a new cobra.Command for sign
func NewCommand(ctx context.Context) *cobra.Command {
	cmd := &cobra.Command{
		Args:  cobra.NoArgs,
		Use:   "sign",
		Short: "Generate presigned URLs for S3 operations",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	cmd.AddCommand(ls.NewCommand(ctx))
	cmd.AddCommand(get.NewCommand(ctx))
	cmd.AddCommand(put.NewCommand(ctx))
	cmd.AddCommand(head.NewCommand(ctx))
	cmd.AddCommand(rm.NewCommand(ctx))
	cmd.AddCommand(cp.NewCommand(ctx))

	return cmd
}
