package main

import (
	"context"
	"log"
	"os"

	"github.com/spf13/cobra"

	"github.com/wzshiming/sss/cmd/sss/cp"
	"github.com/wzshiming/sss/cmd/sss/find"
	"github.com/wzshiming/sss/cmd/sss/get"
	"github.com/wzshiming/sss/cmd/sss/ls"
	"github.com/wzshiming/sss/cmd/sss/part"
	"github.com/wzshiming/sss/cmd/sss/put"
	"github.com/wzshiming/sss/cmd/sss/rm"
	"github.com/wzshiming/sss/cmd/sss/serve"
	"github.com/wzshiming/sss/cmd/sss/sign"
	"github.com/wzshiming/sss/cmd/sss/stat"
)

func main() {
	ctx := context.Background()
	err := NewCommand(ctx).Execute()
	if err != nil {
		log.Println(err)
		os.Exit(1)
		return
	}
}

// NewCommand returns a new cobra.Command for root
func NewCommand(ctx context.Context) *cobra.Command {
	cmd := &cobra.Command{
		Args: cobra.NoArgs,
		Use:  "sss",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}

	cmd.AddCommand(
		sign.NewCommand(ctx),
		part.NewCommand(ctx),
		get.NewCommand(ctx),
		ls.NewCommand(ctx),
		find.NewCommand(ctx),
		stat.NewCommand(ctx),
		cp.NewCommand(ctx),
		put.NewCommand(ctx),
		rm.NewCommand(ctx),
		serve.NewCommand(ctx),
	)
	return cmd
}
