package serve

import (
	"context"
	"net/http"
	"time"

	"github.com/spf13/cobra"

	"github.com/wzshiming/sss"
	"github.com/wzshiming/sss/serve"
)

type flagpole struct {
	URL      string
	Address  string
	Redirect bool
	Expires  time.Duration

	AllowList   bool
	AllowPut    bool
	AllowDelete bool
}

// NewCommand returns a new cobra.Command for serve
func NewCommand(ctx context.Context) *cobra.Command {
	flags := &flagpole{
		Address: ":8080",
		Expires: 10 * time.Second,
	}

	cmd := &cobra.Command{
		Args:  cobra.NoArgs,
		Use:   "serve",
		Short: "Start HTTP server for S3 content",
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := sss.NewSSS(sss.WithURL(flags.URL))
			if err != nil {
				return err
			}

			h := serve.NewServe(
				serve.WithSSS(s),
				serve.WithRedirect(flags.Redirect, flags.Expires),
				serve.WithAllowList(flags.AllowList),
				serve.WithAllowPut(flags.AllowPut),
				serve.WithAllowDelete(flags.AllowDelete),
			)

			return http.ListenAndServe(flags.Address, h)
		},
	}
	cmd.Flags().StringVar(&flags.URL, "url", flags.URL, "config url")
	cmd.Flags().StringVar(&flags.Address, "address", flags.Address, "address")
	cmd.Flags().BoolVar(&flags.Redirect, "redirect", flags.Redirect, "redirect")
	cmd.Flags().DurationVar(&flags.Expires, "expires", flags.Expires, "redirect expires")
	cmd.Flags().BoolVar(&flags.AllowList, "allow-list", flags.AllowList, "allow list")
	cmd.Flags().BoolVar(&flags.AllowPut, "allow-put", flags.AllowPut, "allow put")
	cmd.Flags().BoolVar(&flags.AllowDelete, "allow-delete", flags.AllowDelete, "allow delete")
	return cmd
}
