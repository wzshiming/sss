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
	
	S3Compatible bool
	S3Bucket     string
}

// NewCommand returns a new cobra.Command for serve
func NewCommand(ctx context.Context) *cobra.Command {
	flags := &flagpole{
		Address: ":8080",
		Expires: 10 * time.Second,
	}

	cmd := &cobra.Command{
		Args: cobra.NoArgs,
		Use:  "serve",
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := sss.NewSSS(sss.WithURL(flags.URL))
			if err != nil {
				return err
			}

			var h http.Handler
			if flags.S3Compatible {
				// S3 compatibility mode
				h = serve.NewServe(
					serve.WithSSS(s),
					serve.WithS3Compatibility(flags.S3Bucket),
				)
			} else {
				// Regular HTTP file server mode
				h = serve.NewServe(
					serve.WithSSS(s),
					serve.WithRedirect(flags.Redirect, flags.Expires),
					serve.WithAllowList(flags.AllowList),
					serve.WithAllowPut(flags.AllowPut),
					serve.WithAllowDelete(flags.AllowDelete),
				)
			}

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
	cmd.Flags().BoolVar(&flags.S3Compatible, "s3-compatible", flags.S3Compatible, "enable S3 API compatibility mode")
	cmd.Flags().StringVar(&flags.S3Bucket, "s3-bucket", flags.S3Bucket, "bucket name for S3 API compatibility mode")
	return cmd
}
