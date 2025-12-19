// Command s3inv-index builds and queries S3 inventory indexes.
package main

import (
	"os"

	"github.com/eunmann/s3-inv-db/internal/cli"
	"github.com/eunmann/s3-inv-db/pkg/logging"
)

func main() {
	if err := cli.Run(os.Args[1:]); err != nil {
		logging.L().Error().Err(err).Msg("command failed")
		os.Exit(1)
	}
}
