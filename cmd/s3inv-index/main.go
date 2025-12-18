// Command s3inv-index builds and queries S3 inventory indexes.
package main

import (
	"fmt"
	"os"

	"github.com/eunmann/s3-inv-db/internal/cli"
)

func main() {
	if err := cli.Run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
