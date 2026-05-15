package appconfig

import (
	"bytes"
	"io"
)

func newReader(data []byte) io.Reader { return bytes.NewReader(data) }
