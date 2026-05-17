package logging_test

import (
	"testing"

	"github.com/eunmann/s3-inv-db/pkg/logging"
)

func TestInit_DoesNotPanic(_ *testing.T) {
	logging.Init(false, false)
	log := logging.L()
	log.Info().Msg("test json info")
	log.Debug().Msg("test json debug (should not appear at info level)")

	logging.Init(true, false)
	log = logging.L()
	log.Debug().Msg("test json debug (should appear)")

	logging.Init(false, true)
	log = logging.L()
	log.Info().Msg("test human info")

	logging.Init(true, true)
	log = logging.L()
	log.Debug().Msg("test human debug")
}
