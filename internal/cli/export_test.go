package cli

// Exported aliases for unexported helpers so tests in package cli_test
// can exercise them without weakening the package's public surface.
var (
	LoadPriceTable = loadPriceTable
)
