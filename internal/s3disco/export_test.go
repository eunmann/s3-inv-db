package s3disco

// RunFolderRE exposes the unexported runFolderRE regex for tests.
var RunFolderRE = runFolderRE

// TrimPrefix exposes the unexported trimPrefix helper for tests.
func TrimPrefix(s, prefix string) string { return trimPrefix(s, prefix) }

// ManifestTotalBytes exposes the unexported manifestTotalBytes helper
// for tests.
var ManifestTotalBytes = manifestTotalBytes
