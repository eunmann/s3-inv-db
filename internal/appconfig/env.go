package appconfig

import (
	"os"
	"strconv"
	"time"
)

// EnvBool reads a bool from environment variable k, falling back to
// def when the variable is unset or unparseable. Accepts any value
// strconv.ParseBool understands.
func EnvBool(k string, def bool) bool {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}

	return b
}

// EnvInt reads an int from environment variable k, falling back to
// def when unset or unparseable.
func EnvInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}

	return n
}

// EnvFloat reads a float64 from environment variable k, falling
// back to def when unset or unparseable.
func EnvFloat(k string, def float64) float64 {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}

	return f
}

// EnvDuration reads a time.Duration from environment variable k,
// falling back to def when unset or unparseable.
func EnvDuration(k string, def time.Duration) time.Duration {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}

	return d
}

// EnvOr reads a string from environment variable k, returning def
// when the variable is unset (empty string counts as unset).
func EnvOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}

	return def
}
