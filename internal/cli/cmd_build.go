package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/eunmann/s3-inv-db/internal/appconfig"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/eunmann/s3-inv-db/pkg/sysmem"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func runBuild(args []string) error {
	fs := flag.NewFlagSet("build", flag.ContinueOnError)
	configPath := fs.String("config", "", "path to JSON config file (overridden by explicit flags)")
	outDir := fs.String("out", "", "output directory for index files")
	s3Manifest := fs.String("s3-manifest", "", "S3 URI to inventory manifest.json (s3://bucket/path/manifest.json)")
	verbose := fs.Bool("verbose", false, "enable debug level logging")
	prettyLogs := fs.Bool("pretty-logs", false, "use human-friendly console output")
	maxDepth := fs.Int("max-depth", 0, "maximum prefix depth to track (0 = unlimited)")
	prefixDictionary := fs.Bool("prefix-dictionary", true, "enable dictionary-encoded prefix storage")
	eventLogPath := fs.String("event-log", "", "append build events as JSONL to this path (overrides config)")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	fileCfg, err := appconfig.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	explicit := explicitFlags(fs)
	finalVerbose := resolveBool(fileCfg, *verbose, explicit["verbose"], func(c *appconfig.Config) *bool { return c.Verbose })
	finalPretty := resolveBool(fileCfg, *prettyLogs, explicit["pretty-logs"], func(c *appconfig.Config) *bool { return c.PrettyLogs })
	finalEventLog := resolveString(fileCfg, *eventLogPath, explicit["event-log"], func(c *appconfig.Config) *string { return c.BuildEventLog })

	baseLogger := logging.NewLogger(logging.Options{Debug: finalVerbose, Human: finalPretty})
	log.Logger = baseLogger
	logging.Init(logging.Options{Debug: finalVerbose, Human: finalPretty})

	memLimit := sysmem.ApplyMemoryLimit(sysmem.DefaultMemoryLimitFraction)
	baseLogger.Info().
		Int64("bytes", memLimit.Bytes).
		Str("source", string(memLimit.Source)).
		Int64("env_bytes", memLimit.EnvBytes).
		Int64("cgroup_bytes", memLimit.CgroupBytes).
		Int64("sysmem_fraction_bytes", memLimit.SysmemFractionBytes).
		Msg("process memory limit configured")

	if *outDir == "" {
		return ErrOutRequired
	}
	if *s3Manifest == "" {
		return ErrManifestRequired
	}

	return runBuildExtSort(*outDir, *s3Manifest, *maxDepth, *prefixDictionary, finalEventLog, baseLogger)
}

func runBuildExtSort(outDir, s3Manifest string, maxDepth int, prefixDictionary bool, eventLogPath string, baseLogger zerolog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx = baseLogger.WithContext(ctx)
	logger := zerolog.Ctx(ctx)

	client, err := s3fetch.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("create S3 client: %w", err)
	}

	tracker := newBuildTracker(outDir, s3Manifest, eventLogPath, logger)
	defer tracker.close()

	config := extsort.DefaultConfig()
	if maxDepth > 0 {
		config.MaxDepth = maxDepth
	}
	config.PrefixDictionary = prefixDictionary
	config.Observe = tracker.wire()

	logger.Info().
		Int("s3_part_concurrency", config.S3.DownloadPartConcurrency).
		Int("max_depth", config.MaxDepth).
		Bool("prefix_dictionary", config.PrefixDictionary).
		Msg("pipeline configuration")

	pipeline := extsort.NewPipeline(config, client)

	tracker.start()

	result, err := pipeline.Run(ctx, s3Manifest, outDir)
	if err != nil {
		tracker.finish(nil, err)

		if errors.Is(err, context.Canceled) {
			logger.Warn().Msg("build cancelled by user")

			return fmt.Errorf("build cancelled: %w", err)
		}

		return fmt.Errorf("run pipeline: %w", err)
	}

	tracker.finish(result, nil)

	return nil
}
