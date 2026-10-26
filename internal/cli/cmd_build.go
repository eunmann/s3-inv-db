package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/eunmann/s3-inv-db/internal/appconfig"
	"github.com/eunmann/s3-inv-db/pkg/extsort"
	"github.com/eunmann/s3-inv-db/pkg/indexstore"
	"github.com/eunmann/s3-inv-db/pkg/logging"
	"github.com/eunmann/s3-inv-db/pkg/s3fetch"
	"github.com/eunmann/s3-inv-db/pkg/sysmem"
	"github.com/rs/zerolog"
)

func runBuild(args []string) error {
	fs := flag.NewFlagSet("build", flag.ContinueOnError)
	configPath := fs.String("config", "", "path to JSON config file (overridden by explicit flags)")
	outDir := fs.String("out", "", "output directory for index files, or s3://bucket/prefix/ to publish the index for a server's index_source")
	workDir := fs.String("work-dir", ".", "with --out s3://...: local directory the index is built in before it is published (scratch included; not /tmp, which may be RAM)")
	s3Manifest := fs.String("s3-manifest", "", "S3 URI to inventory manifest.json (s3://bucket/path/manifest.json)")
	logFlags := addLoggingFlags(fs)
	maxDepth := fs.Int("max-depth", 0, "maximum prefix depth to track (0 = unlimited)")
	allowMalformed := fs.Bool("allow-malformed-rows", false, "build even when inventory rows can't be read exactly (they're skipped or indexed with size 0; totals then differ from the inventory)")
	eventLogPath := fs.String("event-log", "", "append build events as JSONL to this path (overrides config)")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	fileCfg, err := appconfig.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	initLogging(logFlags, fs, fileCfg)
	finalEventLog := appconfig.PickFile(*eventLogPath, explicitFlags(fs)["event-log"], fileCfg, func(c *appconfig.Config) *string { return c.BuildEventLog })

	baseLogger := *logging.L()

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

	if strings.HasPrefix(*outDir, "s3://") {
		return runBuildPublish(*outDir, *workDir, *s3Manifest, *maxDepth, *allowMalformed, finalEventLog, baseLogger)
	}

	return runBuildExtSort(*outDir, *s3Manifest, *maxDepth, *allowMalformed, finalEventLog, baseLogger)
}

// runBuildPublish builds the index in a hidden directory under workDir,
// then publishes it to dest under <source-bucket>/<inventory-id>/<run>/
// (the run's path in the inventory manifest's key), manifest.json last,
// and removes the local copy.
func runBuildPublish(dest, workDir, s3Manifest string, maxDepth int, allowMalformed bool, eventLogPath string, baseLogger zerolog.Logger) error {
	destURI, err := s3fetch.ParseS3URI(dest)
	if err != nil {
		return fmt.Errorf("--out %q: %w", dest, err)
	}
	manifestURI, err := s3fetch.ParseS3URI(s3Manifest)
	if err != nil {
		return fmt.Errorf("--s3-manifest %q: %w", s3Manifest, err)
	}
	run, err := indexstore.RunFromManifestKey(manifestURI.Key)
	if err != nil {
		return fmt.Errorf("--s3-manifest: %w", err)
	}
	local, err := os.MkdirTemp(workDir, ".s3-inv-db-publish-")
	if err != nil {
		return fmt.Errorf("create work dir under %s: %w", workDir, err)
	}
	defer os.RemoveAll(local)
	indexDir := filepath.Join(local, "index")
	if err := runBuildExtSort(indexDir, s3Manifest, maxDepth, allowMalformed, eventLogPath, baseLogger); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client, err := s3fetch.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("create S3 client: %w", err)
	}
	prefix := run.Prefix(destURI.Key)
	if err := indexstore.New(client.Raw(), destURI.Bucket).Publish(ctx, indexDir, prefix); err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	baseLogger.Info().Str("index", "s3://"+destURI.Bucket+"/"+prefix).Msg("index published")

	return nil
}

func runBuildExtSort(outDir, s3Manifest string, maxDepth int, allowMalformed bool, eventLogPath string, baseLogger zerolog.Logger) error {
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
	config.AllowAnomalies = allowMalformed
	config.Observe = tracker.wire()

	logger.Info().
		Int("max_depth", config.MaxDepth).
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
