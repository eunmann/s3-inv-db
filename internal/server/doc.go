// Package server provides the HTTP server for the s3-inv-db service.
//
// The server exposes a chi-based router serving the HTML UI (HTMX +
// Tailwind), a read-only JSON API under /api, HTMX partial endpoints
// under /partials, and an SSE job stream at /api/jobs/stream. It owns
// the inventory and job state stores (SQLite-backed) and the optional
// S3 discovery + loader pipeline.
//
// # Quick start
//
// The simplest way to embed the server in another binary is
// [BootstrapAndRun], which parses [RuntimeOptions], opens the state DB,
// loads the price table, constructs the server, and runs it until ctx
// is cancelled:
//
//	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
//	defer stop()
//	err := server.BootstrapAndRun(ctx, server.RuntimeOptions{
//	    Addr:     ":8080",
//	    S3Source: "s3://my-bucket/inventory-data/",
//	    CacheDir: "/var/cache/s3inv",
//	    Logger:   logger,
//	})
//
// # Lifecycle control
//
// When you need to manage shutdown yourself, use [Bootstrap] for the
// configured-from-options path or [New] for full control:
//
//	srv, cleanup, err := server.Bootstrap(opts)
//	if err != nil { return err }
//	defer cleanup() // closes the state DB
//	if err := srv.Run(ctx); err != nil { return err }
//
// [New] takes a fully-populated [Config] (you supply the *sql.DB) for
// callers that share a DB across multiple subsystems or run multiple
// servers from one process. See [OpenStateDB] for opening a SQLite
// handle with the same pragmas the binary uses.
//
// # Mounting under a prefix
//
// [Server.Router] returns the chi router so the server can be embedded
// inside another HTTP application:
//
//	parent := chi.NewRouter()
//	parent.Use(myAuthMiddleware)
//	parent.Mount("/inv", srv.Router())
//
// # Configuration
//
// [RuntimeOptions] mirrors the binary's flag set (suitable for callers
// who want to expose the same surface) while [Config] holds the wired
// dependencies. S3Source is optional: when empty, discovery is disabled
// and the /partials/discovered/* routes return 503; the rest of the
// server runs normally against whatever's already in the state DB.
package server
