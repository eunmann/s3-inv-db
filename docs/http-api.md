# HTTP API

`s3-inv-db-server` exposes the index over JSON and an HTMX-driven SSR
web UI. Page actions (load/unload, browse, compare) submit via
`hx-post` / `hx-get` and the server returns HTML row/level partials
that swap in place — there is no JSON-then-reload pattern.

For startup flags and the JSON config schema see the
[README](../README.md#configuration). For why auto-load + budgeting
exist, see [overview.md](overview.md#server-behaviour).

## Routes

### HTML pages

| Route | Returns | Notes |
|---|---|---|
| `GET /` | HTML | Dashboard — counters, disk-budget gauge, auto-load summary |
| `GET /inventories` | HTML | Discovery list with per-config auto-load toggle + retention input |
| `GET /browse` | HTML or row partial | Prefix explorer; content-negotiated on `HX-Request` |
| `GET /compare` | HTML or level partial | Compare two runs at one prefix |
| `GET /help` | HTML | Built-in glossary + workflows |
| `GET /healthz` | text/plain `ok` | Liveness probe (no auth, no S3) |
| `GET /static/tailwind.css` | text/css | Embedded compiled stylesheet, ETag-revalidated |
| `GET /static/help.js` | application/javascript | Embedded help-page JS (TOC active-link + filter), ETag-revalidated |

### HTMX partials

These mutate state and return HTML for the swapped row.

| Route | Method | Notes |
|---|---|---|
| `/partials/inventory-row/{id}` | GET | Single row of the (non-discovered) inventory list |
| `/partials/inventories/{id}/load` | POST | Loads + returns updated row |
| `/partials/inventories/{id}/unload` | POST | Unloads + returns updated row |
| `/partials/inventories/{id}` | DELETE | Deletes (htmx outerHTML removes row) |
| `/partials/discovered/{src}/{id}/{run}` | GET | Latest state of one discovered run (used by SSE refresh) |
| `/partials/discovered/{src}/{id}/{run}/load` | POST | Submits an async build job; returns 202 + the row in `queued` state |
| `/partials/discovered/{src}/{id}/{run}/unload` | POST | Unload + wipe on-disk cache |
| `/partials/discovered/{src}/{id}/{run}/pin` | POST | Toggle pin state (`pinned=true|false`) on a discovered run |
| `/partials/notifications` | GET | Failure-aggregation banner; the layout polls this every 30s |

### JSON APIs

Read-only:

| Route | Notes |
|---|---|
| `GET /api/inventories/{id}` | One inventory's metadata (includes `load_duration_ns`, the wall-clock time of the most recent successful load — present and non-zero only when `state == "loaded"`) |
| `GET /api/inventories/{id}/stats?prefix=…` | Per-prefix stats |
| `GET /api/inventories/{id}/descendants?prefix=…&depth=…` | Depth-walk descendants |
| `GET /api/inventories/{id}/top?prefix=&depth=&limit=&by=bytes\|count` | Top-N descendants ranked by metric (also at `/api/top?inventory_id=…`) |
| `GET /metrics` | Prometheus text format — request counters, latency histograms, queue counters. Mounted on the main router. |
| `GET /api/discovered` | List discovered inventories (when discovery configured) |
| `GET /api/stats?inventory_id=&prefix=` | Stats for one prefix in one inventory |
| `GET /api/configurations` | Inventory configurations grouped by `<src>/<inv>` with their runs |
| `GET /api/browse?inventory_id=&prefix=&sort=&dir=&page=&page_size=` | Browse one prefix in one run |
| `GET /api/compare?from=&to=&prefix=&sort=&dir=&page=&page_size=&show_unchanged=` | Compare two runs at one prefix (includes PUT API one-time costs) |
| `GET /api/disk-budget` | Tracker counters: cap, used, reserved, available, headroom |
| `GET /api/notifications` | Aggregated poll-failure + auto-load-failure list |

Mutating:

| Route | Method | Notes |
|---|---|---|
| `/api/inventories` | POST | Register a non-discovered inventory |
| `/api/inventories/{id}/load` | POST | Synchronous load |
| `/api/inventories/{id}/unload` | POST | Unload |
| `/api/inventories/{id}` | DELETE | Remove |
| `/api/inventories/{id}/stats:batch` | POST | Body: `{"prefixes":[...],"show_tiers":bool,"estimate_cost":bool}`. Returns one row per prefix with `found=false` for misses. Cap defaults to 1000; override with `query_batch_max`. |
| `/api/inventories/{id}/pin` | POST | Toggle pin state (`pinned=true|false`); pinned runs are never auto-evicted |
| `/api/configurations/{src}/{name}/auto-load` | POST | Set auto-load + retention for a configuration (`auto_load=on|off`, `retention=N`) |
| `/api/jobs/{id}/cancel` | POST | Cancel an in-flight job |

### Server-Sent Events

| Route | Notes |
|---|---|
| `GET /api/jobs/stream` | One frame per job state change, with a `: ping` heartbeat every 15s |

The UI's discovered-row partial listens for `sse:row-<src>/<inv>/<run>`
events and re-fetches itself through the `/partials/discovered/…`
route on every state change.

## Typed identifiers

Two named string types flow through the API for compile-time safety
on the Go side; both serialise as plain JSON strings.

- `inventory.ID` — `<source-bucket>/<inventory-name>/<run-timestamp>`,
  primary key for one inventory run.
- `jobs.ID` — random hex-encoded job identifier.

## Async load contract

`POST /partials/discovered/{src}/{id}/{run}/load` returns **202** plus
the row in `queued` state and starts a background build. The row's
`hx-trigger="sse:row-<src>/<id>/<run>"` listens to `/api/jobs/stream`
and re-fetches itself on every state change; the live row shows
spinner, stage label, progress bar, and ETA. `POST /api/jobs/{id}/cancel`
signals the build context — the row swaps back with a **Retry**
button.

## Cross-origin policy

Mutating requests (POST / PUT / PATCH / DELETE) are rejected by a
same-origin middleware if `Origin` or `Referer` doesn't match the
request host. Reads are public.
