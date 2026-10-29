# HTTP API

`s3-inv-db-server` exposes the index over JSON and an HTMX-driven SSR
web UI. Page actions (load/unload, browse, compare) submit via
`hx-post` / `hx-get` and the server returns HTML row/level partials
that swap in place — there is no JSON-then-reload pattern.

For startup flags and the JSON config schema see
[configuration](configuration.md). For why auto-load and budgeting
exist, see [operations](operations.md).

## Routes

### HTML pages

| Route | Returns | Notes |
|---|---|---|
| `GET /` | HTML | Dashboard — each configuration's latest loaded run: objects, size, est. cost and change since the previous loaded run; then system status (builds, failures, auto-load, disk budget) |
| `GET /inventories` | HTML | Runs per configuration: what each loaded run holds, load/unload/pin, per-config auto-load settings when the server runs `--auto-load` |
| `GET /browse` | HTML or row partial | Prefix explorer; content-negotiated on `HX-Request` |
| `GET /compare` | HTML or level partial | Compare two runs at one prefix |
| `GET /help` | HTML | Built-in glossary + workflows |
| `GET /healthz` | text/plain `ok` | Liveness probe (no auth, no S3) |
| `GET /static/tailwind.css` | text/css | Embedded compiled stylesheet, ETag-revalidated |
| `GET /static/help.js` | application/javascript | Embedded help-page JS (TOC active-link + filter), ETag-revalidated |
| `GET /static/htmx.min.js` | application/javascript | Embedded htmx 1.9.10, so pages load no third-party script; ETag-revalidated |

### HTMX partials

These mutate state and return HTML for the swapped row.

| Route | Method | Notes |
|---|---|---|
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
| `GET /api/inventories/{id}/descendants?prefix=…&depth=…&limit=…&offset=…` | Depth-walk descendants, paged: `{"descendants":[…],"total":N,"limit":L,"offset":O}`. `limit` defaults to 1000 and is clamped to 10000; `offset` defaults to 0; `total` counts every match. Also accepts `min_count` / `min_bytes` |
| `GET /api/inventories/{id}/top?prefix=&depth=&limit=&by=bytes\|count` | Top-N descendants ranked by metric (also at `/api/top?inventory_id=…`) |
| `GET /metrics` | Prometheus text format — request counters, latency histograms, queue counters. Mounted on the main router. |
| `GET /api/discovered` | List discovered inventories (when discovery configured) |
| `GET /api/stats?inventory_id=&prefix=` | Stats for one prefix in one inventory |
| `GET /api/configurations` | Inventory configurations grouped by `<src>/<inv>` with their runs |
| `GET /api/browse?inventory_id=&prefix=&sort=&dir=&page=&page_size=` | Browse one prefix in one run; `sort` is `size`, `objects`, `cost` or `segment` (default `size`, `desc`) |
| `GET /api/compare?from=&to=&prefix=&sort=&dir=&page=&page_size=&show_unchanged=` | Compare two runs at one prefix (includes PUT API one-time costs) |
| `GET /api/disk-budget` | Tracker counters: cap, used, reserved, available, headroom |
| `GET /api/notifications` | Aggregated poll-failure + auto-load-failure list; each has the raw `message` and a one-sentence `summary` |

Mutating:

| Route | Method | Notes |
|---|---|---|
| `/api/inventories` | POST | Register a non-discovered inventory |
| `/api/inventories/{id}/load` | POST | Synchronous load |
| `/api/inventories/{id}/unload` | POST | Unload; a discovered run's cache dir is deleted |
| `/api/inventories/{id}` | DELETE | Remove; a discovered run's cache dir is deleted. **409** while the run is loading (cancel its job first) |
| `/api/inventories/{id}/stats:batch` | POST | Body: `{"prefixes":[...],"show_tiers":bool,"estimate_cost":bool}`. Returns one row per prefix with `found=false` for misses. Cap defaults to 1000; override with `query_batch_max`. |
| `/api/inventories/{id}/pin` | POST | Toggle pin state (`pinned=true|false`); pinned runs are never auto-evicted |
| `/api/configurations/{src}/{name}/auto-load` | POST | Set auto-load + retention for a configuration (`auto_load=on|off`, `retention=N`). **409** when the request changes a setting the config file's `inventories[]` entry sets. Returns JSON, except a non-htmx request whose `Accept` includes `text/html` (a script-free form submit) gets a 303 to `/inventories` on success and a plain-text error otherwise |
| `/api/indexes/refresh` | POST | List the inventory or index source now and start an auto-load poll, instead of waiting for the next interval. Returns `{"auto_load": bool}`, plus `HX-Refresh: true` for an htmx request (the inventories page's Refresh); 503 with no source configured |
| `/api/jobs/{id}/cancel` | POST | Cancel an in-flight job |

### Server-Sent Events

`GET /api/jobs/stream` sends `retry: 2000`, then a `job` frame for every
job whose run is still in progress (queued, running, or terminal while
that final state is being written) and a `synced` frame; after that:

| Event | Data |
|---|---|
| `job` | A job's JSON snapshot whenever it changes: `ID`, `InventoryID`, `State`, `Stage`, `StageLabel`, `Step` (the step within the stage, when it has steps, such as finalizing's), the current counter's `StageDone`/`StageTotal` and `StepStartedAt` (when that counter started), `Stages`, … |
| `inventory` | `{"InventoryID": …}` when a run's catalog state changes outside a job (evicted, unloaded, pinned) |
| `synced` | End of the connect snapshot |

Frames are coalesced per job and paced to one batch per 250ms, so a
reader sees each job's latest state (terminal states always arrive)
rather than every progress tick. A reader too far behind is
disconnected; it reconnects and resyncs from the snapshot. A `: ping`
comment every 15s keeps idle connections honest. At most 8 streams per
client IP (429 past that).

## Typed identifiers

Two named string types flow through the API for compile-time safety
on the Go side; both serialise as plain JSON strings.

- `inventory.ID` — `<source-bucket>/<inventory-name>/<run-timestamp>`,
  primary key for one inventory run.
- `jobs.ID` — random hex-encoded job identifier.

## Async load contract

`POST /partials/discovered/{src}/{id}/{run}/load` returns **202** plus
the row in `queued` state and starts a background build. The page's
script (`/static/live.js`) follows `/api/jobs/stream`: it updates the
row's stage, percent, time left and per-stage bar in place, and
re-fetches the row only when the job changes state or the run's catalog
state changes. `POST /api/jobs/{id}/cancel` signals the build context;
the row then shows a **Retry** button.

## Request body limits

Every handler that reads a body caps it and answers **413** when it is
exceeded: 4 KiB for `POST /api/inventories` and the form-encoded
pin / auto-load toggles; for `stats:batch`, about 6 KiB per allowed
prefix (`query_batch_max`, default 1000) — enough for the longest S3
key fully JSON-escaped.

## Cross-origin policy

Mutating requests (POST / PUT / PATCH / DELETE) are rejected by a
same-origin middleware if `Origin` or `Referer` doesn't match the
request host. Reads are public.
