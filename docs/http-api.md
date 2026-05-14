# HTTP API

`s3-inv-db-server` exposes the same query surface as the CLI plus an
HTMX-driven SSR web UI. Page actions (load/unload an inventory, drill
into a prefix, compare runs) submit via `hx-post` / `hx-get`; the
server returns HTML row/level partials that swap in place — no
JSON-and-reload patterns.

## Run

```bash
# Standalone, listening on :8080, no discovery configured
s3-inv-db-server

# With S3 discovery: the server walks the s3:// source and shows
# every inventory + run + load state in the UI
s3-inv-db-server \
  --addr :8080 \
  --s3-source s3://my-bucket/inventory-data/ \
  --cache-dir /var/cache/s3inv
```

## Routes

### HTML pages

| Route | Returns | Notes |
|---|---|---|
| `GET /` | HTML | Dashboard — counters + recent inventories |
| `GET /inventories` | HTML | Discovery list (when `--s3-source` is set) |
| `GET /browse` | HTML or row partial | Prefix explorer; content-negotiated on `HX-Request` |
| `GET /diff` | HTML or level partial | Compare two runs at one prefix |
| `GET /help` | HTML | Built-in glossary + workflows |
| `GET /healthz` | text/plain `ok` | Liveness probe (no auth, no S3) |
| `GET /static/tailwind.css` | text/css | Embedded compiled stylesheet, ETag-revalidated |

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

### JSON APIs

Read-only:

| Route | Notes |
|---|---|
| `GET /api/inventories` | Flat list of managed inventories |
| `GET /api/inventories/{id}` | One inventory's metadata |
| `GET /api/inventories/{id}/stats?prefix=…` | Per-prefix stats |
| `GET /api/inventories/{id}/descendants?prefix=…&depth=…` | Depth-walk descendants |
| `GET /api/discovered` | List discovered inventories (when discovery configured) |
| `GET /api/stats?inventory_id=&prefix=` | Stats for one prefix in one inventory |
| `GET /api/configurations` | Inventory configurations grouped by `<src>/<inv>` with their runs |
| `GET /api/browse?inventory_id=&prefix=&sort=&dir=&page=&page_size=` | Browse one prefix in one run |
| `GET /api/diff?from=&to=&prefix=&sort=&dir=&page=&page_size=&show_unchanged=` | Compare two runs at one prefix |

Mutating:

| Route | Method | Notes |
|---|---|---|
| `/api/inventories` | POST | Register a non-discovered inventory |
| `/api/inventories/{id}/load` | POST | Synchronous load |
| `/api/inventories/{id}/unload` | POST | Unload |
| `/api/inventories/{id}` | DELETE | Remove |
| `/api/jobs/{id}/cancel` | POST | Cancel an in-flight job |

### Server-Sent Events

| Route | Notes |
|---|---|
| `GET /api/jobs/stream` | One frame per job state change, with a `: ping` heartbeat every 15s |

The UI's discovered-row partial listens for `sse:row-<src>/<inv>/<run>`
events and re-fetches itself through the `/partials/discovered/…`
route on every state change.

## Typed identifiers

All IDs that flow through the API are stable strings. Two named
types exist on the Go side for compile-time safety:

- `inventory.ID` — formatted `<source-bucket>/<inventory-name>/<run-timestamp>`,
  used as the primary key for one inventory run.
- `jobs.ID` — random hex-encoded job identifier.

Both serialise as plain JSON strings.

## Async load flow

A click on **Load** for a discovered inventory:

1. `POST /partials/discovered/{src}/{id}/{run}/load` → **202** + the
   row in `queued` state. No waiting for the build pipeline.
2. The row's `hx-trigger="sse:row-<src>/<id>/<run>"` listens to
   `/api/jobs/stream` and re-fetches itself on every state change.
3. A spinner + stage label (`Downloading & parsing`) + progress bar
   + ETA render alongside the row's State chip while the job is live.
4. **Cancel** posts to `/api/jobs/{job-id}/cancel`; the build context
   is signalled, the goroutine winds down, and the row swaps back
   with a **Retry** button.

## Persistence

Inventory state + job history live in `$CACHE_DIR/state.db` (SQLite,
WAL mode, pure-Go driver). On boot the server rehydrates the
in-memory Manager from this file and marks any job left running by
the previous process as `aborted`. Inventories caught mid-load get
flipped to `error` so the UI shows Retry instead of a forever
spinner.

## Cross-origin policy

Mutating requests (POST / PUT / PATCH / DELETE) are rejected by a
same-origin middleware if `Origin` or `Referer` doesn't match the
request host. Reads are public.
