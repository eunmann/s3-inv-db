---
name: deep-research
description: "Rigorous pre-implementation research: broad repo scan, feature inventory, parallel sub-agent deep-dives, synthesis, and parallelizable task breakdown. Use when a task warrants max-effort understanding before acting."
---

# Deep Research

> **Mindset:** Be skeptical, challenge assumptions, and use first principles. Do not stop at the first match. Do not shortcut. Verify behavior by reading code — names are claims, not proof.

Systematic process for understanding the full blast radius of a change before implementation. Use for non-trivial work: new features, refactors, cross-cutting bug fixes, PR reviews.

**Skip this for trivial tasks** — typo fixes, one-line changes, and mechanical edits. The calling command is responsible for deciding whether the work warrants the full process.

## The Six Steps

### 1. High-Level Repo Review

Orient yourself to the whole repo before narrowing:

- Read `CLAUDE.md` and `README.md` for the project's stated conventions.
- Scan `docs/` for design notes (overview, index format, library API, performance).
- Survey top-level directories:
  - `cmd/` — binaries (`s3-inv-db`, `s3-inv-db-server`, `seeder`)
  - `internal/` — server-internal packages (`handlers`, `inventory`, `server`, `templates`, `seeder`, `cli`, `logctx`)
  - `pkg/` — public library packages (`indexread`, `extsort`, `triebuild`, `pricing`, `tiers`, `humanfmt`, `benchutil`, `s3fetch`, `format`, `inventory`, `membudget`, `memdiag`, `sysmem`, `logging`)
  - `infra/` — Dockerfiles and compose stack
- Identify the architectural boundaries — index build pipeline (`extsort` → `triebuild` → `indexread`) vs server runtime (`handlers` → `inventory` Manager → `indexread`) vs CLI surfaces.

The output is a mental map. Do not deep-dive yet.

### 2. List All Relevant Features

Exhaustively enumerate every subsystem, package, or feature potentially affected by the task. Err toward "relevant" — it is cheaper to rule out than to miss.

- **Index pipeline:** `pkg/extsort` (external sort + aggregation), `pkg/triebuild` (preorder trie layout), `pkg/indexread` (mmap reader), `cmd/s3-inv-db build`.
- **Query surfaces:** `pkg/indexread` reads, `cmd/s3-inv-db query`, `internal/handlers` (HTTP API + HTML), `internal/templates` (renderer + HTML pages/partials).
- **Server runtime:** `cmd/s3-inv-db-server`, `internal/server` (routes, middleware, lifecycle), `internal/handlers`, `internal/inventory` (Manager + state machine), `internal/templates`.
- **Tier / cost:** `pkg/tiers`, `pkg/pricing`. Cost calculation is consumed by both the CLI and the HTTP handlers.
- **Synthetic data:** `pkg/benchutil`, `internal/seeder`, `cmd/s3-inv-db-seeder`. Tests use the seeder to build real indexes against `t.TempDir()`.
- **Cross-cutting:** `internal/logctx` (logger plumbing), `pkg/membudget` / `pkg/sysmem` / `pkg/memdiag` (memory budget enforcement), `pkg/humanfmt` / `pkg/format`.
- **Tooling / infra:** `Makefile`, `.golangci.yml`, `.air.toml`, `infra/Dockerfile*`, `infra/docker-compose.yml`.

Output: a list with a one-line note per item on why it matters.

### 3. Sub-Agent Deep Dive Per Feature

Spawn one `Explore` subagent per feature on the list, **in parallel**, each with:

- The feature's directory and the key files it should start from.
- The specific questions relevant to the task (what data flows, what invariants hold, what assumptions are embedded).
- Explicit instruction: *"Do not stop at the first match. Trace every call path. Be skeptical of naming — verify behavior by reading code."*

Wait for **all** sub-agents to complete before synthesizing. Do not proceed with partial findings.

### 4. Synthesize

Bring all findings together. Apply first-principles thinking:

- What does the codebase actually do today — as evidenced by code — versus what the user (or prior docs) assumed?
- Which patterns are consistent across packages? Which packages diverge, and why?
- What is the true blast radius of the proposed change? In particular: do `extsort` / `triebuild` / `indexread` agree on the on-disk format? Does a server-side change need a corresponding CLI update?
- Are the user's stated requirements achievable with the existing architecture, or do they demand foundational changes?

Challenge every assumption that lacks a code citation. Present alternatives when the stated approach is wrong, suboptimal, or more expensive than necessary.

### 5. Plan Parallelizable Implementation

Decompose the work into tasks. For each task, determine:

- **Independent** — can run in parallel with others (distinct files, distinct subsystems, no shared types being introduced).
- **Dependent** — must run after another task (index-format change before reader change, type before consumer).

Build the dependency graph. Schedule parallel batches.

For research-only commands (`/create-issue`, `/review-pr`, `/review-loop`), stop here — the output is an artifact, not code.

### 6. Spawn Sub-Agents and Wait

For each parallelizable batch, spawn one subagent per task with a crisp prompt:

- Exact files to change and the modification required.
- Tests that must accompany the change.
- Acceptance criteria the agent can verify before reporting done.

Wait for **all** agents in the batch to complete. Verify their output before starting the next batch — do not trust an agent's summary without reading its diff.

## Mindset Reminders

- **Be skeptical.** A name is a claim, not proof. A comment is a claim, not proof. Verify behavior by reading the code path end to end.
- **Challenge assumptions.** When the user (or a prior memory, or a doc) says "X works like Y," check that Y actually happens in the code. Cite line numbers.
- **Use first principles.** Decompose each claim to the evidence that makes it true. If there is no evidence, it is an assumption — treat it as one.
- **No shortcuts.** Stopping at the first match is how bugs ship. Follow every thread to its end.
- **Max effort.** If a task is worth doing, it is worth understanding completely first. The research cost is small compared to shipping the wrong thing.

## When Not to Use

- Typo fixes, one-line logic corrections, trivial renames.
- Mechanical commands that have no synthesis step (`/create-pr`, `/checklist`).
- Narrow PR-comment responses where the comment already locates the issue.

The calling command decides. If in doubt, do the research — wasted research is cheaper than a wrong implementation.
