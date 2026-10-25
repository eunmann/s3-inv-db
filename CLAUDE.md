# CLAUDE Workflow Guidelines

Follow this workflow for **every** change to this repo:

1. **Branching & Commits**
   - Always create a **feature branch** from `main` for any change.
   - Use **small, atomic commits** with clear, descriptive messages.
   - When the feature is complete and verified, **merge into `main`**, then **delete the merged feature branch** (local and remote).

2. **Linting**
   - Run the linter (e.g. `make lint`) **early and often**.
   - Let the linter **auto-fix** everything it can (e.g. `golangci-lint run --fix` if configured).
   - **Manually fix all remaining lint errors**. Do not ignore, silence, or disable linters without a strong reason.

3. **Testing**
   - For all new code, add **appropriate tests** (unit tests, integration tests, etc.).
   - Before declaring a feature branch “done”, run **all tests** (e.g. `make test` or equivalent) and ensure they pass.
   - Never merge code that introduces **failing or flaky tests**.

4. **Performance & Benchmarks**
   - For any change on a **hot path** or performance-sensitive area:
     - Ensure there are **benchmarks** covering the affected code.
     - Run those benchmarks **before and after** your changes.
     - Compare results and keep or improve performance; do not regress without explicit justification in the commit message.
   - Benchmarks run **only** for changes to ingestion (`make bench-ingest BASE=main`) or to the index data structures / query code (`make bench-query BASE=main`), and only that area's suite. Not for dependency bumps, server, catalog, jobs, security, lifecycle, templates, docs or tooling. See `bench/README.md` "When to run".
   - Even there, run the gate only when the change can plausibly move hot-path time, memory or allocations. A change that cannot (a correctness fix that adds a check, a cleanup call, error handling, validation, a log line, a rename) skips it and says why in one line. When unsure, run it.
   - `make bench-scale BASE=<ref>` (10M-object builds, large indexes) runs on demand for decisions the 1M fixtures can't show, and weekly on main in CI.
   - Every PR that ran a benchmark states the expected performance change in its description (**Performance** section: before → after time, memory and allocations per op for each suite entry); otherwise one line saying no benchmark was needed and why.

5. **Final Checklist Before Merge**
   - `make lint` passes with **zero** unaddressed issues.
   - `make test` (or equivalent) passes.
   - Relevant **benchmarks** have been run and compared when a change could move ingestion or query performance, and the PR description reports them (or says in one line why none was needed).
   - `/review-loop` has run on the branch, and PR Validation (its `Gate` job) is green on the final head.
   - The feature branch is merged cleanly into `main` (squash merge).
   - The merged **feature branch is deleted**.

Always follow this workflow consistently for all changes.

## North star

The correctness and performance bar every change and every review is judged against:

1. **Exact answers.** Per-prefix object counts, bytes and per-tier stats equal what a direct scan of the inventory would give, for every supported format, key encoding and versioning mode. A build that cannot guarantee that fails loudly; it never reports success with missing data.
2. **Bounded builds.** Build memory stays within `GOMEMLIMIT` by the documented spill rules, and scratch disk within a documented multiple of the input. Nothing on the build path allocates per object or per prefix without a bound.
3. **Query cost independent of index size.** Lookups are O(1); listings are O(log n + the page shown), never O(all children) per page view.
4. **Never destroy what the user had.** A failed or cancelled build leaves no partial index and never deletes data it did not create; the server never evicts, unloads or overwrites an index out from under a reader.

## Pre-release

The project is pre-release: no backwards compatibility, API versioning or index-format migrations. Change formats and APIs freely (bump the single format `Version`; old indexes are rebuilt) and note breaking changes in the PR.

## Working style

- **Never give time estimates.** No "this will take X hours/days/sessions," no "realistic for one session," no scoping language that anchors on duration. Quote the work, not the clock.
- **Never claim work is too big.** Do not propose stopping, deferring, or splitting because of perceived scope. Execute the task as given.
- **Do not short-circuit.** Do not stop mid-task list to ask for permission to continue, do not offer "checkpoint" off-ramps, do not summarize progress in lieu of doing the next item. When given a multi-task plan, run it to completion.

## Package structure

- **Domain-based, flat packages.** Name packages after the domain they own; keep the package tree shallow.
- **Split only on a clean need.** A package divides when the boundary is forced by an API contract, a circular import, a build constraint, or a clear domain seam. Size, line counts, or "lots of files" are not reasons to split. When in doubt, leave it.
