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
   - The comparison is `make bench-ab BASE=main`: base and head on the same machine, interleaved, judged by benchstat (see `bench/README.md`). Benchmarks run only for performance changes, never as routine CI.

5. **Final Checklist Before Merge**
   - `make lint` passes with **zero** unaddressed issues.
   - `make test` (or equivalent) passes.
   - Relevant **benchmarks** have been run and compared when performance is affected.
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

## Working style

- **Never give time estimates.** No "this will take X hours/days/sessions," no "realistic for one session," no scoping language that anchors on duration. Quote the work, not the clock.
- **Never claim work is too big.** Do not propose stopping, deferring, or splitting because of perceived scope. Execute the task as given.
- **Do not short-circuit.** Do not stop mid-task list to ask for permission to continue, do not offer "checkpoint" off-ramps, do not summarize progress in lieu of doing the next item. When given a multi-task plan, run it to completion.

## Package structure

- **Domain-based, flat packages.** Name packages after the domain they own; keep the package tree shallow.
- **Split only on a clean need.** A package divides when the boundary is forced by an API contract, a circular import, a build constraint, or a clear domain seam. Size, line counts, or "lots of files" are not reasons to split. When in doubt, leave it.
