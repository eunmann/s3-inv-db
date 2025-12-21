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

5. **Final Checklist Before Merge**
   - `make lint` passes with **zero** unaddressed issues.
   - `make test` (or equivalent) passes.
   - Relevant **benchmarks** have been run and compared when performance is affected.
   - The feature branch is merged cleanly into `main`.
   - The merged **feature branch is deleted**.

Always follow this workflow consistently for all changes.
