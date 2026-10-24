# Releasing

Releases are published only by the `Release` workflow
([`.github/workflows/release.yml`](../.github/workflows/release.yml)),
dispatched by hand. Pushing a tag releases nothing.

## Dry run first

```bash
gh workflow run release.yml -f version=v0.2.0
```

`dry_run` defaults to true: the run validates the commit, builds the
four archives, `SHA256SUMS` and the multi-arch image, and publishes
nothing. The archives and checksums are attached to the run as
workflow artifacts for inspection.

## Publish

```bash
gh workflow run release.yml --ref main \
  -f version=v0.2.0 -f ref=main -f dry_run=false \
  -f notes="$(cat notes.md)"
```

- `version` is `vX.Y.Z`, or `vX.Y.Z-<suffix>` for a pre-release. A
  pre-release is marked as such on GitHub and does not move the image's
  `latest` tag.
- `ref` (default `main`) is the branch, tag or SHA to release.
- `notes` is optional Markdown shown above the changelog GitHub
  generates from the merged PRs, grouped by
  [`.github/release.yml`](../.github/release.yml): PRs labelled
  `dependencies` under "Dependency updates", every other PR under
  "Changes".
- A run that publishes must be dispatched from `main` (`--ref main`);
  dry runs may use a branch to try out workflow changes.

## What the workflow refuses

The `validate` job stops the run unless all of these hold:

- the tag `version` does not exist yet;
- `ref` is a commit on `main` that is the single-parent squash merge of
  a merged PR into `main`;
- that PR's head commit has the same tree as the merge commit, so the
  PR was up to date with `main`;
- the head commit has a successful `PR Validation` run.

The tag and the GitHub release are created last, at the validated
commit, after the archives, checksums, provenance attestation and image
all succeeded. The image is pushed by its own job before that, so a run
that fails in the final job leaves `ghcr.io/eunmann/s3-inv-db-server:<version>`
pushed without a release: fix the cause and re-run the same version,
which overwrites that image tag. A draft release left behind by a failed
asset upload must be deleted before the re-run.

After the first published release, check that the
`s3-inv-db-server` package on GitHub is public, so `docker pull` works
without logging in.
