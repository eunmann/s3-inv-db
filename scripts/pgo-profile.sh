#!/usr/bin/env bash
# Usage: scripts/pgo-profile.sh
#
# Regenerates the profiles behind profile-guided optimization: the go
# command builds cmd/s3-inv-db-server and cmd/s3-inv-db with the
# default.pgo in each directory. The profile is the CPU profile of the
# query gate benchmarks (pkg/indexread and internal/handlers, as in
# bench/suite/query.txt), so the compiler inlines along the read paths
# the server and the CLI's query commands spend their time in. The build
# path is left out: it is bound by memory and map access, PGO did not
# move it, and its samples would crowd the query call sites out of the
# profile's hot set.
#
# Rerun it after a large change to the read path; profiles of older code
# still apply, they just cover less of it.
set -euo pipefail

root=$(git rev-parse --show-toplevel)
cd "$root"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

for pkg in pkg/indexread internal/handlers; do
	name=$(basename "$pkg")
	go test -run '^$' -bench '^BenchmarkGate' -benchtime 2s \
		-o "$tmp/$name.test" -cpuprofile "$tmp/$name.pprof" "./$pkg" >/dev/null
done
go tool pprof -proto "$tmp/indexread.pprof" "$tmp/handlers.pprof" >"$tmp/default.pgo" 2>/dev/null

for dir in cmd/s3-inv-db-server cmd/s3-inv-db; do
	cp "$tmp/default.pgo" "$dir/default.pgo"
	echo "wrote $dir/default.pgo"
done
