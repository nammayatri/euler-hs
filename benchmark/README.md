# KV findAll benchmark

Harness for measuring where latency comes from when KV is enabled on an `In [pks]`
style `findAll` query (the shape used by e.g. `getDriverInfosWithCond` in
nammayatri's dynamic-offer-driver-app).

Seeds a synthetic table with a configurable mix of rows live in Redis vs only in
Postgres, then times several candidate `findAll` strategies — pure DB, pure KV,
the current production `callKVDBAsync` path, and several experimental variants
(serial fallback, race-and-cancel, optimistic KV).

## What you'll need

- Haskell toolchain via the project's nix flake (`nix develop` from repo root)
- Local Postgres 14 (Homebrew: `brew install postgresql@14`)
- Local Redis 7+ (Homebrew: `brew install redis`)
- `redis-cli` on PATH

## 1. Set up Postgres

```sh
# One-shot start; the launchd-based `brew services` route often fails to read
# the data dir as a different user — start it directly as your own user:
/opt/homebrew/opt/postgresql@14/bin/pg_ctl \
    -D /opt/homebrew/var/postgresql@14 \
    -l /tmp/pg.log \
    start

createdb -h localhost -p 5432 -U "$(whoami)" bench_kv
pg_isready -h localhost -p 5432
```

The bench connects as the OS user with empty password; if your setup differs,
edit `pgCfg` in `benchmark/EulerHS/Test.hs`.

## 2. Set up a 10-master Redis cluster

Run the helper:

```sh
./benchmark/scripts/setup-redis-cluster.sh
```

What it does:

- Spawns 10 `redis-server` processes on ports 7100–7109 (config under
  `/Users/$USER/misc/tmp/redis-cluster/<port>`)
- Forms a cluster with `--cluster-replicas 0` (10 masters, no replicas — max
  slot fanout for perf testing; **not** a prod topology)
- Verifies all 16384 slots are covered

Sanity check after:

```sh
redis-cli -p 7100 cluster info   # cluster_state:ok
```

To tear the cluster down:

```sh
./benchmark/scripts/teardown-redis-cluster.sh
```

## 3. Build and run

```sh
# From repo root
nix develop --command cabal build bench-kv
nix develop --command cabal run bench-kv +RTS -N -RTS
```

Output is TSV — `mode`, `n`, `kvFrac`, `p50_us`, `p95_us`, `p99_us`, `mean_us`,
one row per (mode, scenario) combination. Preceded by a `# scenario ...` header
for each scenario block.

## What the modes measure

Every mode runs the same query — `id IN [...]` on the seeded primary keys.
That's the shape `getDriverInfosWithCond` actually uses in production.

| mode | what it does | correctness |
|---|---|---|
| `pureDb` | `findAllWithDb`; skip KV entirely | ❌ misses KV-live rows |
| `pureKvPipelined` | `redisFindAll` only; skip SQL | ❌ misses DB-only rows |
| `kvAsyncCurrent` | Current production path: `callKVDBAsync` fires KV and SQL in parallel and awaits both | ✅ |
| `kvEarlyReturn` | Serial: KV first; fetch only missing PKs from SQL | ✅ |
| `kvNoSecondary` | Same as `kvAsyncCurrent` with `secondaryRedisEnabled=False` | ✅ |
| `kvRaceEarlyCancel` | Fork both; on KV-complete, `cancelAwaitable` the SQL leg | ✅ |
| `kvOptimistic` | Fork both; any non-empty KV → cancel SQL + return | ❌ misses on KV-partial |

## Scenarios

Controlled by the `scenarios` list in `Test.hs` — each is `(n, kvFrac, alsoAllInDb)`:

- `alsoAllInDb=False` — split: first `n*kvFrac` rows in KV only, rest in DB only
- `alsoAllInDb=True` — every row in DB **and** the KV-hot portion also in Redis.
  Models "drainer caught up" prod state where both stores have the same data.

Tune the scenarios list, the `reps` count (per-mode iterations — default 100),
and the MeshConfig (shard range, compression, etc.) in `Test.hs`.

## Key findings (current hardware + local cluster, YMMV)

At n=1000, 100 reps, typical numbers:

- `pureKvPipelined` ≈ 1700 μs flat — fastest, but unsafe in general
- `pureDb` ≈ 1800 μs (mixed state) to 3000 μs (drainer caught up)
- `kvAsyncCurrent` ≈ 3300 μs (mixed) to 4300 μs (drainer caught up)
- `kvRaceEarlyCancel` ≈ `kvAsyncCurrent`, sometimes slightly slower
- `kvOptimistic` is consistently slower

**The fundamental reason** `kvRaceEarlyCancel` / `kvOptimistic` don't deliver a
speedup: GHC's `killThread` is delivered via `throwTo`, which is deferred while
the target thread is inside a C FFI call. The SQL leg is inside `libpq`'s
blocking recv on the Postgres socket, so the exception only lands once libpq
returns — by which time the query is mostly done. `cancelAwaitable` prevents
zombie threads leaking past the main flow (fixes the pool-backpressure bug vs
"just skip await") but doesn't interrupt SQL mid-query.

**Practical consequence**: in this stack, the only way to make KV findAll
*faster* than pure SQL is to never fire SQL in the first place. That's a
correctness choice, not a perf tweak.

## Library-level changes this depends on

These live in `src/` (not `benchmark/`) because they're real library changes,
not bench-local code:

- `EulerHS.Common.Awaitable` — now carries a `ThreadId` alongside the `MVar`, plus
  a `cancelAwaitable :: Awaitable s -> IO ()` helper
- `EulerHS.Framework.Interpreter` — `Fork` stores the `tid`; `Await` ignores it
- `EulerHS.KVConnector.Flow` — `redisFindAll` added to module exports so the
  bench can call it directly

The `cancelAwaitable` patch is small, correct, and prevents zombie-thread leaks
in any fork+skip-await pattern in the codebase. Keep or revert independently
of the benchmark.
