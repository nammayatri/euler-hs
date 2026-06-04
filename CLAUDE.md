# CLAUDE.md — euler-hs

Guidance for Claude Code when working in this repo. **Primary focus: the `KVConnector` (the Redis-as-KV layer over Postgres) and its multi-cloud (AWS + GCP) behavior**, because that is where almost all subtle, production-impacting behavior lives. Read this before changing anything under `src/EulerHS/KVConnector/`.

> ⚠️ This is a **shared library** consumed by every Namma Yatri service (rider-app/BAP, dynamic-offer-driver-app/BPP, dashboards, FRFS). A change here has **fleet-wide blast radius** across all tables and both clouds. Build with `-Werror`, and treat any change to `Flow.hs`/`Utils.hs` as needing UAT validation before prod.

---

## 1. What this library is

euler-hs provides `L.Flow`, the effect monad, plus the **KVConnector**: a write-through/read-through cache that makes Postgres tables behave as a **KV-first store in Redis**, with an async **drain stream** to Postgres.

- Mental model: **Redis holds whole-object row snapshots** keyed by primary key; the DB is the durable backstop, updated asynchronously via a Redis stream (`ecRedisDBStream`, drained by a separate drainer service).
- A table is "KV-enabled" via the consuming app's config (`system_configs.kv_configs` → `Tables` option in shared-kernel). If disabled, the table is DB-only.
- Build: `cabal build` (inside `nix develop` / `direnv allow`). `-Wall -Werror`. `package.yaml` (hpack) is source of truth, not the `.cabal`.

---

## 2. Key files (`src/EulerHS/KVConnector/`)

| File | Contains |
|---|---|
| `Flow.hs` | All the connector entrypoints + the core read/write/delete logic. **Most important file.** |
| `Utils.hs` | `findOneFromRedis` helpers, `updateModel` (the merge), `getDataFromPKeysRedis`, `getPrimaryKeyFromFieldsAndValues`, shard/hashtag, `getUniqueDBRes`/dedup, decode |
| `Encoding.hs` | `encode_` / `decodeLiveOrDead` — the `CBOR`/`JSON` self-describing per-value header |
| `Compression.hs` | optional value compression |
| `DBSync.hs` | builds the DB-drain command (`getUpdateQuery`) pushed to the stream |
| `Types.hs` | `MeshConfig` (the per-table/per-request config), `MeshResult` |
| `InMemConfig/` | in-memory config cache path (memcache-enabled tables) — **separate, primary-only** |

The **`MeshConfig`** carries the multi-cloud knobs: `kvRedis` (this pod's/primary cloud), `kvRedisSecondary` (the other cloud), `secondaryRedisEnabled`, `redisTtl`, `tableShardModRange`, `meshEnabled`, `cerealEnabled`. It is **constructed per-call in shared-kernel** (`Kernel.Beam.Functions`), not here — see §4.

---

## 3. The multi-cloud model (READ THIS FIRST)

Production runs **two clouds, AWS + GCP**, each with its **own Redis** (NOT replicated) and a Postgres (AWS = single writer, GCP = logical replica). The KVConnector sees only:

- **primary** = `meshCfg.kvRedis` = **the cloud the pod is running in** (pod-local).
- **secondary** = `meshCfg.kvRedisSecondary` = **the other cloud**.
- `secondaryRedisEnabled` gates whether cross-cloud (secondary) is consulted.

**Invariants that cause most multi-cloud surprises:**
1. **Redis is NOT replicated cross-cloud.** Only Postgres replicates (AWS→GCP). So a row can exist in one cloud's Redis, both, or neither — independently.
2. **Reads:** secondary is consulted **only on a primary MISS** (see `findOneFromRedis`). A **stale primary HIT is returned blindly** — the other cloud is never checked, no `updatedAt`/version comparison. (Architecture "rule #4": *stale cache hit bypasses secondary*.)
3. **Writes:** go to **the cloud where the row is found** (primary-first; secondary only if primary misses). If the row exists in **both** clouds, a single update touches **only one** → the copies **diverge** with no reconciliation ("rule #3": *updates happen only in the Redis where data was found, no cross-Redis sync*).
4. **New copies are created pod-local** (create + recache-on-miss). The other cloud never gets a copy from a write unless it already had one.
5. There is **no version/`updatedAt` tiebreak anywhere** in the read merge or write routing. "Where found wins"; "primary hit wins."

The consequence: once a row is cached in **both** clouds (or stale in the cloud being read), the system can serve a stale value indefinitely. This is the root of the "`onRide=false` after accept" class of bugs.

---

## 4. How `MeshConfig.secondaryRedisEnabled` actually gets set (shared-kernel)

The connector here only *reads* `meshCfg.secondaryRedisEnabled`; it's set in `shared-kernel/lib/mobility-core/src/Kernel/Beam/Functions.hs`:

- **`findOneWithKV`, `updateOneWithKV`, `updateWithKV`, `createWithKV`** → `withUpdatedMeshConfig` → `setMeshConfig` → `secondaryRedisEnabled = (enableSecondaryCloudRead && table ∈ tablesForSecondaryCloudRead) || enableAllTablesForSecondaryCloudRead`. **(honors the global flag)**
- **`findAllWithKV`, `findAllWithOptionsKV`** → `withUpdatedMeshConfigForFindAll` → `setMeshConfigForFindAll` → `secondaryRedisEnabled = MultiCloudEnabled (local option, set only inside `runInMultiCloud`) || enableFindAllForMultiCloud`. **(does NOT honor the global flag)** — so plain `findAllWithKV` is **primary-only** unless explicitly multi-cloud.
- **`findAllWithKVAndConditionalDB`** → `withUpdatedMeshConfig` (the *normal* one) → **does** honor the global flag (different from `findAllWithKV`!).

**Gotcha:** two functions both named "findAll…" have *opposite* secondary-read behavior. Check which wrapper a query uses before assuming cross-cloud reads happen.

`redisTtl` default is **18000s (5h)** (base `MeshConfig`), overridable per table via `kvTablesTtl`. Shard = `getShardedHashTag` (CRC16 `keyToSlot` mod `tableShardModRange`) — **deterministic only if `tableShardModRange`/`defaultShardMod` are identical on both clouds** (they come from per-cloud `kv_configs`).

---

## 5. Env flags (all default **False** unless noted)

| Env var | Gates | Effect |
|---|---|---|
| `IS_RECACHING_ENABLED` | the **update-path** recache (`updateInKVOrSQL` `Flow.hs:428`; `updateKVAndDBResults*` `:754`/`:841`) | ON: an update on a row missing from Redis pulls it from DB and **recaches to pod-local**. OFF: update-on-miss goes **DB-only**, no Redis write. **This flag controls whether a wrong-cloud copy can be born from an update.** |
| `IS_CACHING_DB_FIND_ENABLED` | the **find-path** recache (`Flow.hs:961`, `createInRedis`) | ON: a find that misses Redis and reads DB recaches it. OFF: reads never write Redis. |
| `COMPRESSION_ALLOWED` | value compression on write | |
| `REDIS_CALLS_SOFT/HARD_LIMIT` | per-op Redis call count limits | |
| others | `IS_*_LOGS_*`, `KV_METRIC_ENABLED`, `CONFIG_STREAM_*`, `IS_WHERE_CLAUSE_DIFF_CHECK_ENABLED` | logging/metrics/in-mem-config-stream tuning |

`cerealEnabled` (in `MeshConfig`, not env) picks CBOR vs JSON encoding — but decode is **self-describing** per value (4-byte `CBOR`/`JSON` header in `Encoding.hs`), so mixed-mode writers don't corrupt.

---

## 6. Function behavior reference (the part to get right)

### Reads
- **`findWithKVConnector`** (`Flow.hs:927`) → `findOneFromRedis` (`:1091`):
  - Fetches from **primary** (`getDataFromPKeysRedis meshCfg.kvRedis`, `:1105`).
  - Checks **secondary ONLY** when `null primaryMatching && null primaryDeadRows && meshEnabled && secondaryRedisEnabled` (`:1119`). **A matching primary row short-circuits — secondary is never read.**
  - Both miss → DB; recache only if `IS_CACHING_DB_FIND_ENABLED`.
  - **Implication:** stale primary copy is returned over a fresher secondary copy. No version compare.
- **`findAllWithKVAndConditionalDBInternal`** (`Flow.hs:1529`): when `secondaryRedisEnabled`, reads **both** clouds in parallel (`createMultiCloudConfigs`, `callKVKVAsync`) and `matchAndDeduplicateKVRows`. Dedup is **by primary key only** — primary wins over secondary; KV (possibly stale) wins over fresh DB (`getUniqueDBRes` excludes DB rows whose PK is in *any* KV row, live **or dead**).
- **`searchInMemoryCache`** (memcache tables): **primary-only**, bypasses the multi-cloud merge entirely.

### Writes — single row
- **`updateOneWithKV` → `updateWithKVConnector` → `modifyOneKV`** (`Flow.hs:287`, `:334`):
  - Check **primary** only (`primaryOnlyCfg`, `:339`).
  - Primary HIT → `processKvResult … meshCfg.kvRedis` → **update primary only** (`:353`); **secondary never touched** → dual copies diverge.
  - Primary miss + `secondaryRedisEnabled` → check secondary; if found → update **secondary** (where found, `:348`).
  - Both miss → `updateInKVOrSQL`: if `IS_RECACHING_ENABLED` → `reCacheDBRows` + `updateObjectRedis` (writes pod-local); else **DB-only** (`runUpdateOrDelete`, `:445`).
  - The merge is `updateModel obj updVals` (`Utils.hs:102`): serialize full base obj → JSON object → insert only changed keys. **Additive — cannot null a field the base had.** A partial/stale base is *carried forward*, not fixed (updates never re-read DB on a primary hit).

### Writes — multi row
- **`updateWithKV` → `updateAllReturningWithKVConnector` → `updateKVAndDBResultsMultiCloud`** (`Flow.hs:627`, `:825`):
  - When `secondaryRedisEnabled`: reads primary+secondary+DB; updates **primary-matched rows in primary** (`:850`) **and secondary-matched rows in secondary** (`:852`) → keeps **both existing copies in sync**.
  - **DB-only rows (`uniqueDbRows`, in neither Redis): recache + write to POD-LOCAL only** (`:845`/`:854`, gated by `IS_RECACHING_ENABLED`) — this is a way a wrong-cloud copy is created by a cross-cloud batch write.

### Create / Delete
- **`createWoReturingKVConnector` → `createKV` / `createInRedis`** (`Flow.hs:148`, `:198`): **always pod-local, unconditional** (no secondary check). A create on a cloud that doesn't have the row makes a (possibly divergent) second copy. Auto-increment IDs (`getAutoIncId`, `Utils.hs`) are **per-cloud `INCR`** → cross-cloud collision risk for any auto-inc PK table.
- **`deleteWithKVConnector` / `deleteReturningWithKVConnector` → `modifyOneKV`** (single-row delete): **deletes only the cloud where the row is found** → a copy on the other cloud survives as a **live orphan** after the DB row is gone. `deleteAllReturningWithKVConnector` (bulk) deletes in **both** clouds where found. **The two delete APIs differ cross-cloud.** Also: `deleteObjectRedis` writes a **tombstone** (it does NOT clean the secondary-index `sKey` sets).

### Helpers
- **`reCacheDBRows`** (`Flow.hs:1711`): writes the **secondary-index sets** (`sadd sKey pKey`) to `meshCfg.kvRedis` (pod-local) — does **not** take a `redisConn`, does **not** write the row value (that's `updateObjectRedis`). TTL on `sKey` set and on the value `pKey` are written by **separate, non-atomic** commands → possible TTL skew.
- **`updateObjectRedis`** (`Flow.hs:483`): writes the **full merged row** value via `setexTx pKey redisTtl …` to the passed `redisConn`, inside one `multiExecWithHash` together with the `xaddTx` drain-stream entry (so value + DB-stream land atomically on the **same** cloud). `modifySKeysRedis` maintains secondary keys on the **same** `redisConn`.

---

## 7. Tombstones ("dead rows")

A delete does **not** `DEL` the key — it writes a **dead/tombstone marker** at the same `pKey` (with `redisTtl`). Three states per key: **live** `([row],[])`, **dead/deleted** `([],[tombstone])`, **missing** `([],[])`. The tombstone lets reads return "deleted" authoritatively and the delete drain to DB; without it a deleted row would look like a miss and get resurrected from DB. Tombstone state is **per-cloud and not reconciled** — a delete that tombstoned one cloud leaves the other cloud's copy live.

---

## 8. Multi-cloud gotchas / known sharp edges (verified)

When touching this code, assume these are live in prod:

1. **Stale primary HIT bypasses secondary on reads** (`findOneFromRedis:1119`). "Secondary read enabled" only helps on a **miss**, never on a stale hit. No `updatedAt` reconciliation.
2. **`updateOneWithKV` is single-cloud (where-found)** (`modifyOneKV:353`). Dual copies diverge; the per-driver "guard" pattern (read home cloud, route via `runInMultiCloudRedisWrite`) lives in the *app*, not here.
3. **Wrong-cloud copy genesis = a write on a both-miss row** (TTL expiry on both clouds) + `IS_RECACHING_ENABLED` → recache pod-local. Two near-simultaneous writes (one per cloud) during a both-miss window create a **dual copy**.
4. **Updates never re-read DB on a primary hit** → a stale/partial copy is **self-perpetuating** (kept warm by `setexTx` TTL refresh, never corrected). Only a TTL gap, a clause-miss (forces DB branch), or an explicit invalidation heals it.
5. **`findAllWithKV` ≠ `findAllWithKVAndConditionalDB`** on secondary-read (see §4). Easy to assume cross-cloud and be wrong.
6. **Single-row delete orphans the other cloud** (§6). Use the bulk path or a multi-cloud delete if cross-cloud correctness matters.
7. **`reCacheDBRows` is hardcoded pod-local** and writes only index sets → value/index can land on different clouds in some `updateInKVOrSQL` paths; TTL skew between `sKey` set and value.
8. **Shard determinism depends on `tableShardModRange` parity** across clouds; mismatch → same PK hashes to a different `{shard-N}` → silent cross-cloud miss.
9. **Per-cloud auto-increment** → PK collisions for auto-inc tables written on both clouds.
10. A partial/null-field row almost always means a **stale Redis snapshot written by an older binary** (missing newer JSON keys → decode to null), preserved by the additive `updateModel` merge — **not** a connector bug. Fix by recaching from DB / invalidating the stale key.

---

## 9. When changing `Flow.hs` / `Utils.hs`

- The correct multi-cloud fix direction (if asked) is **version/`updatedAt`-aware reconciliation** on reads, and **write-to-all-existing-copies** (or home-cloud-aware routing) on writes — not point patches.
- Preserve the **`updateModel` additive merge** and the **whole-object snapshot** invariant — many callers rely on KV holding the full row.
- Keep value write + drain-stream `xadd` in the **same `multiExecWithHash`** (atomic, same cloud). Don't split them across clouds.
- `-Werror`: watch unused imports; `show` is Universum-polymorphic (`Show a, IsString b => a -> b`) so `"…" <> show x` yields `Text` — fine, don't add `T.pack`.
- Any behavioral change → **UAT first**, gate risky changes behind an env flag (follow the `IS_RECACHING_ENABLED` pattern).

---

## 10. Related repos / context

- **shared-kernel** (`Kernel.Beam.Functions`): builds `MeshConfig`, the `findOneWithKV`/`updateOneWithKV`/etc. wrappers, `runInMultiCloudRedisWrite`, `lookupCloudType`. The secondary-enable decision lives there, not here.
- **nammayatri** backend: the consumers (rider-app/BAP, driver-app/BPP). App-layer multi-cloud guard reference: `Lib/DriverCoins/Coins.hs` (`lookupCloudType` vs `driver.cloudType` → `runInMultiCloudRedisWrite`).
- Multi-cloud architecture rules (drivers poll home cloud; BAP→BPP cross-cloud by URL; updates only in the Redis where found; secondary only on miss; Redis not replicated; DB replicates AWS→GCP).
