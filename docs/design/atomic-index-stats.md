# Design: Collect index statistics atomically with schema change

## Problem

When a new index is created or altered on a table, SQLite starts with default
(essentially empty) statistics for that index. Until a subsequent `ANALYZE`
runs, the query planner has no cardinality/selectivity data for the new index
and can pick poor plans — skipping the index entirely, choosing a bad join
order, or misjudging range/skew selectivity.

Today the schema-change path only *invalidates* stats: `scdone_sc_analyze`
bumps `gbl_analyze_gen`, which forces query-plan caches to re-plan. But there is
no fresh stat *collection* — the new index simply has no stats until autoanalyze
or a manual `ANALYZE` eventually runs.

## Goal

Collect statistics for newly built indexes as part of the schema change, such
that on **every node** the good stats are visible no later than the moment the
new index becomes queryable. No window in which a client can see the new index
with default stats.

## Key correctness constraint: replication ordering

It is **not** sufficient to run `ANALYZE` right after the schema change commits
on the master. Consider:

1. Master commits the schema change (new index visible).
2. Schema-change transaction replicates to replicant R.
3. A client queries R against the new index — with default stats — and gets a
   bad plan.
4. Only later does the stats transaction arrive at R.

Because replication is **ordered**, the fix is to ensure the stats transaction
commits *before* the schema-change transaction on the master. Then every
replicant receives (and applies) the stats before it receives the schema
change, closing the window on all nodes.

Concretely: the stats must be committed before `finalize_alter_table` runs.

## Options considered

| # | Approach | Verdict |
|---|----------|---------|
| 1 | Run `ANALYZE` synchronously after `finalize_alter_table` commits, before returning to client | **Rejected.** Replication race above — another client on a replicant can query before stats replicate. |
| 2 | Write `sqlite_stat1`/`sqlite_stat4` records directly as BDB ops inside the finalize transaction (`transac`) | Truly atomic, but requires reimplementing stat serialization — especially `sqlite_stat4`'s binary key-encoded `sample` blob — at the BDB level. High complexity. |
| 3 | Run analyze against the pre-commit `newdb`, commit the stats in their own transaction *before* finalize | **Chosen.** Reuses the entire existing analyze pipeline; solves the replication race; requires a bypass of `dispatch_sql_query` and backout cleanup. |

### Why stat4 matters (and why option 2's cost is real)

`sqlite_stat1` stores average selectivity per index-column-prefix — enough for
the planner to decide whether to use an index at all. `sqlite_stat4` stores
sampled rows with per-value `nEq`/`nLt`/`nDLt` counts, and is what the planner
uses for **skewed distributions** and **range predicates**. For a new index on
a skewed column or one used in range scans, stat1 alone can still yield a bad
plan. So we want both stat1 and stat4.

The `sqlite_stat4.sample` column (see `csc2files/comdb2_stats4.csc2`) is a blob
holding a complete index key in **SQLite's internal Vdbe key encoding**
(varint, affinity-aware) — not comdb2's BDB key format. Producing it outside
SQLite means reimplementing that encoding. This is the crux of why option 2 is
expensive and why option 3 (which lets the VDBE do the encoding) is preferred.

## Chosen design (Option 3)

Run the existing SQLite `analyzesqlite` command against the **pre-commit
`newdb`** temp table, so the analysis sees the new indexes and writes correctly
encoded stat1 + stat4 rows under the real table/index names, committing in its
own transaction before `finalize_alter_table`.

### Background: how the pieces fit

- **Rootpages.** comdb2 repurposes SQLite's "root page" concept. `thd->rootpages`
  (`master_entry_t[]`) maps each SQLite rootpage number to a comdb2
  `(dbtable, index)` pair. When the VDBE runs `OP_OpenRead <tnum>`, comdb2's
  btree layer indexes `thd->rootpages[tnum]` (`get_sqlite_db`) to find the BDB
  handle to open a cursor against. Whatever is loaded there defines both which
  objects SQLite sees and which physical files its cursors read.

- **`sqlite3Analyze` loop.** For `ANALYZE <table>`, SQLite locates the table
  (`sqlite3LocateTable`) and iterates `pTab->pIndex` — one `OP_OpenRead` +
  stat computation per index (`analyze.c:1307`). The set of indexes analyzed is
  exactly what is present in the loaded schema.

- **Sampling.** For large tables, `sample_indexes(dbtable*)` pre-scans each
  index into an in-memory sampler; during analyze, cursor opens on a sampled
  index are redirected to the sampler (`is_sampled_idx`, `sqlglue.c:7364`).
  This redirects the *data source* only — it does **not** make new indexes
  visible to the analyze loop. Schema visibility still comes from rootpages.

- **`sql_syntax_check` (`sqlglue.c:2278`).** The existing precedent for making a
  *non-live* `newdb` visible to SQLite. It calls `create_sqlmaster_record(newdb)`
  → `create_master_entry_array(&newdb, 1, ...)` → `get_copy_rootpages_custom()`
  to load `newdb`'s entries, then opens the connection with
  `sqlite3_open_serial` and runs SQL via `sqlite3_exec` **directly** — never
  through `dispatch_sql_query`.

### The obstacle

`run_internal_sql_clnt` → `dispatch_sql_query` calls
`get_copy_rootpages_nolock` at the start of every query (`sqlinterfaces.c:4442`),
which overwrites `thd->rootpages` with the **live** schema. So the standard
analyze path (`analyze_regular_table` → `run_internal_sql_clnt("analyzesqlite …")`)
cannot see `newdb`'s new indexes: they'd be clobbered before the query runs.

Therefore the analyze-on-newdb must follow the `sql_syntax_check` pattern —
open the connection directly and drive analyze without going through
`dispatch_sql_query`.

### Flow

In `do_alter_table` (`sc_alter_table.c`), after `convert_all_records` succeeds
and after `check_for_idx_rename`, but before returning `SC_OK` (which precedes
`finalize_alter_table`):

1. **Detect new indexes.** Iterate `newdb->nix`; collect indexes where
   `newdb->plan->ix_plan[ixnum] == -1` (newly built vs. reused). If none, skip.
2. **Set up `newdb`'s schema.** Following `sql_syntax_check`:
   `create_sqlmaster_record(newdb)`, `create_master_entry_array(&newdb, 1, …)`,
   `get_copy_rootpages_custom(sqlthd, ents, nents)`.
3. **Open a dedicated SQLite connection** with `sqlite3_open_serial` (not
   `dispatch_sql_query`), on an internal clnt with `clnt.is_analyze = 1`.
4. **(Optional) sample** the new indexes via `sample_indexes(newdb, …)` if the
   table exceeds the sampling threshold. With `newdb`'s rootpages active, an
   ordinary cursor already reads `newdb`'s new index files directly, so sampling
   is a large-table optimization, not a requirement.
5. **Run `analyzesqlite main."<realtable>"`** on that connection inside a
   transaction. SQLite iterates `newdb`'s `pTab->pIndex` (including new
   indexes), opens cursors that resolve — via `newdb`'s rootpages — to
   `newdb`'s index files, and the VDBE computes and writes stat1 + stat4 rows
   (stat4 sample blobs encoded natively by the VDBE) under the real table/index
   names.
6. **Commit** this transaction. Because it commits before
   `finalize_alter_table`, it replicates ahead of the schema change on all
   nodes.
7. **Teardown** mirrors `sql_syntax_check`: `put_curtran`,
   `sqlite3_close_serial`, restore live rootpages, `end_internal_sql_clnt`,
   `done_sql_thread`.

### Backout

Since the stats are committed in a separate transaction *before* finalize, if
`finalize_alter_table` subsequently fails/backs out, the committed stats would
reference index names that never went live. Remedy: in the `BACKOUT` path of
`finalize_alter_table`, delete stat rows for the affected indexes
(`cleanup_stats` / `backout_stats_frm_tbl` in `sqlanalyze.c` already provide the
SQL DELETE machinery). Orphaned stats are otherwise harmless (the planner
ignores stats for indexes it can't resolve), but cleaning them keeps the tables
tidy and avoids surprising a later rename/analyze.

## Affected code

- `schemachange/sc_alter_table.c` — `do_alter_table`: new-index detection +
  call the new analyze-on-newdb routine after `convert_all_records`;
  `finalize_alter_table`: backout cleanup.
- New routine (likely `db/sqlanalyze.c` or `db/sqlglue.c`) — a variant that
  combines the `sql_syntax_check` setup (rootpages for `newdb`,
  `sqlite3_open_serial`, direct exec) with `analyze_regular_table`'s execution,
  targeting `newdb` instead of the live table.
- Reused as-is: `create_sqlmaster_record`, `create_master_entry_array`,
  `get_copy_rootpages_custom`, `sample_indexes`, sampler btree redirect,
  `cleanup_stats`/`backout_stats_frm_tbl`.

## Locking & the write-vs-rootpages tension (investigated)

**Schema-lock timing — clear.** In `do_alter_table` the schema write lock is
acquired for setup only (`sc_alter_table.c:458`) and released at line 568,
*before* `convert_all_records`. At the proposed insertion point (after
conversion, before returning `SC_OK`) no schema lock is held. `do_alter_table`
is invoked as `pre()` with a NULL tran (`sc_logic.c:499`); the finalize schema
write lock is taken later inside `do_finalize` (line 540), so the analyze is not
nested inside the finalize lock. `sql_syntax_check` already performs
`get_curtran` + `sqlite3_open_serial` + `sqlite3_exec` at this stage (under the
*write* lock, even), so the mechanics are proven and self-deadlock is not a
concern.

**The real obstacle — stat writes need the path that clobbers rootpages.**
The stat-table INSERTs commit through the osql machinery:
`do_commitrollback` → `recom_commit` → `osql_sock_commit`
(`sqlinterfaces.c:2080+`). That path is only reached by going *through*
`dispatch_sql_query`. But `dispatch_sql_query` → `prepare_engine` calls
`get_copy_rootpages_nolock` (`sqlinterfaces.c:4442`) whenever it opens/refreshes
the engine — which happens on our fresh analyze clnt's first query — replacing
`newdb`'s rootpages with the live schema. So:

- keep `newdb` visible → must avoid the `dispatch_sql_query` rootpage reload;
- write & commit the stats → need the `dispatch_sql_query` osql commit path.

`sql_syntax_check` only avoids this because it is read-only (`select 1`); it
never exercises the write/commit path, so it never needed to reconcile the two.

**Resolution options:**

1. **Suppress the reload (recommended).** Add a clnt flag that makes
   `prepare_engine` skip `get_copy_rootpages_nolock` and instead honor
   pre-loaded `newdb` rootpages. Writes then flow through the normal osql commit
   path while `newdb` stays visible. Cost: modifies the hot `prepare_engine`
   path.

   **Version-check interaction — investigated, largely safe.** `check_thd_gen`
   (`sqlinterfaces.c:2796`) has three staleness triggers; only one can drop
   custom rootpages, and its window is narrow:
   - `analyze_gen` (line 2812): calls `reload_analyze`, which (a) early-returns
     when `analyze_running_flag` is set (line 2755) and (b) only calls
     `sqlite3AnalysisLoad` — it reloads stat *data* into the existing engine and
     does **not** reopen it or rebuild rootpages. Harmless. Our inline analyze
     must set `analyze_running_flag` (as the normal path does via
     `set_analyze_running`) — this also serializes against concurrent user
     analyzes.
   - `views_gen` (line 2822): returns `SQLITE_SCHEMA_REMOTE`, which refreshes
     remote/fdb schema, not local rootpages. Not a concern here.
   - `dbopen_gen` (line 2808): returns `SQLITE_SCHEMA` → `prepare_engine`
     recreate path → close + reopen + `get_copy_rootpages_nolock`. This *would*
     drop custom rootpages. Exposure is narrow: a fresh internal clnt opens the
     engine on its first statement with `thd->sqldb == NULL`, so `check_thd_gen`
     is skipped (guarded at line 4373) and gens are snapshotted at open. Only a
     `bdb_get_dbopen_gen()` bump *between* our later statements
     (`analyzesqlite`, `COMMIT`) forces a reopen. `dbopen_gen` is bumped by
     table open/close — principally this SC's own finalize (which runs *after*
     the analyze) or a concurrent unrelated SC (rare; our window is short).
     Mitigation if needed: keep the engine open across the analyze and/or have
     the suppress flag also short-circuit the `dbopen_gen` recreate for this
     clnt.
2. **Direct exec + manual osql lifecycle.** Open SQLite directly like
   `sql_syntax_check` (keeping `newdb` rootpages), but manually drive the osql
   session (`osql_sock_start` … `recom_commit`/`osql_sock_commit`) around the
   direct `sqlite3_exec`. Keeps changes out of the hot dispatch path, at the
   cost of hand-managing the transaction lifecycle that `do_commitrollback`
   normally provides.

## Other open questions / risks

- **Rootpage save/restore.** Must restore the thread's live rootpages after the
  analyze so nothing downstream in the SC path sees `newdb`'s rootpages.
- **Analyze cost in the SC critical path.** Running analyze inline lengthens the
  schema change. Mitigate with sampling for large tables (existing threshold
  logic). Decide whether stat4 collection should be gated by table size.
- **Coverage / percent.** Decide the sampling coverage for the inline analyze
  (reuse the table's saved scale, or a fixed default for the new-index case).
- **Partial builds / resume.** Ensure the new-index detection and analyze behave
  correctly on schema-change resume and preempt paths.
- **Index-creation entry points — confirmed unified.** `CREATE INDEX`
  (`comdb2CreateIndex`, `comdb2build.c:6349`) sets `sc->kind =
  SC_ALTERTABLE_INDEX`. In the `do_schema_change_if` table (`sc_logic.c`), kinds
  24–29 (`SC_ALTERTABLE`, `SC_ALTERTABLE_PENDING`, `SC_REBUILDTABLE`,
  `SC_ALTERTABLE_INDEX`, `SC_DROPTABLE_INDEX`, `SC_REBUILDTABLE_INDEX`) all
  dispatch to `do_alter_table` / `finalize_alter_table`. So `CREATE INDEX`,
  `ALTER … ADD INDEX`, and `REBUILD INDEX` share one path, and the
  `ix_plan[i] == -1` detection covers all of them without branching on
  `sc->kind`:
  - `CREATE INDEX` / `ALTER ADD INDEX`: new index → `ix_plan == -1` → analyzed.
    A new index forces `plan_convert = 1` (`sc_schema.c:937`), so
    `convert_all_records` builds the index files in `newdb` before our insertion
    point — the data is present to analyze.
  - `REBUILD INDEX`: rebuilt index → `ix_plan == -1` → re-analyzed (desirable;
    stats may be stale).
  - `DROP INDEX`: no new index → matches nothing → analyze correctly skipped.

## Testing

- New `.test` under `tests/` that: creates a table, populates skewed + wide data,
  adds an index via ALTER, and asserts `sqlite_stat1`/`sqlite_stat4` are
  populated for the new index immediately after the SC completes (no manual
  ANALYZE).
- Plan-quality assertion: a query that should use the new index picks it
  immediately post-SC (e.g. via `EXPLAIN`/selectivity check).
- Replicant check: query the new index on a replicant right after SC and confirm
  the good plan / presence of stats (guards the replication-ordering property).
- Backout: force a finalize failure and confirm no orphaned stat rows remain.
- Large-table path: confirm sampling engages above threshold and SC latency
  stays bounded.
