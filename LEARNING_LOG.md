# Learning Log

A short, first-person note per sprint on what clicked and what I'd do differently.

---

## Sprint 2 — Data quality (S2.1)

**What I built:** a drop-vs-quarantine data-quality layer on the two skeleton entities.
Structural failures (null key, bad `_operation`, rescued data) are dropped with
`@dp.expect_or_drop`; business-rule failures (e.g. negative gross written premium) are
routed to a `*_quarantine` table tagged with the rules they broke, instead of being
silently discarded.

**Concepts that clicked:**

- **Expectations vs. quarantine are different tools.** Expectations are the right call for
  rows that are structurally unusable — I want them gone and just counted. Quarantine is
  for rows that are *valid shape but suspect value*: I keep them so I can inspect/replay
  them. Losing data quietly is the thing to avoid.

- **DELETE tombstones must bypass business rules.** In the CDC stream a delete carries only
  the key + `_operation='DELETE'` with null business columns. If I'd run the business rules
  on them they'd "fail" and get quarantined, and the delete would never reach the CDC flow.
  So business rules are evaluated only when `_operation != 'DELETE'`.

- **The big gotcha: streaming tables don't retro-apply new logic.** After I added the
  quarantine filter and re-ran, silver still showed all 20,000 rows — the 466 bad ones
  weren't removed. A streaming table is append-only/incremental: changing its query only
  affects *new* files, not data already materialized. A **full refresh**
  (`start-update --full-refresh`) rebuilds it from source, which is when the filter actually
  took effect (silver → 19,534, gold current → 9,767). Materialized views don't have this
  problem — `dim_broker`'s silver MV recomputed fully on its own. Lesson: when I change the
  *logic* of a streaming table (not just append data), I need a full refresh to backfill it.

**Reconciliation proven:** bronze 20,000 = silver 19,534 + quarantine 466. The 466 rows are
233 policy keys across both snapshot batches, all failing `gwp_non_negative` / `net_le_gross`.

**What I'd do differently:** I duplicated the dedup/typing logic across silver and quarantine
in each file. That's fine for two entities but won't scale to 20 — S2.2 extracts this into a
metadata-driven factory so each entity is a config row, not a copy-pasted file.

---

## Sprint 2 — Metadata-driven factory (S2.2)

**What I built:** replaced the two hand-written entity files with one factory (`commercial_lines.py`).
An `ENTITIES` config list drives `build_batch_entity` / `build_cdc_entity`, which register each entity's
bronze → silver → quarantine → gold datasets. Adding an entity is now a config dict, not a new file.

**Concepts that clicked:**

- **SDP tables can be defined in a loop via a factory function.** The decorators (`@dp.table`,
  `@dp.materialized_view`) run at import time, so calling a builder inside a `for` loop registers
  tables dynamically. Table identity comes from the `name=` argument, not the Python function name —
  so reusing inner names like `silver()` across entities is fine.

- **Why a *function* per entity, not an inline loop body.** Each builder takes the config as an
  argument, so the inner dataset functions close over fixed values (`e`, `key`, `rules`). Writing the
  defs directly in the loop would make every closure capture the *last* loop value — the classic
  late-binding bug. The factory-function boundary avoids it.

- **Rules as SQL strings + `F.expr`.** Keeping business rules as `"col >= 0"` strings in config
  (evaluated with `F.expr`) reads far better than Column lambdas and is what makes the config
  genuinely declarative.

**Proof it's a true refactor, not a rewrite:** a full-refresh produced the *exact* S2.1 numbers
across all 8 tables, reconciliation intact. Same behaviour, 1 file instead of 2.

**Operational lessons (not SDP, but worth recording):** my Databricks OAuth token expired over a
~10-day gap (`databricks auth login` to refresh), and a serverless run failed instantly with
"exhausted your available credits" — a billing block at cluster creation, not a code error. Reading
the raw event `error.exceptions[].message` (not just the summary `message`) is how I found it.
