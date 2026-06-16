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
