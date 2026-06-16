# PROGRESS — Insurance Data Platform on Lakeflow SDP

Living status + decision log. **This file is the resume point**: read it (with `Project Plan.md`)
to know where the project stands and why decisions were made. Update it at the end of each story.

_Last updated: 2026-06-16 (end of S2.1)._

---

## Current position
- **Sprint:** S2 — *Harden quality; complete all entities in LOB 1 (Commercial Lines)* → milestone **M3**.
- **Last completed story:** **S2.1** — data-quality hardening (drop structural / quarantine business). Merged to `main`.
- **Next story:** **S2.2** — extract a metadata-driven factory so bronze/silver/gold/DQ are generated per entity from config (not copy-pasted files). *Not started.*
- **WIP limit:** 1 story at a time.

## Roadmap status
| Sprint | Goal | Milestone | Status |
|---|---|---|---|
| S0 | Foundation & discovery; classify entities | M1 | ✅ done |
| S1 | Walking skeleton (1 LOB, 1 batch + 1 CDC entity) | M2 | ✅ done |
| **S2** | Harden quality; complete all LOB-1 entities | M3 | 🔄 in progress (S2.1 ✅, S2.2–S2.5 pending) |
| S3 | Scale to all 3 LOBs; conform shared dims | M4 | ⬜ |
| S4 | Migration realism (bulk vs incremental, reconciliation) | M5 | ⬜ |
| S5 | Orchestrate (Job + schedule + monitoring) | M6 | ⬜ |
| S6 | Document & package as portfolio | M7 | ⬜ |

## What is built (deployed reality)
- **Pipeline:** `[dev iyengarbalram97] ins_sdp_etl`, id `cfb851a2-8594-4b42-a166-3190e57fb2ec`, serverless, dev target.
- **Entities live:** Commercial Lines `dim_broker` (BATCH) + `fact_policy_cl` (CDC) — bronze→silver→(quarantine)→gold.
- **Tables (insurance_sdp):** `bronze.*`, `silver.*`, `silver.*_quarantine`, `gold.*` (+ `_current`/`_history` for CDC).
- Last run: full-refresh update COMPLETED, no failed flows. silver fact 19,534 + quarantine 466 = bronze 20,000.

## Decision log
| # | Decision | Rationale | When |
|---|---|---|---|
| D1 | Output to **per-layer schemas** `bronze`/`silver`/`gold` (not one `medallion` schema) | User preference; cleaner separation | S1 |
| D2 | **Metadata-driven factory** for table generation (over one-file-per-entity) | Scales to 50 entities; less duplication | S2 plan |
| D3 | DQ = **drop structural / quarantine business**; DELETE tombstones exempt | Keep suspect rows for inspection, never silently lose data | S2.1 |
| D4 | **Quarantine** the 466 negative-GWP / net>gross policy rows (treat as anomalies) | PO call — flagged, not assumed | S2.1 |
| D5 | Build CDC path now on insert-only data; **no generator code change** needed for real CDC | `batch_generator.run_batch()` already emits 60/30/10 I/U/D — just never run | S0/S1 |
| D6 | Quarantine tables live in the **silver** schema with `_quarantine` suffix | Avoids creating a new schema; silver-stage concern | S2.1 |

## Key gaps / open items
- **Real CDC data not yet generated.** History (SCD2) holds one open row per key until `batch_generator.run_batch()` is run for a new batch. Pipeline is forward-compatible (no code change needed).
- Dev and prod currently share the same catalog/schemas (single-user learning setup).

## Resume protocol (for a fresh/refreshed context)
1. Read `Project Plan.md` (the plan) and this file (position + decisions).
2. `git log --oneline` + `gh pr list` — what's committed / in flight.
3. Query workspace: `databricks pipelines get cfb851a2-8594-4b42-a166-3190e57fb2ec`, table counts.
4. Reconcile plan ↔ code ↔ reality; flag any disagreement before acting.

## Pointers
- Plan & discovery: workspace `/Users/iyengarbalram97@gmail.com/Ins_SDP/` (`Project Plan.md`, `Phase0_Discovery.md`, `classification.csv`, `discovery_raw.json`).
- Repo: https://github.com/Balram97215/Databricks_Project
- Source volume (do not modify): `/Volumes/insurance_sdp/raw/brand_raw`
- Per-sprint learnings: `LEARNING_LOG.md`.
