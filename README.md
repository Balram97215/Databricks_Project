# Databricks_Project
Insurance domain | data generator model | end-to-end pipeline | analytics

End-to-end Databricks project on synthetic insurance data: a Lakeflow Spark
Declarative Pipeline (SDP) following the medallion architecture (bronze → silver → gold),
supporting both batch (full-snapshot dimensions) and CDC (SCD Type 1 + Type 2 facts)
ingestion patterns. Built as a Databricks Asset Bundle (DAB).

- `src/ins_sdp_etl/transformations/` — pipeline source (one file per entity)
- `resources/` — pipeline (and later, job) definitions
- `databricks.yml` — bundle manifest (dev/prod targets)

---
