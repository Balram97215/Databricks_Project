"""
commercial_lines_dim_broker.py — BATCH path (full-snapshot reference data)
==========================================================================
S1 walking skeleton + S2.1 data-quality hardening.

  Bronze (streaming table)  : Auto Loader ingest of full-snapshot parquet.
  Silver (materialized view): dedup to latest row per key; STRUCTURAL rules dropped,
                              BUSINESS-rule violators routed to a quarantine table.
  Quarantine (mat. view)    : rows that fail a business rule, with the failed-rule list.
  Gold   (materialized view): conformed broker dimension.

DQ strategy (S2.1): structural failures (null key / rescued data) are dropped via
expectations; business-rule failures are quarantined (not silently lost).
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Parameters (set in the pipeline configuration) ──────────────────────────
CATALOG = spark.conf.get("catalog")
BRONZE  = spark.conf.get("bronze_schema")
SILVER  = spark.conf.get("silver_schema")
GOLD    = spark.conf.get("gold_schema")
VOL     = spark.conf.get("volume_base")

LOB     = "commercial_lines"
ENTITY  = "dim_broker"
KEY     = "broker_key"
SOURCE_PATH = f"{VOL}/{LOB}/{ENTITY}/"

BRONZE_TBL     = f"{CATALOG}.{BRONZE}.{LOB}_{ENTITY}"
SILVER_TBL     = f"{CATALOG}.{SILVER}.{LOB}_{ENTITY}"
QUARANTINE_TBL = f"{CATALOG}.{SILVER}.{LOB}_{ENTITY}_quarantine"
GOLD_TBL       = f"{CATALOG}.{GOLD}.{LOB}_{ENTITY}"

# ── Business rules (discovered from data, not invented) ─────────────────────
# name -> condition that a VALID row satisfies. Violators are quarantined.
BUSINESS_RULES = {
    "broker_id_not_null":    F.col("broker_id").isNotNull(),
    "geography_key_not_null": F.col("geography_key").isNotNull(),
    "commission_in_range":   F.col("commission_rate_pct").between(0, 100),
    "broker_type_known":     F.col("broker_type").isin("National", "Regional", "Wholesale/MGA"),
    "is_active_not_null":    F.col("is_active").isNotNull(),
}


def _with_failed_rules(df):
    """Add _dq_failed_rules: array of business-rule names this row violates (empty = clean)."""
    flags = [F.when(~cond, F.lit(name)) for name, cond in BUSINESS_RULES.items()]
    return df.withColumn("_dq_failed_rules", F.array_compact(F.array(*flags)))


def _deduped_bronze():
    """Latest row per key from the full-snapshot bronze table."""
    latest = Window.partitionBy(KEY).orderBy(F.col("_extract_ts").desc())
    return (
        spark.read.table(BRONZE_TBL)
        .withColumn("_rn", F.row_number().over(latest))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# ── Bronze: Auto Loader ingest (streaming table) ────────────────────────────
@dp.table(
    name=BRONZE_TBL,
    comment=f"[Bronze] Auto Loader ingest — {LOB}/{ENTITY} (full snapshot).",
    cluster_by=[KEY],
)
def bronze_dim_broker():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(SOURCE_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


# ── Silver: clean current rows (structural drop + business filter) ──────────
@dp.materialized_view(
    name=SILVER_TBL,
    comment=f"[Silver] Cleaned + deduplicated {LOB}/{ENTITY} — one current row per {KEY}.",
    cluster_by=[KEY],
)
@dp.expect_or_drop("valid_broker_key", f"{KEY} IS NOT NULL")
@dp.expect_or_drop("no_rescued_data", "_rescued_data IS NULL")
def silver_dim_broker():
    return (
        _with_failed_rules(_deduped_bronze())
        .filter(F.size("_dq_failed_rules") == 0)
        .drop("_dq_failed_rules", "_operation", "_batch_id")
    )


# ── Quarantine: business-rule violators (kept for inspection) ───────────────
@dp.materialized_view(
    name=QUARANTINE_TBL,
    comment=f"[Quarantine] {LOB}/{ENTITY} rows failing a business rule.",
)
def quarantine_dim_broker():
    return (
        _with_failed_rules(_deduped_bronze())
        .filter(F.size("_dq_failed_rules") > 0)
        .withColumn("_quarantined_at", F.current_timestamp())
    )


# ── Gold: conformed broker dimension (materialized view) ────────────────────
@dp.materialized_view(
    name=GOLD_TBL,
    comment=f"[Gold] Conformed broker dimension — {LOB}.",
    cluster_by=[KEY],
)
def gold_dim_broker():
    return (
        spark.read.table(SILVER_TBL)
        .select(
            "broker_key", "broker_id", "broker_name", "broker_type", "npn",
            "license_state", "primary_lob_specialty", "commission_rate_pct",
            "geography_key", "is_active",
        )
    )
