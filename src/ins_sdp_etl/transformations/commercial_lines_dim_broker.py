"""
commercial_lines_dim_broker.py — BATCH path (full-snapshot reference data)
==========================================================================
Phase 1 walking skeleton — BATCH entity.

  Bronze (streaming table) : Auto Loader ingest of full-snapshot parquet from the volume.
  Silver (materialized view): deduplicate to latest row per key + quality expectations.
  Gold   (materialized view): conformed broker dimension.

The generator re-emits a full snapshot per run (every key repeats each batch, _operation
always INSERT), so silver dedups to one current row per key by latest _extract_ts.
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

BRONZE_TBL = f"{CATALOG}.{BRONZE}.{LOB}_{ENTITY}"
SILVER_TBL = f"{CATALOG}.{SILVER}.{LOB}_{ENTITY}"
GOLD_TBL   = f"{CATALOG}.{GOLD}.{LOB}_{ENTITY}"


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


# ── Silver: dedup to current row per key + expectations (materialized view) ─
@dp.materialized_view(
    name=SILVER_TBL,
    comment=f"[Silver] Cleaned + deduplicated {LOB}/{ENTITY} — one current row per {KEY}.",
    cluster_by=[KEY],
)
@dp.expect_or_drop("valid_broker_key", f"{KEY} IS NOT NULL")
@dp.expect_or_drop("no_rescued_data", "_rescued_data IS NULL")
def silver_dim_broker():
    latest = Window.partitionBy(KEY).orderBy(F.col("_extract_ts").desc())
    return (
        spark.read.table(BRONZE_TBL)
        .withColumn("_rn", F.row_number().over(latest))
        .filter(F.col("_rn") == 1)
        # keep _rescued_data so the no_rescued_data expectation can evaluate;
        # gold selects business columns only, so it is dropped there.
        .drop("_rn", "_operation", "_batch_id")
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
