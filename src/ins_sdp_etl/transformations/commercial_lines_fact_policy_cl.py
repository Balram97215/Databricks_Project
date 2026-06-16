"""
commercial_lines_fact_policy_cl.py — CDC path (change events, SCD1 + SCD2)
=========================================================================
Phase 1 walking skeleton — CDC entity.

  Bronze  (streaming table) : Auto Loader ingest of CDC event parquet; keeps
                              _operation / _extract_ts / _rescued_data.
  Silver  (streaming table) : quality expectations; DELETE tombstones pass through.
  Gold    current (SCD1 ST) : AUTO CDC flow — one latest row per key.
  Gold    history (SCD2 ST) : AUTO CDC flow — full change history (__START_AT/__END_AT).

DELETE rows are tombstones (PK + _operation='DELETE', business cols null). Silver
expectations validate only the key / operation / rescue columns so deletes are NOT
dropped before reaching the CDC flow. The current data is INSERT-only, so history will
hold one open row per key until the incremental generator (run_batch) emits real changes.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.functions import col

# ── Parameters (set in the pipeline configuration) ──────────────────────────
CATALOG = spark.conf.get("catalog")
BRONZE  = spark.conf.get("bronze_schema")
SILVER  = spark.conf.get("silver_schema")
GOLD    = spark.conf.get("gold_schema")
VOL     = spark.conf.get("volume_base")

LOB     = "commercial_lines"
ENTITY  = "fact_policy_cl"
KEY     = "policy_key"
SOURCE_PATH = f"{VOL}/{LOB}/{ENTITY}/"

BRONZE_TBL  = f"{CATALOG}.{BRONZE}.{LOB}_{ENTITY}"
SILVER_TBL  = f"{CATALOG}.{SILVER}.{LOB}_{ENTITY}"
CURRENT_TBL = f"{CATALOG}.{GOLD}.{LOB}_{ENTITY}_current"
HISTORY_TBL = f"{CATALOG}.{GOLD}.{LOB}_{ENTITY}_history"

# CDC + pipeline metadata stripped from the SCD target schema
CDC_META = [
    "_operation", "_extract_ts", "_batch_id",
    "_rescued_data", "_ingested_at", "_source_file",
]


# ── Bronze: Auto Loader ingest (streaming table) ────────────────────────────
@dp.table(
    name=BRONZE_TBL,
    comment=f"[Bronze] Auto Loader CDC ingest — {LOB}/{ENTITY}. CDC cols preserved.",
    cluster_by=[KEY],
)
def bronze_fact_policy_cl():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(SOURCE_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


# ── Silver: validate + type-cast (streaming table) ──────────────────────────
# Expectations check ONLY key / operation / rescue so DELETE tombstones survive.
@dp.table(
    name=SILVER_TBL,
    comment=f"[Silver] Validated CDC stream — {LOB}/{ENTITY}; DELETE tombstones retained.",
    cluster_by=[KEY],
)
@dp.expect_or_drop("valid_policy_key", f"{KEY} IS NOT NULL")
@dp.expect_or_drop("valid_operation", "_operation IN ('INSERT', 'UPDATE', 'DELETE')")
@dp.expect_or_drop("no_rescued_data", "_rescued_data IS NULL")
def silver_fact_policy_cl():
    return (
        spark.readStream.table(BRONZE_TBL)
        .withColumn("effective_date", F.to_date("effective_date"))
        .withColumn("expiry_date", F.to_date("expiry_date"))
    )


# ── Gold current: SCD Type 1 via AUTO CDC ───────────────────────────────────
dp.create_streaming_table(
    name=CURRENT_TBL,
    comment=f"[Gold] {LOB}/{ENTITY} — SCD Type 1 (current state, one row per {KEY}).",
)
dp.create_auto_cdc_flow(
    target=CURRENT_TBL,
    source=SILVER_TBL,
    keys=[KEY],
    sequence_by=col("_extract_ts"),
    apply_as_deletes=col("_operation") == "DELETE",
    except_column_list=CDC_META,
    stored_as_scd_type="1",
)


# ── Gold history: SCD Type 2 via AUTO CDC ───────────────────────────────────
dp.create_streaming_table(
    name=HISTORY_TBL,
    comment=f"[Gold] {LOB}/{ENTITY} — SCD Type 2 (full history, __START_AT/__END_AT).",
)
dp.create_auto_cdc_flow(
    target=HISTORY_TBL,
    source=SILVER_TBL,
    keys=[KEY],
    sequence_by=col("_extract_ts"),
    apply_as_deletes=col("_operation") == "DELETE",
    except_column_list=CDC_META,
    stored_as_scd_type=2,
)
