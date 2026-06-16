"""
commercial_lines_fact_policy_cl.py — CDC path (change events, SCD1 + SCD2)
=========================================================================
S1 walking skeleton + S2.1 data-quality hardening.

  Bronze    (streaming table) : Auto Loader CDC ingest; keeps _operation/_extract_ts/_rescued_data.
  Silver    (streaming table) : STRUCTURAL rules dropped; BUSINESS-rule violators quarantined.
  Quarantine(streaming table) : business-rule violators (+ failed-rule list), DELETE-exempt.
  Gold current (SCD1 ST)      : AUTO CDC flow — one latest row per key.
  Gold history (SCD2 ST)      : AUTO CDC flow — full change history (__START_AT/__END_AT).

DQ strategy (S2.1): structural failures (null key / bad operation / rescued data) dropped;
business-rule failures quarantined. Business rules are evaluated only for non-DELETE rows so
DELETE tombstones (PK + _operation='DELETE', null business cols) always reach the CDC flow.
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

BRONZE_TBL     = f"{CATALOG}.{BRONZE}.{LOB}_{ENTITY}"
SILVER_TBL     = f"{CATALOG}.{SILVER}.{LOB}_{ENTITY}"
QUARANTINE_TBL = f"{CATALOG}.{SILVER}.{LOB}_{ENTITY}_quarantine"
CURRENT_TBL    = f"{CATALOG}.{GOLD}.{LOB}_{ENTITY}_current"
HISTORY_TBL    = f"{CATALOG}.{GOLD}.{LOB}_{ENTITY}_history"

# CDC + pipeline metadata stripped from the SCD target schema
CDC_META = [
    "_operation", "_extract_ts", "_batch_id",
    "_rescued_data", "_ingested_at", "_source_file",
]

# ── Business rules (discovered from data, not invented) ─────────────────────
# Evaluated only for non-DELETE rows; violators are quarantined.
BUSINESS_RULES = {
    "policy_id_not_null":     col("policy_id").isNotNull(),
    "policy_number_not_null": col("policy_number").isNotNull(),
    "customer_key_not_null":  col("customer_key").isNotNull(),
    "broker_key_not_null":    col("broker_key").isNotNull(),
    "product_key_not_null":   col("product_key").isNotNull(),
    "gwp_non_negative":       col("gross_written_premium_usd") >= 0,
    "net_le_gross":           col("net_written_premium_usd") <= col("gross_written_premium_usd"),
    "expiry_ge_effective":    col("expiry_date") >= col("effective_date"),
    "uw_year_in_range":       col("underwriting_year").between(2000, 2100),
}


def _typed_bronze():
    """Bronze CDC stream with date columns cast to DATE."""
    return (
        spark.readStream.table(BRONZE_TBL)
        .withColumn("effective_date", F.to_date("effective_date"))
        .withColumn("expiry_date", F.to_date("expiry_date"))
    )


def _with_failed_rules(df):
    """Add _dq_failed_rules; business rules are skipped for DELETE tombstones."""
    is_delete = col("_operation") == "DELETE"
    flags = [F.when(~is_delete & ~cond, F.lit(name)) for name, cond in BUSINESS_RULES.items()]
    return df.withColumn("_dq_failed_rules", F.array_compact(F.array(*flags)))


# ── Bronze: Auto Loader CDC ingest (streaming table) ────────────────────────
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


# ── Silver: validated CDC stream (structural drop + business filter) ─────────
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
        _with_failed_rules(_typed_bronze())
        .filter(F.size("_dq_failed_rules") == 0)
        .drop("_dq_failed_rules")
    )


# ── Quarantine: business-rule violators (kept for inspection) ───────────────
@dp.table(
    name=QUARANTINE_TBL,
    comment=f"[Quarantine] {LOB}/{ENTITY} non-DELETE rows failing a business rule.",
    cluster_by=[KEY],
)
def quarantine_fact_policy_cl():
    return (
        _with_failed_rules(_typed_bronze())
        .filter(F.size("_dq_failed_rules") > 0)
        .withColumn("_quarantined_at", F.current_timestamp())
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
