"""
commercial_lines.py — LOB 1 (Commercial Lines) metadata-driven factory
======================================================================
S2.2: one factory, many entities. Each entity is a config row in ENTITIES; the
factory registers its bronze -> silver -> quarantine -> gold datasets.

  BATCH (dimensions): bronze ST -> silver MV (dedup + DQ) -> quarantine MV -> gold MV
  CDC   (facts)     : bronze ST -> silver ST (DQ) -> quarantine ST
                      -> gold *_current (SCD1) + *_history (SCD2) via AUTO CDC

DQ strategy (from S2.1): structural failures dropped via expectations; business-rule
violators routed to a *_quarantine table tagged with _dq_failed_rules. For CDC, business
rules are evaluated only for non-DELETE rows so DELETE tombstones reach the CDC flow.

Adding an entity = appending a config dict to ENTITIES. No new code.
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

LOB = "commercial_lines"

# CDC + pipeline metadata stripped from the SCD target schema
CDC_META = ["_operation", "_extract_ts", "_batch_id", "_rescued_data", "_ingested_at", "_source_file"]


# ── Naming helpers ──────────────────────────────────────────────────────────
def _bronze(e):     return f"{CATALOG}.{BRONZE}.{LOB}_{e}"
def _silver(e):     return f"{CATALOG}.{SILVER}.{LOB}_{e}"
def _quarantine(e): return f"{CATALOG}.{SILVER}.{LOB}_{e}_quarantine"
def _gold(e):       return f"{CATALOG}.{GOLD}.{LOB}_{e}"
def _current(e):    return f"{CATALOG}.{GOLD}.{LOB}_{e}_current"
def _history(e):    return f"{CATALOG}.{GOLD}.{LOB}_{e}_history"


# ── Shared building blocks ──────────────────────────────────────────────────
def _autoloader(entity):
    """Auto Loader streaming read of an entity's parquet folder + ingest metadata."""
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{VOL}/{LOB}/{entity}/")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


def _apply_casts(df, casts):
    for column, dtype in casts.items():
        df = df.withColumn(column, F.col(column).cast(dtype))
    return df


def _failed_rules(rules, exempt_delete):
    """Column of business-rule names this row violates ([] = clean).
    When exempt_delete, DELETE tombstones are never flagged."""
    not_delete = F.col("_operation") != "DELETE"
    flags = []
    for name, cond in rules.items():
        violated = ~F.expr(cond)
        if exempt_delete:
            violated = not_delete & violated
        flags.append(F.when(violated, F.lit(name)))
    return F.array_compact(F.array(*flags))


# ── BATCH factory (full-snapshot dimension) ─────────────────────────────────
def build_batch_entity(cfg):
    e, key, rules = cfg["entity"], cfg["key"], cfg["business_rules"]
    casts, gold_cols = cfg.get("casts", {}), cfg["gold_select"]

    @dp.table(name=_bronze(e), comment=f"[Bronze] Auto Loader ingest — {LOB}/{e} (full snapshot).", cluster_by=[key])
    def bronze():
        return _autoloader(e)

    def _deduped():
        latest = Window.partitionBy(key).orderBy(F.col("_extract_ts").desc())
        df = _apply_casts(spark.read.table(_bronze(e)), casts)
        return df.withColumn("_rn", F.row_number().over(latest)).filter(F.col("_rn") == 1).drop("_rn")

    @dp.materialized_view(name=_silver(e), comment=f"[Silver] Cleaned + deduplicated {LOB}/{e}.", cluster_by=[key])
    @dp.expect_or_drop(f"valid_{key}", f"{key} IS NOT NULL")
    @dp.expect_or_drop("no_rescued_data", "_rescued_data IS NULL")
    def silver():
        return (
            _deduped().withColumn("_dq_failed_rules", _failed_rules(rules, exempt_delete=False))
            .filter(F.size("_dq_failed_rules") == 0)
            .drop("_dq_failed_rules", "_operation", "_batch_id")
        )

    @dp.materialized_view(name=_quarantine(e), comment=f"[Quarantine] {LOB}/{e} business-rule violations.")
    def quarantine():
        return (
            _deduped().withColumn("_dq_failed_rules", _failed_rules(rules, exempt_delete=False))
            .filter(F.size("_dq_failed_rules") > 0)
            .withColumn("_quarantined_at", F.current_timestamp())
        )

    @dp.materialized_view(name=_gold(e), comment=f"[Gold] Conformed {e} dimension — {LOB}.", cluster_by=[key])
    def gold():
        return spark.read.table(_silver(e)).select(*gold_cols)


# ── CDC factory (change-event fact: SCD1 current + SCD2 history) ─────────────
def build_cdc_entity(cfg):
    e, key, rules = cfg["entity"], cfg["key"], cfg["business_rules"]
    casts = cfg.get("casts", {})

    @dp.table(name=_bronze(e), comment=f"[Bronze] Auto Loader CDC ingest — {LOB}/{e}. CDC cols preserved.", cluster_by=[key])
    def bronze():
        return _autoloader(e)

    def _typed():
        return _apply_casts(spark.readStream.table(_bronze(e)), casts)

    @dp.table(name=_silver(e), comment=f"[Silver] Validated CDC stream — {LOB}/{e}; DELETE tombstones retained.", cluster_by=[key])
    @dp.expect_or_drop(f"valid_{key}", f"{key} IS NOT NULL")
    @dp.expect_or_drop("valid_operation", "_operation IN ('INSERT', 'UPDATE', 'DELETE')")
    @dp.expect_or_drop("no_rescued_data", "_rescued_data IS NULL")
    def silver():
        return (
            _typed().withColumn("_dq_failed_rules", _failed_rules(rules, exempt_delete=True))
            .filter(F.size("_dq_failed_rules") == 0)
            .drop("_dq_failed_rules")
        )

    @dp.table(name=_quarantine(e), comment=f"[Quarantine] {LOB}/{e} non-DELETE rows failing a business rule.", cluster_by=[key])
    def quarantine():
        return (
            _typed().withColumn("_dq_failed_rules", _failed_rules(rules, exempt_delete=True))
            .filter(F.size("_dq_failed_rules") > 0)
            .withColumn("_quarantined_at", F.current_timestamp())
        )

    dp.create_streaming_table(name=_current(e), comment=f"[Gold] {LOB}/{e} — SCD Type 1 (current, one row per {key}).")
    dp.create_auto_cdc_flow(
        target=_current(e), source=_silver(e), keys=[key],
        sequence_by=F.col("_extract_ts"), apply_as_deletes=F.col("_operation") == "DELETE",
        except_column_list=CDC_META, stored_as_scd_type="1",
    )

    dp.create_streaming_table(name=_history(e), comment=f"[Gold] {LOB}/{e} — SCD Type 2 (history, __START_AT/__END_AT).")
    dp.create_auto_cdc_flow(
        target=_history(e), source=_silver(e), keys=[key],
        sequence_by=F.col("_extract_ts"), apply_as_deletes=F.col("_operation") == "DELETE",
        except_column_list=CDC_META, stored_as_scd_type=2,
    )


# ── Entity configuration (one row per entity) ───────────────────────────────
ENTITIES = [
    {
        "entity": "dim_broker", "pattern": "BATCH", "key": "broker_key",
        "business_rules": {
            "broker_id_not_null":     "broker_id IS NOT NULL",
            "geography_key_not_null": "geography_key IS NOT NULL",
            "commission_in_range":    "commission_rate_pct BETWEEN 0 AND 100",
            "broker_type_known":      "broker_type IN ('National', 'Regional', 'Wholesale/MGA')",
            "is_active_not_null":     "is_active IS NOT NULL",
        },
        "gold_select": [
            "broker_key", "broker_id", "broker_name", "broker_type", "npn",
            "license_state", "primary_lob_specialty", "commission_rate_pct",
            "geography_key", "is_active",
        ],
    },
    {
        "entity": "fact_policy_cl", "pattern": "CDC", "key": "policy_key",
        "casts": {"effective_date": "date", "expiry_date": "date"},
        "business_rules": {
            "policy_id_not_null":     "policy_id IS NOT NULL",
            "policy_number_not_null": "policy_number IS NOT NULL",
            "customer_key_not_null":  "customer_key IS NOT NULL",
            "broker_key_not_null":    "broker_key IS NOT NULL",
            "product_key_not_null":   "product_key IS NOT NULL",
            "gwp_non_negative":       "gross_written_premium_usd >= 0",
            "net_le_gross":           "net_written_premium_usd <= gross_written_premium_usd",
            "expiry_ge_effective":    "expiry_date >= effective_date",
            "uw_year_in_range":       "underwriting_year BETWEEN 2000 AND 2100",
        },
    },
]

_BUILDERS = {"BATCH": build_batch_entity, "CDC": build_cdc_entity}

for _cfg in ENTITIES:
    _BUILDERS[_cfg["pattern"]](_cfg)
