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
        "entity": "dim_aviation_risk", "pattern": "BATCH", "key": "aviation_risk_key",
        "business_rules": {
            "tail_number_not_null": "tail_number IS NOT NULL",
            "operator_name_not_null": "operator_name IS NOT NULL",
        },
        "gold_select": ["aviation_risk_key", "tail_number", "aircraft_type", "aircraft_make", "aircraft_model", "year_of_manufacture", "hull_value_usd", "seating_capacity", "operator_name", "operator_class", "total_pilot_hours", "base_airport_iata", "geographic_territory", "airworthiness_cert_date"],
    },
    {
        "entity": "dim_cause_of_loss", "pattern": "BATCH", "key": "cause_key",
        "business_rules": {
            "cause_code_not_null": "cause_code IS NOT NULL",
            "cause_name_not_null": "cause_name IS NOT NULL",
        },
        "gold_select": ["cause_key", "cause_code", "cause_name", "cause_category", "lob_applicability", "severity_class"],
    },
    {
        "entity": "dim_coverage_type", "pattern": "BATCH", "key": "coverage_type_key",
        "business_rules": {
            "coverage_code_not_null": "coverage_code IS NOT NULL",
            "coverage_name_not_null": "coverage_name IS NOT NULL",
        },
        "gold_select": ["coverage_type_key", "coverage_code", "coverage_name", "coverage_category", "lob_applicability"],
    },
    {
        "entity": "dim_customer_cl", "pattern": "BATCH", "key": "customer_key",
        "business_rules": {
            "customer_id_not_null": "customer_id IS NOT NULL",
            "company_name_not_null": "company_name IS NOT NULL",
            "geography_key_not_null": "geography_key IS NOT NULL",
        },
        "gold_select": ["customer_key", "customer_id", "company_name", "industry_sector", "annual_revenue_usd", "employee_count", "ein", "duns_number", "headquarters_state", "headquarters_country", "years_in_business", "credit_rating", "customer_segment", "geography_key", "is_active"],
    },
    {
        "entity": "dim_cyber_risk", "pattern": "BATCH", "key": "cyber_risk_key",
        "business_rules": {
            "insured_company_name_not_null": "insured_company_name IS NOT NULL",
        },
        "gold_select": ["cyber_risk_key", "insured_company_name", "industry_sector", "annual_revenue_usd", "employee_count", "it_security_score", "mfa_implemented", "endpoint_detection", "security_framework", "prior_breach_history", "data_records_protected", "primary_cloud_provider", "cyber_maturity_rating", "patch_cadence"],
    },
    {
        "entity": "dim_energy_risk", "pattern": "BATCH", "key": "energy_risk_key",
        "business_rules": {
            "asset_name_not_null": "asset_name IS NOT NULL",
            "asset_type_not_null": "asset_type IS NOT NULL",
        },
        "gold_select": ["energy_risk_key", "asset_name", "asset_type", "energy_sector", "jurisdiction", "operator_name", "total_insured_value_usd", "production_capacity_mw", "year_commissioned", "environmental_risk_score", "offshore"],
    },
    {
        "entity": "dim_excess_casualty_risk", "pattern": "BATCH", "key": "excess_casualty_risk_key",
        "business_rules": {
            "risk_name_not_null": "risk_name IS NOT NULL",
        },
        "gold_select": ["excess_casualty_risk_key", "risk_name", "industry_sector", "attachment_point_usd", "limit_usd", "excess_layer_number", "primary_carrier", "primary_policy_limit_usd", "total_insured_value_usd", "risk_profile", "underlying_insurer"],
    },
    {
        "entity": "dim_marine_risk", "pattern": "BATCH", "key": "marine_risk_key",
        "business_rules": {
            "imo_number_not_null": "imo_number IS NOT NULL",
            "vessel_name_not_null": "vessel_name IS NOT NULL",
        },
        "gold_select": ["marine_risk_key", "imo_number", "vessel_name", "vessel_type", "flag_state", "year_built", "gross_tonnage_gt", "hull_value_usd", "cargo_type", "trade_route", "classification_society", "average_voyage_duration_days", "port_of_registry"],
    },
    {
        "entity": "dim_peril", "pattern": "BATCH", "key": "peril_key",
        "business_rules": {
            "peril_code_not_null": "peril_code IS NOT NULL",
            "peril_name_not_null": "peril_name IS NOT NULL",
        },
        "gold_select": ["peril_key", "peril_code", "peril_name", "peril_category", "is_named_peril", "is_cat_peril", "lob_applicability"],
    },
    {
        "entity": "dim_product_cl", "pattern": "BATCH", "key": "product_key",
        "business_rules": {
            "product_code_not_null": "product_code IS NOT NULL",
            "product_name_not_null": "product_name IS NOT NULL",
        },
        "gold_select": ["product_key", "product_code", "product_name", "lob", "coverage_trigger", "admitted_status", "min_premium_usd", "max_premium_usd"],
    },
    {
        "entity": "dim_reinsurance_treaty", "pattern": "BATCH", "key": "treaty_key",
        "business_rules": {
            "treaty_id_not_null": "treaty_id IS NOT NULL",
            "treaty_name_not_null": "treaty_name IS NOT NULL",
        },
        "gold_select": ["treaty_key", "treaty_id", "treaty_name", "treaty_type", "effective_year", "retention_pct", "attachment_usd", "limit_usd", "lob_scope", "cedant_commission_pct"],
    },
    {
        "entity": "dim_reinsurer", "pattern": "BATCH", "key": "reinsurer_key",
        "business_rules": {
            "reinsurer_code_not_null": "reinsurer_code IS NOT NULL",
            "reinsurer_name_not_null": "reinsurer_name IS NOT NULL",
        },
        "gold_select": ["reinsurer_key", "reinsurer_code", "reinsurer_name", "domicile_country", "am_best_rating"],
    },
    {
        "entity": "dim_underwriter", "pattern": "BATCH", "key": "underwriter_key",
        "business_rules": {
            "underwriter_id_not_null": "underwriter_id IS NOT NULL",
            "last_name_not_null": "last_name IS NOT NULL",
        },
        "gold_select": ["underwriter_key", "underwriter_id", "first_name", "last_name", "title", "lob_specialty", "years_experience", "authority_limit_usd", "office_location", "is_active"],
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
