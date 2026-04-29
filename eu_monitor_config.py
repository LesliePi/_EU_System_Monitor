# EU System Monitor — Central Pipeline Configuration
# eu_monitor_config.py
# Version: v1.0  (2026-04-25)
# Author: László Tatai / BarefootRealism Labs
# License: Apache License 2.0 WITH Commons Clause v1.0
# -*- coding: utf-8 -*-

"""
Central configuration for the EU System Monitor pipeline.

Edit this file to control:
  - Which datasets are enabled
  - Source URLs and API endpoints
  - Output paths and directory structure
  - Audit / hash behaviour
  - Dashboard appearance settings
"""

import os

# ============================================================
# ROOT PATHS
# ============================================================

_HERE         = os.path.dirname(os.path.abspath(__file__))
ROOT          = _HERE

DATA_DIR      = os.path.join(ROOT, "data")
RAW_DIR       = os.path.join(DATA_DIR, "raw")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")
OUTPUT_DIR    = os.path.join(ROOT, "output")
AUDIT_DIR     = os.path.join(DATA_DIR, "audit")

# Sub-directories per source (created automatically by downloader)
RAW_GIE_AGSI  = os.path.join(RAW_DIR, "gie_agsi")    # Gas storage
RAW_GIE_ALSI  = os.path.join(RAW_DIR, "gie_alsi")    # LNG terminals
RAW_EUROSTAT  = os.path.join(RAW_DIR, "eurostat")     # Industrial production
RAW_FAO       = os.path.join(RAW_DIR, "fao")          # Food price index
RAW_BALTIC    = os.path.join(RAW_DIR, "baltic_dry")   # Shipping cost proxy

# Final output files
PROCESSED_CSV     = os.path.join(PROCESSED_DIR, "eu_monitor_monthly.csv")
PROCESSED_PARQUET = os.path.join(PROCESSED_DIR, "eu_monitor_monthly.parquet")
AUDIT_LOG         = os.path.join(AUDIT_DIR,     "audit_log.json")
CHECKPOINT_FILE   = os.path.join(PROCESSED_DIR, "checkpoint.json")
DASHBOARD_HTML    = os.path.join(OUTPUT_DIR,    "eu_system_monitor.html")

# ============================================================
# GLOBAL BEHAVIOUR FLAGS
# ============================================================

# Keep raw downloaded files for audit (recommended: True)
DELETE_RAW = False

# Flush processed buffer to disk every N datasets
BATCH_SIZE = 5

# HTTP request timeout in seconds
HTTP_TIMEOUT = 30

# Polite delay between HTTP requests (seconds)
HTTP_SLEEP = 0.3

# Number of retries on failed downloads
HTTP_RETRIES = 3

# ============================================================
# SOURCE ENABLE / DISABLE
# ============================================================
# Set False to skip a source entirely (download + process both skipped).

ENABLE_GIE_AGSI = True    # Gas storage levels
ENABLE_GIE_ALSI = True    # LNG terminal fill levels
ENABLE_EUROSTAT  = True    # EU industrial production index
ENABLE_FAO       = True    # FAO food price index
ENABLE_BALTIC    = True    # Baltic Dry Index (logistics proxy)

# ============================================================
# PILLAR 1 — ENERGY
# ============================================================

# --- GIE AGSI+ : EU Natural Gas Storage ---
# Aggregate Gas Storage Inventories — official GIE REST API
# Documentation: https://agsi.gie.eu/api-doc/swagger
# No API key required for aggregate EU data.
# Returns: storage level (TWh), injection rate, withdrawal rate,
#          working gas volume, trend, full indicator

GIE_AGSI = {
    "dataset_id":   "energy.gas.storage",
    "label":        "EU Gas Storage (AGSI+)",
    "source":       "GIE",
    "base_url":     "https://agsi.gie.eu/api",
    "region":       "eu",
    "country_code": "eu",        # aggregate EU
    "unit":         "TWh",
    "frequency":    "daily",

    # How many days of history to fetch per run
    # GIE API supports ?from=YYYY-MM-DD&to=YYYY-MM-DD
    "history_start": "2017-01-01",

    "output_dir":   RAW_GIE_AGSI,
    "file_prefix":  "agsi",

    # Columns to extract from API response
    "keep_fields": [
        "gasDayStart",       # date
        "gasInStorage",      # TWh — current storage level
        "injection",         # TWh/day
        "withdrawal",        # TWh/day
        "workingGasVolume",  # TWh — total capacity
        "injectionCapacity", # TWh/day
        "withdrawalCapacity",# TWh/day
        "trend",             # % change day-on-day
        "full",              # % full
        "status",            # data status flag
    ],

    # Derived metrics computed in processor
    "derived_metrics": [
        "injection_gap",        # injectionCapacity - injection (unused capacity)
        "storage_pct",          # gasInStorage / workingGasVolume * 100
        "cumulative_deficit",   # rolling sum of injection_gap
    ],
}

# --- GIE ALSI : EU LNG Terminal Storage ---
# Aggregated LNG Storage Inventories
# Documentation: https://alsi.gie.eu/api-doc/swagger
# Same API structure as AGSI, different endpoint.

GIE_ALSI = {
    "dataset_id":   "energy.lng.storage",
    "label":        "EU LNG Terminal Storage (ALSI)",
    "source":       "GIE",
    "base_url":     "https://alsi.gie.eu/api",
    "region":       "eu",
    "country_code": "eu",
    "unit":         "TWh",
    "frequency":    "daily",

    "history_start": "2017-01-01",

    "output_dir":   RAW_GIE_ALSI,
    "file_prefix":  "alsi",

    "keep_fields": [
        "gasDayStart",
        "lngInventory",      # TWh
        "sendOut",           # TWh/day — gas sent to grid
        "sendOutCapacity",   # TWh/day
        "full",              # % full
        "status",
    ],

    "derived_metrics": [
        "sendout_gap",       # sendOutCapacity - sendOut
        "lng_storage_pct",   # lngInventory / capacity * 100
    ],
}

# ============================================================
# PILLAR 2 — INDUSTRY / SUPPLY CHAIN
# ============================================================

# --- Eurostat : EU Industrial Production Index ---
# STS_INPR_M — Eurostat SDMX REST API
# Coverage: EU27, monthly, seasonally adjusted
# Documentation: https://ec.europa.eu/eurostat/web/sdmx-web-services
# No API key required.

EUROSTAT_IPI = {
    "dataset_id":  "industry.production.index",
    "label":       "EU Industrial Production Index (Eurostat)",
    "source":      "Eurostat",
    "frequency":   "monthly",
    "unit":        "index (2015=100)",

    # SDMX REST endpoint — industrial production, total industry, EU27
    # s_adj=SCA → seasonally + calendar adjusted
    "url": (
        "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/"
        "dataflow/ESTAT/STS_INPR_M/1.0?"
        "c[geo]=EU27_2020&c[nace_r2]=C&c[s_adj]=SCA&c[unit]=I15"
        "&compress=false&format=JSON&lang=EN"
    ),

    "history_start": "2000-01",

    "output_dir":  RAW_EUROSTAT,
    "file_prefix": "eurostat_ipi",

    "keep_fields": [
        "time_period",   # YYYY-MM
        "obs_value",     # index value
        "obs_status",    # data quality flag
    ],

    "derived_metrics": [
        "ipi_yoy_pct",   # year-on-year % change
        "ipi_mom_pct",   # month-on-month % change
    ],
}

# --- Baltic Dry Index : Global Shipping Cost Proxy ---
# Proxy for logistics stress — no single free API exists.
# Source: Investing.com historical CSV export (manual download)
# or web scrape from publicly available sources.
# File format: CSV with columns [Date, Price, Open, High, Low, Change%]
# Frequency: daily (business days)

BALTIC_DRY = {
    "dataset_id":  "industry.logistics.bdi",
    "label":       "Baltic Dry Index (Logistics Proxy)",
    "source":      "Baltic Exchange / Investing.com",
    "frequency":   "daily",
    "unit":        "index points",
    "fetch_mode":  "manual_csv",   # "manual_csv" | "scrape" | "api"

    # Place downloaded CSV files here; processor auto-detects all *.csv
    "output_dir":  RAW_BALTIC,
    "file_prefix": "bdi",

    # Expected column names after normalisation
    "keep_fields": [
        "date",
        "bdi_close",     # closing index value (renamed from "Price")
        "bdi_change_pct",
    ],

    "derived_metrics": [
        "bdi_30d_ma",    # 30-day moving average
        "bdi_stress",    # binary flag: bdi_close < 1000 → elevated stress
    ],

    # Audit note appended to audit log when data is loaded from manual file
    "manual_note": (
        "BDI data loaded from manually downloaded CSV. "
        "Original source: Baltic Exchange via Investing.com. "
        "SHA256 hash recorded at ingest."
    ),
}

# ============================================================
# PILLAR 3 — FOOD / AGRICULTURE
# ============================================================

# --- FAO Food Price Index ---
# FFPI — Food and Agriculture Organization of the UN
# Monthly composite index: cereals, dairy, meat, oils, sugar
# Direct CSV download — no API key required.
# Documentation: https://www.fao.org/world-food-situation/foodpricesindex/en/

FAO_FPPI = {
    "dataset_id":  "food.price.index",
    "label":       "FAO Food Price Index (FFPI)",
    "source":      "FAO",
    "base_url":    "https://www.fao.org/3/cb5416en/cb5416en.xlsx",
    "frequency":   "monthly",
    "unit":        "index (2014-2016=100)",

    # FAO also publishes sub-indices by category
    "sub_indices": [
        "Cereals Price Index",
        "Dairy Price Index",
        "Meat Price Index",
        "Oils Price Index",
        "Sugar Price Index",
    ],

    "output_dir":  RAW_FAO,
    "file_prefix": "fao_fppi",

    "keep_fields": [
        "date",              # YYYY-MM
        "ffpi",              # composite index
        "cereals",
        "dairy",
        "meat",
        "oils",
        "sugar",
    ],

    "derived_metrics": [
        "ffpi_yoy_pct",      # year-on-year % change
        "food_stress",       # binary: ffpi > 130 → elevated
    ],
}

# ============================================================
# METRICS — System Stress Index
# ============================================================
# Composite headline indicator combining all three pillars.
# Each component is normalised 0–1 before aggregation.
# Weights must sum to 1.0.

STRESS_INDEX = {
    "label":   "EU System Stress Index",
    "version": "v1.0",

    "components": {
        # Pillar 1 — Energy (weight 0.45)
        "gas_storage_deficit": {
            "source_metric": "storage_pct",
            "direction":     "inverse",   # low storage = high stress
            "weight":        0.30,
            "norm_range":    [20.0, 100.0],  # % full: 20% = max stress, 100% = no stress
        },
        "lng_sendout_gap": {
            "source_metric": "sendout_gap_gwh",
            "direction":     "direct",    # high gap = spare capacity = low stress
            "weight":        0.15,
            "norm_range":    [0.0, 50.0],    # TWh/day
        },

        # Pillar 2 — Industry (weight 0.30)
        "ipi_deviation": {
            "source_metric": "ipi_yoy_pct",
            "direction":     "inverse",   # negative growth = stress
            "weight":        0.15,
            "norm_range":    [-15.0, 10.0],  # % yoy
        },
        "logistics_stress": {
            "source_metric": "bdi_30d_ma",
            "direction":     "inverse",   # low BDI = logistics stress
            "weight":        0.15,
            "norm_range":    [500.0, 3000.0],
        },

        # Pillar 3 — Food (weight 0.25)
        "food_price_pressure": {
            "source_metric": "ffpi_yoy_pct",
            "direction":     "direct",    # high food inflation = stress
            "weight":        0.25,
            "norm_range":    [-22.0, 44.0],   # % yoy
        },
    },

    # Stress bands for dashboard colour coding
    "bands": {
        "low":      [0.00, 0.30],   # green
        "moderate": [0.30, 0.55],   # yellow
        "elevated": [0.55, 0.75],   # orange
        "critical": [0.75, 1.00],   # red
    },
}

# ============================================================
# DASHBOARD SETTINGS
# ============================================================

DASHBOARD = {
    "title":       "EU System Monitor",
    "subtitle":    "Auditable Supply & System Risk Platform",
    "version":     "v1.0",
    "author":      "BarefootRealism Labs",

    # Refresh note shown in footer
    "data_note":   (
        "All data sourced from official public sources. "
        "SHA256 hashes recorded at ingest. "
        "This dashboard does not interpret — it shows."
    ),

    # Chart time window (months of history shown by default)
    "default_lookback_months": 36,

    # Colours
    "colors": {
        "stress_low":      "#2ecc71",
        "stress_moderate": "#f1c40f",
        "stress_elevated": "#e67e22",
        "stress_critical": "#e74c3c",
        "primary":         "#2c3e50",
        "secondary":       "#7f8c8d",
        "background":      "#f9f9f9",
        "chart_gas":       "#3498db",
        "chart_lng":       "#1abc9c",
        "chart_ipi":       "#9b59b6",
        "chart_bdi":       "#e67e22",
        "chart_food":      "#e74c3c",
    },

    "output_path": DASHBOARD_HTML,
}

# ============================================================
# AUDIT RECORD SCHEMA
# ============================================================
# Template for every audit entry written to audit_log.json.
# All downloaders must populate this for each file fetched.

AUDIT_SCHEMA = {
    "dataset_id":   None,     # e.g. "energy.gas.storage"
    "source":       None,     # e.g. "GIE"
    "source_url":   None,     # full URL used
    "retrieved_at": None,     # ISO 8601 UTC timestamp
    "published_at": None,     # as reported by source (if available)
    "period_start": None,     # data coverage start YYYY-MM-DD
    "period_end":   None,     # data coverage end   YYYY-MM-DD
    "local_path":   None,     # path to saved raw file
    "raw_hash":     None,     # SHA256 of raw file
    "row_count":    None,     # number of records in file
    "unit":         None,     # measurement unit
    "notes":        "",       # free text (manual downloads etc.)
}

# ============================================================
# VARIABLE RENAME MAP
# ============================================================
# Maps raw source column names → canonical names used in processed output.
# Add entries here when source APIs change column names.

VAR_RENAME = {
    # GIE AGSI
    "gasDayStart":        "date",
    "gasInStorage":       "gas_storage_twh",
    "injection":          "gas_injection_twh",
    "withdrawal":         "gas_withdrawal_twh",
    "workingGasVolume":   "gas_capacity_twh",
    "injectionCapacity":  "gas_injection_cap_twh",
    "withdrawalCapacity": "gas_withdrawal_cap_twh",
    "trend":              "gas_trend_pct",
    "full":               "gas_full_pct",

    # GIE ALSI
    "lngInventory":       "lng_inventory_twh",
    "sendOut":            "lng_sendout_twh",
    "sendOutCapacity":    "lng_sendout_cap_twh",

    # Eurostat IPI
    "obs_value":          "ipi",
    "time_period":        "date",

    # Baltic Dry
    "Price":              "bdi_close",
    "Change %":           "bdi_change_pct",

    # FAO
    "Food Price Index":   "ffpi",
    "Cereals Price Index":"cereals",
    "Dairy Price Index":  "dairy",
    "Meat Price Index":   "meat",
    "Oils Price Index":   "oils",
    "Sugar Price Index":  "sugar",
}

# All canonical variable names kept after normalisation
ALL_KEEP_VARS = {
    # Energy
    "date", "gas_storage_twh", "gas_injection_twh", "gas_withdrawal_twh",
    "gas_capacity_twh", "gas_injection_cap_twh", "gas_withdrawal_cap_twh",
    "gas_trend_pct", "gas_full_pct",
    "lng_inventory_twh", "lng_sendout_twh", "lng_sendout_cap_twh",
    # Derived energy
    "injection_gap", "storage_pct", "cumulative_deficit",
    "sendout_gap", "lng_storage_pct",
    # Industry
    "ipi", "ipi_yoy_pct", "ipi_mom_pct",
    "bdi_close", "bdi_change_pct", "bdi_30d_ma", "bdi_stress",
    # Food
    "ffpi", "cereals", "dairy", "meat", "oils", "sugar",
    "ffpi_yoy_pct", "food_stress",
    # Composite
    "stress_index",
}
