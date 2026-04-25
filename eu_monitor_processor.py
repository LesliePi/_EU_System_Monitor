# EU System Monitor - Unified Processor
# eu_monitor_processor.py
# Version: v1.0  (2026-04-25)
# Author: Laszlo Tatai / BarefootRealism Labs
# License: Apache License 2.0 WITH Commons Clause v1.0
# -*- coding: utf-8 -*-

"""
Unified Processor for EU System Monitor
=========================================

Reads raw downloaded files and produces two output datasets:

  eu_monitor_daily.csv   - daily resolution (GIE gas/LNG, BDI)
                           used for dashboard time series charts
  eu_monitor_monthly.csv - monthly aggregates (all four sources)
                           used for Stress Index calculation

NOTE ON RESOLUTION:
  GIE AGSI/ALSI data is daily. BDI is daily (business days).
  Eurostat IPI is monthly. FAO FFPI is monthly.
  The Stress Index is calculated exclusively on monthly aggregates.
  This is documented here and shown in the dashboard footer.
  No interpolation is applied - missing values remain NaN.

Processors:
  GIEProcessor       - agsi_YYYY.json / alsi_YYYY.json
  EurostatProcessor  - eurostat_ipi_DATE.json (Eurostat SDMX JSON)
  FAOProcessor       - fao_fppi_YYYYMM.xlsx
  BalticDryProcessor - bdi_*.csv (manually ingested)
  StressIndexBuilder - monthly composite from all four sources
  run_all_processing - master runner with checkpoint + export

Derived metrics computed here (not in downloader):
  injection_gap       = gas_injection_cap_twh - gas_injection_twh
  storage_pct         = gas_storage_twh / gas_capacity_twh * 100
  cumulative_deficit  = rolling 30-day sum of injection_gap
  sendout_gap         = lng_sendout_cap_twh - lng_sendout_twh
  ipi_yoy_pct         = IPI year-on-year % change
  ipi_mom_pct         = IPI month-on-month % change
  ffpi_yoy_pct        = FFPI year-on-year % change
  bdi_30d_ma          = BDI 30-day moving average
  stress_index        = weighted composite 0-1 (see StressIndexBuilder)
"""

import os
import sys
import json
import glob
import datetime
import numpy as np
import pandas as pd

try:
    from eu_monitor_config import (
        RAW_GIE_AGSI, RAW_GIE_ALSI, RAW_EUROSTAT, RAW_FAO, RAW_BALTIC,
        PROCESSED_DIR, OUTPUT_DIR,
        PROCESSED_CSV, PROCESSED_PARQUET, CHECKPOINT_FILE,
        VAR_RENAME, STRESS_INDEX,
        ENABLE_GIE_AGSI, ENABLE_GIE_ALSI, ENABLE_EUROSTAT,
        ENABLE_FAO, ENABLE_BALTIC,
        BATCH_SIZE,
    )
except ImportError:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from eu_monitor_config import (
        RAW_GIE_AGSI, RAW_GIE_ALSI, RAW_EUROSTAT, RAW_FAO, RAW_BALTIC,
        PROCESSED_DIR, OUTPUT_DIR,
        PROCESSED_CSV, PROCESSED_PARQUET, CHECKPOINT_FILE,
        VAR_RENAME, STRESS_INDEX,
        ENABLE_GIE_AGSI, ENABLE_GIE_ALSI, ENABLE_EUROSTAT,
        ENABLE_FAO, ENABLE_BALTIC,
        BATCH_SIZE,
    )

os.makedirs(PROCESSED_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR,    exist_ok=True)

# Output file paths
DAILY_CSV   = os.path.join(OUTPUT_DIR, "eu_monitor_daily.csv")
MONTHLY_CSV = os.path.join(OUTPUT_DIR, "eu_monitor_monthly.csv")


# ============================================================
# CHECKPOINT
# ============================================================

def _load_checkpoint() -> dict:
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    return {"processed": []}


def _save_checkpoint(state: dict):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ============================================================
# SHARED UTILITIES
# ============================================================

def _section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def _safe_float(val) -> float:
    """Convert string or numeric to float, return NaN on failure."""
    try:
        if val is None or val == "" or val == "N/A":
            return np.nan
        # GIE uses comma as decimal separator in some fields
        if isinstance(val, str):
            val = val.replace(",", ".")
        return float(val)
    except (ValueError, TypeError):
        return np.nan


def _yoy_pct(series: pd.Series) -> pd.Series:
    """Year-on-year % change for a monthly-indexed series."""
    return series.pct_change(periods=12) * 100.0


def _mom_pct(series: pd.Series) -> pd.Series:
    """Month-on-month % change."""
    return series.pct_change(periods=1) * 100.0


def _normalise_0_1(series: pd.Series, lo: float, hi: float,
                   inverse: bool = False) -> pd.Series:
    """
    Clamp and normalise a series to [0, 1].
    inverse=True: high raw value -> low stress (e.g. storage level).
    inverse=False: high raw value -> high stress (e.g. food price yoy).
    """
    clipped = series.clip(lower=lo, upper=hi)
    normed  = (clipped - lo) / (hi - lo)   # 0 = lo end, 1 = hi end
    if inverse:
        normed = 1.0 - normed
    return normed


# ============================================================
# 1. GIE PROCESSOR  (AGSI+ gas storage and ALSI LNG)
# ============================================================

class GIEProcessor:
    """
    Reads agsi_YYYY.json / alsi_YYYY.json files produced by GIEDownloader.
    Returns a daily DataFrame with all GIE fields + derived metrics.

    Derived metrics:
      storage_pct       = gas_storage_twh / gas_capacity_twh * 100
      injection_gap     = gas_injection_cap_twh - gas_injection_twh  (GWh/d)
      cumulative_deficit= rolling 30-day sum of injection_gap (GWh)
      sendout_gap       = lng_sendout_cap_twh - lng_sendout_twh      (GWh/d)
    """

    # Fields to extract from GIE JSON records
    AGSI_FIELDS = {
        "gasDayStart":        "date",
        "gasInStorage":       "gas_storage_twh",
        "injection":          "gas_injection_gwh",
        "withdrawal":         "gas_withdrawal_gwh",
        "workingGasVolume":   "gas_capacity_twh",
        "injectionCapacity":  "gas_injection_cap_gwh",
        "withdrawalCapacity": "gas_withdrawal_cap_gwh",
        "trend":              "gas_trend_pct",
        "full":               "gas_full_pct",
        "status":             "gas_status",
    }

    ALSI_FIELDS = {
        "gasDayStart":         "date",
        "inventory_lng":       "lng_inventory_bcm",
        "inventory_gwh":       "lng_inventory_gwh",
        "sendOut":             "lng_sendout_gwh",
        "dtmi_lng":            "lng_capacity_bcm",
        "dtrs":                "lng_sendout_cap_gwh",
        "status":              "lng_status",
    }

    def __init__(self, source: str = "agsi"):
        self.source   = source.lower()
        self.raw_dir  = RAW_GIE_AGSI if source == "agsi" else RAW_GIE_ALSI
        self.prefix   = "agsi" if source == "agsi" else "alsi"
        self.fields   = self.AGSI_FIELDS if source == "agsi" else self.ALSI_FIELDS
        print(f"[GIE/{self.source.upper()}] Raw dir: {self.raw_dir}")

    # --------------------------------------------------------
    def _parse_json_file(self, path: str) -> pd.DataFrame | None:
        """Parse one year JSON file into a flat DataFrame."""
        fname = os.path.basename(path)
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"  [ERROR] Cannot read {fname}: {e}")
            return None

        records = data.get("records", [])
        if not records:
            print(f"  [EMPTY] {fname}")
            return None

        rows = []
        for rec in records:
            # GIE returns list of dicts per page; each has the fields directly
            # or nested under 'data' key depending on API version
            if isinstance(rec, dict):
                row = {}
                for src_key, dst_key in self.fields.items():
                    # Handle nested dicts (ALSI inventory, dtmi)
                    if "_" in src_key and src_key.split("_")[0] in rec:
                        parent, child = src_key.split("_", 1)
                        val = rec.get(parent, {})
                        val = val.get(child) if isinstance(val, dict) else None
                    else:
                        val = rec.get(src_key)

                    if dst_key == "date":
                        row[dst_key] = val
                    elif dst_key.endswith("_status"):
                        row[dst_key] = val
                    else:
                        row[dst_key] = _safe_float(val)
                rows.append(row)

        if not rows:
            return None

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"])
        df = df.sort_values("date").reset_index(drop=True)
        print(f"  [OK] {fname}: {len(df)} rows  "
              f"({df['date'].min().date()} to {df['date'].max().date()})")
        return df

    # --------------------------------------------------------
    def _add_derived_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute derived columns after all years are concatenated."""
        if self.source == "agsi":
            # storage_pct: recompute from raw values (more accurate than API 'full')
            df["storage_pct"] = np.where(
                df["gas_capacity_twh"] > 0,
                df["gas_storage_twh"] / df["gas_capacity_twh"] * 100.0,
                np.nan
            )
            # injection_gap in GWh/d: unused injection capacity
            df["injection_gap_gwh"] = (
                df["gas_injection_cap_gwh"] - df["gas_injection_gwh"]
            ).clip(lower=0)

            # cumulative_deficit: rolling 30-day sum of injection_gap
            df = df.sort_values("date").reset_index(drop=True)
            df["cumulative_deficit_gwh"] = (
                df["injection_gap_gwh"]
                .rolling(window=30, min_periods=1)
                .sum()
            )

        elif self.source == "alsi":
            df["sendout_gap_gwh"] = (
                df["lng_sendout_cap_gwh"] - df["lng_sendout_gwh"]
            ).clip(lower=0)
            # TWh conversion for dashboard display
            df["lng_inventory_twh"] = df["lng_inventory_gwh"] / 1000.0
            df["lng_full_pct"] = (
                df["lng_inventory_gwh"] / df["lng_capacity_bcm"].replace(0, float('nan')) * 100.0
            ).clip(0, 100)

        return df

    # --------------------------------------------------------
    def run(self) -> pd.DataFrame | None:
        """Process all year files and return combined daily DataFrame."""
        _section(f"GIE/{self.source.upper()} - Processing")

        files = sorted(glob.glob(
            os.path.join(self.raw_dir, f"{self.prefix}_*.json")
        ))
        if not files:
            print(f"  [WARN] No files found in {self.raw_dir}")
            return None

        print(f"  Found {len(files)} files")
        frames = []
        for path in files:
            df = self._parse_json_file(path)
            if df is not None:
                frames.append(df)

        if not frames:
            print("  [FAILED] No data parsed.")
            return None

        combined = pd.concat(frames, ignore_index=True)
        combined = combined.drop_duplicates(subset=["date"]).sort_values("date")
        combined = self._add_derived_metrics(combined)

        print(f"\n  [DONE] {self.source.upper()}: {len(combined)} daily rows total")
        return combined


# ============================================================
# 2. EUROSTAT PROCESSOR  (Industrial Production Index)
# ============================================================

class EurostatProcessor:
    """
    Reads eurostat_ipi_DATE.json (Eurostat SDMX 3.0 JSON format).
    Extracts monthly IPI time series and computes yoy/mom changes.

    SDMX 3.0 JSON structure:
      data -> dataSets -> [0] -> observations -> {"0:0:0:0:N": [value, ...]}
      data -> structure -> dimensions -> observation -> [time_period values]
    """

    def __init__(self):
        self.raw_dir = RAW_EUROSTAT
        print(f"[EUROSTAT] Raw dir: {self.raw_dir}")

    # --------------------------------------------------------
    def _find_latest_file(self) -> str | None:
        files = sorted(glob.glob(
            os.path.join(self.raw_dir, "eurostat_ipi_*.json")
        ))
        if not files:
            return None
        return files[-1]   # most recent by filename date

    # --------------------------------------------------------
    def _parse_sdmx_json(self, path: str) -> pd.DataFrame | None:
        """
        Parse Eurostat SDMX 2.0 JSON format.
        Returns DataFrame with columns [date, ipi].

        SDMX 2.0 JSON structure:
          dimension["time"]["category"]["index"] = {"2000-01": 0, ...}
          value = {"0": 98.5, "1": 99.1, ...}
        """
        fname = os.path.basename(path)
        try:
            with open(path, "r", encoding="utf-8") as f:
                wrapper = json.load(f)
        except Exception as e:
            print(f"  [ERROR] Cannot read {fname}: {e}")
            return None

        sdmx = wrapper.get("data", wrapper)

        try:
            # Build index->date mapping from time dimension
            time_index  = sdmx["dimension"]["time"]["category"]["index"]
            # time_index is {"2000-01": 0, "2000-02": 1, ...}
            # invert to {0: "2000-01", 1: "2000-02", ...}
            idx_to_date = {v: k for k, v in time_index.items()}

            values_raw = sdmx["value"]

            rows = []
            for idx_str, val in values_raw.items():
                idx = int(idx_str)
                if idx in idx_to_date and val is not None:
                    rows.append({
                        "date": idx_to_date[idx],
                        "ipi":  float(val),
                    })

            if not rows:
                print(f"  [EMPTY] {fname}")
                return None

            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(
                df["date"], format="%Y-%m", errors="coerce"
            )
            df = df.dropna(subset=["date"])
            df = df.sort_values("date").reset_index(drop=True)

            print(f"  [OK] {fname}: {len(df)} monthly rows  "
                  f"({df['date'].min().strftime('%Y-%m')} to "
                  f"{df['date'].max().strftime('%Y-%m')})")
            return df

        except (KeyError, IndexError, TypeError) as e:
            print(f"  [ERROR] SDMX parse failed for {fname}: {e}")
            return None

    # --------------------------------------------------------
    def _add_derived_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values("date").reset_index(drop=True)
        df["ipi_yoy_pct"] = _yoy_pct(df["ipi"])
        df["ipi_mom_pct"] = _mom_pct(df["ipi"])
        return df

    # --------------------------------------------------------
    def run(self) -> pd.DataFrame | None:
        _section("EUROSTAT - Processing")

        path = self._find_latest_file()
        if path is None:
            print(f"  [WARN] No Eurostat files in {self.raw_dir}")
            return None

        df = self._parse_sdmx_json(path)
        if df is None:
            return None

        df = self._add_derived_metrics(df)
        print(f"\n  [DONE] IPI: {len(df)} monthly rows")
        return df


# ============================================================
# 3. FAO PROCESSOR  (Food Price Index)
# ============================================================

class FAOProcessor:
    """
    Reads fao_fppi_YYYYMM.xlsx downloaded from fao.org.
    Extracts the composite FFPI and all five sub-indices.
    Computes year-on-year % change.

    FAO XLSX structure (as of 2024):
      Sheet "Food Price Index" -- first few rows are headers/notes.
      Data columns: Year | Month | Food Price Index | Cereals | ... | Sugar
      Month column contains month names (January, February, ...) or numbers.
    """

    # Expected column name fragments (case-insensitive partial match)
    COL_MAP = {
        "food price index": "ffpi",
        "cereals":          "cereals",
        "dairy":            "dairy",
        "meat":             "meat",
        "oils":             "oils",
        "sugar":            "sugar",
    }

    def __init__(self):
        self.raw_dir = RAW_FAO
        print(f"[FAO] Raw dir: {self.raw_dir}")

    # --------------------------------------------------------
    def _find_latest_file(self) -> str | None:
        files = sorted(glob.glob(
            os.path.join(self.raw_dir, "fao_fppi_*.xlsx")
        ))
        return files[-1] if files else None

    # --------------------------------------------------------
    def _parse_xlsx(self, path: str) -> pd.DataFrame | None:
        """
        Parse FAO FFPI XLSX.
        Sheet: FFPI_Historical
        Structure: rows 0-3 = title/notes, col 0 = Year, col 1 = Nominal index.
        Frequency: ANNUAL. Sub-indices not in this file.
        NOTE: Annual data used as-is. Monthly rows will have NaN except January.
        No interpolation applied -- honest representation of data frequency.
        """
        fname = os.path.basename(path)
        try:
            df_raw = pd.read_excel(
                path, sheet_name="FFPI_Historical",
                header=None, engine="openpyxl"
            )
        except Exception as e:
            print(f"  [ERROR] Cannot open {fname}: {e}")
            return None

        # Data starts at row 4 (0-indexed), col 0 = year, col 1 = nominal FFPI
        data = df_raw.iloc[4:, [0, 1]].copy()
        data.columns = ["year", "ffpi"]
        data["year"] = pd.to_numeric(data["year"], errors="coerce")
        data["ffpi"] = pd.to_numeric(data["ffpi"], errors="coerce")
        data = data.dropna(subset=["year", "ffpi"])
        data["year"] = data["year"].astype(int)

        # Anchor each annual value to January of that year
        data["date"] = pd.to_datetime(
            data["year"].astype(str) + "-01", format="%Y-%m"
        )
        data = data[["date", "ffpi"]].sort_values("date").reset_index(drop=True)

        print(f"  [OK] {fname} (annual FFPI, {len(data)} years, "
              f"{data['date'].dt.year.min()}-{data['date'].dt.year.max()})")
        print(f"  NOTE: Sub-indices not in this file. "
              f"Annual frequency -- monthly rows NaN except January.")
        return data

    # --------------------------------------------------------
    def _add_derived_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values("date").reset_index(drop=True)
        if "ffpi" in df.columns:
            df["ffpi_yoy_pct"] = _yoy_pct(df["ffpi"])
        return df

    # --------------------------------------------------------
    def run(self) -> pd.DataFrame | None:
        _section("FAO - Processing")

        path = self._find_latest_file()
        if path is None:
            print(f"  [WARN] No FAO files in {self.raw_dir}")
            return None

        df = self._parse_xlsx(path)
        if df is None:
            return None

        df = self._add_derived_metrics(df)
        print(f"\n  [DONE] FFPI: {len(df)} monthly rows")
        return df


# ============================================================
# 4. BALTIC DRY PROCESSOR
# ============================================================

class BalticDryProcessor:
    """
    Reads manually ingested BDI CSV files from RAW_BALTIC.
    Accepts Investing.com export format:
      Date, Price, Open, High, Low, Change %

    Derived metrics:
      bdi_30d_ma  = 30-day moving average of closing price
    """

    def __init__(self):
        self.raw_dir = RAW_BALTIC
        print(f"[BALTIC/BDI] Raw dir: {self.raw_dir}")

    # --------------------------------------------------------
    def _parse_csv(self, path: str) -> pd.DataFrame | None:
        """Parse one BDI CSV file."""
        fname = os.path.basename(path)
        try:
            df = pd.read_csv(path, thousands=",", encoding="utf-8-sig")
        except Exception as e:
            print(f"  [ERROR] Cannot read {fname}: {e}")
            return None

        df.columns = [c.strip() for c in df.columns]

        # Date column
        if "Date" not in df.columns:
            print(f"  [ERROR] No 'Date' column in {fname}")
            return None

        df["date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["date"])

        # Price column
        price_col = next((c for c in df.columns if "Price" in c), None)
        if price_col is None:
            print(f"  [ERROR] No 'Price' column in {fname}")
            return None

        df["bdi_close"] = pd.to_numeric(
            df[price_col].astype(str).str.replace(",", ""),
            errors="coerce"
        )

        # Change %
        chg_col = next((c for c in df.columns if "Change" in c), None)
        if chg_col:
            df["bdi_change_pct"] = pd.to_numeric(
                df[chg_col].astype(str)
                .str.replace("%", "").str.replace(",", "."),
                errors="coerce"
            )

        result = df[["date", "bdi_close"] +
                    (["bdi_change_pct"] if "bdi_change_pct" in df.columns else [])
                   ].copy()
        result = result.sort_values("date").reset_index(drop=True)

        print(f"  [OK] {fname}: {len(result)} rows  "
              f"({result['date'].min().date()} to {result['date'].max().date()})")
        return result

    # --------------------------------------------------------
    def _add_derived_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values("date").reset_index(drop=True)
        df["bdi_30d_ma"] = (
            df["bdi_close"]
            .rolling(window=30, min_periods=5)
            .mean()
        )
        return df

    # --------------------------------------------------------
    def run(self) -> pd.DataFrame | None:
        _section("BALTIC DRY - Processing")

        files = sorted(glob.glob(os.path.join(self.raw_dir, "bdi_*.csv")))
        if not files:
            print(f"  [WARN] No BDI files in {self.raw_dir}")
            return None

        print(f"  Found {len(files)} file(s)")
        frames = []
        for path in files:
            df = self._parse_csv(path)
            if df is not None:
                frames.append(df)

        if not frames:
            return None

        combined = pd.concat(frames, ignore_index=True)
        combined = combined.drop_duplicates(subset=["date"]).sort_values("date")
        combined = self._add_derived_metrics(combined)

        print(f"\n  [DONE] BDI: {len(combined)} daily rows")
        return combined


# ============================================================
# 5. STRESS INDEX BUILDER
# ============================================================

class StressIndexBuilder:
    """
    Builds the EU System Stress Index from monthly aggregates.

    THREE-PILLAR CASCADE MODEL
    --------------------------
    Step 1: Compute pillar-level stress scores (0-1) independently.
            stress_energy   = f(gas_storage, lng_sendout)
            stress_industry = f(ipi_yoy, bdi_30d_ma)
            stress_food     = f(ffpi_yoy)

    Step 2: Apply cascade amplification (threshold-based, explicit rules):
            Rule A: if stress_energy > 0.6
                    -> industry_weight *= 2.0
                    -> cascade_active = True
            Rule B: if stress_energy > 0.6 OR stress_industry > 0.6
                    -> food_weight *= 1.5
                    -> cascade_active = True
            Rationale: energy crisis amplifies industrial stress;
            either upper-pillar crisis amplifies food stress.
            This reflects real supply chain cascade dynamics.

    Step 3: Weighted sum with amplified weights, normalised to [0,1].

    Outputs per row:
      stress_energy    - energy pillar score [0,1]
      stress_industry  - industry pillar score [0,1]
      stress_food      - food pillar score [0,1]
      stress_index     - cascade composite [0,1]
      stress_band      - low / moderate / elevated / critical
      cascade_active   - True if any threshold rule fired

    NOTE: Stress Index is computed on MONTHLY aggregates only.
    Daily fluctuations create noise in composite indicators.
    Documented in dashboard footer.

    CASCADE THRESHOLDS (explicit, auditable):
      ENERGY_CASCADE_THRESHOLD  = 0.6
      INDUSTRY_CASCADE_THRESHOLD = 0.6
      INDUSTRY_AMPLIFIER         = 2.0  (weight multiplier)
      FOOD_AMPLIFIER             = 1.5  (weight multiplier)
    """

    # Cascade thresholds -- explicit constants, not hidden parameters
    ENERGY_CASCADE_THRESHOLD   = 0.6
    INDUSTRY_CASCADE_THRESHOLD = 0.6
    INDUSTRY_AMPLIFIER         = 2.0
    FOOD_AMPLIFIER             = 1.5

    # Pillar membership -- which config components belong to which pillar
    PILLAR_ENERGY   = {"gas_storage_deficit", "lng_sendout_gap"}
    PILLAR_INDUSTRY = {"ipi_deviation", "logistics_stress"}
    PILLAR_FOOD     = {"food_price_pressure"}

    # Base pillar weights (before cascade amplification)
    PILLAR_BASE_WEIGHTS = {
        "energy":   0.45,
        "industry": 0.30,
        "food":     0.25,
    }

    def __init__(self, cfg: dict = None):
        self.cfg = cfg or STRESS_INDEX

    # --------------------------------------------------------
    def _to_monthly(self, df: pd.DataFrame, date_col: str = "date",
                    agg: str = "mean") -> pd.DataFrame:
        """Resample any DataFrame to monthly frequency."""
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        df = df.set_index(date_col)
        monthly = df.resample("MS").agg(agg, numeric_only=True)
        monthly.index.name = "date"
        return monthly.reset_index()

    # --------------------------------------------------------
    def _compute_pillar(
        self,
        merged: pd.DataFrame,
        pillar_components: set,
    ) -> pd.Series:
        """
        Compute a single pillar stress score as weighted mean
        of its normalised components. Returns Series [0,1].
        Missing components are skipped (weight redistributed).
        """
        components = self.cfg["components"]
        normed = []

        for comp_name, comp_cfg in components.items():
            if comp_name not in pillar_components:
                continue

            src_col   = comp_cfg["source_metric"]
            direction = comp_cfg["direction"]
            weight    = comp_cfg["weight"]
            lo, hi    = comp_cfg["norm_range"]
            inverse   = (direction == "inverse")

            if src_col not in merged.columns:
                continue

            s = _normalise_0_1(merged[src_col], lo, hi, inverse=inverse)
            normed.append((s, weight))

        if not normed:
            # No data for this pillar -- return neutral 0.5
            return pd.Series(np.full(len(merged), np.nan), index=merged.index)

        total_w = sum(w for _, w in normed)
        pillar  = pd.Series(np.zeros(len(merged)), index=merged.index)
        for s, w in normed:
            pillar += s.fillna(0.5) * (w / total_w)

        return pillar.clip(0.0, 1.0)

    # --------------------------------------------------------
    def build(
        self,
        agsi_daily:  pd.DataFrame | None,
        alsi_daily:  pd.DataFrame | None,
        ipi_monthly: pd.DataFrame | None,
        fao_monthly: pd.DataFrame | None,
        bdi_daily:   pd.DataFrame | None,
    ) -> pd.DataFrame | None:
        """
        Merge all sources, compute pillar scores, apply cascade
        amplification, and produce the composite stress_index.
        """
        _section("STRESS INDEX - Cascade Model")

        frames = {}

        if agsi_daily is not None:
            frames["agsi"] = self._to_monthly(agsi_daily)
            print(f"  AGSI:  {len(frames['agsi'])} monthly rows")

        if alsi_daily is not None:
            frames["alsi"] = self._to_monthly(alsi_daily)
            print(f"  ALSI:  {len(frames['alsi'])} monthly rows")

        if ipi_monthly is not None:
            frames["ipi"] = ipi_monthly.copy()
            print(f"  IPI:   {len(frames['ipi'])} monthly rows")

        if fao_monthly is not None:
            frames["fao"] = fao_monthly.copy()
            print(f"  FAO:   {len(frames['fao'])} monthly rows")

        if bdi_daily is not None:
            frames["bdi"] = self._to_monthly(bdi_daily)
            print(f"  BDI:   {len(frames['bdi'])} monthly rows")

        if not frames:
            print("  [FAILED] No data available for stress index")
            return None

        # Merge all sources on date (outer join)
        merged = None
        for name, df in frames.items():
            df = df.copy()
            df["date"] = pd.to_datetime(df["date"])
            if merged is None:
                merged = df
            else:
                merged = pd.merge(merged, df, on="date", how="outer",
                                  suffixes=("", f"_{name}"))

        merged = merged.sort_values("date").reset_index(drop=True)

        # --------------------------------------------------
        # STEP 1: Compute pillar scores independently
        # --------------------------------------------------
        print("\n  STEP 1: Pillar scores")
        merged["stress_energy"]   = self._compute_pillar(merged, self.PILLAR_ENERGY)
        merged["stress_industry"] = self._compute_pillar(merged, self.PILLAR_INDUSTRY)
        merged["stress_food"]     = self._compute_pillar(merged, self.PILLAR_FOOD)

        e_latest = merged["stress_energy"].dropna().iloc[-1]   if merged["stress_energy"].notna().any()   else np.nan
        i_latest = merged["stress_industry"].dropna().iloc[-1] if merged["stress_industry"].notna().any() else np.nan
        f_latest = merged["stress_food"].dropna().iloc[-1]     if merged["stress_food"].notna().any()     else np.nan

        print(f"  Latest stress_energy   : {e_latest:.3f}" if not np.isnan(e_latest) else "  stress_energy  : N/A")
        print(f"  Latest stress_industry : {i_latest:.3f}" if not np.isnan(i_latest) else "  stress_industry: N/A")
        print(f"  Latest stress_food     : {f_latest:.3f}" if not np.isnan(f_latest) else "  stress_food    : N/A")

        # --------------------------------------------------
        # STEP 2: Cascade amplification (row by row, explicit rules)
        # --------------------------------------------------
        print(f"\n  STEP 2: Cascade amplification")
        print(f"  Rule A threshold (energy->industry): {self.ENERGY_CASCADE_THRESHOLD}")
        print(f"  Rule B threshold (upper->food):      {self.INDUSTRY_CASCADE_THRESHOLD}")
        print(f"  Industry amplifier: x{self.INDUSTRY_AMPLIFIER}")
        print(f"  Food amplifier:     x{self.FOOD_AMPLIFIER}")

        w_base = self.PILLAR_BASE_WEIGHTS.copy()

        stress_index   = np.zeros(len(merged))
        cascade_active = np.zeros(len(merged), dtype=bool)

        for i, row in merged.iterrows():
            se = row["stress_energy"]   if pd.notna(row["stress_energy"])   else 0.5
            si = row["stress_industry"] if pd.notna(row["stress_industry"]) else 0.5
            sf = row["stress_food"]     if pd.notna(row["stress_food"])     else 0.5

            we = w_base["energy"]
            wi = w_base["industry"]
            wf = w_base["food"]

            cascade = False

            # Rule A: energy crisis amplifies industry weight
            if se > self.ENERGY_CASCADE_THRESHOLD:
                wi *= self.INDUSTRY_AMPLIFIER
                cascade = True

            # Rule B: any upper pillar crisis amplifies food weight
            if se > self.ENERGY_CASCADE_THRESHOLD or si > self.INDUSTRY_CASCADE_THRESHOLD:
                wf *= self.FOOD_AMPLIFIER
                cascade = True

            # Normalise weights to sum to 1
            total_w = we + wi + wf
            composite = (se * we + si * wi + sf * wf) / total_w

            stress_index[i]   = min(max(composite, 0.0), 1.0)
            cascade_active[i] = cascade

        merged["stress_index"]   = stress_index
        merged["cascade_active"] = cascade_active

        # --------------------------------------------------
        # STEP 3: Band labels
        # --------------------------------------------------
        bands = self.cfg["bands"]

        def _band_label(v):
            if pd.isna(v):
                return "unknown"
            for label, (lo, hi) in bands.items():
                if lo <= v <= hi:
                    return label
            return "critical"

        merged["stress_band"] = merged["stress_index"].apply(_band_label)

        # Summary
        last_valid = merged[merged["stress_index"].notna()].iloc[-1]
        n_cascade  = int(cascade_active.sum())

        print(f"\n  [DONE] Stress Index: {len(merged)} monthly rows")
        print(f"  Cascade active in {n_cascade}/{len(merged)} months "
              f"({100*n_cascade/len(merged):.1f}%)")
        print(f"  Latest stress_index : {last_valid['stress_index']:.3f} "
              f"({last_valid['stress_band']})")
        print(f"  Cascade active now  : {bool(last_valid['cascade_active'])}")

        return merged


# ============================================================
# OUTPUT WRITERS
# ============================================================

def _save_daily(agsi: pd.DataFrame | None,
                alsi: pd.DataFrame | None,
                bdi:  pd.DataFrame | None):
    """
    Merge daily sources and save eu_monitor_daily.csv.
    Only GIE and BDI are daily -- IPI and FAO are excluded here.
    """
    _section("SAVING - Daily CSV")

    frames = []
    if agsi is not None:
        frames.append(agsi)
    if alsi is not None:
        # Merge ALSI into AGSI on date (outer join)
        frames.append(alsi)
    if bdi is not None:
        frames.append(bdi)

    if not frames:
        print("  [SKIP] No daily data to save")
        return

    # Merge all on date
    merged = None
    for df in frames:
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])
        if merged is None:
            merged = df
        else:
            merged = pd.merge(merged, df, on="date", how="outer")

    merged = merged.sort_values("date").reset_index(drop=True)

    # Drop status columns from CSV (keep for audit but not useful in charts)
    drop_cols = [c for c in merged.columns if c.endswith("_status")]
    merged = merged.drop(columns=drop_cols, errors="ignore")

    tmp = DAILY_CSV + ".tmp"
    merged.to_csv(tmp, index=False)
    os.replace(tmp, DAILY_CSV)

    print(f"  [SAVED] {DAILY_CSV}")
    print(f"  Rows: {len(merged)}  Columns: {list(merged.columns)}")


def _save_monthly(monthly: pd.DataFrame):
    """Save eu_monitor_monthly.csv (stress index + all monthly metrics)."""
    _section("SAVING - Monthly CSV")

    if monthly is None or monthly.empty:
        print("  [SKIP] No monthly data to save")
        return

    drop_cols = [c for c in monthly.columns if c.endswith("_status")]
    monthly   = monthly.drop(columns=drop_cols, errors="ignore")

    tmp = MONTHLY_CSV + ".tmp"
    monthly.to_csv(tmp, index=False)
    os.replace(tmp, MONTHLY_CSV)

    print(f"  [SAVED] {MONTHLY_CSV}")
    print(f"  Rows: {len(monthly)}  Columns: {list(monthly.columns)}")

    # Summary of latest month
    last = monthly.dropna(subset=["stress_index"]).iloc[-1] \
           if "stress_index" in monthly.columns else None
    if last is not None:
        print(f"\n  Latest month: {last['date'].strftime('%Y-%m') if hasattr(last['date'], 'strftime') else last['date']}")
        for col in ["gas_full_pct", "ipi", "ffpi", "bdi_close", "stress_index"]:
            if col in last.index and pd.notna(last[col]):
                print(f"    {col:25s}: {last[col]:.2f}")


# ============================================================
# MASTER PROCESSOR
# ============================================================

def run_all_processing(
    gie_agsi:  bool = None,
    gie_alsi:  bool = None,
    eurostat:  bool = None,
    fao:       bool = None,
    bdi:       bool = None,
):
    """
    Run all processors and produce daily + monthly output CSVs.

    Parameters default to ENABLE_* flags in eu_monitor_config.py.
    """
    do_agsi     = gie_agsi if gie_agsi   is not None else ENABLE_GIE_AGSI
    do_alsi     = gie_alsi if gie_alsi   is not None else ENABLE_GIE_ALSI
    do_eurostat = eurostat if eurostat   is not None else ENABLE_EUROSTAT
    do_fao      = fao      if fao        is not None else ENABLE_FAO
    do_bdi      = bdi      if bdi        is not None else ENABLE_BALTIC

    print("\n" + "="*60)
    print("  EU System Monitor - Unified Processor")
    print(f"  {datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}")
    print("="*60)
    print(f"  GIE AGSI  : {'ON' if do_agsi     else 'OFF'}")
    print(f"  GIE ALSI  : {'ON' if do_alsi     else 'OFF'}")
    print(f"  Eurostat  : {'ON' if do_eurostat else 'OFF'}")
    print(f"  FAO       : {'ON' if do_fao      else 'OFF'}")
    print(f"  Baltic Dry: {'ON' if do_bdi      else 'OFF'}")
    print(f"\n  NOTE: Stress Index uses monthly aggregates only.")
    print(f"  Daily CSV uses GIE and BDI at native daily resolution.")

    # Run processors
    agsi_data = GIEProcessor("agsi").run()     if do_agsi     else None
    alsi_data = GIEProcessor("alsi").run()     if do_alsi     else None
    ipi_data  = EurostatProcessor().run()      if do_eurostat else None
    fao_data  = FAOProcessor().run()           if do_fao      else None
    bdi_data  = BalticDryProcessor().run()     if do_bdi      else None

    # Build stress index (monthly)
    monthly = StressIndexBuilder().build(
        agsi_daily  = agsi_data,
        alsi_daily  = alsi_data,
        ipi_monthly = ipi_data,
        fao_monthly = fao_data,
        bdi_daily   = bdi_data,
    )

    # Save outputs
    _save_daily(agsi_data, alsi_data, bdi_data)
    _save_monthly(monthly)

    print("\n" + "="*60)
    print("  Processing complete.")
    print(f"  Daily   : {DAILY_CSV}")
    print(f"  Monthly : {MONTHLY_CSV}")
    print("="*60)

    return {
        "agsi":    agsi_data,
        "alsi":    alsi_data,
        "ipi":     ipi_data,
        "fao":     fao_data,
        "bdi":     bdi_data,
        "monthly": monthly,
    }


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(
        description="EU System Monitor - Unified Processor"
    )
    p.add_argument("--no-agsi",     action="store_true", help="Skip GIE AGSI")
    p.add_argument("--no-alsi",     action="store_true", help="Skip GIE ALSI")
    p.add_argument("--no-eurostat", action="store_true", help="Skip Eurostat")
    p.add_argument("--no-fao",      action="store_true", help="Skip FAO")
    p.add_argument("--no-bdi",      action="store_true", help="Skip Baltic Dry")
    args = p.parse_args()

    run_all_processing(
        gie_agsi  = not args.no_agsi,
        gie_alsi  = not args.no_alsi,
        eurostat  = not args.no_eurostat,
        fao       = not args.no_fao,
        bdi       = not args.no_bdi,
    )
