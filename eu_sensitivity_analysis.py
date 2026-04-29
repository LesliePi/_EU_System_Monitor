# EU System Monitor -- Sensitivity Analysis
# eu_sensitivity_analysis.py
# Version: v1.0  (2026-04-29)
# Author: Laszlo Tatai / BarefootRealism Labs
# -*- coding: utf-8 -*-

"""
Sensitivity Analysis -- Cascade Threshold Robustness
======================================================

PURPOSE:
  Tests whether the stress index results are robust to changes
  in the cascade threshold parameters.

  If the conclusions hold across a range of parameter values,
  the model is robust and the specific parameter choice is
  less critical.

  This is standard practice in risk modeling and econometrics.
  It answers the question:
    "What if you had chosen 0.55 instead of 0.60?"

PARAMETERS TESTED:
  1. ENERGY_CASCADE_THRESHOLD: 0.50, 0.55, 0.60 (base), 0.65, 0.70
  2. INDUSTRY_AMPLIFIER:       1.5, 2.0 (base), 2.5
  3. FOOD_AMPLIFIER:           1.25, 1.5 (base), 1.75

OUTPUT:
  - Table showing stress_index for each parameter combination
  - Stability score: how much the result varies
  - Conclusion: robust or sensitive

INTERPRETATION:
  If stress_index stays ELEVATED (0.55-0.75) across all combinations:
    -> Model is robust. Parameter choice is not critical.
  If stress_index changes band (e.g. MODERATE to ELEVATED):
    -> Parameter is influential. Needs justification.
"""

import os
import sys
import copy
import datetime
import numpy as np
import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

from eu_monitor_processor import (
    GIEProcessor, EurostatProcessor, FAOProcessor,
    BalticDryProcessor, StressIndexBuilder
)
from eu_monitor_config import STRESS_INDEX, OUTPUT_DIR

# ============================================================
# PARAMETER GRID
# ============================================================

# Base configuration (current values)
BASE = {
    "energy_threshold":    0.60,
    "industry_threshold":  0.60,
    "industry_amplifier":  2.0,
    "food_amplifier":      1.5,
}

# Test ranges
THRESHOLD_RANGE  = [0.50, 0.55, 0.60, 0.65, 0.70]
INDUSTRY_AMP     = [1.5, 2.0, 2.5]
FOOD_AMP         = [1.25, 1.5, 1.75]

# Stress bands for reference
BANDS = {
    "low":      (0.00, 0.30),
    "moderate": (0.30, 0.55),
    "elevated": (0.55, 0.75),
    "critical": (0.75, 1.00),
}

def _band(v):
    if pd.isna(v): return "unknown"
    for label, (lo, hi) in BANDS.items():
        if lo <= v <= hi: return label
    return "critical"

# ============================================================
# STRESS INDEX CALCULATOR
# ============================================================

def compute_stress_with_params(
    merged: pd.DataFrame,
    energy_threshold:   float,
    industry_threshold: float,
    industry_amplifier: float,
    food_amplifier:     float,
    cfg: dict,
) -> pd.Series:
    """
    Compute stress index with custom cascade parameters.
    Returns Series of stress_index values.
    """
    # Build pillar scores (same as StressIndexBuilder._compute_pillar)
    def _normalise(series, lo, hi, inverse=False):
        clipped = series.clip(lower=lo, upper=hi)
        normed  = (clipped - lo) / (hi - lo)
        return (1.0 - normed) if inverse else normed

    PILLAR_ENERGY   = {"gas_storage_deficit", "lng_sendout_gap"}
    PILLAR_INDUSTRY = {"ipi_deviation", "logistics_stress"}
    PILLAR_FOOD     = {"food_price_pressure"}

    components = cfg["components"]

    def pillar_score(pillar_set):
        normed = []
        for name, comp in components.items():
            if name not in pillar_set:
                continue
            col = comp["source_metric"]
            if col not in merged.columns:
                continue
            s = _normalise(
                merged[col],
                lo=comp["norm_range"][0],
                hi=comp["norm_range"][1],
                inverse=(comp["direction"] == "inverse")
            )
            normed.append((s, comp["weight"]))

        if not normed:
            return pd.Series(np.full(len(merged), np.nan), index=merged.index)

        total_w = sum(w for _, w in normed)
        score   = pd.Series(np.zeros(len(merged)), index=merged.index)
        for s, w in normed:
            score += s.fillna(0.5) * (w / total_w)
        return score.clip(0.0, 1.0)

    se = pillar_score(PILLAR_ENERGY)
    si = pillar_score(PILLAR_INDUSTRY)
    sf = pillar_score(PILLAR_FOOD)

    w_base = {"energy": 0.45, "industry": 0.30, "food": 0.25}

    stress = np.zeros(len(merged))
    for i in range(len(merged)):
        e = se.iloc[i] if pd.notna(se.iloc[i]) else 0.5
        d = si.iloc[i] if pd.notna(si.iloc[i]) else 0.5
        f = sf.iloc[i] if pd.notna(sf.iloc[i]) else 0.5

        we = w_base["energy"]
        wi = w_base["industry"]
        wf = w_base["food"]

        if e > energy_threshold:
            wi *= industry_amplifier
        if e > energy_threshold or d > industry_threshold:
            wf *= food_amplifier

        total_w   = we + wi + wf
        composite = (e * we + d * wi + f * wf) / total_w
        stress[i] = min(max(composite, 0.0), 1.0)

    return pd.Series(stress, index=merged.index)


# ============================================================
# LOAD DATA ONCE
# ============================================================

def load_data():
    """Load all source data once -- reused across all parameter tests."""
    print("Loading source data (once)...")
    agsi = GIEProcessor("agsi").run()
    alsi = GIEProcessor("alsi").run()
    ipi  = EurostatProcessor().run()
    fao  = FAOProcessor().run()
    bdi  = BalticDryProcessor().run()

    # Build merged monthly DataFrame (same as StressIndexBuilder.build)
    builder = StressIndexBuilder()
    frames  = {}
    if agsi is not None: frames["agsi"] = builder._to_monthly(agsi)
    if alsi is not None: frames["alsi"] = builder._to_monthly(alsi)
    if ipi  is not None: frames["ipi"]  = ipi.copy()
    if fao  is not None: frames["fao"]  = fao.copy()
    if bdi  is not None: frames["bdi"]  = builder._to_monthly(bdi)

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
    print(f"  Merged: {len(merged)} monthly rows")
    return merged


# ============================================================
# SENSITIVITY TEST 1: CASCADE THRESHOLD
# ============================================================

def test_threshold_sensitivity(merged: pd.DataFrame) -> pd.DataFrame:
    """
    Test how stress_index changes as energy cascade threshold varies.
    All other parameters held at base values.
    """
    print("\n" + "="*60)
    print("  TEST 1: Energy Cascade Threshold Sensitivity")
    print(f"  Range: {THRESHOLD_RANGE}")
    print(f"  Base value: {BASE['energy_threshold']}")
    print("="*60)

    results = []
    for thresh in THRESHOLD_RANGE:
        stress = compute_stress_with_params(
            merged,
            energy_threshold   = thresh,
            industry_threshold = thresh,
            industry_amplifier = BASE["industry_amplifier"],
            food_amplifier     = BASE["food_amplifier"],
            cfg                = STRESS_INDEX,
        )
        latest = stress.dropna().iloc[-1] if stress.notna().any() else np.nan
        cascade_months = int((stress > 0.55).sum())

        results.append({
            "threshold":      thresh,
            "latest_stress":  round(latest, 4),
            "band":           _band(latest),
            "cascade_months": cascade_months,
            "mean_stress":    round(stress.dropna().mean(), 4),
            "is_base":        thresh == BASE["energy_threshold"],
        })

    df = pd.DataFrame(results)
    print(f"\n  {'Threshold':>10} {'Latest':>8} {'Band':>10} {'Mean':>8} {'Cascade mo':>12} {'Base':>6}")
    print("  " + "-"*58)
    for _, row in df.iterrows():
        marker = " <--" if row["is_base"] else ""
        print(f"  {row['threshold']:>10.2f} {row['latest_stress']:>8.4f} "
              f"{row['band']:>10} {row['mean_stress']:>8.4f} "
              f"{row['cascade_months']:>12}{marker}")

    # Stability score: std of latest stress across all thresholds
    std = df["latest_stress"].std()
    print(f"\n  Stability: std={std:.4f} "
          f"({'ROBUST' if std < 0.05 else 'SENSITIVE -- review threshold'})")
    return df


# ============================================================
# SENSITIVITY TEST 2: AMPLIFIER VALUES
# ============================================================

def test_amplifier_sensitivity(merged: pd.DataFrame) -> pd.DataFrame:
    """
    Test how stress_index changes as amplifier values vary.
    Threshold held at base value.
    """
    print("\n" + "="*60)
    print("  TEST 2: Cascade Amplifier Sensitivity")
    print(f"  Industry amplifier range: {INDUSTRY_AMP}")
    print(f"  Food amplifier range:     {FOOD_AMP}")
    print("="*60)

    results = []
    for ind_amp in INDUSTRY_AMP:
        for food_amp in FOOD_AMP:
            stress = compute_stress_with_params(
                merged,
                energy_threshold   = BASE["energy_threshold"],
                industry_threshold = BASE["industry_threshold"],
                industry_amplifier = ind_amp,
                food_amplifier     = food_amp,
                cfg                = STRESS_INDEX,
            )
            latest = stress.dropna().iloc[-1] if stress.notna().any() else np.nan
            is_base = (ind_amp == BASE["industry_amplifier"] and
                      food_amp == BASE["food_amplifier"])
            results.append({
                "industry_amp":  ind_amp,
                "food_amp":      food_amp,
                "latest_stress": round(latest, 4),
                "band":          _band(latest),
                "mean_stress":   round(stress.dropna().mean(), 4),
                "is_base":       is_base,
            })

    df = pd.DataFrame(results)
    print(f"\n  {'Ind.Amp':>8} {'Food.Amp':>9} {'Latest':>8} {'Band':>10} {'Mean':>8} {'Base':>6}")
    print("  " + "-"*55)
    for _, row in df.iterrows():
        marker = " <--" if row["is_base"] else ""
        print(f"  {row['industry_amp']:>8.1f} {row['food_amp']:>9.2f} "
              f"{row['latest_stress']:>8.4f} {row['band']:>10} "
              f"{row['mean_stress']:>8.4f}{marker}")

    std = df["latest_stress"].std()
    print(f"\n  Stability: std={std:.4f} "
          f"({'ROBUST' if std < 0.05 else 'SENSITIVE -- review amplifiers'})")
    return df


# ============================================================
# SENSITIVITY TEST 3: NORM RANGE
# ============================================================

def test_norm_range_sensitivity(merged: pd.DataFrame) -> pd.DataFrame:
    """
    Test how stress_index changes when gas storage norm_range varies.
    Tests [15,100], [20,100] (base), [25,100].
    """
    print("\n" + "="*60)
    print("  TEST 3: Gas Storage Norm Range Sensitivity")
    print("  Base: [20, 100]")
    print("="*60)

    test_ranges = [
        (15.0, 100.0, "pessimistic (lower floor)"),
        (20.0, 100.0, "base"),
        (25.0, 100.0, "optimistic (higher floor)"),
        (20.0,  95.0, "base + realistic ceiling"),
    ]

    results = []
    for lo, hi, label in test_ranges:
        # Temporarily modify the config
        cfg_copy = copy.deepcopy(STRESS_INDEX)
        cfg_copy["components"]["gas_storage_deficit"]["norm_range"] = [lo, hi]

        stress = compute_stress_with_params(
            merged,
            energy_threshold   = BASE["energy_threshold"],
            industry_threshold = BASE["industry_threshold"],
            industry_amplifier = BASE["industry_amplifier"],
            food_amplifier     = BASE["food_amplifier"],
            cfg                = cfg_copy,
        )
        latest = stress.dropna().iloc[-1] if stress.notna().any() else np.nan
        results.append({
            "range":         f"[{lo:.0f}, {hi:.0f}]",
            "label":         label,
            "latest_stress": round(latest, 4),
            "band":          _band(latest),
            "mean_stress":   round(stress.dropna().mean(), 4),
        })

    df = pd.DataFrame(results)
    print(f"\n  {'Range':>12} {'Latest':>8} {'Band':>10} {'Mean':>8}  Label")
    print("  " + "-"*60)
    for _, row in df.iterrows():
        print(f"  {row['range']:>12} {row['latest_stress']:>8.4f} "
              f"{row['band']:>10} {row['mean_stress']:>8.4f}  {row['label']}")

    std = df["latest_stress"].std()
    print(f"\n  Stability: std={std:.4f} "
          f"({'ROBUST' if std < 0.03 else 'SENSITIVE -- review norm range'})")
    return df


# ============================================================
# SUMMARY
# ============================================================

def print_summary(t1: pd.DataFrame, t2: pd.DataFrame, t3: pd.DataFrame):
    print("\n" + "="*60)
    print("  SENSITIVITY ANALYSIS SUMMARY")
    print("="*60)

    std1 = t1["latest_stress"].std()
    std2 = t2["latest_stress"].std()
    std3 = t3["latest_stress"].std()

    base_stress = t1[t1["is_base"]]["latest_stress"].values[0]
    base_band   = t1[t1["is_base"]]["band"].values[0]

    print(f"\n  Base configuration result:")
    print(f"    stress_index = {base_stress:.4f} ({base_band.upper()})")

    print(f"\n  Parameter sensitivity (std of stress_index):")
    print(f"    Cascade threshold : {std1:.4f} "
          f"({'ROBUST' if std1 < 0.05 else 'SENSITIVE'})")
    print(f"    Amplifier values  : {std2:.4f} "
          f"({'ROBUST' if std2 < 0.05 else 'SENSITIVE'})")
    print(f"    Norm range        : {std3:.4f} "
          f"({'ROBUST' if std3 < 0.03 else 'SENSITIVE'})")

    all_stresses = pd.concat([
        t1["latest_stress"], t2["latest_stress"], t3["latest_stress"]
    ])
    all_bands = [_band(v) for v in all_stresses]
    unique_bands = set(all_bands)

    print(f"\n  Stress bands across ALL parameter combinations:")
    for band in ["low", "moderate", "elevated", "critical"]:
        count = all_bands.count(band)
        if count > 0:
            print(f"    {band:10s}: {count} combinations")

    print(f"\n  CONCLUSION:")
    if len(unique_bands) == 1:
        print(f"    All parameter combinations give the same band: {unique_bands.pop().upper()}")
        print(f"    The model is ROBUST to parameter choice.")
        print(f"    The {base_band.upper()} classification holds regardless")
        print(f"    of reasonable parameter variations.")
    elif "elevated" in unique_bands and "critical" in unique_bands and \
         "low" not in unique_bands and "moderate" not in unique_bands:
        print(f"    All combinations fall in ELEVATED or CRITICAL.")
        print(f"    The model is ROBUST -- system is clearly stressed")
        print(f"    regardless of parameter choice.")
    else:
        print(f"    Results span multiple bands: {unique_bands}")
        print(f"    The model is PARAMETER SENSITIVE.")
        print(f"    Review parameter justification.")

    # Save results
    out_path = os.path.join(OUTPUT_DIR, "sensitivity_analysis.csv")
    combined = pd.concat([
        t1.assign(test="threshold"),
        t2.assign(test="amplifier"),
        t3.assign(test="norm_range"),
    ], ignore_index=True)
    combined.to_csv(out_path, index=False)
    print(f"\n  Full results saved: {out_path}")


# ============================================================
# MASTER RUNNER
# ============================================================

def run_sensitivity_analysis():
    print("\n" + "="*60)
    print("  EU System Monitor -- Sensitivity Analysis")
    print(f"  {datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}")
    print("="*60)
    print("\n  Purpose: Test robustness of stress_index to parameter changes.")
    print("  Method:  Run stress computation across parameter grid.")
    print("  Output:  Stability scores + band consistency check.")

    merged = load_data()

    t1 = test_threshold_sensitivity(merged)
    t2 = test_amplifier_sensitivity(merged)
    t3 = test_norm_range_sensitivity(merged)

    print_summary(t1, t2, t3)

    return {"threshold": t1, "amplifier": t2, "norm_range": t3}


if __name__ == "__main__":
    run_sensitivity_analysis()
