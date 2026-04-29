# EU System Monitor -- Sensitivity Analysis Report

**Version:** 1.0
**Date:** 2026-04-29
**Script:** eu_sensitivity_analysis.py
**Author:** Laszlo Tatai / BarefootRealism Labs

---

## What This Report Is

This document presents the results of a formal sensitivity analysis
of the EU System Monitor stress index parameters.

Sensitivity analysis answers the question:
> "Does the conclusion change if we choose different parameter values?"

If the answer is NO -- the model is robust.
If the answer is YES -- the parameter needs stronger justification.

---

## Parameters Tested

### 1. Energy Cascade Threshold

**What it is:** The stress level at which an energy crisis begins
to amplify industrial stress. Above this threshold, the industry
pillar weight is doubled.

**Base value:** 0.60

**Tested range:** 0.50, 0.55, 0.60, 0.65, 0.70

**Results:**

| Threshold | Latest stress | Band | Mean stress |
|-----------|-------------|------|-------------|
| 0.50 | 0.5741 | ELEVATED | 0.4806 |
| 0.55 | 0.5741 | ELEVATED | 0.4809 |
| **0.60** | **0.5741** | **ELEVATED** | **0.4813** |
| 0.65 | 0.5741 | ELEVATED | 0.4821 |
| 0.70 | 0.5741 | ELEVATED | 0.4829 |

**Stability:** std = 0.0000 (ROBUST)

**Interpretation:** The latest stress index is identical (0.5741)
regardless of the threshold chosen. This is because the current
energy stress (0.922) is far above any of the tested thresholds.
The cascade is active under all configurations.

---

### 2. Cascade Amplifier Values

**What it is:** How much the industry and food weights increase
when cascade thresholds are exceeded.

**Base values:** Industry x2.0, Food x1.5

**Tested combinations:** 9 (3 industry x 3 food amplifiers)

**Results:**

| Industry amp | Food amp | Latest stress | Band |
|-------------|---------|-------------|------|
| 1.5 | 1.25 | 0.6044 | ELEVATED |
| 1.5 | 1.50 | 0.5993 | ELEVATED |
| 1.5 | 1.75 | 0.5947 | ELEVATED |
| 2.0 | 1.25 | 0.5775 | ELEVATED |
| **2.0** | **1.50** | **0.5741** | **ELEVATED** |
| 2.0 | 1.75 | 0.5710 | ELEVATED |
| 2.5 | 1.25 | 0.5558 | ELEVATED |
| 2.5 | 1.50 | 0.5536 | ELEVATED |
| 2.5 | 1.75 | 0.5516 | ELEVATED |

**Stability:** std = 0.0201 (ROBUST)

**Interpretation:** All 9 combinations give ELEVATED band.
The stress index ranges from 0.5516 to 0.6044 -- a spread of
0.053 index points. The band does not change.

---

### 3. Gas Storage Norm Range

**What it is:** The reference range for normalising gas storage %.
Lower bound = maximum stress level. Upper bound = zero stress level.

**Base values:** [20%, 100%]

**Tested variants:**

| Range | Interpretation | Latest stress | Band |
|-------|---------------|-------------|------|
| [15, 100] | Pessimistic floor | 0.5631 | ELEVATED |
| **[20, 100]** | **Base** | **0.5741** | **ELEVATED** |
| [25, 100] | Optimistic floor | 0.5865 | ELEVATED |
| [20, 95] | Realistic ceiling | 0.5724 | ELEVATED |

**Stability:** std = 0.0096 (ROBUST)

**Interpretation:** Whether we assume maximum stress starts at 15%
or 25% full, the result is ELEVATED. The current 29.36% storage
level is below all tested lower bounds, meaning it registers as
near-maximum stress regardless.

---

## Overall Conclusion

**Total parameter combinations tested:** 18

**Stress bands observed:**
- ELEVATED: 18 combinations (100%)
- No combination produced LOW, MODERATE, or CRITICAL

**Stability statistics:**
- Threshold sensitivity: std = 0.0000
- Amplifier sensitivity: std = 0.0201
- Norm range sensitivity: std = 0.0096

**Conclusion:**

> The EU System Stress Index classification of ELEVATED is
> parameter-independent. Across all 18 tested parameter
> combinations -- spanning the full range of reasonable choices --
> the system remains in the ELEVATED stress band.
>
> This means the conclusion does not depend on the specific
> parameter values chosen. The model is robust.

---

## What Robustness Means

A critic might ask: "Why 0.60 and not 0.55 as the threshold?"

The honest answer is: the 0.60 threshold was chosen based on
engineering judgment and consistency with similar risk frameworks
(see methodology.md, Section 6).

But this sensitivity analysis shows that it does not matter.
Whether we use 0.50 or 0.70, the system is ELEVATED.
The parameter is not driving the conclusion -- the data is.

---

## How to Reproduce

```bash
cd EU_ECO_MODULE
python eu_sensitivity_analysis.py
```

Requirements: same as eu_monitor_processor.py
Output: output/sensitivity_analysis.csv

All results are deterministic. Running the script again
will produce identical output given the same input CSVs.

---

## Limitations

1. The sensitivity analysis tests individual parameters one at a time
   (univariate). A full multivariate analysis would test all
   combinations simultaneously. This is planned for v2.0.

2. The tested ranges are "reasonable" variations (+/- ~20%).
   Extreme values (e.g. threshold = 0.10 or amplifier = 10.0)
   are not tested as they would be physically unreasonable.

3. The analysis uses data from 2017-2026. Results may differ
   for earlier historical periods where energy stress was lower.
