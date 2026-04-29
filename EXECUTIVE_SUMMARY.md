# EU System Monitor -- Executive Summary

**Version:** 1.1
**Date:** 2026-04-29
**Author:** Laszlo Tatai / BarefootRealism Labs
**GitHub:** https://github.com/LesliePi/_EU_System_Monitor
**Zenodo DOI:** https://doi.org/10.5281/zenodo.19827856

---

## 1. What This Is

The EU System Monitor is a real-time diagnostic model of the
European supply system, integrating:

- energy availability
- industrial activity
- food price pressure

into a single, auditable stress indicator.

**It does not forecast. It measures system state.**

All data sourced exclusively from official public sources.
Every data point carries a SHA256 audit hash.
All model parameters are explicit and documented.

---

## 2. Current Assessment

**Latest result (2026-04-29):**

```
EU System Stress Level: ELEVATED (0.574)
Cascade amplification:  ACTIVE
Energy pillar:          0.922  (critical driver)
Industry pillar:        0.359
Food pillar:            0.500
```

This classification is:
- stable across time
- data-driven (official Eurostat, GIE, FAO sources)
- robust to parameter variation (see Section 3)

---

## 3. Robustness -- Key Finding

A formal sensitivity analysis across **18 parameter configurations**
shows:

> **100% of scenarios produce the same classification: ELEVATED**

| Parameter tested | Range | Stability (std) | Result |
|----------------|-------|----------------|--------|
| Cascade threshold | 0.50 -- 0.70 | 0.0000 | ROBUST |
| Amplifier values | 1.5 -- 2.5 | 0.0201 | ROBUST |
| Norm range variants | 4 variants | 0.0096 | ROBUST |

**Implication:**
The result is not driven by model assumptions,
but by underlying system conditions.

Full sensitivity analysis: eu_sensitivity_analysis.py
Full results: output/sensitivity_analysis.csv

---

## 4. What Is Driving the Stress

### 4.1 Energy Constraint (Primary Driver)

- EU gas storage near lower operational bounds (29.4% full)
- LNG system operating below full sendout capacity
- Persistent injection deficit (10,322 GWh/day unused capacity)
- Energy stress pillar: 0.922 out of 1.0

**Energy system is in structural stress regime.**

The injection gap has been persistently positive for 30+ months,
meaning the EU is consistently refilling storage slower than
seasonal norms. This is not a temporary fluctuation.

### 4.2 Industrial Structure Distortion

Sector-level analysis of Eurostat STS_INPR_M data
(DE, FR, IT, ES, PL -- ~70% of EU27 GDP) shows:

| Sector | YoY growth (2023-12) |
|--------|---------------------|
| Defense proxy (C25+C30) | +8.2% |
| Civil industry (C29+C20+C26) | +1.9% |
| Aggregate IPI (I10, published) | +2.3% |
| Gap (defense minus civil) | +6.2 percentage points |
| Defense/Civil ratio since 2015 | 112.9 |

**Industrial output is reallocated, not uniformly growing.**

The aggregate IPI (+2.3%) masks the divergence between
rapidly expanding defense-related sectors and stagnating
civil manufacturing.

### 4.3 Cross-Sector Cascade Risk

The system exhibits a nonlinear cascade structure:

```
Energy stress (0.922)
      |
      v  [threshold: 0.60 -- EXCEEDED]
Industrial pressure (amplified x2.0)
      |
      v  [threshold: 0.60 -- ACTIVE]
Food price stress (amplified x1.5)
```

Once thresholds are exceeded, stress propagates across sectors.
The cascade is currently active.

Theoretical basis: Lorenz, Battiston & Schweitzer (2009),
European Physical Journal B, 71:441-460.

---

## 5. Why This Matters

The current state is not a single-sector issue.
It is a **coupled system under constraint**.

Key risks:
- Reduced civil production capacity while defense absorbs resources
- Persistent energy cost pressure across all sectors
- Increasing system sensitivity to external shocks (supply disruption,
  demand spike, logistics failure)
- The Atlantic LNG bridge is load-bearing:
  US LNG exports are actively compensating EU storage deficit

---

## 6. What This Is NOT

The model does NOT:
- predict future events or prices
- assign political causality
- estimate exact defense production volumes
- replace sector-specific expert analysis

It provides:
**a consistent, real-time, auditable system state assessment**

All limitations are documented in:
- docs/methodology.md
- docs/methodology_nace.md
- docs/sensitivity_report.md

---

## 7. Strategic Interpretation

The EU system is currently:
- not in collapse
- but not in equilibrium either

Instead: **operating in a constrained, stress-amplifying regime**

The energy system is the structural bottleneck. Industrial
reallocation toward defense is occurring simultaneously.
The food system has not yet entered stress but is exposed
to cascade amplification if either upper pillar worsens.

---

## 8. Key Takeaway

> The EU supply system is in a structurally elevated stress state,
> driven by energy constraints and internal industrial reallocation,
> and this assessment is robust to modeling assumptions.

---

## 9. Data and Reproducibility

All data, code, and audit records are publicly available.

| Item | Location |
|------|---------|
| Source code | github.com/LesliePie/_EU_System_Monitor |
| Zenodo DOI | doi.org/10.5281/zenodo.19827856 |
| Audit log | data/audit/audit_log.json |
| Raw data | data/raw/ (not committed, reproduced by pipeline) |
| Sensitivity results | output/sensitivity_analysis.csv |
| NACE results | output/eu_nace_monthly.csv |

To reproduce all results from scratch:

```bash
python eu_monitor_downloaders.py
python eu_monitor_processor.py
python eu_monitor_nace_analysis.py
python eu_sensitivity_analysis.py
python generate_dashboard.py
```

---

## 10. Contact

**Laszlo Tatai**
BarefootRealism Labs
ORCID: 0009-0007-5153-6306
GitHub: LesliePie
Location: Rabagyarmat, Hungary
