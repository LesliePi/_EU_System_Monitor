# EU System Monitor -- NACE Defense vs Civil Industry Methodology

**Version:** 1.0
**Date:** 2026-04-28
**Script:** eu_monitor_nace_analysis.py
**Author:** Laszlo Tatai / BarefootRealism Labs

---

## Why This Analysis Exists

The EU aggregate Industrial Production Index (IPI, Eurostat STS_INPR_M,
series I10) combines all industrial sectors into a single number.
Since 2022, EU defense production has grown significantly faster
than civil industry. The aggregate IPI masks this divergence.

**Example (2023-12, latest available):**
- Aggregate IPI (I10): +2.3% year-on-year -- "industry is fine"
- Defense proxy (C25+C30): +8.2% year-on-year
- Civil IPI (C29+C20+C26): +1.9% year-on-year

The +2.3% aggregate is the weighted result of +8.2% defense growth
pulling up a +1.9% civil baseline.

This matters because:
- Civil industry reflects productive capacity available to households
- Defense industry reflects resource allocation to non-civilian output
- In a resource-constrained system, they compete for energy and labor

---

## Data Source

**Eurostat Statistics API**
Dataset: STS_INPR_M (Production in industry, monthly data)
Endpoint: https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/STS_INPR_M
Adjustment: SCA (seasonally and calendar adjusted)
Unit: I15 (Index, 2015=100)
Access: Free, no registration required

---

## Why Country-Level Data

The Eurostat API does not provide EU27 aggregate data at the
NACE sector level for the modern period. EU27 aggregate is only
available for the total industry index (I10).

Country-level data is the official Eurostat data -- the same
underlying measurements used to compute the EU27 aggregate.
Using the five largest EU economies gives ~70% GDP coverage.

This limitation is explicitly documented in the audit log
for every API call.

---

## Countries Covered

| Country | GDP share (approx) | Included |
|---------|-------------------|---------|
| Germany (DE) | ~25% | Yes |
| France (FR) | ~17% | Yes |
| Italy (IT) | ~12% | Yes |
| Spain (ES) | ~9% | Yes |
| Poland (PL) | ~5% | Yes |
| Netherlands, Belgium, others | ~32% | No |

**Total coverage: ~68% of EU27 GDP**

Aggregation method: unweighted mean across available countries.
GDP weighting was considered but not applied -- it would require
annual GDP data alignment and introduce additional assumptions.
The unweighted mean is simpler and more transparent.

---

## NACE Sector Classification

### Defense Proxy

**C25 -- Fabricated metal products, except machinery**
- Includes: C25.4 Weapons and ammunition
- Also includes: structural metal, metal containers, tools
- Defense share: estimated 15-25% of C25 output in EU
- Limitation: majority of C25 is NOT defense

**C30 -- Other transport equipment**
- Includes: military vehicles, warships, military aircraft
- Also includes: railway equipment, civilian ships, spacecraft
- Defense share: varies significantly by country
- Limitation: civilian transport is a large component

**Why use these despite limitations:**
The defense signal is real and directional. When C25 and C30
grow faster than civil sectors, it indicates increased metal
and transport equipment production -- consistent with defense
ramp-up even if the signal is not pure.

The correct interpretation is:
> "The fabricated metals and transport equipment sectors are
> growing significantly faster than civil manufacturing,
> consistent with a defense production increase."

Not:
> "Defense production is growing at exactly X%."

### Civil Industry

**C29 -- Motor vehicles**
- Predominantly civilian. Negligible military component.
- Indicator of consumer goods and export manufacturing health.

**C20 -- Chemicals and chemical products**
- Energy-intensive civil industry.
- Includes fertilizers, plastics, industrial chemicals.
- Some dual-use chemicals (limited).

**C26 -- Computer, electronic and optical products**
- Civil electronics, computing, optical instruments.
- Some defense electronics (dual-use).
- Indicator of technology sector health.

---

## Derived Metrics

| Metric | Formula | Interpretation |
|--------|---------|---------------|
| defense_proxy_ipi | mean(C25, C30) | Proxy for defense sector production |
| civil_ipi | mean(C29, C20, C26) | Civil manufacturing health |
| defense_vs_civil | defense/civil * 100 | Relative growth since 2015 |
| defense_yoy_pct | 12-month % change of defense_proxy_ipi | Annual growth rate |
| civil_yoy_pct | 12-month % change of civil_ipi | Annual growth rate |
| ipi_gap_pct | defense_yoy - civil_yoy | Growth divergence in pp |

---

## Key Findings (2023-12)

```
Index values (2015=100):
  Defense proxy IPI:  129.2
  Civil IPI:          114.5
  Total IPI (I10):    116.1

Year-on-year change:
  Defense proxy YoY:  +8.2%
  Civil IPI YoY:      +1.9%
  Gap:                +6.2 percentage points

Defense/Civil ratio: 112.9
  Since 2015, defense proxy has grown 12.9% faster than civil.
```

---

## What This Does NOT Show

1. **It does not prove that defense spending causes civil decline.**
   Correlation is not causation. Civil industry may be declining
   for other reasons (energy costs, demand shifts, competition).

2. **It does not quantify actual defense production.**
   C25 and C30 are proxies. The true defense share is unknown
   without classified procurement data.

3. **It does not predict future trends.**
   This is a monitoring tool, not a forecast model.

---

## Audit Trail

Every API call is recorded in:
```
data/audit/audit_log.json
```

Each record includes:
- Full URL used
- Timestamp (UTC)
- SHA256 of raw response
- Row count and date range
- Known limitations for that sector

Raw JSON responses are saved in:
```
data/raw/eurostat_nace/
```

The analysis is fully reproducible. Running eu_monitor_nace_analysis.py
on any machine with internet access will produce the same results.

---

## How to Reproduce

```bash
cd EU_ECO_MODULE
python eu_monitor_nace_analysis.py
```

Output: output/eu_nace_monthly.csv

The script downloads data fresh from Eurostat.
Previously cached files (same-day) are reused automatically.
