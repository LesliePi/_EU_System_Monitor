# EU System Monitor -- Methodology

**Version:** 1.0  
**Date:** 2026-04-25  
**Author:** Laszlo Tatai / BarefootRealism Labs

---

## 1. Core Philosophy

> "What matters is not how much resource exists,
> but when and where it is available."

The EU System Monitor does not interpret. It does not forecast.
It shows the current state of three interconnected systems,
and makes visible the cascade dynamics between them.

Explicit limitations are a credibility feature, not a weakness.

---

## 2. Data Sources and Coverage

### 2.1 Energy -- GIE AGSI+ (Gas Storage)

- **Provider:** Gas Infrastructure Europe (GIE)
- **Dataset:** Aggregated Gas Storage Inventories (AGSI+)
- **Coverage:** EU aggregate, daily, 2017-01-01 to present
- **Access:** Free REST API, registration required
- **Key fields:** gasInStorage (TWh), workingGasVolume (TWh),
  injection (GWh/d), injectionCapacity (GWh/d), full (%)
- **Update frequency:** Daily at 19:30 CET

### 2.2 Energy -- GIE ALSI (LNG Terminals)

- **Provider:** Gas Infrastructure Europe (GIE)
- **Dataset:** Aggregated LNG Storage Inventories (ALSI)
- **Coverage:** EU aggregate, daily, 2017-01-01 to present
- **Access:** Free REST API, same key as AGSI+
- **Key fields:** inventory (LNG bcm, GWh), sendOut (GWh/d),
  dtmi (maximum inventory), dtrs (send-out capacity)

### 2.3 Industry -- Eurostat Industrial Production Index

- **Provider:** Eurostat
- **Dataset:** STS_INPR_M (Short-Term Business Statistics)
- **Series:** M.I10.PROD.EU27_2020.SCA.I15
  (Monthly, Total Industry, EU27, Seasonally adjusted, Index 2015=100)
- **Coverage:** 1991-01 to present (approx. 2-month lag)
- **Access:** Free SDMX REST API, no registration

### 2.4 Industry -- Baltic Dry Index (Logistics Proxy)

- **Provider:** Baltic Exchange (via Investing.com)
- **Dataset:** Baltic Dry Index (BDI)
- **Coverage:** Daily, business days, 2017-01-01 to present
- **Access:** Manual CSV download (no free API for historical data)
- **Audit:** SHA256 hash recorded at ingest, source noted in audit log
- **Limitation:** Manual process -- update requires re-download and re-ingest

### 2.5 Food -- FAO Food Price Index

- **Provider:** Food and Agriculture Organization of the UN (FAO)
- **Dataset:** FAO Food Price Index (FFPI)
- **Coverage:** Annual, 1961 to present
- **Access:** Direct XLSX download from fao.org
- **Key fields:** Nominal FFPI (index 2014-2016=100)
- **Limitation:** Annual frequency only in the historical file.
  Monthly rows are NaN except January anchor points. No interpolation applied.
- **Sub-indices:** Cereals, dairy, meat, oils, sugar not available
  in the downloadable historical file.

---

## 3. Derived Metrics

All derived metrics are computed in `eu_monitor_processor.py`.
None are sourced from external computations.

| Metric | Formula | Unit |
|--------|---------|------|
| storage_pct | gas_storage_twh / gas_capacity_twh * 100 | % |
| injection_gap_gwh | gas_injection_cap_gwh - gas_injection_gwh | GWh/day |
| cumulative_deficit_gwh | rolling 30-day sum of injection_gap_gwh | GWh |
| sendout_gap_gwh | lng_sendout_cap_gwh - lng_sendout_gwh | GWh/day |
| lng_inventory_twh | lng_inventory_gwh / 1000 | TWh |
| ipi_yoy_pct | (IPI_t / IPI_(t-12) - 1) * 100 | % |
| ipi_mom_pct | (IPI_t / IPI_(t-1) - 1) * 100 | % |
| ffpi_yoy_pct | (FFPI_t / FFPI_(t-1year) - 1) * 100 | % |
| bdi_30d_ma | 30-day rolling mean of bdi_close | index points |

---

## 4. Stress Index -- Three-Pillar Cascade Model

### 4.1 Pillar Scores

Each pillar score is computed independently as a weighted mean
of its normalised components.

**Normalisation:**

Each component is clamped to its `norm_range` and scaled to [0, 1]:

```
normed = (value - lo) / (hi - lo)
if direction == "inverse": normed = 1 - normed
```

`direction = "inverse"` means: high raw value -> low stress
(e.g. high gas storage = low stress)

`direction = "direct"` means: high raw value -> high stress
(e.g. high food price inflation = high stress)

**Components:**

| Component | Source metric | Direction | Weight | Norm range |
|-----------|--------------|-----------|--------|-----------|
| gas_storage_deficit | storage_pct | inverse | 0.30 | [20, 100] % |
| lng_sendout_gap | sendout_gap_gwh | direct | 0.15 | [0, 50] TWh/d |
| ipi_deviation | ipi_yoy_pct | inverse | 0.15 | [-15, 10] % |
| logistics_stress | bdi_30d_ma | inverse | 0.15 | [500, 3000] pts |
| food_price_pressure | ffpi_yoy_pct | direct | 0.25 | [-5, 30] % |

Note: if a component is unavailable (no data), its weight is
redistributed proportionally among available components.

### 4.2 Cascade Amplification

The composite stress index uses threshold-based cascade amplification.
All thresholds are explicit constants defined in `eu_monitor_processor.py`.

```
ENERGY_CASCADE_THRESHOLD   = 0.60
INDUSTRY_CASCADE_THRESHOLD = 0.60
INDUSTRY_AMPLIFIER         = 2.0
FOOD_AMPLIFIER             = 1.5
```

**Rule A** (energy -> industry):
```
if stress_energy > 0.60:
    industry_weight *= 2.0
    cascade_active = True
```

**Rule B** (upper pillars -> food):
```
if stress_energy > 0.60 OR stress_industry > 0.60:
    food_weight *= 1.5
    cascade_active = True
```

**Composite:**
```
total_weight = energy_weight + industry_weight + food_weight
stress_index = (stress_energy * energy_weight
              + stress_industry * industry_weight
              + stress_food * food_weight) / total_weight
stress_index = clip(stress_index, 0, 1)
```

**Rationale:**
An energy crisis does not merely add to industrial stress.
It amplifies it -- through gas shortages affecting production,
through logistics disruptions, through price cascades.
The threshold-based model captures this nonlinearity explicitly,
without black-box weighting.

### 4.3 Stress Bands

| Band | Range | Interpretation |
|------|-------|---------------|
| Low | [0.00, 0.30) | No significant system stress |
| Moderate | [0.30, 0.55) | Elevated vigilance warranted |
| Elevated | [0.55, 0.75) | Active monitoring required |
| Critical | [0.75, 1.00] | Systemic stress, cascade risk high |

---

## 5. Resolution and Aggregation

| Source | Native resolution | Stress Index aggregation |
|--------|-----------------|------------------------|
| GIE AGSI+ | Daily | Monthly mean |
| GIE ALSI | Daily | Monthly mean |
| Eurostat IPI | Monthly | Used as-is |
| FAO FFPI | Annual | Used as-is (NaN for non-January months) |
| Baltic Dry | Daily (business days) | Monthly mean |

**Design decision:** The Stress Index is computed exclusively on
monthly aggregates. Daily fluctuations create noise in a composite
indicator. This choice is explicit and documented.

Daily data is preserved at native resolution for the gas storage
and injection gap charts in the dashboard.

---

## 6. Audit Architecture

Every downloaded file produces a JSON record in `data/audit/audit_log.json`
(append-only, one JSON object per line):

```json
{
  "dataset_id": "energy.gas.storage",
  "source": "GIE",
  "source_url": "https://agsi.gie.eu/api?type=eu&from=2024-01-01&to=2024-12-31",
  "retrieved_at": "2026-04-25T14:12:00Z",
  "published_at": null,
  "period_start": "2024-01-01",
  "period_end": "2024-12-31",
  "local_path": "data/raw/gie_agsi/agsi_2024.json",
  "raw_hash": "sha256:e3b0c44298fc1c149afb...",
  "row_count": 366,
  "unit": "TWh",
  "notes": ""
}
```

For manually ingested files (Baltic Dry), the `source_url` field
is set to `"manual_download"` and the `notes` field records the
original source and download circumstances.

---

## 7. Known Limitations

**Baltic Dry Index:** No free API exists for historical BDI data.
The current implementation requires manual CSV download from
Investing.com and ingest via `BalticDryIngestor`. This introduces
a manual step in the update workflow.

**FAO FFPI frequency:** The publicly available historical file
contains annual data only. Monthly FFPI data exists but requires
direct request to FAO. This means food stress scores are only
meaningful at annual resolution.

**GIE data coverage:** AGSI+ data is available from 2017-01-01.
Some SSOs provide historical data from 2011 or 2016. The EU
aggregate series used here starts in 2017.

**Eurostat IPI lag:** Eurostat publishes IPI with approximately
a 2-month lag. The most recent months in the stress index
therefore reflect IPI conditions from 2 months prior.

**Stress Index is partial when data is missing:** If a pillar
has no data (e.g. BDI not yet ingested), its weight is
redistributed to available components. The dashboard displays
N/A for missing pillar scores. This is documented and visible.

---

## 8. What This Is Not

This system does not forecast. It does not predict future stress levels.
It does not model economic outcomes or policy responses.
It shows the current and historical state of supply system indicators,
integrated into a single auditable view.

Interpretation is the responsibility of the reader.
