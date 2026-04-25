# EU System Monitor

**Auditable Supply & System Risk Platform**

> "Papíron elegendo az eroforrás -- a valóságban az ido és a hálózat dönti el, hogy eljut-e oda, ahol szükség van rá."

A single-researcher project demonstrating that EU-wide supply chain risk
can be monitored, integrated, and visualised from entirely public data sources --
without institutional infrastructure.

---

## What This Is

The EU System Monitor is an auditable, data-driven platform that:

- Draws exclusively on **official public data sources**
- Records **SHA256 hashes and timestamps** for every downloaded file
- Integrates three critical systems: **energy, industry, food**
- Computes a **cascade stress index** reflecting real supply chain dynamics
- Produces a **self-contained static HTML dashboard** requiring no server

This is not analysis. It shows reality.

---

## Live Dashboard

The static dashboard is in `output/eu_system_monitor_static.html`.  
Open it directly in any browser -- no server required.

For the GitHub Pages version: [link to be added after Pages setup]

---

## Three Pillars

### Energy
- **GIE AGSI+** -- EU natural gas storage (daily, 2017--)
- **GIE ALSI** -- EU LNG terminal storage (daily, 2017--)
- Key metrics: storage %, injection gap, cumulative deficit

### Industry
- **Eurostat STS_INPR_M** -- EU Industrial Production Index (monthly)
- **Baltic Dry Index** -- global shipping cost proxy (daily, manual ingest)
- Key metrics: IPI year-on-year change, BDI 30-day moving average

### Food
- **FAO FFPI** -- Food and Agriculture Price Index (annual)
- Key metrics: FFPI level, year-on-year change

---

## Cascade Stress Model

The composite stress index uses a **threshold-based cascade amplification**:

```
Rule A: if stress_energy > 0.60
        -> industry weight x2.0 (energy crisis amplifies industrial stress)

Rule B: if stress_energy > 0.60 OR stress_industry > 0.60
        -> food weight x1.5 (upper pillar crisis amplifies food stress)
```

This reflects real supply chain dynamics:
gas shortage -> fertiliser shortage -> harvest reduction -> food price increase.

All thresholds are explicit constants, not hidden parameters. Fully auditable.

**Stress bands:**
| Band | Range | Meaning |
|------|-------|---------|
| Low | 0.00 -- 0.30 | No significant stress |
| Moderate | 0.30 -- 0.55 | Elevated vigilance warranted |
| Elevated | 0.55 -- 0.75 | Active monitoring required |
| Critical | 0.75 -- 1.00 | System stress, cascade risk |

---

## Architecture

```
[OFFICIAL SOURCES]
       |
[eu_monitor_downloaders.py]   -- fetch, hash, audit
       |
[data/raw/]                   -- immutable raw files
       |
[eu_monitor_processor.py]     -- normalise, compute metrics, cascade model
       |
[output/eu_monitor_daily.csv]    -- daily resolution (GIE, BDI)
[output/eu_monitor_monthly.csv]  -- monthly aggregates + stress index
       |
[generate_dashboard.py]       -- inject data into HTML template
       |
[output/eu_system_monitor_static.html]  -- self-contained dashboard
```

---

## Audit Trail

Every downloaded file produces an audit record in `data/audit/audit_log.json`:

```json
{
  "dataset_id": "energy.gas.storage",
  "source": "GIE",
  "source_url": "https://agsi.gie.eu/api?type=eu&from=2024-01-01&to=2024-12-31",
  "retrieved_at": "2026-04-25T14:12:00Z",
  "local_path": "data/raw/gie_agsi/agsi_2024.json",
  "raw_hash": "sha256:...",
  "row_count": 366,
  "unit": "TWh"
}
```

Core principle: a dataset is not trusted because it cannot be changed,
but because **any change is detectable**.

---

## Quick Start

### Requirements

```
Python 3.11+
pandas
numpy
openpyxl
requests
beautifulsoup4
```

Install:
```bash
pip install pandas numpy openpyxl requests beautifulsoup4
```

### Setup

1. Clone the repository
2. Register at [https://agsi.gie.eu](https://agsi.gie.eu) to get a free GIE API key
3. Create a `.env` file in the project root:
   ```
   GIE_API_KEY=your_key_here
   ```
   (`.env` is in `.gitignore` -- never committed)

### Run

```bash
# Download all data
python eu_monitor_downloaders.py

# For Baltic Dry Index (manual download required from investing.com):
python eu_monitor_downloaders.py --ingest-bdi path/to/Baltic_Dry.csv

# Process data and compute stress index
python eu_monitor_processor.py

# Generate self-contained static dashboard
python generate_dashboard.py
```

Open `output/eu_system_monitor_static.html` in any browser.

### Monthly Update

Run the same three commands. Downloaders skip already-fetched files.
Only new data is downloaded. Generate dashboard to publish updated version.

---

## Data Sources

| Source | Dataset | Frequency | Access |
|--------|---------|-----------|--------|
| GIE AGSI+ | EU Gas Storage | Daily | Free API (registration required) |
| GIE ALSI | EU LNG Terminals | Daily | Free API (registration required) |
| Eurostat | Industrial Production STS_INPR_M | Monthly | Free, no key |
| FAO | Food Price Index FFPI | Annual | Free, direct download |
| Baltic Exchange | Baltic Dry Index | Daily | Manual CSV (investing.com) |

All sources are official and publicly available.
No proprietary data. No paywalls.

---

## Methodology Notes

- Stress Index computed on **monthly aggregates only** (daily fluctuations create noise)
- Daily charts use **native resolution** -- no interpolation
- Missing data remains **NaN** -- never imputed
- FAO FFPI is annual -- monthly rows are NaN except January anchor points
- BDI requires manual download -- no free API exists for historical data

These limitations are features, not bugs. Explicit limitations are a credibility feature.

---

## Repository Structure

```
eu-system-monitor/
  eu_monitor_config.py        -- central configuration, all parameters
  eu_monitor_downloaders.py   -- data acquisition, audit logging
  eu_monitor_processor.py     -- normalisation, metrics, cascade model
  generate_dashboard.py       -- static HTML generator
  .gitignore
  README.md
  LICENSE
  output/
    eu_system_monitor_static.html   -- self-contained dashboard (published)
    eu_system_monitor.html          -- template (data injected by generator)
  data/
    audit/
      audit_log.json          -- immutable audit trail (committed)
```

---

## Current Status (2026-04-25)

| Metric | Value |
|--------|-------|
| EU Gas Storage | 30.9% |
| Stress Index | 0.574 (ELEVATED) |
| Cascade Active | Yes (energy-driven) |
| Data coverage | 2017-04-25 (GIE), 1991-2023 (IPI), 1961-2026 (FAO) |
| Daily rows | 3,400 |
| Monthly rows | 454 |

---

## Author

**Laszlo Tatai** / BarefootRealism Labs  
Independent researcher and developer  
Rabagyarmat, Orseg region, Hungary  
ORCID: [0009-0007-5153-6306](https://orcid.org/0009-0007-5153-6306)  
GitHub: [LesliePie](https://github.com/LesliePie)

---

## License

Apache License 2.0 WITH Commons Clause v1.0

Free for personal, academic, and non-commercial use.  
Commercial use requires written permission.

---

## Citation

If you use this work, please cite:

```
Tatai, L. (2026). EU System Monitor: Auditable Supply & System Risk Platform.
BarefootRealism Labs. GitHub: https://github.com/LesliePie/eu-system-monitor
```

Zenodo DOI: [to be added after upload]

---

## Why This Exists

The EU has Eurostat, JRC, ENTSO-G, ACER, and dozens of other institutions
collecting data in parallel. Each operates in its own silo.
There is no integrated layer that says:
**"these three numbers are moving together, and that is a problem."**

This project demonstrates that such integration is possible,
from entirely public data, by a single independent researcher,
in a single working day.

The question this raises is institutional, not technical.
