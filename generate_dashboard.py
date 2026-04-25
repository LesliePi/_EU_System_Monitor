# EU System Monitor - Dashboard Generator
# generate_dashboard.py
# Version: v1.0  (2026-04-25)
# Author: Laszlo Tatai / BarefootRealism Labs
# License: Apache License 2.0 WITH Commons Clause v1.0
# -*- coding: utf-8 -*-

"""
Generates a fully self-contained static HTML dashboard by injecting
eu_monitor_monthly.csv and eu_monitor_daily.csv directly into the
HTML template as JavaScript data.

The output file (eu_system_monitor_static.html) can be:
  - Opened directly in any browser (no server needed)
  - Uploaded to GitHub Pages
  - Shared as a single file by email or download

Usage:
    python generate_dashboard.py

Output:
    output/eu_system_monitor_static.html

Run this script every time you want to publish a fresh version.
For automated monthly updates, use eu_monitor_update.py instead.
"""

import os
import sys
import json
import datetime
import pandas as pd

# ============================================================
# PATHS
# ============================================================

_HERE = os.path.dirname(os.path.abspath(__file__))

OUTPUT_DIR  = os.path.join(_HERE, "output")
MONTHLY_CSV = os.path.join(OUTPUT_DIR, "eu_monitor_monthly.csv")
DAILY_CSV   = os.path.join(OUTPUT_DIR, "eu_monitor_daily.csv")
TEMPLATE    = os.path.join(OUTPUT_DIR, "eu_system_monitor.html")
OUT_HTML    = os.path.join(OUTPUT_DIR, "eu_system_monitor_static.html")

# ============================================================
# HELPERS
# ============================================================

def _load_csv(path: str, name: str) -> pd.DataFrame:
    if not os.path.exists(path):
        print(f"  [ERROR] Not found: {path}")
        sys.exit(1)
    df = pd.read_csv(path)
    print(f"  [OK] {name}: {len(df)} rows, {len(df.columns)} columns")
    return df


def _df_to_json(df: pd.DataFrame) -> str:
    """Convert DataFrame to compact JSON string. NaN -> null."""
    df = df.copy()
    # Convert date column to string
    if "date" in df.columns:
        df["date"] = df["date"].astype(str)
    # Replace NaN with None (-> null in JSON)
    records = df.where(pd.notna(df), None).to_dict(orient="records")
    return json.dumps(records, separators=(",", ":"))


def _patch_loaddata(html: str) -> str:
    """
    Replace the fetch-based loadData() with a version that reads
    from window._MONTHLY and window._DAILY (injected data).
    """
    old = """async function loadData() {
  try {
    const [mRes, dRes] = await Promise.all([
      fetch(MONTHLY_CSV),
      fetch(DAILY_CSV)
    ]);

    if (!mRes.ok) throw new Error('Cannot load ' + MONTHLY_CSV + ' (HTTP ' + mRes.status + ')');
    if (!dRes.ok) throw new Error('Cannot load ' + DAILY_CSV   + ' (HTTP ' + dRes.status + ')');

    const [mText, dText] = await Promise.all([mRes.text(), dRes.text()]);

    monthlyData = parseCSV(mText);
    dailyData   = parseCSV(dText);

    // Sort by date
    monthlyData.sort((a, b) => new Date(a.date) - new Date(b.date));
    dailyData.sort((a, b)   => new Date(a.date) - new Date(b.date));

    document.getElementById('loading').style.display   = 'none';
    document.getElementById('dashboard').style.display = 'block';

    renderAll();

  } catch (err) {
    document.getElementById('loading').style.display = 'none';
    const errEl = document.getElementById('error-msg');
    errEl.style.display = 'block';
    errEl.textContent = 'Data load error: ' + err.message +
      ' -- Place eu_monitor_monthly.csv and eu_monitor_daily.csv in the same folder as this HTML file, then open via a local web server.';
  }
}"""

    new = """function loadData() {
  // Data injected by generate_dashboard.py -- no server needed
  if (!window._MONTHLY || !window._DAILY) {
    document.getElementById('loading').style.display = 'none';
    const errEl = document.getElementById('error-msg');
    errEl.style.display = 'block';
    errEl.textContent = 'Data not found. Regenerate with generate_dashboard.py.';
    return;
  }
  monthlyData = window._MONTHLY;
  dailyData   = window._DAILY;

  monthlyData.sort((a, b) => new Date(a.date) - new Date(b.date));
  dailyData.sort((a, b)   => new Date(a.date) - new Date(b.date));

  document.getElementById('loading').style.display   = 'none';
  document.getElementById('dashboard').style.display = 'block';

  renderAll();
}"""

    if old not in html:
        # Fallback: try to find and replace the async function signature only
        print("  [WARN] loadData pattern not matched exactly -- trying fallback patch")
        html = html.replace(
            "async function loadData()",
            "function loadData()"
        )
        return html

    return html.replace(old, new)


# ============================================================
# MAIN
# ============================================================

def generate(
    monthly_csv: str = MONTHLY_CSV,
    daily_csv:   str = DAILY_CSV,
    template:    str = TEMPLATE,
    out_html:    str = OUT_HTML,
):
    print("\n" + "="*60)
    print("  EU System Monitor - Dashboard Generator")
    print(f"  {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    # Load data
    print("\nLoading data:")
    monthly = _load_csv(monthly_csv, "Monthly")
    daily   = _load_csv(daily_csv,   "Daily")

    # Load template
    if not os.path.exists(template):
        print(f"  [ERROR] Template not found: {template}")
        print(f"  Make sure eu_system_monitor.html is in the output/ directory.")
        sys.exit(1)

    with open(template, "r", encoding="utf-8") as f:
        html = f.read()
    print(f"  [OK] Template: {os.path.basename(template)} ({len(html):,} chars)")

    # Convert data to JSON
    print("\nConverting data:")
    monthly_json = _df_to_json(monthly)
    daily_json   = _df_to_json(daily)
    print(f"  Monthly JSON: {len(monthly_json):,} chars")
    print(f"  Daily JSON:   {len(daily_json):,} chars")

    # Build data injection block
    generated_at = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    data_block = f"""<script>
/* EU System Monitor - Injected Data
   Generated: {generated_at}
   Monthly rows: {len(monthly)}
   Daily rows:   {len(daily)}
   Sources: GIE AGSI+ / ALSI, Eurostat STS_INPR_M, FAO FFPI
   All data from official public sources. SHA256 hashes in audit_log.json
*/
window._MONTHLY = {monthly_json};
window._DAILY   = {daily_json};
window._GENERATED_AT = "{generated_at}";
</script>"""

    # Inject before </head>
    if "</head>" not in html:
        print("  [ERROR] Template missing </head> tag")
        sys.exit(1)
    html = html.replace("</head>", data_block + "\n</head>")

    # Patch loadData function
    print("\nPatching loadData():")
    html = _patch_loaddata(html)
    print("  [OK] loadData patched to use injected data")

    # Update version comment in header
    html = html.replace(
        "v1.0 &mdash; 2026-04-25",
        f"v1.0 &mdash; {datetime.date.today().strftime('%Y-%m-%d')}"
    )

    # Write output
    os.makedirs(os.path.dirname(out_html), exist_ok=True)
    with open(out_html, "w", encoding="utf-8") as f:
        f.write(html)

    size_kb = os.path.getsize(out_html) // 1024
    print(f"\n  [DONE] {out_html}")
    print(f"  File size: {size_kb} KB")
    print(f"  Monthly rows injected: {len(monthly)}")
    print(f"  Daily rows injected:   {len(daily)}")
    print(f"\n  Open in any browser -- no server required.")
    print(f"  Upload to GitHub Pages to publish.")
    print("="*60)

    return out_html


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(
        description="EU System Monitor - Dashboard Generator"
    )
    p.add_argument("--monthly", default=MONTHLY_CSV, help="Monthly CSV path")
    p.add_argument("--daily",   default=DAILY_CSV,   help="Daily CSV path")
    p.add_argument("--template",default=TEMPLATE,    help="HTML template path")
    p.add_argument("--out",     default=OUT_HTML,     help="Output HTML path")
    args = p.parse_args()

    generate(
        monthly_csv = args.monthly,
        daily_csv   = args.daily,
        template    = args.template,
        out_html    = args.out,
    )
