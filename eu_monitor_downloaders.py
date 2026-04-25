# EU System Monitor - Unified Downloader
# eu_monitor_downloaders.py
# Version: v1.0  (2026-04-25)
# Author: Laszlo Tatai / BarefootRealism Labs
# License: Apache License 2.0 WITH Commons Clause v1.0
# -*- coding: utf-8 -*-

"""
Unified Downloader for EU System Monitor
==========================================

Downloads all data sources required by the EU System Monitor pipeline:

  1. GIEDownloader    - AGSI+ (gas storage) and ALSI (LNG) via GIE REST API
  2. EurostatDownloader - Industrial Production Index via Eurostat SDMX API
  3. FAODownloader    - Food and Agriculture Price Index (XLSX)
  4. BalticDryIngestor - Manual CSV ingest for Baltic Dry Index (with audit)

All downloaders share:
  - Skip-if-exists logic (idempotent: safe to re-run)
  - SHA256 hash recorded for every saved file (audit trail)
  - Configurable retry with backoff
  - Rate-limit-aware request pacing
  - Audit log entry written per file

GIE API key:
  Set environment variable GIE_API_KEY before running.
  export GIE_API_KEY="your_key_here"   (Linux/macOS)
  set GIE_API_KEY=your_key_here        (Windows CMD)

Usage:
    python eu_monitor_downloaders.py              # all sources
    python eu_monitor_downloaders.py --gie-only
    python eu_monitor_downloaders.py --no-gie
    python eu_monitor_downloaders.py --ingest-bdi path/to/bdi.csv
"""

import os
import sys
import json
import time
import hashlib
import argparse
import datetime
import requests

try:
    from eu_monitor_config import (
        GIE_AGSI, GIE_ALSI, EUROSTAT_IPI, FAO_FPPI, BALTIC_DRY,
        ENABLE_GIE_AGSI, ENABLE_GIE_ALSI, ENABLE_EUROSTAT,
        ENABLE_FAO, ENABLE_BALTIC,
        HTTP_TIMEOUT, HTTP_SLEEP, HTTP_RETRIES,
        AUDIT_DIR, AUDIT_LOG,
        RAW_GIE_AGSI, RAW_GIE_ALSI, RAW_EUROSTAT, RAW_FAO, RAW_BALTIC,
        AUDIT_SCHEMA,
    )
except ImportError:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from eu_monitor_config import (
        GIE_AGSI, GIE_ALSI, EUROSTAT_IPI, FAO_FPPI, BALTIC_DRY,
        ENABLE_GIE_AGSI, ENABLE_GIE_ALSI, ENABLE_EUROSTAT,
        ENABLE_FAO, ENABLE_BALTIC,
        HTTP_TIMEOUT, HTTP_SLEEP, HTTP_RETRIES,
        AUDIT_DIR, AUDIT_LOG,
        RAW_GIE_AGSI, RAW_GIE_ALSI, RAW_EUROSTAT, RAW_FAO, RAW_BALTIC,
        AUDIT_SCHEMA,
    )

# Ensure output directories exist
for _d in [AUDIT_DIR, RAW_GIE_AGSI, RAW_GIE_ALSI, RAW_EUROSTAT, RAW_FAO, RAW_BALTIC]:
    os.makedirs(_d, exist_ok=True)

# Load .env file if present (needed for Spyder which does not
# inherit Windows environment variables from the shell)
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.exists(_env_path):
    with open(_env_path) as _ef:
        for _line in _ef:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip())


# ============================================================
# SHARED UTILITIES
# ============================================================

def _utc_now() -> str:
    """Return current UTC time as ISO 8601 string."""
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sha256(path: str) -> str:
    """Compute SHA256 hash of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _file_ok(path: str) -> bool:
    """Return True if file exists and is non-empty."""
    return os.path.exists(path) and os.path.getsize(path) > 0


def _section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def _write_audit(entry: dict):
    """
    Append one audit entry to the audit log (JSON Lines format).
    Each line is a self-contained JSON object - easy to parse,
    easy to inspect, append-only (tamper-evident by structure).
    """
    os.makedirs(AUDIT_DIR, exist_ok=True)
    with open(AUDIT_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _make_audit_entry(**kwargs) -> dict:
    """Build an audit record from AUDIT_SCHEMA template + provided values."""
    entry = dict(AUDIT_SCHEMA)  # copy template
    entry.update(kwargs)
    return entry


def _save_json(data: dict | list, path: str):
    """Save data as indented JSON file."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _request_with_retry(
    url: str,
    headers: dict = None,
    params: dict = None,
    retries: int = HTTP_RETRIES,
    timeout: int = HTTP_TIMEOUT,
    sleep_s: float = HTTP_SLEEP,
    stream: bool = False,
) -> requests.Response | None:
    """
    GET request with retry + exponential backoff.
    Returns Response on success, None after all retries exhausted.
    """
    for attempt in range(retries):
        try:
            r = requests.get(
                url,
                headers=headers or {},
                params=params or {},
                timeout=timeout,
                stream=stream,
            )
            r.raise_for_status()
            time.sleep(sleep_s)
            return r
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else "?"
            print(f"    [HTTP {status}] {url}  (attempt {attempt+1}/{retries})")
            if status == 429:
                # Rate limited - wait longer
                wait = 60
                print(f"    Rate limited. Waiting {wait}s ...")
                time.sleep(wait)
            elif attempt < retries - 1:
                time.sleep(5 * (attempt + 1))
        except Exception as e:
            print(f"    [ERROR] {url}: {e}  (attempt {attempt+1}/{retries})")
            if attempt < retries - 1:
                time.sleep(5 * (attempt + 1))

    print(f"    [FAILED] {url} - all {retries} attempts exhausted")
    return None


# ============================================================
# 1. GIE DOWNLOADER  (AGSI+ and ALSI)
# ============================================================

class GIEDownloader:
    """
    Downloads EU aggregate gas storage (AGSI+) or LNG terminal (ALSI)
    data from the GIE REST API.

    API documentation: https://agsi.gie.eu/api-doc/swagger
    Authentication: x-key header (personal API key from registration)

    Parameters
    ----------
    source : str
        "agsi" or "alsi"
    api_key : str, optional
        GIE API key. Defaults to GIE_API_KEY environment variable.
    """

    SOURCE_MAP = {
        "agsi": GIE_AGSI,
        "alsi": GIE_ALSI,
    }

    # Max records per page (API cap is 300)
    PAGE_SIZE = 300

    # Minimum delay between page requests (rate limit: 60 calls/min)
    PAGE_SLEEP = 1.1   # seconds - safely under the 60/min threshold
    
    def __init__(self, source: str = "agsi", api_key: str = None):
        self.source  = source.lower()
        self.cfg     = self.SOURCE_MAP[self.source]
        self.api_key = api_key or os.environ.get("GIE_API_KEY", "")

        if not self.api_key:
            raise ValueError(
                "GIE API key not found. Set the GIE_API_KEY environment variable.\n"
                "Register at https://agsi.gie.eu (free) to get your key."
            )

        self.base_url   = self.cfg["base_url"]
        self.output_dir = self.cfg["output_dir"]
        os.makedirs(self.output_dir, exist_ok=True)

        print(f"[GIE/{self.source.upper()}] Output -> {self.output_dir}")

    # --------------------------------------------------------
    @property
    def _headers(self) -> dict:
        return {"x-key": self.api_key}

    # --------------------------------------------------------
    def _fetch_page(self, page: int, from_date: str, to_date: str) -> dict | None:
        """Fetch one page of EU aggregate data."""
        params = {
            "type":    "eu",
            "from":    from_date,
            "to":      to_date,
            "page":    page,
            "size":    self.PAGE_SIZE,
            "reverse": "false",   # oldest first
        }
        r = _request_with_retry(
            url     = f"{self.base_url}/api",
            headers = self._headers,
            params  = params,
            sleep_s = self.PAGE_SLEEP,
        )
        if r is None:
            return None
        try:
            return r.json()
        except Exception as e:
            print(f"    [JSON ERROR] page {page}: {e}")
            return None

    # --------------------------------------------------------
    def _year_file_path(self, year: int) -> str:
        prefix = self.cfg["file_prefix"]
        return os.path.join(self.output_dir, f"{prefix}_{year}.json")

    # --------------------------------------------------------
    def download_year(self, year: int):
        """Download all data for a given year, saved as one JSON file."""
        path  = self._year_file_path(year)
        fname = os.path.basename(path)

        if _file_ok(path):
            print(f"  [SKIP]  {fname}")
            return

        from_date = f"{year}-01-01"
        to_date   = f"{year}-12-31"
        retrieved_at = _utc_now()

        print(f"  [FETCH] {fname}  ({from_date} -> {to_date})")

        # First page - tells us last_page
        first = self._fetch_page(1, from_date, to_date)
        if first is None:
            print(f"  [FAILED] {fname}")
            return

        all_records = list(first.get("data", []))
        last_page   = int(first.get("last_page", 1))

        # Remaining pages
        for page in range(2, last_page + 1):
            print(f"    page {page}/{last_page} ...")
            result = self._fetch_page(page, from_date, to_date)
            if result is None:
                print(f"    [WARN] page {page} failed - partial data saved")
                break
            all_records.extend(result.get("data", []))

        if not all_records:
            print(f"  [EMPTY] {fname} - no records returned")
            return

        # Build output structure
        output = {
            "dataset_id":    self.cfg["dataset_id"],
            "source":        self.cfg["source"],
            "region":        self.cfg["region"],
            "year":          year,
            "retrieved_at":  retrieved_at,
            "record_count":  len(all_records),
            "records":       all_records,
        }

        _save_json(output, path)
        file_hash = _sha256(path)

        print(f"  [DONE]  {fname}  ({len(all_records)} records)  SHA256: {file_hash[:16]}...")

        # Audit entry
        _write_audit(_make_audit_entry(
            dataset_id   = self.cfg["dataset_id"],
            source       = self.cfg["source"],
            source_url   = f"{self.base_url}/api?type=eu&from={from_date}&to={to_date}",
            retrieved_at = retrieved_at,
            period_start = from_date,
            period_end   = to_date,
            local_path   = path,
            raw_hash     = file_hash,
            row_count    = len(all_records),
            unit         = self.cfg["unit"],
        ))

    # --------------------------------------------------------
    def run(self, start_year: int = None, end_year: int = None):
        """Download all years from history_start to current year."""
        y0 = start_year or int(self.cfg["history_start"][:4])
        y1 = end_year   or datetime.date.today().year

        _section(f"GIE/{self.source.upper()}  {y0}-{y1}")

        for year in range(y0, y1 + 1):
            self.download_year(year)

        print(f"\n[GIE/{self.source.upper()}] Complete.")


# ============================================================
# 2. EUROSTAT DOWNLOADER  (Industrial Production Index)
# ============================================================

class EurostatDownloader:
    """
    Downloads EU Industrial Production Index from Eurostat SDMX REST API.
    No API key required.

    Returns JSON format. Full dataset saved as a single file
    (Eurostat provides the complete time series in one call).

    Documentation:
    https://ec.europa.eu/eurostat/web/sdmx-web-services/rest-sdmx-2.1
    """

    def __init__(self, cfg: dict = None):
        self.cfg        = cfg or EUROSTAT_IPI
        self.output_dir = self.cfg["output_dir"]
        os.makedirs(self.output_dir, exist_ok=True)
        print(f"[EUROSTAT/IPI] Output -> {self.output_dir}")

    # --------------------------------------------------------
    def _output_path(self) -> str:
        today = datetime.date.today().strftime("%Y%m%d")
        fname = f"{self.cfg['file_prefix']}_{today}.json"
        return os.path.join(self.output_dir, fname)

    # --------------------------------------------------------
    def run(self):
        """Download full IPI time series. Skips if today's file exists."""
        _section("EUROSTAT / Industrial Production Index")

        path  = self._output_path()
        fname = os.path.basename(path)

        if _file_ok(path):
            print(f"  [SKIP]  {fname}  (already downloaded today)")
            return

        url          = self.cfg["url"]
        retrieved_at = _utc_now()

        print(f"  [FETCH] {fname}")
        print(f"    URL: {url}")

        r = _request_with_retry(url=url, timeout=60)
        if r is None:
            print("  [FAILED] Eurostat download failed.")
            return

        try:
            data = r.json()
        except Exception as e:
            print(f"  [JSON ERROR] {e}")
            return

        # Wrap in envelope with metadata
        output = {
            "dataset_id":   self.cfg["dataset_id"],
            "source":       self.cfg["source"],
            "retrieved_at": retrieved_at,
            "source_url":   url,
            "data":         data,
        }

        _save_json(output, path)
        file_hash = _sha256(path)

        print(f"  [DONE]  {fname}  SHA256: {file_hash[:16]}...")

        _write_audit(_make_audit_entry(
            dataset_id   = self.cfg["dataset_id"],
            source       = self.cfg["source"],
            source_url   = url,
            retrieved_at = retrieved_at,
            period_start = self.cfg["history_start"],
            period_end   = datetime.date.today().strftime("%Y-%m"),
            local_path   = path,
            raw_hash     = file_hash,
            unit         = self.cfg["unit"],
        ))

        print("\n[EUROSTAT/IPI] Complete.")


# ============================================================
# 3. FAO DOWNLOADER  (Food Price Index)
# ============================================================

class FAODownloader:
    """
    Downloads the FAO Food and Agriculture Price Index (FFPI) XLSX file.
    No API key required. Direct file download from fao.org.

    The XLSX contains all sub-indices (cereals, dairy, meat, oils, sugar)
    and is updated monthly.
    """

    def __init__(self, cfg: dict = None):
        self.cfg        = cfg or FAO_FPPI
        self.output_dir = self.cfg["output_dir"]
        os.makedirs(self.output_dir, exist_ok=True)
        print(f"[FAO/FFPI] Output -> {self.output_dir}")

    # --------------------------------------------------------
    def _output_path(self) -> str:
        today = datetime.date.today().strftime("%Y%m")
        fname = f"{self.cfg['file_prefix']}_{today}.xlsx"
        return os.path.join(self.output_dir, fname)

    # --------------------------------------------------------
    def run(self):
        """Download FFPI XLSX. Skips if this month's file already exists."""
        _section("FAO / Food Price Index")

        path  = self._output_path()
        fname = os.path.basename(path)

        if _file_ok(path):
            print(f"  [SKIP]  {fname}  (already downloaded this month)")
            return

        url          = self.cfg["base_url"]
        retrieved_at = _utc_now()

        print(f"  [FETCH] {fname}")
        print(f"    URL: {url}")

        fao_headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        }
        r = _request_with_retry(url=url, headers=fao_headers, timeout=60, stream=True)
        if r is None:
            print("  [FAILED] FAO download failed.")
            return

        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=65536):
                f.write(chunk)

        file_hash = _sha256(path)
        size_kb   = os.path.getsize(path) // 1024

        print(f"  [DONE]  {fname}  ({size_kb} KB)  SHA256: {file_hash[:16]}...")

        _write_audit(_make_audit_entry(
            dataset_id   = self.cfg["dataset_id"],
            source       = self.cfg["source"],
            source_url   = url,
            retrieved_at = retrieved_at,
            local_path   = path,
            raw_hash     = file_hash,
            unit         = self.cfg["unit"],
        ))

        print("\n[FAO/FFPI] Complete.")


# ============================================================
# 4. BALTIC DRY INGESTOR  (manual CSV -> audit)
# ============================================================

class BalticDryIngestor:
    """
    Ingests a manually downloaded Baltic Dry Index CSV file.

    Does NOT download from the internet - BDI has no free API.
    Instead, this class:
      1. Validates the CSV structure
      2. Copies it to the raw data directory with a timestamped name
      3. Computes SHA256 and writes a full audit entry

    Accepted CSV format (Investing.com export):
      Date, Price, Open, High, Low, Change %
      Apr 25 2026, 1234, 1220, 1240, 1210, "0.75%"

    Usage:
        ingestor = BalticDryIngestor()
        ingestor.ingest("path/to/downloaded_bdi.csv")
    """

    REQUIRED_COLUMNS = {"Date", "Price"}   # minimum required

    def __init__(self, cfg: dict = None):
        self.cfg        = cfg or BALTIC_DRY
        self.output_dir = self.cfg["output_dir"]
        os.makedirs(self.output_dir, exist_ok=True)
        print(f"[BALTIC/BDI] Output -> {self.output_dir}")

    # --------------------------------------------------------
    def _validate_csv(self, path: str) -> tuple[bool, str, int]:
        """
        Check that the CSV has required columns and at least one data row.
        Returns (ok, message, row_count).
        """
        try:
            import csv
            with open(path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                headers = set(reader.fieldnames or [])

                missing = self.REQUIRED_COLUMNS - headers
                if missing:
                    return False, f"Missing columns: {missing}", 0

                rows = list(reader)
                if not rows:
                    return False, "CSV has no data rows", 0

                return True, "OK", len(rows)

        except Exception as e:
            return False, str(e), 0

    # --------------------------------------------------------
    def ingest(self, source_path: str, notes: str = ""):
        """
        Ingest a BDI CSV file: validate -> copy -> hash -> audit.

        Parameters
        ----------
        source_path : str
            Path to the downloaded CSV file.
        notes : str, optional
            Free-text note appended to the audit entry.
        """
        _section("BALTIC DRY INDEX / Manual Ingest")

        if not os.path.exists(source_path):
            print(f"  [ERROR] File not found: {source_path}")
            return

        print(f"  [VALIDATE] {source_path}")
        ok, msg, row_count = self._validate_csv(source_path)
        if not ok:
            print(f"  [INVALID] {msg}")
            return
        print(f"  [OK] {row_count} data rows, columns validated")

        # Destination path with ingest timestamp
        retrieved_at = _utc_now()
        timestamp    = retrieved_at.replace(":", "").replace("-", "")[:15]
        ext          = os.path.splitext(source_path)[1]
        dest_fname   = f"{self.cfg['file_prefix']}_{timestamp}{ext}"
        dest_path    = os.path.join(self.output_dir, dest_fname)

        # Copy file
        import shutil
        shutil.copy2(source_path, dest_path)
        file_hash = _sha256(dest_path)

        print(f"  [SAVED]  {dest_fname}")
        print(f"  SHA256:  {file_hash}")

        full_notes = self.cfg.get("manual_note", "")
        if notes:
            full_notes = full_notes + " | " + notes if full_notes else notes

        _write_audit(_make_audit_entry(
            dataset_id   = self.cfg["dataset_id"],
            source       = self.cfg["source"],
            source_url   = "manual_download",
            retrieved_at = retrieved_at,
            local_path   = dest_path,
            raw_hash     = file_hash,
            row_count    = row_count,
            unit         = self.cfg["unit"],
            notes        = full_notes,
        ))

        print(f"\n[BALTIC/BDI] Ingest complete. Audit entry written.")


# ============================================================
# MASTER DOWNLOAD RUNNER
# ============================================================

def run_all_downloads(
    gie_agsi:  bool = None,
    gie_alsi:  bool = None,
    eurostat:  bool = None,
    fao:       bool = None,
    api_key:   str  = None,
):
    """
    Run all enabled downloaders sequentially.

    Parameters default to ENABLE_* flags in eu_monitor_config.py.
    Pass explicit True/False to override for this run only.

    Note: Baltic Dry is NOT included here - it requires manual ingest.
    Use BalticDryIngestor().ingest("path/to/file.csv") separately.
    """
    do_agsi     = gie_agsi  if gie_agsi  is not None else ENABLE_GIE_AGSI
    do_alsi     = gie_alsi  if gie_alsi  is not None else ENABLE_GIE_ALSI
    do_eurostat = eurostat  if eurostat  is not None else ENABLE_EUROSTAT
    do_fao      = fao       if fao       is not None else ENABLE_FAO

    print("\n" + "="*60)
    print("  EU System Monitor - Unified Download")
    print(f"  {_utc_now()}")
    print("="*60)
    print(f"  GIE AGSI  : {'ON' if do_agsi     else 'OFF'}")
    print(f"  GIE ALSI  : {'ON' if do_alsi     else 'OFF'}")
    print(f"  Eurostat  : {'ON' if do_eurostat else 'OFF'}")
    print(f"  FAO       : {'ON' if do_fao      else 'OFF'}")
    print(f"  Baltic Dry: manual ingest (run separately)")

    if do_agsi:
        try:
            GIEDownloader("agsi", api_key=api_key).run()
        except ValueError as e:
            print(f"\n[GIE/AGSI] SKIPPED - {e}")

    if do_alsi:
        try:
            GIEDownloader("alsi", api_key=api_key).run()
        except ValueError as e:
            print(f"\n[GIE/ALSI] SKIPPED - {e}")

    if do_eurostat:
        EurostatDownloader().run()

    if do_fao:
        FAODownloader().run()

    print("\n" + "="*60)
    print("  All downloads complete.")
    print(f"  Audit log: {AUDIT_LOG}")
    print("="*60)


# ============================================================
# ENTRY POINT
# ============================================================

def _parse_args():
    p = argparse.ArgumentParser(
        description="EU System Monitor - Unified Downloader"
    )
    p.add_argument("--gie-only",    action="store_true",
                   help="Download GIE (AGSI + ALSI) only")
    p.add_argument("--no-gie",      action="store_true",
                   help="Skip GIE downloads")
    p.add_argument("--no-eurostat", action="store_true",
                   help="Skip Eurostat download")
    p.add_argument("--no-fao",      action="store_true",
                   help="Skip FAO download")
    p.add_argument("--ingest-bdi",  metavar="CSV_PATH",
                   help="Ingest a manually downloaded Baltic Dry Index CSV")
    p.add_argument("--bdi-notes",   metavar="NOTES", default="",
                   help="Optional note to attach to BDI audit entry")
    p.add_argument("--api-key",     metavar="KEY",
                   help="GIE API key (overrides GIE_API_KEY env var)")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    # BDI manual ingest - standalone action
    if args.ingest_bdi:
        BalticDryIngestor().ingest(args.ingest_bdi, notes=args.bdi_notes)
        sys.exit(0)

    if args.gie_only:
        run_all_downloads(
            gie_agsi=True, gie_alsi=True,
            eurostat=False, fao=False,
            api_key=args.api_key,
        )
    else:
        run_all_downloads(
            gie_agsi  = not args.no_gie,
            gie_alsi  = not args.no_gie,
            eurostat  = not args.no_eurostat,
            fao       = not args.no_fao,
            api_key   = args.api_key,
        )
