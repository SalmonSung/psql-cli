import shutil
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone
import json
import subprocess
import click
import logging
import google.auth
from google.auth.exceptions import DefaultCredentialsError


def bytes_to_unit(value_bytes: float, unit: str = "GiB") -> float:
    """
    Convert raw bytes to the requested unit.

    Notes
    -----
    - Returns 0.0 for None to keep Plotly traces and sunbursts stable.
    """
    if value_bytes is None:
        return 0.0

    unit = unit.lower()

    if unit in ("b", "bytes"):
        return float(value_bytes)
    if unit == "mib":
        return float(value_bytes) / (1024.0 ** 2)
    if unit == "gib":
        return float(value_bytes) / (1024.0 ** 3)

    # Safe fallback: return bytes unchanged
    return float(value_bytes)



def ensure_adc_login():
    """
    Ensures Google Application Default Credentials (ADC) are available.
    Runs `gcloud auth application-default login` only if needed.
    """
    try:
        credentials, project = google.auth.default()
        logging.info('Application Default Credentials already configured.')
        if project:
            logging.info(f'Project: {project}')
        return True

    except DefaultCredentialsError:
        logging.info('ADC not found. Launching gcloud login...')

        gcloud = shutil.which("gcloud") or shutil.which("gcloud.cmd")
        if not gcloud:
            logging.error(f'gcloud command not found')
            raise RuntimeError("gcloud command not installed: Please check: https://docs.cloud.google.com/sdk/docs/install-sdk")

        try:
            subprocess.run(
                [gcloud, "auth", "application-default", "login"],
                check=True,
            )
        except Exception as e:
            logging.error(e)
            return False
        logging.info("ADC login successful.")
        return True

# Todo: that means your Monitoring API auth + project are correct, and Cloud SQL metrics are visible.
def check_project_endpoints():
    from datetime import datetime, timedelta, timezone
    from google.cloud import logging_v2

    PROJECT_ID = "psql-hotspots"
    client = logging_v2.Client(project=PROJECT_ID)

    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=1)

    start_s = start.isoformat().replace("+00:00", "Z")
    end_s = end.isoformat().replace("+00:00", "Z")

    log_filter = f'''
    resource.type="cloudsql_database"
    timestamp>="{start_s}"
    timestamp<="{end_s}"
    '''

    it = client.list_entries(filter_=log_filter, order_by=logging_v2.DESCENDING, page_size=50)

    found = 0
    for entry in it:
        found += 1
        # Print raw-ish content to see where text lives
        print("logName:", entry.log_name)
        print("labels:", dict(entry.resource.labels))
        print("payload type:", type(entry.payload))
        print("payload:", entry.payload)
        print("----")
        if found >= 5:
            break

    if found == 0:
        print(
            "No Cloud SQL log entries found in this project/time window (wrong project, no permissions, or logs not in this project).")



def load_db_secret_list(path: str) -> list[dict]:
    path = Path(path)
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("[]", encoding="utf-8")
        return []

    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def parse_utc_minute(value: Optional[str]) -> Optional[datetime]:
    """Parse 'YYYY-MM-DDTHH:MM' (UTC), no seconds. Returns tz-aware UTC datetime."""
    if value is None:
        return None

    s = value.strip()
    # allow space separator
    s = s.replace(" ", "T")
    # allow trailing Z (we always treat as UTC anyway)
    if s.endswith("Z"):
        s = s[:-1]

    try:
        dt = datetime.strptime(s, "%Y-%m-%dT%H:%M")
    except ValueError as e:
        raise click.BadParameter(
            "Invalid datetime. Use UTC format: YYYY-MM-DDTHH:MM (no seconds), "
            "e.g. 2026-01-29T10:15"
        ) from e

    return dt.replace(tzinfo=timezone.utc)