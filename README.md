# PostgreSQL Hotspots

**PostgreSQL Hotspots** is a diagnostic tool built for high-pressure “database down” situations. It analyzes database
behavior without connecting to the database itself, allowing you to identify performance bottlenecks and resource
contention even when PostgreSQL is completely unresponsive. Besides, it generates a static `.html` file along with suspicious info in `.txt` files - Simply the analysis report sharing process
<img width="1781" height="881" alt="image" src="https://github.com/user-attachments/assets/569acc47-c61a-450e-b326-d6ce9ba13a00" />


# Key Features

* **Emergency-Ready Diagnostics:** The tool pulls metrics from Google Cloud Monitoring, not from PostgreSQL system views. This means it continues to work when the database is hung, locked, or under extreme load.
* **Zero-Impact Observation:** Unlike `pg_stat_statements` or active SQL polling, PostgreSQL Hotspots adds no load to your production database. All analysis is performed out-of-band, ensuring diagnostics never make an ongoing incident worse.
* **Interactive Visual Correlation:** Automatically generates rich, interactive reports that correlate:
  - database activity history
  - most expensive SQL statements
  - lock waits and contention  
These views make it easy to pinpoint when a performance hotspot started and what caused it, while providing more actionable context than raw Google Cloud Monitoring dashboards.

# Safety & Security

Security is a core design principle of this project. PostgreSQL Hotspots is intentionally non-intrusive and strictly follows the principle of least privilege.

* **IAM-Based Authentication:** The tool uses your existing Google Cloud environment via
  `gcloud auth application-default login`. It relies on your local Application Default Credentials and inherits only the permissions you have already granted—nothing more.
* **Read-Only Access:** PostgreSQL Hotspots is strictly limited to reading monitoring metrics.
It cannot modify database configuration, execute SQL, drop tables, or change any part of your GCP infrastructure.
* **Fully Local Execution:** All data processing and report generation happen entirely on your local machine.
No telemetry, metrics, or sensitive metadata are sent to external or third-party services.

# Table of Contents
- [Prerequisites](#prerequisites)
- [Installation](#installation)

## Prerequisites  

* Google Cloud CLI (`gcloud`) installed if you've never generated ADC on your own machine.

# Installation  

### From source (recommended for development)

```bash
pip install -e .
```

# Usage

Once installed, the `pshs` console script is available:

```bash
pshs generate PROJECT_ID INSTANCE_ID OUTPUT_DIR --start-time 2026-01-01T14:00 duration-hours 3
```

# Command notes

* Provide exactly two of `--start-time`, `--end-time`, and `--duration-hours`.
* `--safe` (default) skips ADC login; use `--no-safe` to trigger ADC login.
