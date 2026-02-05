<div align="center">

<img width="1024" height="604" alt="postgreSQL Hotspots logo v2" src="https://github.com/user-attachments/assets/981ce993-5a90-4c05-b510-a4475bd5367d" />

*Postgres on fire? Start here.*


<p>

[![GitHub Stars](https://img.shields.io/github/stars/SalmonSung/psql-cli?style=square)](https://github.com/SalmonSung/psql-cli/stargazers)
[![Version](https://img.shields.io/badge/version-v1.1.0-4CAF50.svg)](https://github.com/SalmonSung/psql-cli)


</p> 

<b> A diagnostic tool built for high-pressure “database down” situations... <br></b>
<b> Analyzes database behavior without connecting to the database itself </b>

<b> Build on Google Cloud Monitoring </b>

</p>

</div>

# Key Features

* **Emergency-Ready Diagnostics:** The tool pulls metrics from Google Cloud Monitoring, not from PostgreSQL system views. This means it continues to work when the database is hung, locked, or under extreme load.
* **Zero-Impact Observation:** Unlike `pg_stat_statements` or active SQL polling, PostgreSQL Hotspots adds no load to your production database. All analysis is performed out-of-band, ensuring diagnostics never make an ongoing incident worse.
* **Interactive Visual Correlation:** Automatically generates rich, interactive reports that correlate:
  - database activity history  
  - most expensive SQL statements  
  - lock waits and contention

and more...  
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
- [Usage](#usage)
- [Command notes](#command-notes)
- [History](#history)
  - [v.1.0.0](#v100)
  - [v.0.1.0](#v010)

## Prerequisites  

- Google Cloud CLI (`gcloud`) installed if you've never generated ADC on your own machine.
- Being granted viewer role for `Cloud Monitoring API` and `Cloud SQL Admin API`

# Installation  

### From source (recommended for development)

```bash
pip install -e .
```

# Usage  

Once installed, the `pshs` console script is available:

```bash
pshs generate PROJECT_ID INSTANCE_ID OUTPUT_DIR --start-time 2026-01-01T14:00 --duration-hours 3
```
> [!TIP]  
> - Please replace `PROJECT_ID`, `INSTANCE_ID`, `OUTPUT_DIR` with the actual value.  
> - `--start-time` is using UTC time zone.  
> - If you set `--duration-hours` to 24 or above, you're encourage to change `GROOUP_BY_MINUTES` in `config.py` to a bigger value for better visualization

# Command notes

* Provide exactly two of `--start-time`, `--end-time`, and `--duration-hours`.
* `--safe` (default) skips ADC login; use `--no-safe` to trigger ADC login.

# History  
## v.1.0.0
- **Breaking change:** Introduces new minimum requirements — Cloud Monitoring API and Cloud SQL Admin API. This version is not backward-compatible with v0.1.x.

## v.0.1.0  
- Support command `generate`
- Customised observation time range enable
- Provide General History and SQL analysis
