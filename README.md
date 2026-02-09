<div align="center">

<img width="1024" height="604" alt="postgreSQL Hotspots logo v2" src="https://github.com/user-attachments/assets/981ce993-5a90-4c05-b510-a4475bd5367d" />

*Postgres on fire? Start here.*


<p>

[![GitHub Stars](https://img.shields.io/github/stars/SalmonSung/psql-cli?style=square)](https://github.com/SalmonSung/psql-cli/stargazers)
[![Version](https://img.shields.io/badge/version-v1.4.1-4CAF50.svg)](https://github.com/SalmonSung/psql-cli)


</p> 

<b> A diagnostic tool built for high-pressure “database down” situations... <br></b>
<b> Analyzes database behavior without connecting to the database itself </b>

<b> Build on Google Cloud Monitoring </b>

</div>

# Key Features

* **Emergency-Ready Diagnostics:** The tool pulls metrics from Google Cloud Monitoring, not from PostgreSQL system views. This means it continues to work when the database is hung, locked, or under extreme load.
* **Zero-Impact Observation:** Unlike `pg_stat_statements` or active SQL polling, PostgreSQL Hotspots adds no load to your production database. All analysis is performed out-of-band, ensuring diagnostics never make an ongoing incident worse.
* **Interactive Visual Correlation:** generates rich, interactive reports into a `.html` file:
  - database activity history  
  - most expensive SQL statements  
  - lock waits and contention

and more...  
<div align="center">
  <p>
    <img width="1243" height="651" alt="image" src="https://github.com/user-attachments/assets/3d92c5c4-a46f-440b-ad2e-fca93e5de4af" />  
    <b>The Metrics have been mapped into a diagnosis-ready format for rapid trouble-shooting</b>b<br>
  </p>


  
<img width="1892" height="874" alt="image" src="https://github.com/user-attachments/assets/f3d91132-c172-4cf4-af2f-30333871f5cb" />
<b>Compare Mode:</b> Displayed multiple figures in a row for better comparison <br>

</div>

# Use Case
You can find some cases that have been solved with PostgreSQL Hotspots here:  
- [When High CPU Is Normal — Until It Isn’t: A PostgreSQL Incident Analysis](https://medium.com/@SungSalmon/most-postgresql-performance-articles-focus-on-query-optimization-missing-indexes-bad-execution-57be5366eb61)

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

or Download [v1.4.1 app](https://github.com/SalmonSung/psql-cli/releases/tag/v1.4.1)


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
