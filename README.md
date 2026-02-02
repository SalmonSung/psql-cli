# psql-hotspots

CLI tool for generating PostgreSQL hotspots reports.

## Installation

### From source (recommended for development)

```bash
pip install -e .
```

### Standard install

```bash
pip install .
```

## Usage

### Run via module (without installing)

```bash
python -m src.pshs generate psql-hotspots psql-prc-01 "C:\Users\kaiyi\Desktop\psql_report" --start-time 2026-02-01T13:30 --duration-hours 6 --no-safe
```

### Run after installing with pyproject

Once installed, the `pshs` console script is available:

```bash
pshs generate psql-hotspots psql-prc-01 "C:\Users\kaiyi\Desktop\psql_report" --start-time 2026-02-01T13:30 --duration-hours 6 --no-safe
```

### Command notes

* Provide exactly two of `--start-time`, `--end-time`, and `--duration-hours`.
* `--safe` (default) skips ADC login; use `--no-safe` to trigger ADC login.
