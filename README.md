# BRDF Data Downloader

A script for downloading and converting BRDF (Bidirectional Reflectance Distribution Function) satellite data
from NASA's USGS Earth Observation servers.

It has default settings for GA's BRDF downloading at NCI, with support for flexible configuration files
to manage multiple products and collections.

## Supported Products

- **MODIS**: MCD43A1.061 - Terra/Aqua combined BRDF data
- **VIIRS-M**: VNP43MA1.001 - VIIRS moderate resolution BRDF data
- **VIIRS-I**: VNP43IA1.001 - VIIRS imaging resolution BRDF data

## Prerequisites

- [uv package manager](https://github.com/astral-sh/uv#installation)
- NASA Earthdata login credentials
- `swfo-convert` tool (for HDF to H5 conversion)

## Quick Run

```bash
uv run fetch-brdf --help
```

Or make a dev environment:

```bash
uv sync
uv pip install -e .
```

## Authentication

Ideally, set your NASA Earthdata credentials as environment variables:

```bash
export EARTHDATA_USERNAME="your_username"
export EARTHDATA_PASSWORD="your_password"
```

Alternatively, they can be set in the config file (below), but be aware of its accessibility.

## Usage

### Configuration-Based Usage (Recommended)

Generate an example configuration file:

```bash
# Print example config to screen
uv run fetch-brdf generate-config

# Save example config to file
uv run fetch-brdf generate-config -o my-config.yaml
```

Run with configuration file:

```bash
# Specify config file
uv run fetch-brdf run --config-path my-config.yaml

# Or set environment variable
export BRDF_CONFIG_PATH=/path/to/my-config.yaml
uv run fetch-brdf run
```

The configuration file supports:

- **Multiple products** in a single run
- **Per-product settings** (different output paths, date ranges, tiles)
- **Relative dates** (e.g., `-30` for 30 days ago)
- **Flexible logging** with file output patterns
- **Tile sets** by name or custom file paths

### Legacy Command Line Usage

For backward compatibility, the original command-line interface is still supported:

```bash
# Basic usage - downloads MODIS for Australian tiles
uv run fetch-brdf modis

# Download specific product with date range
uv run fetch-brdf viirs_m --start-date 2024-01-01 --end-date 2024-01-31

# Custom output directory and verbose logging
uv run fetch-brdf modis --verbose --start-date=$(date -d "yesterday" +%F) --output-base /testing/eoancillarydata-2
```

### Configuration File Examples

**Basic Configuration:**

Run the generate-config command and pipe it to a file.

```bash
uv run fetch-brdf generate-config > config.yaml
```

An example config:

```yaml

logging:
    verbose: false
    log_file_pattern: "/g/data/v10/logs/fetch/{year}-{month:02d}/{day:02d}-{hour:02d}{minute:02d}{second:02d}-{product}.jsonl"

date_range:
  # Can be either a specific date or relative date
  begin: 2025-01-01
  end: -7 # 7 days ago

tiles: mainland+offshore

# Products to download
products:
  modis:
    enabled: true
  viirs_m:
    enabled: true
    date_range:
      begin: 2024-01-01
```

## Command Line Options

### Main Commands

- `fetch-brdf generate-config`: Generate a configuration file
- `fetch-brdf run`: Run downloader with configuration file

## Tile Configuration

### Built-in Tile Sets

- `mainland`: Australian mainland tiles (h27v09 to h32v13)
- `offshore`: Australian offshore tiles (h22v14, h27v14)
- Combination, eg `mainland+offshore`: All Australian tiles (default)

## Output Structure

Files are organized by date following GA conventions:

```
output_base/
└── BRDF/
    └── [PRODUCT]/
        └── YYYY.MM.DD/
            ├── file1.h5
            ├── file1.h5.xml
            ├── file2.h5
            └── file2.h5.xml
```

Example:

```
/g/data/v10/eoancillarydata-2/
├── BRDF/
│   ├── MCD43A1.061/
│   │   ├── 2024.03.15/
│   │   │   ├── MCD43A1.A2024075.h27v09.061.2024084123456.h5
│   │   │   └── MCD43A1.A2024075.h27v09.061.2024084123456.h5.xml
│   └── VNP43MA1.001/
│       └── 2024.03.15/
│           ├── VNP43MA1.A2024075.h27v09.001.2024084123456.h5
│           └── VNP43MA1.A2024075.h27v09.001.2024084123456.h5.xml
```

## Logging

### Console Logging

- **Interactive terminal**: Pretty-printed colored output
- **Redirected/scripted**: Structured JSON logging

### File Logging

Configure in your config file:

```yaml
logging:
  log_file_pattern: "/var/log/brdf/{year}-{month:02d}/{day:02d}-{hour:02d}{minute:02d}{second:02d}-{product}.jsonl"
```

Supports variables:

- `{year}`, `{month}`, `{day}`, `{hour}`, `{minute}`, `{second}`
- `{product}` - The product being downloaded

## Configuration File Locations

The `run` command searches for configuration files in order:

1. `--config-path` argument
2. `BRDF_CONFIG_PATH` environment variable
3. `./brdf-config.yaml`
4. `./brdf-config.yml`
5. `~/.config/brdf-downloader/config.yaml`
6. `/etc/brdf-downloader/config.yaml`
