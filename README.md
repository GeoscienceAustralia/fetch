# fetch2

This is a rewrite of the BRDF downloader that allows gaps to be filled in a more efficient way.

It uses NASA's CMR (Common Metadata Repository) API to download BRDF (Bidirectional Reflectance Distribution Function) data products from satellite missions.

It takes a date range (defaulting to all available) and tiles (defaulting to DEA's mainland+offshore areas), and will
find and fill in any gaps in the output folder.

It has default settings for GA's BRDF downloading at NCI, with support for configuration files
to manage multiple products and collections.

## Supported Products

- **MODIS**: MCD43A1.061 - Terra/Aqua combined BRDF data
- **VIIRS-M**: VNP43MA1.002 - VIIRS moderate resolution BRDF data (750m)
- **VIIRS-I**: VNP43IA1.002 - VIIRS imaging resolution BRDF data (375m + pan)

## Prerequisites

- [uv package manager](https://github.com/astral-sh/uv#installation)
- NASA Earthdata login credentials
- `swfo-convert` tool (For Modis: HDF to H5 conversion)

## Authentication

You need a NASA Earthdata Bearer Token for authentication. Set it as an environment variable:

```bash
export EARTHDATA_TOKEN="your_bearer_token"
```

**How to get a Bearer Token:**
1. Go to [NASA Earthdata Login](https://urs.earthdata.nasa.gov/)
2. Log in with your NASA Earthdata account
3. Go to "My Profile" → "Generate Token"
4. Copy the generated token

Alternatively, the token can be set in the config file (below), but be careful of its permissions.

## Install

`uv run` will automatically set up an environment for it, to run directly:
```bash
uv run fetch2-brdf --help
```

Otherwise, you can pip install it:

```bash
uv pip install -e .
```

Install the optional water vapour CLI and its dependencies (assuming you already have wagl):

```bash
uv sync --extra water-vapour
uv run --extra water-vapour fetch2-wv --help
uv run --extra water-vapour fetch2-wv eoancillary/water_vapour
```

`wagl` is required for water vapour processing. NCI environments already have an installed, but otherwise see https://github.com/OpenDataCubePipelines/ard-pipeline

If you've built the ard:dev docker container from ard-pipeline repo, there are convenient Justfile commands for running inside docker:

```
# Run wv tests in docker
just test
# Run a fetch of data (2026, stopping at January 10). Assumes you have a env.sh file with ecmwf credentials.
just fetch-wv test-data-directory --year 2026 --through 2026-01-10
```


Run tests:

```bash
uv sync --extra test
uv run pytest
```

(or enter the environment with `. .venv/bin/activate` to be able to run tools like `pytest` directly)


## Usage

### Basic Configuration

Generate an example configuration file:

```bash
# Print example config
fetch2-brdf generate-config

# Or write it to a file
fetch2-brdf generate-config -o my-config.yaml
```

Run with a configuration file:

```bash
fetch2-brdf run --config-path my-config.yaml

# Or set environment variable
export BRDF_CONFIG_PATH=/path/to/my-config.yaml
fetch2-brdf run
```

An example config:

```yaml
# Authentication (alternatively set EARTHDATA_TOKEN environment variable)
auth:
    token: "your_earthdata_bearer_token_here"

logging:
    verbose: false
    log_file_pattern: "/g/data/v10/logs/fetch/{year}-{month:02d}/{day:02d}-{hour:02d}{minute:02d}{second:02d}-{product}.jsonl"

date_range:
    # Can be either a specific date or relative date
    begin: 2025-01-01
    end: -7 # 7 days ago

tiles: mainland+offshore
output_path: /g/data/v10/eoancillarydata-2

# Products to download
products:
    modis:
        enabled: true
    viirs_m:
        enabled: false  # Enable when needed
        date_range:
            begin: 2024-01-01
    viirs_i:
        enabled: false  # Enable when needed
```

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
│   └── VNP43MA1.002/
│       └── 2024.03.15/
│           ├── VNP43MA1.A2024075.h27v09.002.2024084123456.h5
│           └── VNP43MA1.A2024075.h27v09.002.2024084123456.h5.xml
```

## Logging

### File Logging

Configure in your config file:

```yaml
logging:
    log_file_pattern: "/var/log/brdf/{year}-{month:02d}/{day:02d}-{hour:02d}{minute:02d}{second:02d}-{product}.jsonl"
```

Supported variables:

- `{year}`, `{month}`, `{day}`, `{hour}`, `{minute}`, `{second}`
- `{product}` - The product being downloaded

## Data Source

The downloader uses NASA's CMR (Common Metadata Repository) API to discover and download BRDF data:

- **API Endpoint**: `https://cmr.earthdata.nasa.gov/search/granules`
- **Collection Concept IDs**:
  - MODIS MCD43A1.061: `C2343116130-LPCLOUD`
  - VIIRS VNP43MA1.002: `C2545314596-LPCLOUD`
  - VIIRS VNP43IA1.002: `C2545314578-LPCLOUD`

## Configuration File Locations

The `run` command searches for configuration files in order:

1. `--config-path` argument
2. `BRDF_CONFIG_PATH` environment variable
3. `./brdf-config.yaml`
4. `./brdf-config.yml`
5. `~/.config/brdf-downloader/config.yaml`
6. `/etc/brdf-downloader/config.yaml`
