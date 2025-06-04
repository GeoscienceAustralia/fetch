# fetch2

This is a rewrite of the brdf downloader that allow gaps to be filled in a more efficient way.

It takes a date range (defaulting to all available) and tiles (defaulting to DEA's mainland+offshore areas), and will
find and fill in any gaps in the output folder.

It has default settings for GA's BRDF downloading at NCI, with support for configuration files
to manage multiple products and collections.

## Supported Products

- **MODIS**: MCD43A1.061 - Terra/Aqua combined BRDF data
- **VIIRS-M**: VNP43MA1.001 - VIIRS moderate resolution BRDF data (750m)
- **VIIRS-I**: VNP43IA1.001 - VIIRS imaging resolution BRDF data (375m + pan)

## Prerequisites

- [uv package manager](https://github.com/astral-sh/uv#installation)
- NASA Earthdata login credentials
- `swfo-convert` tool (For Modis: HDF to H5 conversion)

## Authentication

Ideally, set your NASA Earthdata credentials as environment variables:

```bash
export EARTHDATA_USERNAME="your_username"
export EARTHDATA_PASSWORD="your_password"
```

Alternatively, they can be set in the config file (below), but be careful of its permissions.

## Install

`uv run` will automatically set up an environment for it, to run directly:
```bash
uv run fetch2-brdf --help
```

Otherwise, you can pip install it:

```bash
uv pip install -e .
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

### File Logging

Configure in your config file:

```yaml
logging:
    log_file_pattern: "/var/log/brdf/{year}-{month:02d}/{day:02d}-{hour:02d}{minute:02d}{second:02d}-{product}.jsonl"
```

Supported variables:

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
