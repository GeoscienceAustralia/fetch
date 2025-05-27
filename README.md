# BRDF Data Downloader

A script for downloading and converting BRDF (Bidirectional Reflectance Distribution Function) satellite data
from NASA's USGS Earth Observation servers.

It has default settings for GA's BRDF downloading at NCI.

## Supported Products

- **MODIS**: MCD43A1.061 - Terra/Aqua combined BRDF data
- **VIIRS-M**: VNP43MA1.001 - VIIRS moderate resolution BRDF data
- **VIIRS-I**: VNP43IA1.001 - VIIRS imaging resolution BRDF data

## Prerequisites

- Python 3.8+
- UV package manager
- NASA Earthdata login credentials
- `swfo-convert` tool (for HDF to H5 conversion)

## Quick Install

```bash
# Clone the repository
git clone <repository-url>
cd brdf-downloader

# Install dependencies with uv
uv sync
```

## Authentication

Set your NASA Earthdata credentials as environment variables:

```bash
export EARTHDATA_USERNAME="your_username"
export EARTHDATA_PASSWORD="your_password"
```

Alternatively, you can pass them as command-line arguments (not recommended for security).

## Usage

### Basic Usage

The defaults are for GA's typical BRDF download at NCI.

With no arguments, it will scan and fill in all missing MODIS data for Australian mainland
and offshore tiles, in the default folders:

Default logging is "pretty", but if you redirect to a file it will log in json.

```bash
uv run main.py modis
```
You probably want to set a `--output-base` for testing, and a restricted date/tile range.

(default output-base is `/g/data/v10/eoancillarydata-2`)

```bash
# Download specific product with date range
uv run main.py viirs_m --start-date 2024-01-01 --end-date 2024-01-31

# Specify output base, custom dir and verbose
uv run main.py modis --verbose --start-date=$(date -d "yesterday" +%F) --output-base /testing/eoancillarydata-2

```

### Command Line Options

- `product`: Choose from `modis`, `viirs_m`, or `viirs_i`
- `--start-date YYYY-MM-DD`: Oldest date to download
- `--end-date YYYY-MM-DD`: Newest date to download
- `--min-age-days N`: Only download files at least N days old
- `--no-offshore-tiles`: Skip Australian offshore tiles
- `--no-mainland-tiles`: Skip Australian mainland tiles
- `--tiles-path FILE`: Use custom tile list (one tile per line, e.g., "h27v09"), instead of Aus
- `--output-base PATH`: Output directory (default: `/g/data/v10/eoancillarydata-2`)
- `--verbose, -v`: Enable debug logging

### Custom Tiles File

A text file with one tile identifier per line:

```
h27v09
h28v09
h29v09
```

Then use `--tiles-path tiles.txt`.

## Output Structure

Files are organized by date in the following structure, following GA conventions:

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

## Marketing Features

- **Parallel Downloads**: Configurable concurrent downloads with rate limiting
- **Rate Limiting**: Respects USGS server policies (0.3s between requests)
- **Retry with backoff**: Built-in retry logic for failed downloads
- **Resume Support**: Skips already downloaded files
- **Progress Tracking**: Structured logging with optional verbose output
- **Format Conversion**: HDF to H5 conversion for MODIS data
