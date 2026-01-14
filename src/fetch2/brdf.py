import datetime
import os
import re
import shlex
import shutil
import socket
import subprocess
import sys
import time
from concurrent.futures import (
    Future,
    ThreadPoolExecutor,
    as_completed,
)
from concurrent.futures import (
    wait as wait_for_future,
)
from datetime import datetime as dt
from datetime import date as date_type
from datetime import timedelta
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional, Set, Tuple, Union, Dict

import click
import httpx
import structlog
import yaml
from pydantic import BaseModel, Field, field_validator
from httpx import URL

# ipv6 has issues to USGS from NCI. Disable it.
import urllib3.util.connection

urllib3.util.connection.HAS_IPV6 = False
socket.has_ipv6 = False

# One hour
LOGIN_TIMEOUT_SECONDS = 60 * 60

LOG = structlog.get_logger()

# Example file name:
# MCD43A1.A2011039.h22v14.061.2021182171212.hdf
# ie. product.acquisition_date.tile_number.product_version.timestamp.extension
#
# On download, there are two files for each HDF file, the .hdf and the .hdf.xml
# We convert them into one .h5 file.
#
_BRDF_FILENAME_PATTERN = re.compile(
    r"(MCD43A1|VNP43IA1|VNP43MA1)\.A(?P<acquisition_date>[0-9]{7})\.(?P<tile_number>h[0-9]{2}v[0-9]{2})\.[0-9]{3}\.(?P<timestamp>[0-9]{13})(?P<extension>[.a-z5]+)?$"
)
# Folder pattern YYYY.MM.DD
_DATE_FOLDER_PATTERN = re.compile(r"[0-9]{4}\.[0-9]{2}\.[0-9]{2}")

_KNOWN_PRODUCTS = {
    "modis": {"concept_id": "C2343116130-LPCLOUD", "output_path": "BRDF/MCD43A1.061"},
    "viirs_m": {
        "concept_id": "C2545314596-LPCLOUD",
        "output_path": "BRDF/VNP43MA1.002",
    },
    "viirs_i": {
        "concept_id": "C2545314578-LPCLOUD",
        "output_path": "BRDF/VNP43IA1.002",
    },
}
# Tiles are (h, v) coordinates, as seen in the filename
TILE_SETS = {
    # The whole range of Australia
    "mainland": tuple((h, v) for h in range(27, 33) for v in range(9, 14)),
    # The two offshore tiles
    "offshore": ((22, 14), (27, 14)),
}
# Two corners. No off-by-one errors
assert (27, 9) in TILE_SETS["mainland"]
assert (32, 13) in TILE_SETS["mainland"]
assert (32, 14) not in TILE_SETS["mainland"]

USE_S3 = os.environ.get("BRDF_USE_S3", "false").lower() == "true"
S3_BUCKET = os.environ.get("BRDF_S3_BUCKET")

# Set up an S3 client
if USE_S3:
    import boto3
    S3_CLIENT = boto3.client("s3")

    
def finalize_download(log: structlog.BoundLogger, src_path: Path, dest_path: Path) -> None:
    """Moves a downloaded file from a temporary location to its final location. Will upload to s3 if configured."""
    if USE_S3:
        log.info(f"Uploading to {dest_path}")
        S3_CLIENT.upload_file(
            Filename=str(src_path),
            Bucket=S3_BUCKET,
            Key=str(dest_path),
        )
        src_path.unlink()
    else:
        src_path.rename(dest_path)

def get_children(path: Path) -> Optional[Iterator[Path]]:
    """List children of the given path, either locally or in S3, depending on configuration."""
    if USE_S3:
        LOG.debug(f"Listing S3 path {path}")

        # Get a paginator for listed objects
        paginator = S3_CLIENT.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=str(path))
        first_page = True
        for page in pages:
            if first_page and page.get("KeyCount", 0) == 0:
                LOG.debug(f"No S3 objects were found at {path}")
                return None
            first_page = False

            for obj in page.get('Contents', []):
                yield Path(obj['Key'])
    else:
        if path.exists() and path.is_dir():
            for child in path.iterdir():
                yield child
        else:
            return None



# Configuration Models
class DateRange(BaseModel):
    """Date range configuration with support for relative dates."""

    begin: Optional[Union[datetime.date, int]] = Field(
        default=None,
        description="Start date (YYYY-MM-DD) or relative days (negative number)",
    )
    end: Optional[Union[datetime.date, int]] = Field(
        default=None,
        description="End date (YYYY-MM-DD) or relative days (negative number)",
    )

    def resolve_dates(self) -> Tuple[Optional[datetime.date], Optional[datetime.date]]:
        """Resolve relative dates to absolute dates.

        >>> import datetime
        >>> dr = DateRange(begin=-7, end=-1)
        >>> start, end = dr.resolve_dates()
        >>> isinstance(start, datetime.date) and isinstance(end, datetime.date)
        True
        >>> end > start
        True
        """
        today = datetime.date.today()

        start = None
        if isinstance(self.begin, int):
            start = today + timedelta(days=self.begin)
        elif isinstance(self.begin, datetime.date):
            start = self.begin

        end = None
        if isinstance(self.end, int):
            end = today + timedelta(days=self.end)
        elif isinstance(self.end, datetime.date):
            end = self.end

        return start, end


class ProductConfig(BaseModel):
    """Configuration for a specific product."""

    enabled: bool = Field(default=True, description="Whether to download this product")
    date_range: Optional[DateRange] = Field(
        default=None, description="Product-specific date range (overrides global)"
    )
    max_downloads: Optional[int] = Field(
        default=None, description="Maximum number of files to download"
    )


class DownloadConfig(BaseModel):
    """Global download configuration."""

    max_retries: int = Field(
        default=4, description="Maximum number of retries for failed downloads"
    )
    max_queue_size: int = Field(
        default=3, description="Maximum number of queued downloads"
    )
    max_workers: int = Field(
        default=3, description="Maximum number of concurrent workers"
    )
    request_timeout_secs: float = Field(
        default=180, description="Request timeout in seconds"
    )
    min_request_period_secs: float = Field(
        default=0.3, description="Minimum time between requests"
    )


class AuthConfig(BaseModel):
    """Authentication configuration."""

    token: Optional[str] = Field(
        default=None, description="earthdata token (if not $EARTHDATA_TOKEN)"
    )

    def get_token(self) -> str:
        """Get token from config or environment variables."""
        token = self.token or os.environ.get("EARTHDATA_TOKEN")

        if not token:
            raise ValueError(
                "No token supplied. Set in config file or use "
                "EARTHDATA_TOKEN environment variable"
            )

        return token


class LoggingConfig(BaseModel):
    """Logging configuration."""

    verbose: bool = Field(default=False, description="Enable verbose logging")
    log_file_pattern: Optional[str] = Field(
        default="/g/data/v10/logs/fetch/{year}-{month:02d}/{day:02d}-{hour:02d}{minute:02d}{second:02d}-{product}.jsonl",
        description="Log file pattern with date and product variables",
    )

    def get_log_path(self, product: str) -> Optional[Path]:
        """Generate log file path for given product."""
        if not self.log_file_pattern:
            return None

        now = datetime.datetime.now()
        return Path(
            self.log_file_pattern.format(
                year=now.year,
                month=now.month,
                day=now.day,
                hour=now.hour,
                minute=now.minute,
                second=now.second,
                product=product,
            )
        )


class BrdfConfig(BaseModel):
    """Main BRDF downloader configuration."""

    # Global settings
    date_range: Optional[DateRange] = Field(
        default=None, description="Global date range"
    )
    download: DownloadConfig = Field(default_factory=DownloadConfig)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    tiles: Union[str, Path] = Field(
        default="mainland+offshore",
        description="Tile set name (mainland, offshore, mainland+offshore) or path to tile list file",
    )
    output_path: Path = Field(description="Base output directory for this product")
    clean_up: bool = Field(
        default=True, description="Clean up intermediate files after conversion"
    )

    # Product configurations
    products: Dict[str, ProductConfig] = Field(
        default_factory=dict, description="Product-specific configurations"
    )

    @field_validator("products")
    @classmethod
    def validate_products(cls, v: Dict[str, ProductConfig]):
        for product_name in v.keys():
            if product_name not in _KNOWN_PRODUCTS:
                raise ValueError(
                    f"Unknown product '{product_name}'. Must be one of {list(_KNOWN_PRODUCTS.keys())}"
                )
        return v

    @field_validator("tiles")
    @classmethod
    def validate_tiles(cls, v: Union[str, Path]):
        if isinstance(v, str):
            # Validate tile set names
            valid_sets = {"mainland", "offshore", "mainland+offshore"}
            if v not in valid_sets:
                raise ValueError(f"Tile set must be one of {valid_sets} or a file path")
        return v

    def get_enabled_products(self) -> Dict[str, ProductConfig]:
        """Get only enabled products."""
        return {
            name: config for name, config in self.products.items() if config.enabled
        }


def generate_example_config() -> str:
    """Generate an example configuration file."""
    config = BrdfConfig(
        date_range=DateRange(
            begin=(datetime.datetime.now() - datetime.timedelta(days=-30)).date(),
            end=-7,  # 7 days ago
        ),
        output_path=Path("/g/data/v10/eoancillarydata-2"),
        download=DownloadConfig(),
        logging=LoggingConfig(
            verbose=False,
        ),
        products={
            "modis": ProductConfig(
                enabled=True,
            ),
            "viirs_m": ProductConfig(
                enabled=False,
            ),
            "viirs_i": ProductConfig(
                enabled=False,
            ),
        },
    )

    # Convert to dict and then to YAML for better formatting
    config_dict = config.model_dump(mode="json", exclude_none=True)
    yaml_content = yaml.dump(config_dict, default_flow_style=False, sort_keys=False)
    header = """# BRDF Downloader Configuration
#
# This configuration file controls the download of BRDF products from USGS.
#
# Date format: YYYY-MM-DD or relative days (negative numbers for past dates)
# Examples:
#   begin: "2024-01-01"  # Absolute date
#   begin: -30           # 30 days ago
#   end_date: -1              # Yesterday
#
# Tile sets: "mainland", "offshore", "mainland+offshore"
#
# Products: modis, viirs_m, viirs_i
#
"""

    return header + yaml_content


def resolve_tiles(tiles_config: Union[str, Path]) -> Set[str]:
    """Resolve tiles configuration to a set of tile names.

    >>> tiles = resolve_tiles("mainland")
    >>> "h27v09" in tiles and "h32v13" in tiles
    True
    >>> len(resolve_tiles("offshore"))
    2
    >>> len(resolve_tiles("mainland+offshore")) > len(resolve_tiles("mainland"))
    True
    """
    if isinstance(tiles_config, Path):
        # Load from file
        return load_tiles_from_file(tiles_config)

    # Or use tile set names (concatenated by a plus)
    tile_sets = tiles_config.split("+")
    tile_coords: set[tuple[int, int]] = set()
    for tile_set in tile_sets:
        tile_set = tile_set.strip()
        if tile_set not in TILE_SETS:
            raise ValueError(f"Unknown tile set: {tile_set}")
        tile_coords.update(TILE_SETS[tile_set])

    return {f"h{h:02d}v{v:02d}" for h, v in tile_coords}


def clean_log_values(logger, name, event_dict: dict[str, Any]):
    """Custom processor to clean up log values for better readability."""
    for key, value in event_dict.items():
        if isinstance(value, datetime.date):
            event_dict[key] = value.isoformat()
        elif isinstance(value, URL):
            event_dict[key] = str(value)
        elif isinstance(value, Path):
            event_dict[key] = str(value)
    return event_dict


def setup_logging(logging_config: LoggingConfig):
    """Setup logging configuration."""
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
        clean_log_values,
    ]

    # Setup file logging if configured

    log_path = (
        logging_config.get_log_path("brdf") if logging_config.log_file_pattern else None
    )
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_obj = log_path.open("a")
        sys.stderr.write(f"Logging to {log_path}\n")
    else:
        log_obj = sys.stderr

    if log_obj.isatty():
        # Pretty printing when run in a terminal session.
        processors = shared_processors + [structlog.dev.ConsoleRenderer()]
    else:
        # Log JSON when run otherwise
        processors = shared_processors + [
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(),
        ]

    import logging

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.NOTSET if logging_config.verbose else logging.INFO
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=log_obj),
        cache_logger_on_first_use=False,
    )


# Keep all the existing functions (BrdfClient, download_files, etc.) unchanged
# ... [Include all the previous functions here - they remain the same] ...


def get_working_dir(path: Path, create=True) -> Path:
    """
    Get a working directory we could use for processing in the given path.

    It should always be on the same drive, so that we can rename the file into place.

    >>> get_working_dir(Path('/g/data/v10/eoancillarydata-2/BRDF/MCD43A1.061/2024.03.15/A2024075'), create=False)
    PosixPath('/g/data/v10/eoancillarydata-2/BRDF/.tmp-work/MCD43A1.061/2024.03.15/A2024075')
    """
    expected_folders = [p for p in path.parents if p.name == "BRDF"]
    if not expected_folders:
        raise ValueError(
            f"Expected path to be inside a BRDF directory, but got '{path}'"
        )
    expected_prefix = expected_folders[0]
    relative_path = path.relative_to(expected_prefix)

    temp_prefix = expected_prefix / ".tmp-work"
    temp_path = temp_prefix / relative_path
    if create:
        temp_path.mkdir(parents=True, exist_ok=True)

    return temp_path


def parse_acq_date(acquisition_date: str) -> date_type:
    """
    Parse the acquisition date from the filename into a date object.

    Acquisition is year and day of year.

    >>> parse_acq_date('2024075')
    datetime.date(2024, 3, 15)
    >>> parse_acq_date('2025175')
    datetime.date(2025, 6, 24)
    """
    return dt.strptime(acquisition_date, "%Y%j").date()


def _iterate_dates(start: date_type, end: date_type) -> Iterator[date_type]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


class BrdfClient:
    CMR_GRANULE_URL = "https://cmr.earthdata.nasa.gov/search/granules"

    def __init__(
        self,
        collection_concept_id: str,
        *,
        max_retries: Optional[int] = 3,
        token: Optional[str] = None,
        min_request_period_secs: Optional[float] = 0.3,
        request_timeout_secs: Optional[float] = 180,
    ):
        self.max_retries = max_retries
        self.collection_concept_id = collection_concept_id
        self.token = token

        self.session = httpx.Client(timeout=request_timeout_secs)

        self.min_request_period_secs: float = min_request_period_secs or 0
        self.last_request_monotonic: float = (
            time.monotonic() - (self.min_request_period_secs) - 1.0
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

    def _get_cmr_headers(self) -> Dict[str, str]:
        """Returns headers for CMR API requests, including auth token if available."""
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def find_available_date_range(self) -> Tuple[datetime.date, datetime.date]:
        """Find the available date range for this collection."""
        # Query CMR for earliest and latest granules
        params = {
            "collection_concept_id": self.collection_concept_id,
            "page_size": 1,
            "sort_key": "start_date",
        }

        # Get earliest
        response = self._get_with_retries(URL(self.CMR_GRANULE_URL), params=params)
        data = response.json()
        earliest_granule = data.get("feed", {}).get("entry", [])

        # Get latest
        params["sort_key"] = "-start_date"
        response = self._get_with_retries(URL(self.CMR_GRANULE_URL), params=params)
        data = response.json()
        latest_granule = data.get("feed", {}).get("entry", [])

        if not earliest_granule or not latest_granule:
            raise RuntimeError(
                f"No granules found for collection {self.collection_concept_id}"
            )

        earliest_date = dt.fromisoformat(
            earliest_granule[0]["time_start"].replace("Z", "+00:00")
        ).date()
        latest_date = dt.fromisoformat(
            latest_granule[0]["time_start"].replace("Z", "+00:00")
        ).date()

        return earliest_date, latest_date

    def find_available_remote_files(
        self, date: date_type, tile_numbers: Optional[Set[str]] = None
    ) -> Iterator[Tuple[URL, URL]]:
        """
        Find available BRDF files for the given date using CMR API.

        Returns tuples of (data_url, xml_url) for each available file.
        """
        start_date = date
        end_date = date + timedelta(days=1)
        temporal_range = (
            f"{start_date.isoformat()}T00:00:00Z,{end_date.isoformat()}T00:00:00Z"
        )

        params = {
            "collection_concept_id": self.collection_concept_id,
            "temporal": temporal_range,
            "page_size": 2000,
        }

        all_granules = []
        search_after = None

        while True:
            headers = self._get_cmr_headers()
            if search_after:
                headers["CMR-Search-After"] = search_after

            response = self._get_with_retries(
                URL(self.CMR_GRANULE_URL),
                params=params,
                headers=headers,
                search_date=date,
            )
            data = response.json()

            granules = data.get("feed", {}).get("entry", [])
            if not granules:
                break

            all_granules.extend(granules)

            # Check if there are more results
            search_after = response.headers.get("CMR-Search-After")
            if not search_after:
                break

        for granule in all_granules:
            # Extract tile information from granule title
            title = granule.get("title", "")
            match = _BRDF_FILENAME_PATTERN.match(title)
            if not match:
                continue

            if tile_numbers and match.group("tile_number") not in tile_numbers:
                continue

            # Find download links (prefer HTTPS over S3)
            data_url = None
            xml_url = None

            for link in granule.get("links", []):
                href = link.get("href", "")
                if href.endswith(".hdf") or href.endswith(".h5"):
                    if "opendap" not in href:
                        # Prefer HTTPS URLs over S3 URLs
                        if data_url is None or (
                            href.startswith("https://")
                            and str(data_url).startswith("s3://")
                        ):
                            data_url = URL(href)
                elif href.endswith(".xml"):  # Match any .xml file (including .cmr.xml)
                    # Prefer HTTPS URLs over S3 URLs for XML too
                    if xml_url is None or (
                        href.startswith("https://") and str(xml_url).startswith("s3://")
                    ):
                        xml_url = URL(href)

            if data_url and xml_url:
                yield data_url, xml_url

    def _get_with_retries(
        self,
        url: URL,
        params: Optional[Dict] = None,
        headers: Optional[Dict] = None,
        search_date: Optional[date_type] = None,
    ) -> httpx.Response:
        retries = 0
        delay = 5

        while True:
            log_context = {"url": url, "retries": retries}
            if search_date:
                log_context["search_date"] = search_date
            LOG.info("get_with_retries", **log_context)
            response = None

            try:
                # Delay if needed, to not request more often than allowed.
                next_request_allowed_in = (
                    self.last_request_monotonic + self.min_request_period_secs
                ) - time.monotonic()
                if next_request_allowed_in > 0:
                    time.sleep(next_request_allowed_in)

                request_headers = self._get_cmr_headers()
                if headers:
                    request_headers.update(headers)

                response = self.session.get(url, params=params, headers=request_headers)
                self.last_request_monotonic = time.monotonic()

            except httpx.ReadTimeout:
                LOG.info("request_timeout", url=url)

            if response and (response.is_success or response.status_code == 404):
                break

            if retries >= self.max_retries:
                message = (
                    f"status_code: {response.status_code}, message: {response.content.decode()}"
                    if response
                    else "timeout"
                )
                raise RuntimeError(
                    f"Failed to retrieve {url} after {retries} retries. {message}"
                )

            LOG.info(f"Failed to retrieve {url}, retrying in {delay} seconds")
            retries += 1
            time.sleep(delay)
            delay *= 2

        assert response is not None, (
            "If Response is None, we should have raised an exception"
        )
        return response

    def file_exists(self, log: structlog.BoundLogger, path: Path) -> bool:
        """Check if the file exists either locally or in S3, depending on configuration."""
        from botocore.exceptions import ClientError
        if USE_S3:
            try:
                # TODO prefix?
                S3_CLIENT.head_object(Bucket=S3_BUCKET, Key=str(path))
                log.debug(f"File exists in s3 at {path}")
                return True
            except ClientError as e:
                # Object does not exist.
                if e.response['Error']['Code'] == '404':
                    log.debug(f"File does not exist in s3 at {path}")
                    return False
                # For any other error (e.g., 403 Forbidden, 500 Server Error), re-raise the exception
                else:
                    return path.exists()
        else:
            return False

    def download_file(
        self, url: URL, output_folder: Path, filename: str
    ) -> Optional[Path]:
        """
        Download the given URL to the given output folder with the specified filename.

        Returns the path of the downloaded file.
        """
        output_folder.mkdir(parents=True, exist_ok=True)
        path = output_folder / filename
        log = LOG.bind(url=url, filename=filename)

        if path.exists():
            log.info("already_downloaded", path=path)
            return path

        tmp_path = path.with_name(f".incomplete.{path.name}")

        try:
            # Use streaming download for potentially large files
            with self.session.stream(
                "GET", url, headers=self._get_cmr_headers(), follow_redirects=True
            ) as response:
                response.raise_for_status()
                with tmp_path.open("wb") as f:
                    for chunk in response.iter_bytes(chunk_size=8192):
                        f.write(chunk)

            tmp_path.rename(path)
            log.info("download_complete", path=path)
            return path

        except Exception as e:
            if tmp_path.exists():
                tmp_path.unlink()
            log.error("download_failed", error=str(e))
            raise


def find_days_with_missing_brdf_tiles(
    folder: Path,
    expected_brdf_tiles: Iterable[str],
    start_date: Optional[date_type],
    end_date: Optional[date_type],
) -> Iterator[Tuple[date_type, Set[str]]]:
    """
    For all dates in the folder, yield any dates without the full set of expected brdf tiles.

    Expects the standard structure in the given output folder: folder/YYYY.MM.DD/*.h5
    """
    date = (end_date or datetime.date.today()) + timedelta(days=1)

    while date > start_date:
        date -= timedelta(days=1)

        date_folder: Path = folder / f"{date:%Y.%m.%d}"
        children = get_children(date_folder)

        if not children:
            yield date, set(expected_brdf_tiles)

        if not _DATE_FOLDER_PATTERN.match(date_folder.name):
            continue
        
        if not USE_S3 and not date_folder.is_dir():
            continue

        # Check if we have all tiles for this date
        missing_tile_filenames = set(expected_brdf_tiles)
        for file in children:
            if file.suffix == ".h5":
                match = _BRDF_FILENAME_PATTERN.match(file.name)
                if match:
                    tile = match.group("tile_number")
                    missing_tile_filenames.discard(tile)

        if missing_tile_filenames:
            yield date, missing_tile_filenames


def _parse_day_folder(date: str) -> datetime.date:
    """Parse a day folder string to a date object.

    >>> _parse_day_folder("2024.03.15")
    datetime.date(2024, 3, 15)
    >>> _parse_day_folder("2023.12.31")
    datetime.date(2023, 12, 31)
    """
    return dt.strptime(date, "%Y.%m.%d").date()


def download_files(
    product: str,
    required_brdf_tiles: Set[str],
    output_base_path: Path,
    token: str,
    max_retries: int = 4,
    max_queue_size: int = 3,
    max_workers: int = 3,
    max_downloads: int = sys.maxsize,
    clean_up: bool = True,
    no_older_than: Optional[datetime.date] = None,
    no_newer_than: Optional[datetime.date] = None,
    request_timeout_secs: float = 180,
    min_request_period_secs: float = 0.3,
):
    """
    Download and convert MCD43A1 files from USGS

    The base path will have YYYY.MM.DD subdirectories, matching the download location.
    """
    log = LOG
    count = 0
    if product not in _KNOWN_PRODUCTS:
        raise ValueError(
            f"Unknown product {product}. Must be one of {_KNOWN_PRODUCTS.keys()}"
        )

    product_config = _KNOWN_PRODUCTS[product]
    collection_concept_id = product_config["concept_id"]
    output_offset = product_config["output_path"]
    output_base_path = output_base_path / output_offset

    with BrdfClient(
        collection_concept_id=collection_concept_id,
        max_retries=max_retries,
        token=token,
        request_timeout_secs=request_timeout_secs,
        min_request_period_secs=min_request_period_secs,
    ) as client:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            tasks = []
            active_task_count = [0]  # Using list for mutable reference

            def done_one(_: Future):
                active_task_count[0] -= 1

            def completed(f: Future):
                try:
                    result = f.result()
                    if result:
                        LOG.info("completed_path", result=str(result))
                except Exception:
                    LOG.exception("path_error")

            # start_date, end_date = client.find_available_date_range()
            # log.info("available_date_range", start_date=start_date, end_date=end_date)
            # Clamp the start/end dates to the available range.
            if no_older_than:
                start_date = no_older_than
            if no_newer_than:
                end_date = no_newer_than

            # Search across creation date range instead of limiting to specific acquisition dates
            # This allows us to find files that were created days after acquisition
            current_date = start_date
            while current_date <= end_date:
                day_log = LOG.bind(creation_date=current_date)
                day_log.info("searching_by_creation_date", creation_date=current_date)

                for data_url, xml_url in client.find_available_remote_files(
                    current_date, tile_numbers=required_brdf_tiles
                ):
                    # Extract filename from URL
                    data_filename = data_url.path.split("/")[-1]

                    day_log = day_log.bind(data_filename=data_filename)

                    # Verify the file is actually for the date we're looking for
                    match = _BRDF_FILENAME_PATTERN.match(data_filename)
                    if not match:
                        day_log.info(
                            "filename_pattern_mismatch", filename=data_filename
                        )
                        continue

                    # Parse acquisition date from filename for proper directory placement
                    acquisition_date = parse_acq_date(match.group("acquisition_date"))

                    # Use acquisition date for target directory, not search date
                    actual_target_dir = (
                        output_base_path / f"{acquisition_date:%Y.%m.%d}"
                    )
                    actual_staging_dir = get_working_dir(actual_target_dir)

                    expected_output_h5 = actual_target_dir / data_filename.replace(
                        ".hdf", ".h5"
                    )
                    if client.file_exists(day_log, expected_output_h5):
                        day_log.debug("skip_existing", output_h5=expected_output_h5)
                        continue

                    last_queue_log = 0
                    while active_task_count[0] >= max_queue_size:
                        # Don't log more often than every 10 seconds if stuck here.
                        if time.monotonic() - last_queue_log > 10:
                            day_log.debug(
                                "full_queue",
                                active_task_count=active_task_count[0],
                                max_queue_size=max_queue_size,
                                task_count=len(tasks),
                            )
                        last_queue_log = time.monotonic()
                        wait_for_future(
                            tasks, return_when="FIRST_COMPLETED", timeout=10
                        )

                    task: Future = executor.submit(
                        download_and_convert,
                        client,
                        (data_url, xml_url),
                        actual_staging_dir,
                        actual_target_dir,
                        clean_up,
                    )
                    active_task_count[0] += 1
                    task.add_done_callback(done_one)
                    tasks.append(task)
                    count += 1
                    if max_downloads and count >= max_downloads:
                        day_log.info(
                            "hit_max_downloads_inner", max_downloads=max_downloads
                        )
                        break

                # Trim tasks if needed
                trimmed_tasks = []
                for task in tasks:
                    if task.done():
                        completed(task)
                    else:
                        trimmed_tasks.append(task)
                tasks = trimmed_tasks

                # Move to next creation date
                current_date += timedelta(days=1)

                if max_downloads and count >= max_downloads:
                    day_log.info("hit_max_downloads", max_downloads=max_downloads)
                    break

            log.info("awaiting_final_tasks")
            for future in as_completed(tasks):
                completed(future)

    log.info("done")


def url_filename(url: URL) -> str:
    """
    Get the filename component of the URL

    >>> u = URL('https://e4ftl01.cr.usgs.gov/MOTA/MCD43A1.061/2002.04.12/MCD43A1.A2002102.h23v01.061.2020087195553.hdf')
    >>> url_filename(u)
    'MCD43A1.A2002102.h23v01.061.2020087195553.hdf'
    """
    return url.path.rstrip("/").split("/")[-1]


def download_and_convert(
    client: BrdfClient,
    url_set: Tuple[URL, URL],
    staging_folder: Path,
    output_folder: Path,
    clean_up: bool = True,
) -> Optional[Path]:
    # (Yes, Client is thread safe, and more efficient than a client-per-thread:
    #  https://github.com/encode/httpx/discussions/1633)

    data_url, xml_url = url_set

    # Extract filenames from URLs
    data_filename = data_url.path.split("/")[-1]
    xml_filename_from_url = xml_url.path.split("/")[-1]

    # For .hdf files, the XML should be named {data_filename}.xml for conversion
    if data_filename.endswith(".hdf"):
        xml_filename = f"{data_filename}.xml"
    else:
        xml_filename = xml_filename_from_url

    log = LOG.bind(data_filename=data_filename)

    file_path = client.download_file(data_url, staging_folder, data_filename)
    file_xml_path = client.download_file(xml_url, staging_folder, xml_filename)

    if file_path is None or file_xml_path is None:
        log.error("download_failed", data_path=file_path, xml_path=file_xml_path)
        return

    # VIIRS comes as h5, so no conversion needed!
    # [   ] VNP43MA1.A2024276.h32v10.001.2024284200753.h5
    # [   ] VNP43MA1.A2024276.h32v10.001.2024284200753.h5.xml
    if file_path.suffix == ".hdf":
        do_convert = True
    elif file_path.suffix == ".h5":
        do_convert = False
    else:
        raise ValueError(f"Unknown file type: {file_path.suffix}")

    if do_convert:
        log.info("converting", hdf_path=file_path, hdf_xml_path=file_xml_path)
        final_path = convert_to_h5(file_path, output_folder, log=log)
        log.info("converted", output_h5_path=final_path)
        if clean_up:
            file_path.unlink()
            file_xml_path.unlink()
    else:
        # Move the files without converting
        output_folder.mkdir(parents=True, exist_ok=True)
        output_hdf_path = output_folder / file_path.name
        output_xml_path = output_folder / file_xml_path.name
        finalize_download(log, file_path, output_hdf_path)
        finalize_download(log, file_xml_path, output_xml_path)
        final_path = output_hdf_path

    return final_path


def convert_to_h5(input_hdf_file: Path, out_dir: Path, log=LOG) -> Path:
    expected_hdf_xml_path = input_hdf_file.with_name(f"{input_hdf_file.name}.xml")
    if not expected_hdf_xml_path.exists():
        raise ValueError(
            "Cannot do conversion to h5 until both are downloaded: .hdf and .hdf.xml"
        )

    tmp_output = get_working_dir(out_dir)

    try:
        cmd = (
            "swfo-convert",
            "mcd43a1",
            "h5-md",
            "--fname",
            input_hdf_file.as_posix(),
            "--outdir",
            tmp_output.as_posix(),
            "--filter-opts",
            '{"aggression": 6}',
            "--compression",
            "BLOSC_ZSTANDARD",
        )
        log.debug("swfo-cmd", cmd=" ".join(shlex.quote(str(arg)) for arg in cmd))
        subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as e:
        LOG.exception("error.swfo-convert", output=e.output.decode())

    expected_output_file = tmp_output / input_hdf_file.with_suffix(".h5").name
    if not expected_output_file.exists():
        raise FileNotFoundError(
            f"Failed to convert {input_hdf_file} to {expected_output_file}. Cannot see output?"
        )

    # Make group readable (g+r)
    # (TODO: this should be avoidable using umask?)
    expected_output_file.chmod(expected_output_file.stat().st_mode | 0o40)

    out_dir.mkdir(parents=True, exist_ok=True)
    final_output_file = out_dir / expected_output_file.name
    finalize_download(log, expected_output_file, final_output_file)

    return final_output_file


def load_tiles_from_file(tiles_path: Path) -> Set[str]:
    """Load and validate tile identifiers from file."""
    tiles = set()
    # TODO I don't know how this is supposed to work
    with open(tiles_path) as f:
        for line_num, line in enumerate(f, 1):
            # Remove comments and whitespace
            tile = line.strip().split("#")[0].strip()
            if not tile:
                continue
            if not re.match(r"^h\d{2}v\d{2}", tile):
                raise ValueError(f"Invalid tile format '{tile}' on line {line_num}")
            tiles.add(tile)
    return tiles


def check_swfo_convert_available() -> bool:
    """Check if swfo-convert command is available on the PATH."""
    return shutil.which("swfo-convert") is not None


def run_with_config(config: BrdfConfig):
    """Run the downloader with the given configuration."""
    setup_logging(config.logging)

    log = structlog.get_logger()
    token = config.auth.get_token()

    enabled_products = config.get_enabled_products()
    if not enabled_products:
        log.warning("No enabled products found in configuration")
        return

    # Check if any enabled products will need swfo-convert (modis products need conversion)
    needs_conversion = any(product in enabled_products for product in ["modis"])
    if needs_conversion and not check_swfo_convert_available():
        log.error(
            "swfo-convert command not found on PATH but is required for MODIS products"
        )
        raise RuntimeError(
            "swfo-convert command not found on PATH. "
            "This is required for converting MODIS HDF files to H5 format. "
            "Please install swfo-convert or disable MODIS products in your configuration."
        )

    log.info(
        "starting_brdf_download",
        enabled_products=list(enabled_products.keys()),
        global_date_range=config.date_range,
    )

    for product_name, product_config in enabled_products.items():
        log.info("processing_product", product=product_name)

        # Resolve tiles
        try:
            tiles = resolve_tiles(config.tiles)
            log.debug("resolved_tiles", product=product_name, tiles=sorted(tiles))
        except Exception as e:
            log.error("failed_to_resolve_tiles", product=product_name, error=str(e))
            continue

        # Resolve date range
        global_start, global_end = None, None
        if config.date_range is not None:
            global_start, global_end = config.date_range.resolve_dates()

        product_start, product_end = global_start, global_end
        if product_config.date_range is not None:
            product_start, product_end = product_config.date_range.resolve_dates()

        log.info(
            "date_range_resolved",
            product=product_name,
            start_date=product_start,
            end_date=product_end,
        )

        try:
            download_files(
                product=product_name,
                required_brdf_tiles=tiles,
                output_base_path=config.output_path,
                token=token,
                max_retries=config.download.max_retries,
                max_queue_size=config.download.max_queue_size,
                max_workers=config.download.max_workers,
                max_downloads=product_config.max_downloads,
                clean_up=config.clean_up,
                no_older_than=product_start,
                no_newer_than=product_end,
                request_timeout_secs=config.download.request_timeout_secs,
                min_request_period_secs=config.download.min_request_period_secs,
            )
        except Exception:
            log.exception("product_download_failed", product=product_name)
            continue

    log.info("brdf_download_complete")


@click.group()
def cli():
    """BRDF Downloader - Download and convert BRDF files from USGS."""
    pass


@cli.command()
@click.option(
    "--config-path",
    "-c",
    type=click.Path(exists=True, path_type=Path),
    envvar="BRDF_CONFIG_PATH",
    help="Path to configuration file (can also set BRDF_CONFIG_PATH environment variable)",
)
def run(config_path: Optional[Path]):
    """Run the BRDF downloader with the specified configuration."""
    if not config_path:
        # Try default locations
        default_paths = [
            Path.cwd() / "brdf-config.yaml",
            Path.cwd() / "brdf-config.yml",
            Path.home() / ".config" / "brdf-downloader" / "config.yaml",
            Path("/etc/brdf-downloader/config.yaml"),
        ]

        for path in default_paths:
            if path.exists():
                config_path = path
                break

        if not config_path:
            click.echo(
                "No configuration file found. Use --config-path or set BRDF_CONFIG_PATH",
                err=True,
            )
            click.echo("Default locations searched:", err=True)
            for path in default_paths:
                click.echo(f"  {path}", err=True)
            click.echo(
                "\nGenerate an example config with: brdf-downloader generate-config",
                err=True,
            )
            raise click.Abort()

    try:
        with open(config_path) as f:
            config_data = yaml.safe_load(f)

        config = BrdfConfig(**config_data)
        run_with_config(config)

    except Exception as e:
        click.echo(f"Error loading configuration: {e}", err=True)
        raise click.Abort()


@cli.command()
@click.option(
    "--output",
    "-o",
    type=click.Path(path_type=Path),
    help="Output file path (default: print to stdout)",
)
def generate_config(output: Optional[Path]):
    """Generate an example configuration file."""
    config_content = generate_example_config()

    if output:
        with open(output, "w") as f:
            f.write(config_content)
    else:
        click.echo(config_content)


if __name__ == "__main__":
    cli()
