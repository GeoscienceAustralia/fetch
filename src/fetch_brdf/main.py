import datetime
import os
import re
import shlex
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
from datetime import timedelta
from pathlib import Path
from typing import Iterable, Iterator, Optional, Set, Tuple, Union, Dict

import click
import httpx
import structlog
import yaml
from pydantic import BaseModel, Field, validator
from httpx import URL
from lxml import etree

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
    r"(MCD43A1|VNP43IA1|VNP43MA1)\.A(?P<acquisition_date>[0-9]{7})\.(?P<tile_number>h[0-9]{2}v[0-9]{2})\.[0-9]{3}\.(?P<timestamp>[0-9]{13})(?P<extension>[.a-z5]+)$"
)
# Folder pattern YYYY.MM.DD
_DATE_FOLDER_PATTERN = re.compile(r"[0-9]{4}\.[0-9]{2}\.[0-9]{2}")

_KNOWN_PRODUCTS = {
    "modis": ("MOTA/MCD43A1.061", "BRDF/MCD43A1.061"),
    "viirs_m": ("VIIRS/VNP43MA1.001", "BRDF/VNP43MA1.001"),
    "viirs_i": ("VIIRS/VNP43IA1.001", "BRDF/VNP43IA1.001"),
}
# Tiles are (h, v) coordinates, as seen in the filename
TILE_SETS = {
    # The whole range of australia
    "mainland": tuple((h, v) for h in range(27, 33) for v in range(9, 14)),
    # The two offshore tiles
    "offshore": ((22, 14), (27, 14)),
}
# Two corners. No off-by-one errors
assert (27, 9) in TILE_SETS["mainland"]
assert (32, 13) in TILE_SETS["mainland"]
assert (32, 14) not in TILE_SETS["mainland"]


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
        """Resolve relative dates to absolute dates."""
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

    username: Optional[str] = Field(
        default=None, description="usgs username (if not $EARTHDATA_USERNAME)"
    )
    password: Optional[str] = Field(
        default=None, description="usgs password (if not $EARTHDATA_PASSWORD)"
    )

    def get_credentials(self) -> Tuple[str, str]:
        """Get credentials from config or environment variables."""
        username = self.username or os.environ.get("EARTHDATA_USERNAME")
        password = self.password or os.environ.get("EARTHDATA_PASSWORD")

        if not username or not password:
            raise ValueError(
                "No username/password supplied. Set in config file or use "
                "EARTHDATA_USERNAME/EARTHDATA_PASSWORD environment variables"
            )

        return username, password


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
    date_range: Optional[DateRange] = Field(None, description="Global date range")
    download: DownloadConfig = Field(default_factory=DownloadConfig)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    tiles: Union[str, Path] = Field(
        default="mainland+offshore",
        description="Tile set name (mainland, offshore, mainland+offshore) or path to tile list file",
    )
    output_path: Path = Field(
        default=None, description="Base output directory for this product"
    )
    clean_up: bool = Field(
        default=True, description="Clean up intermediate files after conversion"
    )

    # Product configurations
    products: Dict[str, ProductConfig] = Field(
        default_factory=dict, description="Product-specific configurations"
    )

    @validator("products")
    def validate_products(cls, v):
        for product_name in v.keys():
            if product_name not in _KNOWN_PRODUCTS:
                raise ValueError(
                    f"Unknown product '{product_name}'. Must be one of {list(_KNOWN_PRODUCTS.keys())}"
                )
        return v

    @validator("tiles")
    def validate_tiles(cls, v):
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
    config_dict = config.dict()
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
    """Resolve tiles configuration to a set of tile names."""
    if isinstance(tiles_config, Path):
        # Load from file
        return load_tiles_from_file(tiles_config)

    # Or use tile set names (concatenated by a plus)
    tile_sets = tiles_config.split("+")
    tile_coords = set()
    for tile_set in tile_sets:
        tile_set = tile_set.strip()
        if tile_set not in TILE_SETS:
            raise ValueError(f"Unknown tile set: {tile_set}")
        tile_coords.update(TILE_SETS[tile_set])

    return {f"h{h:02d}v{v:02d}" for h, v in tile_coords}


def setup_logging(logging_config: LoggingConfig, product: str = None):
    """Setup logging configuration."""
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
    ]

    if sys.stderr.isatty():
        # Pretty printing when run in a terminal session.
        processors = shared_processors + [structlog.dev.ConsoleRenderer()]
    else:
        # Log JSON when run otherwise
        processors = shared_processors + [
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(),
        ]

    import logging

    # Setup file logging if configured
    if logging_config.log_file_pattern:
        log_path = logging_config.get_log_path("brdf")
        if log_path:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.FileHandler(log_path)
            file_handler.setLevel(
                logging.DEBUG if logging_config.verbose else logging.INFO
            )
            logging.getLogger().addHandler(file_handler)

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.NOTSET if logging_config.verbose else logging.INFO
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
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


def parse_acq_date(acquisition_date: str) -> dt.date:
    """
    Parse the acquisition date from the filename into a date object.

    Acquisition is year and day of year.

    >>> parse_acq_date('2024075')
    datetime.date(2024, 3, 15)
    """
    return dt.strptime(acquisition_date, "%Y%j").date()


def _iterate_dates(start: dt.date, end: dt.date) -> Iterator[dt.date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


class BrdfClient:
    TRUSTED_HOSTS = {
        "e4ftl01.cr.usgs.gov",
        "urs.earthdata.nasa.gov",
    }

    def __init__(
        self,
        product_offset: str,
        *,
        max_retries: Optional[int] = 3,
        username: Optional[str] = None,
        password: Optional[str] = None,
        min_request_period_secs: Optional[float] = 0.3,
        request_timeout_secs: Optional[float] = 180,
        host_url: Optional[str] = "https://e4ftl01.cr.usgs.gov",
    ):
        self.max_retries = max_retries

        self.service_root_url: URL = URL(f"{host_url}/{product_offset}/")

        self.username = username
        self.password = password

        self.session = httpx.Client(timeout=request_timeout_secs)

        self.min_request_period_secs = min_request_period_secs
        self.last_request_monotonic = time.monotonic() - min_request_period_secs - 1.0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

    def _yield_directory_page_links(self, url: URL) -> Iterable[URL]:
        """
        Assuming the given URL is a directory page, yield all the links it
        can find.

        (ie. folders and filenames linked to)
        """
        response = self._get_with_retries(url)

        # This can be common as we're scanning through days of the year. Some may not exist yet.
        if response.status_code == 404:
            LOG.info("directory_not_found", url=url)
            return

        page = etree.fromstring(response.text, parser=etree.HTMLParser())
        for anchor in page.xpath("//a"):
            name = anchor.text

            if "href" not in anchor.attrib:
                continue

            href = anchor.attrib["href"]
            source_url = response.url.join(href)

            # Many links on page are not files. They're empty (images) or have other names like "back".

            # Empty anchor
            if not name:
                continue

            # Not a filename
            if not href.endswith(name):
                continue

            yield source_url

    def find_available_date_range(self) -> Tuple[datetime.date, datetime.date]:
        # Root page has subfolders for each day: YYYY.MM.DD
        available_days = sorted(
            url_filename(url)
            for url in self._yield_directory_page_links(self.service_root_url)
            if _DATE_FOLDER_PATTERN.match(url_filename(url))
        )
        return _parse_day_folder(available_days[0]), _parse_day_folder(
            available_days[-1]
        )

    def find_available_remote_files(
        self, date: dt.date, tile_numbers: Optional[Set[str]] = None
    ) -> Iterator[Tuple[URL, URL]]:
        """
        This will return a list of available HDF file sets for the given date.

        All files come in pairs: there is a `.hdf` and an identically named `.hdf.xml` extension.

        It will yield each available pair of URLs for the day.
        """
        day_url = self.service_root_url.join(f"{date:%Y.%m.%d}/")

        file_urls = set(self._yield_directory_page_links(day_url))
        for file_url in file_urls:
            match = _BRDF_FILENAME_PATTERN.match(url_filename(file_url))
            if not match:
                continue

            if tile_numbers and match.group("tile_number") not in tile_numbers:
                continue

            # Make sure it's the data extension (we don't need to yield the .hdf.xml separately)
            if match.group("extension") not in (".hdf", ".h5"):
                continue

            # Make sure we have a matching `.hdf.xml` file.
            expected_xml_url = URL(str(file_url) + ".xml")

            if expected_xml_url not in file_urls:
                # Perhaps it's still being generated. This should rarely happen, if ever?
                LOG.info("remote_brdf_without_xml", brdf_url=file_url)
                continue

            # A pair of .hdf and .hdf.xml files
            yield file_url, expected_xml_url

    def _get_with_retries(self, url: URL, include_auth=False) -> httpx.Response:
        retries = 0
        delay = 5
        while True:
            LOG.info("get_with_retries", url=url, retries=retries)
            response = None

            try:
                request = self.session.build_request("GET", url)
                while request is not None:
                    # Delay if needed, to not request more often than allowed.
                    next_request_allowed_in = (
                        self.last_request_monotonic + self.min_request_period_secs
                    ) - time.monotonic()
                    if next_request_allowed_in > 0:
                        time.sleep(next_request_allowed_in)

                    args = {}
                    if include_auth:
                        if not self.username or not self.password:
                            raise ValueError(
                                "No username/password supplied, but operation requires auth"
                            )
                        args["auth"] = (self.username, self.password)

                    response = self.session.send(
                        request, **args, follow_redirects=False
                    )

                    headers_used = dict(request.headers.items())
                    request = response.next_request

                    self.last_request_monotonic = time.monotonic()

                    if (
                        include_auth
                        and request
                        and request.url.host in self.TRUSTED_HOSTS
                    ):
                        if not headers_used["authorization"]:
                            raise RuntimeError(
                                f"Expected to have an authorization header for {request.url}"
                            )
                        request.headers["authorization"] = headers_used["authorization"]

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
        return response

    def download_file(self, url: URL, output_base_folder: Path) -> Optional[Path]:
        """
        Download the given URL to the given output folder.

        It will use the same subdirectory and names structure as USGS's own URLs:

        eg. /MOTA/MCD43A1.061/2002.04.12/MCD43A1.A2002102.h23v01.061.2020087195553.hdf

        It will return the path inside your folder of the downloaded file.
        """
        path = (output_base_folder / url.path[1:]).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        log = LOG.bind(url=url)

        if path.exists():
            log.info("already_downloaded", path=path)
            return path
        tmp_path = path.with_name(f".incomplete.{path.name}")

        # We aren't doing chunked as these are small.
        response = self._get_with_retries(url, include_auth=True)
        response.raise_for_status()
        with tmp_path.open("wb") as f:
            f.write(response.content)

        tmp_path.rename(path)
        return path


def find_days_with_missing_brdf_tiles(
    folder: Path,
    expected_brdf_tiles: Iterable[str],
    start_date: Optional[dt.date],
    end_date: Optional[dt.date],
) -> Iterator[Tuple[dt.date, Set[str]]]:
    """
    For all dates in the folder, yield any dates without the full set of expected brdf tiles.

    Expects the standard structure in the given output folder: folder/YYYY.MM.DD/*.h5
    """
    date = end_date + timedelta(days=1)

    while date > start_date:
        date -= timedelta(days=1)

        date_folder = folder / f"{date:%Y.%m.%d}"
        if not date_folder.exists():
            yield date, set(expected_brdf_tiles)
            continue

        if not _DATE_FOLDER_PATTERN.match(date_folder.name):
            continue

        if not date_folder.is_dir():
            continue

        # Check if we have all tiles for this date
        missing_tile_filenames = set(expected_brdf_tiles)
        for file in date_folder.iterdir():
            if file.suffix == ".h5":
                match = _BRDF_FILENAME_PATTERN.match(file.name)
                if match:
                    tile = match.group("tile_number")
                    missing_tile_filenames.discard(tile)

        if missing_tile_filenames:
            yield date, missing_tile_filenames


def _parse_day_folder(date: str) -> datetime.date:
    return dt.strptime(date, "%Y.%m.%d").date()


def download_files(
    product: str,
    required_brdf_tiles: Set[str],
    output_base_path: Path,
    username: str,
    password: str,
    max_retries: int = 4,
    max_queue_size: int = 3,
    max_workers: int = 3,
    max_downloads: int = sys.maxsize,
    clean_up: bool = True,
    no_older_than: datetime.date = None,
    no_newer_than: datetime.date = None,
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

    product_offset, output_offset = _KNOWN_PRODUCTS[product]
    output_base_path = output_base_path / output_offset

    with BrdfClient(
        product_offset=product_offset,
        max_retries=max_retries,
        username=username,
        password=password,
        request_timeout_secs=request_timeout_secs,
        min_request_period_secs=min_request_period_secs,
    ) as client:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            tasks = []

            active_task_count = 0

            def done_one(_: Future):
                nonlocal active_task_count
                active_task_count -= 1

            def completed(f: Future):
                try:
                    result = f.result()
                    if result:
                        LOG.info("completed_path", result=str(result))
                except Exception:
                    LOG.exception("path_error")

            start_date, end_date = client.find_available_date_range()
            # Clamp the start/end dates to the available range.
            if no_older_than:
                start_date = max(start_date, no_older_than)
            if no_newer_than:
                end_date = min(end_date, no_newer_than)

            for date, missing_tiles in find_days_with_missing_brdf_tiles(
                output_base_path, required_brdf_tiles, start_date, end_date
            ):
                log = LOG.bind(date=date)
                target_dir = output_base_path / f"{date:%Y.%m.%d}"
                staging_dir = get_working_dir(target_dir)

                log.info("running_day", possible_missing_tiles=missing_tiles)

                for hdf_url, hdf_xml_url in client.find_available_remote_files(
                    date, tile_numbers=required_brdf_tiles
                ):
                    hdf_name = url_filename(hdf_url)

                    log = log.bind(hdf_name=hdf_name)

                    expected_output_h5 = target_dir / hdf_name.replace(".hdf", ".h5")
                    if expected_output_h5.exists():
                        log.debug("skip_existing", output_h5=expected_output_h5)
                        continue

                    last_queue_log = 0
                    while active_task_count >= max_queue_size:
                        # Don't log more often than every 10 seconds if stuck here.
                        if time.monotonic() - last_queue_log > 10:
                            log.debug(
                                "full_queue",
                                active_task_count=active_task_count,
                                max_queue_size=max_queue_size,
                                task_count=len(tasks),
                            )
                        last_queue_log = time.monotonic()
                        wait_for_future(
                            tasks, return_when="FIRST_COMPLETED", timeout=10
                        )

                    # Do the first synchronously, no concurrency, to save the auth cookies first.
                    # A slow ramp-up is fine.
                    if count < 1:
                        download_and_convert(
                            client,
                            (hdf_url, hdf_xml_url),
                            staging_dir,
                            target_dir,
                            clean_up,
                        )
                    else:
                        task: Future = executor.submit(
                            download_and_convert,
                            client,
                            (hdf_url, hdf_xml_url),
                            staging_dir,
                            target_dir,
                            clean_up,
                        )
                        active_task_count += 1
                        task.add_done_callback(done_one)
                        tasks.append(task)
                    count += 1
                    if count >= max_downloads:
                        break

                # Trim tasks if needed
                trimmed_tasks = []
                for task in tasks:
                    if task.done():
                        completed(task)
                    else:
                        trimmed_tasks.append(task)
                tasks = trimmed_tasks

                if count >= max_downloads:
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

    file_url, file_xml_url = url_set
    log = LOG.bind(hdf_name=url_filename(file_url))

    file_path = client.download_file(file_url, staging_folder)
    file_xml_path = client.download_file(file_xml_url, staging_folder)

    if file_path is None or file_xml_path is None:
        log.error("download_failed", hdf_path=file_path, hdf_xml_path=file_xml_path)
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
        file_path.rename(output_hdf_path)
        file_xml_path.rename(output_xml_path)
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
        LOG.exception(f"error: {e.output.decode()}")

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
    expected_output_file.rename(final_output_file)

    return final_output_file


def load_tiles_from_file(tiles_path: Path) -> Set[str]:
    """Load and validate tile identifiers from file."""
    tiles = set()
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


def run_with_config(config: BrdfConfig):
    """Run the downloader with the given configuration."""
    setup_logging(config.logging)

    log = structlog.get_logger()
    username, password = config.auth.get_credentials()

    enabled_products = config.get_enabled_products()
    if not enabled_products:
        log.warning("No enabled products found in configuration")
        return

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
        if config.date_range:
            global_start, global_end = config.date_range.resolve_dates()

        product_start, product_end = global_start, global_end
        if product_config.date_range:
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
                username=username,
                password=password,
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
        except Exception as e:
            log.exception("product_download_failed", product=product_name, error=str(e))
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
        click.echo(f"Example configuration written to {output}")
    else:
        click.echo(config_content)


if __name__ == "__main__":
    cli()
