import datetime
import os

import re
import shlex
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
from typing import Iterable, Iterator, Optional, Set, Tuple

import httpx
import structlog
from httpx import URL
from lxml import etree

# ipv6 was not working on the gadi-dm NCI node.
import urllib3.util.connection
urllib3.util.connection.HAS_IPV6 = False
import socket
socket.has_ipv6 = False

COOKIE_JAR = os.path.expanduser("~/.urs_cookies")

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
    r"MCD43A1\.A(?P<acquisition_date>[0-9]{7})\.(?P<tile_number>h[0-9]{2}v[0-9]{2})\.061\.(?P<timestamp>[0-9]{13})(?P<extension>[.a-z]+)$"
)
# Folder pattern YYYY.MM.DD
_DATE_FOLDER_PATTERN = re.compile(r"[0-9]{4}\.[0-9]{2}\.[0-9]{2}")


def parse_acq_date(acquisition_date: str) -> dt.date:
    """
    Parse the acquisition date from the filename into a date object.

    Acquisition is year and day of year.
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
    def __init__(self,
                 *,
                 max_retries: Optional[int] = 3,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 min_request_period_secs: Optional[float] = 0.3,
                 ):

        self.max_retries = max_retries
        self.service_root_url: URL = URL("https://e4ftl01.cr.usgs.gov/MOTA/MCD43A1.061/")

        self.username = username
        self.password = password

        self.session = httpx.Client()

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

            # Make sure it's the .hdf extension (we don't need to yield the .hdf.xml separately)
            if match.group("extension") != ".hdf":
                continue

            # Make sure we have a matching `.hdf.xml` file.
            expected_xml_url = URL(str(file_url) + '.xml')

            if expected_xml_url not in file_urls:
                # Perhaps it's still being generated. This should rarely happen, if ever?
                LOG.info('remote_brdf_without_xml', brdf_url=file_url)
                continue

            # A pair of .hdf and .hdf.xml files
            yield file_url, expected_xml_url

    def _get_with_retries(self, url: URL, include_auth=False) -> httpx.Response:
        retries = 0
        delay = 2
        while True:
            LOG.info("get_with_retries", url=url, retries=retries)
            response = None
            request = self.session.build_request("GET", url)
            while request is not None:

                # Delay if needed, to not request more often than allowed.
                next_request_allowed_in = (self.last_request_monotonic + self.min_request_period_secs) - time.monotonic()
                if next_request_allowed_in > 0:
                    time.sleep(next_request_allowed_in)

                args = {}
                if include_auth:
                    if not self.username or not self.password:
                        raise ValueError("No username/password supplied, but operation requires auth")
                    args['auth'] = (self.username, self.password)

                response = self.session.send(request, **args, follow_redirects=False)

                headers_used = dict(request.headers.items())
                request = response.next_request

                self.last_request_monotonic = time.monotonic()

                if include_auth and request and request.url.host in self.TRUSTED_HOSTS:
                    if not headers_used['authorization']:
                        raise RuntimeError(f"Expected to have an authorization header for {request.url}")
                    request.headers['authorization'] = headers_used['authorization']

            if response.is_success:
                break

            if retries >= self.max_retries:
                raise RuntimeError(
                    f"Failed to retrieve {url} after {retries} retries, message: {response.content.decode()}"
                )

            LOG.info(
                f"Failed to retrieve {url}, status code: {response.status_code}, retrying in {delay} seconds"
            )
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
        tmp_path = path.with_name(
            f".incomplete.{path.name}"
        )

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


_unset = object()


def download_files(required_brdf_tiles: Set[str],
                   output_base_path: Path,
                   username: str = os.environ.get("EARTHDATA_USERNAME", _unset),
                   password: str = os.environ.get("EARTHDATA_PASSWORD", _unset),
                   max_retries: int = 0,
                   max_queue_size: int = 3,
                   max_workers: int = 3,
                   max_downloads: int = sys.maxsize,
                   clean_up: bool = False,
                   no_older_than:datetime.date = None,
                   no_newer_than:datetime.date = None,
                   ):
    """
    Download and convert MCD43A1 files from USGS

    The base path will have YYYY.MM.DD subdirectories, matching the download location.
    :param no_older_than:
    :param no_newer_than:
    """
    if username is _unset or password is _unset:
        raise ValueError("No username/password supplied "
                         "(nor EARTHDATA_USERNAME/EARTHDATA_PASSWORD environment variables)")

    log = LOG
    download_tmp = output_base_path / ".tmp"
    download_tmp.mkdir(parents=True, exist_ok=True)
    count = 0

    with BrdfClient(max_retries=max_retries, username=username, password=password) as client:
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
                        LOG.info("completed_path", result=result)
                except:
                    LOG.exception("path_error")

            # Clamp the date range if needed.
            start_date, end_date = client.find_available_date_range()
            if no_older_than and start_date < no_older_than:
                start_date = no_older_than
            if no_newer_than and end_date > no_newer_than:
                end_date = no_newer_than

            for date, missing_tiles in find_days_with_missing_brdf_tiles(
                    output_base_path, required_brdf_tiles, start_date, end_date
            ):
                log = LOG.bind(date=date)
                target_dir = output_base_path / f"{date:%Y.%m.%d}"
                staging_dir = output_base_path / ".tmp.staging" / f"{date:%Y.%m.%d}"
                staging_dir.mkdir(parents=True, exist_ok=True)

                log.info("running_day", possible_missing_tiles=missing_tiles)

                for hdf_url, hdf_xml_url in client.find_available_remote_files(
                        date, tile_numbers=required_brdf_tiles
                ):
                    hdf_name = url_filename(hdf_url)

                    log = log.bind(hdf_name=hdf_name)

                    expected_output_h5 = target_dir / hdf_name.replace('.hdf', '.h5')
                    if expected_output_h5.exists():
                        log.debug('skip_existing', output_h5=expected_output_h5)
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
                        download_and_convert(client,
                                             (hdf_url, hdf_xml_url),
                                             staging_dir,
                                             target_dir,
                                             clean_up)
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
    return url.path.rstrip('/').split("/")[-1]


def download_and_convert(
        client: BrdfClient,
        url_set: Tuple[URL, URL],
        staging_folder: Path,
        output_folder: Path,
        clean_up: bool = False,
) -> Optional[Path]:
    # (Yes, Client is thread safe, and more efficient than a client-per-thread:
    #  https://github.com/encode/httpx/discussions/1633)

    hdf_url, hdf_xml_url = url_set
    log = LOG.bind(hdf_name=url_filename(hdf_url))

    hdf_path = client.download_file(hdf_url, staging_folder)
    hdf_xml_path = client.download_file(hdf_xml_url, staging_folder)

    if hdf_path is None or hdf_xml_path is None:
        log.error("download_failed", hdf_path=hdf_path, hdf_xml_path=hdf_xml_path)
        return

    log.info('converting', hdf_path=hdf_path, hdf_xml_path=hdf_xml_path)
    output_h5_path = convert_to_h5(hdf_path, output_folder, log=log)
    log.info('converted', output_h5_path=output_h5_path)

    if clean_up:
        hdf_path.unlink()
        hdf_xml_path.unlink()

    return output_h5_path


def convert_to_h5(input_hdf_file: Path, out_dir: Path, log=LOG) -> Path:
    expected_hdf_xml_path = input_hdf_file.with_name(f"{input_hdf_file.name}.xml")
    if not expected_hdf_xml_path.exists():
        raise ValueError(
            "Cannot do conversion to h5 until both are downloaded: .hdf and .hdf.xml"
        )

    tmp_output = out_dir / f".tmp.convert.{input_hdf_file.stem}"
    tmp_output.mkdir(parents=True, exist_ok=True)

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
        log.debug(
            'swfo-cmd',
            cmd=' '.join(shlex.quote(str(arg)) for arg in cmd)
        )
        subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as e:
        LOG.exception(f'error: {e.output.decode()}')

    expected_output_file = tmp_output / input_hdf_file.with_suffix(".h5").name
    if not expected_output_file.exists():
        raise FileNotFoundError(
            f"Failed to convert {input_hdf_file} to {expected_output_file}. Cannot see output?"
        )

    # Make group readable (g+r)
    # (TODO: this should be avoidable using umask?)
    expected_output_file.chmod(expected_output_file.stat().st_mode | 0o40)

    final_output_file = out_dir / expected_output_file.name
    expected_output_file.rename(final_output_file)

    LOG.debug('cleaning_up', tmp_output=tmp_output)
    tmp_output.rmdir()

    return final_output_file


def main(offshore_tiles: bool = True, mainland_tiles: bool = False, output_folder=Path("test_out"),
        min_age_days=30):
    # The two offshore tiles, then the whole range of Australian tiles.
    brdf_tiles = set()

    if offshore_tiles:
        brdf_tiles.update({'h22v14', 'h27v14'})
    if mainland_tiles:
        for h in range(27, 33):
            for v in range(9, 14):
                brdf_tiles.add(f"h{h:02d}v{v:02d}")
        # Two corners. No off-by-one errors
        assert 'h27v09' in brdf_tiles
        assert 'h32v13' in brdf_tiles

    no_newer_than = (datetime.datetime.now() - timedelta(days=min_age_days)).date()
    no_older_than = None

    import logging

    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
    ]

    if sys.stderr.isatty():
        # Pretty printing when run in a terminal session.
        # Automatically prints pretty tracebacks when "rich" is installed
        processors = shared_processors + [
            structlog.dev.ConsoleRenderer(),
        ]
    else:
        # Log JSON when run otherwise
        processors = shared_processors + [
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(),
        ]
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(logging.NOTSET),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )
    download_files(
        required_brdf_tiles=brdf_tiles,
        output_base_path=output_folder,
        no_older_than=no_older_than, no_newer_than=no_newer_than,
    )


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Download and convert MCD43A1 files from USGS')
    parser.add_argument('--offshore-tiles', action='store_true', default=True, help='Download the offshore tiles')
    parser.add_argument('--mainland-tiles', action='store_true', help='Download the mainland tiles')
    parser.add_argument('--output-folder', type=Path, help='Output folder', required=True)
    parser.add_argument('--min-age-days', type=int, default=30, help='Minimum age of files to download')
    args = parser.parse_args()
    main(**vars(args))
