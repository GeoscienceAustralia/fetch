#!/usr/bin/env python

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import re
import shutil
import tempfile
import urllib.parse
import uuid
from pathlib import Path

import h5py
import pandas
import rasterio
import yaml
from osgeo import osr
from requests.exceptions import HTTPError

from .h5utils import atomic_h5_write, write_h5_md

try:
    import cdsapi  # 0.7.7
except ImportError:  # pragma: no cover
    cdsapi = None


CRS = osr.SpatialReference()
CRS.ImportFromEPSG(4326)

UUID_NAMESPACE = uuid.UUID("48682821-4061-4635-83aa-6a6ee8e10ceb")
PRODUCT_HREF = "https://collections.dea.ga.gov.au/ga_c_c_prwtrfallback_1"
PROVIDER_URL = (
    "https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels"
)
SOURCE_DATASET = "reanalysis-era5-single-levels"
SOURCE_VARIABLE = "total_column_water_vapour"
DEFAULT_BOUNDS = [0, 100, -60, 170]  # north, west, south, east
DEFAULT_HOURS = ["00", "06", "12", "18"]

LOGGER = logging.getLogger(__name__)


def _utc_timestamp_from_band(
    ds: rasterio.DatasetReader, index: int
) -> pandas.Timestamp:
    return pandas.to_datetime(int(ds.tags(index)["GRIB_REF_TIME"]), unit="s", utc=True)


def dataset_name_for_datetime(value: dt.datetime) -> str:
    return value.strftime("%Y/%B-%d/%H%M").upper()


def download_tcwv_zip(
    date: dt.date, hours: list[str], bounds: list[float], zip_file: Path
):
    """
    CDS area order is north, west, south, east.
    """
    if cdsapi is None:
        raise RuntimeError("cdsapi is not installed")

    dataset = "reanalysis-era5-single-levels"
    request = {
        "product_type": ["reanalysis"],
        "variable": [SOURCE_VARIABLE],
        "year": [f"{date.year:04d}"],
        "month": [f"{date.month:02d}"],
        "day": [f"{date.day:02d}"],
        "time": [f"{hour}:00" for hour in hours],
        "data_format": "grib",
        "download_format": "zip",
        "area": bounds,
    }

    client = cdsapi.Client(
        url=os.environ.get("CDSAPI_URL"),
        key=os.environ.get("CDSAPI_KEY"),
    )
    client.retrieve(dataset, request).download(str(zip_file))


def h5_index_dataframe(ds: rasterio.DatasetReader) -> pandas.DataFrame:
    df = pandas.DataFrame(
        {
            "timestamp": [_utc_timestamp_from_band(ds, i + 1) for i in range(ds.count)],
            "band_name": [f"BAND-{i + 1}" for i in range(ds.count)],
        }
    )
    df["dataset_name"] = df.timestamp.dt.strftime("%Y/%B-%d/%H%M").str.upper()
    return df


def h5_write_band(ds, df, index, out_h5, compression, filter_opts):
    from wagl.hdf5 import write_h5_image

    ds_name = df.iloc[index - 1].dataset_name
    f_opts = {} if not filter_opts else filter_opts.copy()

    attrs = ds.tags(index)
    attrs["timestamp"] = df.iloc[index - 1]["timestamp"].to_pydatetime()
    attrs["band_name"] = df.iloc[index - 1]["band_name"]
    attrs["geotransform"] = ds.transform.to_gdal()
    attrs["crs_wkt"] = CRS.ExportToWkt()

    if "chunks" not in f_opts:
        f_opts["chunks"] = ds.block_shapes[index - 1]

    write_h5_image(
        ds.read(index),
        ds_name,
        out_h5,
        attrs=attrs,
        compression=compression,
        filter_opts=f_opts,
    )


def write_index(out_h5: h5py.File, rows: pandas.DataFrame, compression):
    from wagl.hdf5 import write_dataframe

    attrs = {"description": "Timestamp and Band Name index information."}
    if "INDEX" in out_h5:
        del out_h5["INDEX"]
    write_dataframe(rows, "INDEX", out_h5, compression, attrs=attrs)


def collect_index_from_h5(h5_file: Path) -> pandas.DataFrame:
    records = []
    with h5py.File(h5_file, "r") as h5:

        def visitor(name, obj):
            if not isinstance(obj, h5py.Dataset):
                return
            if (
                name == "INDEX"
                or name.startswith("METADATA/")
                or name.startswith(".METADATA/")
            ):
                return
            if obj.attrs.get("CLASS") != "IMAGE":
                return

            timestamp = pandas.to_datetime(obj.attrs["timestamp"], utc=True)
            band_name = obj.attrs["band_name"]
            if isinstance(band_name, bytes):
                band_name = band_name.decode("utf-8")

            records.append(
                {
                    "timestamp": timestamp,
                    "band_name": band_name,
                    "dataset_name": name,
                }
            )

        h5.visititems(visitor)

    if not records:
        return pandas.DataFrame(columns=["timestamp", "band_name", "dataset_name"])

    return pandas.DataFrame(records).sort_values("timestamp").reset_index(drop=True)


def existing_dataset_names(h5_file: Path) -> set[str]:
    if not h5_file.exists():
        return set()

    index_df = collect_index_from_h5(h5_file)
    if index_df.empty:
        return set()

    return set(index_df["dataset_name"].tolist())


def load_metadata_docs_from_h5(h5_file: Path) -> list[dict]:
    with h5py.File(h5_file, "r") as h5:
        if "METADATA" not in h5 or "CURRENT-LIST" not in h5["METADATA"]:
            return []

        docs = []
        for raw_doc in h5["METADATA"]["CURRENT-LIST"]:
            if isinstance(raw_doc, bytes):
                raw_doc = raw_doc.decode("utf-8")
            docs.append(yaml.safe_load(raw_doc))
        return docs


def convert_tcwv_zips_to_h5(
    zip_files: list[Path],
    h5_file: Path,
    compression=None,
    filter_opts=None,
):
    from wagl.hdf5 import attach_attributes
    from wagl.hdf5.compression import H5CompressionFilter

    if compression is None:
        compression = H5CompressionFilter.LZF

    dataframes = []
    metadata_records = []
    append_mode = h5_file.exists()

    with atomic_h5_write(h5_file, "a" if append_mode else "w") as out_h5:
        for zip_file in zip_files:
            grib_file = f"zip+file://{zip_file}"
            with rasterio.open(grib_file) as ds:
                if not out_h5.attrs:
                    attach_attributes(out_h5, ds.tags(1))

                df = h5_index_dataframe(ds)
                dataframes.append(df)
                geometry = dataset_geometry(ds.transform, ds.height, ds.width)

                for index in range(1, ds.count + 1):
                    h5_write_band(ds, df, index, out_h5, compression, filter_opts)
                    metadata_records.append(
                        {
                            "band_index": index,
                            "checksum": ds.checksum(index),
                            "ref_time": ds.tags(index).get("GRIB_REF_TIME", ""),
                            "datetime": df.iloc[index - 1]["timestamp"]
                            .to_pydatetime()
                            .isoformat(),
                            "geometry": geometry,
                            "shape": [ds.height, ds.width],
                            "transform": list(ds.transform),
                            "layer_name": df.iloc[index - 1]["dataset_name"],
                        }
                    )

        metadata_docs = build_metadata_docs(h5_file.name, metadata_records)
        dataset_names = [record["layer_name"] for record in metadata_records]
        write_h5_md(out_h5, metadata_docs, dataset_names)
        if not append_mode:
            index_df = pandas.concat(dataframes, ignore_index=True)
            index_df = index_df.sort_values("timestamp").reset_index(drop=True)
            write_index(out_h5, index_df, compression)

    if append_mode:
        index_df = collect_index_from_h5(h5_file)
        with h5py.File(h5_file, "a") as out_h5:
            write_index(out_h5, index_df, compression)

    return metadata_docs, metadata_records


def metadata_uuid(
    checksum: int | str, ref_time: str, band_index: int, output_name: str
) -> uuid.UUID:
    uri = urllib.parse.urlencode(
        {
            "checksum": checksum,
            "band_index": band_index,
            "ref_time": ref_time,
            "filename": output_name,
        }
    )
    return uuid.uuid5(UUID_NAMESPACE, f"{PROVIDER_URL}?{uri}")


def dataset_geometry(transform, height: int, width: int) -> dict:
    west, north = transform * (0, 0)
    east, south = transform * (width, height)
    return {
        "type": "Polygon",
        "coordinates": [
            [
                [west, north],
                [east, north],
                [east, south],
                [west, south],
                [west, north],
            ]
        ],
    }


def build_metadata_docs(output_name: str, metadata_records: list[dict]) -> list[dict]:
    docs = []
    creation_dt = dt.datetime.now(dt.timezone.utc).isoformat()
    for record in metadata_records:
        docs.append(
            {
                "id": str(
                    metadata_uuid(
                        record["checksum"],
                        record["ref_time"],
                        record["band_index"],
                        output_name,
                    )
                ),
                "product": {"href": PRODUCT_HREF},
                "crs": "epsg:4326",
                "datetime": record["datetime"],
                "geometry": record["geometry"],
                "grids": {
                    "default": {
                        "shape": record["shape"],
                        "transform": record["transform"],
                    }
                },
                "lineage": {},
                "measurements": {
                    "water_vapour": {
                        "layer": f"/{record['layer_name'].lstrip('/')}",
                        "path": "",
                    }
                },
                "properties": {
                    "item:providers": [
                        {
                            "name": "ECMWF Copernicus Climate Data Store",
                            "roles": ["producer"],
                            "url": PROVIDER_URL,
                        },
                        {
                            "name": "GeoscienceAustralia",
                            "roles": ["host"],
                        },
                    ],
                    "tcwv:source": "ecmwf",
                    "tcwv:source_dataset": SOURCE_DATASET,
                    "tcwv:source_variable": SOURCE_VARIABLE,
                    "tcwv:source_url": PROVIDER_URL,
                    "odc:creation_datetime": creation_dt,
                    "odc:file_format": "HDF5",
                },
            }
        )

    return docs


def write_metadata_sidecar(h5_file: Path, metadata_records: list[dict]):
    metadata_file = h5_file.with_suffix(".ga-md.yaml")
    docs = load_metadata_docs_from_h5(h5_file)
    if not docs:
        docs = build_metadata_docs(h5_file.name, metadata_records)
    with metadata_file.open("w", encoding="utf-8") as f:
        yaml.safe_dump_all(docs, f, sort_keys=False)


def tcwv_output_filepath(outdir: Path, year: int) -> Path:
    return outdir / f"pr_wtr.eatm.{year}.h5"


def tcwv_snapshot_filepath(outdir: Path, year: int, label: str) -> Path:
    return outdir / f"pr_wtr.eatm.{year}.{label}.h5"


def tcwv_zip_filepath(workdir: Path, year: int) -> Path:
    return workdir / f"pr_wtr.eatm.{year}.zip"


def default_output_label(now: dt.datetime | None = None) -> str:
    now = now or dt.datetime.now(dt.timezone.utc)
    return now.strftime("%Y-%m-%d-%H")


def resolve_output_paths(
    outdir: Path,
    year: int,
    snapshot: bool,
    output_label: str | None,
) -> tuple[Path, Path]:
    latest_path = tcwv_output_filepath(outdir, year)
    if not snapshot:
        return latest_path, latest_path

    label = output_label or default_output_label()
    return tcwv_snapshot_filepath(outdir, year, label), latest_path


def update_latest_symlink(target_path: Path, symlink_path: Path):
    symlink_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_link = symlink_path.with_name(f".{symlink_path.name}.tmp")
    if tmp_link.exists() or tmp_link.is_symlink():
        tmp_link.unlink()

    tmp_link.symlink_to(target_path.name)
    tmp_link.replace(symlink_path)


def initialise_output_file(output_filepath: Path, input_h5: Path | None):
    if output_filepath.exists() or input_h5 is None:
        return

    input_h5 = Path(input_h5)
    if not input_h5.exists():
        raise FileNotFoundError(f"Input HDF5 does not exist: {input_h5}")

    LOGGER.info("Seeding %s from %s", output_filepath, input_h5)
    shutil.copy2(input_h5, output_filepath)


def build_tcwv_dataset(
    dates: list[dt.date],
    hours: list[str],
    outdir,
    workdir,
    bounds: list[float] | None = None,
    input_h5: str | Path | None = None,
    snapshot: bool = False,
    output_label: str | None = None,
    update_latest: bool = False,
):
    if not dates:
        raise ValueError("At least one date is required")

    years = {date.year for date in dates}
    if len(years) != 1:
        raise ValueError("All requested dates must be from the same year")

    bounds = bounds or DEFAULT_BOUNDS
    year = dates[0].year

    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    output_filepath, latest_symlink_path = resolve_output_paths(
        outdir, year, snapshot, output_label
    )

    resolved_input_h5 = Path(input_h5) if input_h5 else None
    if snapshot and resolved_input_h5 is None:
        default_seed = latest_symlink_path
        if default_seed.exists() or default_seed.is_symlink():
            resolved_input_h5 = default_seed

    initialise_output_file(output_filepath, resolved_input_h5)

    workdir = Path(workdir)
    workdir.mkdir(parents=True, exist_ok=True)

    dates = sorted(set(dates))
    total = len(dates)
    processed_any = False

    for idx, date in enumerate(dates, start=1):
        existing_names = existing_dataset_names(output_filepath)
        date_hours = missing_hours_for_date(date, hours, existing_names)
        if not date_hours:
            LOGGER.info(
                "Skipping %s (%s/%s): all requested hours already exist",
                date.isoformat(),
                idx,
                total,
            )
            continue

        with tempfile.TemporaryDirectory(dir=workdir) as tmpdir:
            tmpdir = Path(tmpdir)
            LOGGER.info(
                "Downloading %s (%s/%s) for hours %s",
                date.isoformat(),
                idx,
                total,
                ", ".join(date_hours),
            )
            zip_file = tmpdir / f"tcwv-{date.isoformat()}.zip"
            try:
                download_tcwv_zip(date, date_hours, bounds, zip_file)
            except HTTPError as err:
                latest_date = latest_available_date_from_error(err)
                if latest_date and date > latest_date:
                    LOGGER.warning(
                        "Stopping at %s: CDS reports latest available date is %s",
                        date.isoformat(),
                        latest_date.isoformat(),
                    )
                    break
                raise
            if output_filepath.exists():
                LOGGER.info("Appending %s into %s", date.isoformat(), output_filepath)
                _, metadata_records = convert_tcwv_zips_to_h5(
                    [zip_file], output_filepath
                )
            else:
                h5_file = tcwv_output_filepath(tmpdir, year)
                LOGGER.info("Creating %s from %s", output_filepath, date.isoformat())
                _, metadata_records = convert_tcwv_zips_to_h5([zip_file], h5_file)
                h5_file.replace(output_filepath)

            write_metadata_sidecar(output_filepath, metadata_records)
            LOGGER.info("Committed %s into %s", date.isoformat(), output_filepath)
            processed_any = True

    if not processed_any:
        LOGGER.info("No new data was committed")
        return

    if snapshot and update_latest:
        update_latest_symlink(output_filepath, latest_symlink_path)
        LOGGER.info("Updated %s -> %s", latest_symlink_path, output_filepath.name)


def parse_date(value: str) -> dt.date:
    return dt.datetime.strptime(value, "%Y-%m-%d").date()


def parse_hours(values: list[str] | None) -> list[str]:
    hours = values or DEFAULT_HOURS
    parsed = []
    for hour in hours:
        normalized = str(hour).strip()
        if len(normalized) == 1:
            normalized = f"0{normalized}"
        if len(normalized) != 2 or not normalized.isdigit():
            raise ValueError(f"Invalid hour: {hour}")
        hour_value = int(normalized)
        if hour_value < 0 or hour_value > 23:
            raise ValueError(f"Invalid hour: {hour}")
        parsed.append(normalized)
    return parsed


def date_range(start: dt.date, end: dt.date) -> list[dt.date]:
    if end < start:
        raise ValueError("End date must be on or after start date")
    current = start
    dates = []
    while current <= end:
        dates.append(current)
        current += dt.timedelta(days=1)
    return dates


def dates_for_year_to_date(year: int, through: dt.date | None = None) -> list[dt.date]:
    through = through or dt.date.today()
    start = dt.date(year, 1, 1)
    end = min(through, dt.date(year, 12, 31))
    return date_range(start, end)


def latest_available_date_from_error(error: Exception) -> dt.date | None:
    match = re.search(r"latest date available .*?: (\d{4}-\d{2}-\d{2})", str(error))
    if not match:
        return None
    return parse_date(match.group(1))


def missing_hours_for_date(
    date: dt.date, hours: list[str], existing_names: set[str]
) -> list[str]:
    missing = []
    for hour in hours:
        layer_name = dataset_name_for_datetime(
            dt.datetime.combine(date, dt.time(hour=int(hour), tzinfo=dt.timezone.utc))
        )
        if layer_name not in existing_names:
            missing.append(hour)

    return missing


def add_common_options(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--hour",
        action="append",
        default=None,
        help="Hour in HH format. Repeat to request multiple times. Default: 00, 06, 12, 18.",
    )
    parser.add_argument("--outdir", default="output/ecmwf-tcwv")
    parser.add_argument("--workdir", default="output/ecmwf-tcwv")
    parser.add_argument(
        "--input-h5",
        default=None,
        help="Optional existing annual HDF5 to copy into the output path before appending new data.",
    )
    parser.add_argument(
        "--snapshot",
        action="store_true",
        help="Write to a versioned snapshot file instead of modifying the default annual output in place.",
    )
    parser.add_argument(
        "--output-label",
        default=None,
        help="Snapshot label for versioned output files. Defaults to the current UTC hour.",
    )
    parser.add_argument(
        "--update-latest-symlink",
        action="store_true",
        help="When using --snapshot, update pr_wtr.eatm.<year>.h5 to point to the new snapshot on success.",
    )
    parser.add_argument(
        "--bounds",
        nargs=4,
        type=float,
        metavar=("NORTH", "WEST", "SOUTH", "EAST"),
        default=DEFAULT_BOUNDS,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download ECMWF ERA5 total column water vapour and build DEA-compatible annual HDF5 output."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch = subparsers.add_parser(
        "fetch",
        help="Download one or more explicit dates and append them into the annual output file.",
    )
    fetch.add_argument(
        "--date",
        action="append",
        required=True,
        help="Date in YYYY-MM-DD format. Repeat to fetch multiple dates.",
    )
    add_common_options(fetch)

    backfill = subparsers.add_parser(
        "backfill",
        help="Download a continuous date range, including year-to-date runs.",
    )
    range_group = backfill.add_mutually_exclusive_group(required=True)
    range_group.add_argument(
        "--year",
        type=int,
        help="Backfill from January 1 of this year through --through or today.",
    )
    range_group.add_argument("--start-date", help="Start date in YYYY-MM-DD format.")
    backfill.add_argument(
        "--end-date", help="End date in YYYY-MM-DD format. Required with --start-date."
    )
    backfill.add_argument(
        "--through",
        help="End date in YYYY-MM-DD format for --year runs. Defaults to today.",
    )
    add_common_options(backfill)

    return parser


def resolve_dates(args) -> list[dt.date]:
    if args.command == "fetch":
        return [parse_date(value) for value in args.date]

    if args.year is not None:
        through = parse_date(args.through) if args.through else None
        return dates_for_year_to_date(args.year, through)

    if not args.end_date:
        raise ValueError("--end-date is required with --start-date")
    return date_range(parse_date(args.start_date), parse_date(args.end_date))


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parser = build_parser()
    args = parser.parse_args()
    dates = resolve_dates(args)
    hours = parse_hours(args.hour)
    build_tcwv_dataset(
        dates=dates,
        hours=hours,
        outdir=args.outdir,
        workdir=args.workdir,
        bounds=args.bounds,
        input_h5=args.input_h5,
        snapshot=args.snapshot,
        output_label=args.output_label,
        update_latest=args.update_latest_symlink,
    )


if __name__ == "__main__":
    main()
