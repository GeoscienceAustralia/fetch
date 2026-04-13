from __future__ import annotations

import os
import tempfile
import zipfile
from functools import lru_cache
from pathlib import Path

import pytest
import yaml


@lru_cache(maxsize=1)
def load_ecmwf_tcwv():
    require_wv_deps = os.environ.get("FETCH2_REQUIRE_WV_DEPS") == "1"

    if require_wv_deps:
        import h5py
        import pandas  # noqa: F401
        import rasterio  # noqa: F401
        import requests  # noqa: F401
        import wagl.hdf5  # noqa: F401
    else:
        h5py = pytest.importorskip("h5py")
        pytest.importorskip("rasterio")
        pytest.importorskip("pandas")
        pytest.importorskip("requests")
        pytest.importorskip("wagl.hdf5")

    from fetch2 import watervapour

    return watervapour, h5py


def test_date_range_helpers():
    ecmwf_tcwv, _ = load_ecmwf_tcwv()
    assert ecmwf_tcwv.parse_hours(None) == ["00", "06", "12", "18"]
    assert ecmwf_tcwv.parse_hours(["0", "6", "12", "18"]) == ["00", "06", "12", "18"]
    assert ecmwf_tcwv.date_range(
        ecmwf_tcwv.parse_date("2026-03-20"),
        ecmwf_tcwv.parse_date("2026-03-22"),
    ) == [
        ecmwf_tcwv.parse_date("2026-03-20"),
        ecmwf_tcwv.parse_date("2026-03-21"),
        ecmwf_tcwv.parse_date("2026-03-22"),
    ]
    assert ecmwf_tcwv.dates_for_year_to_date(
        2026,
        ecmwf_tcwv.parse_date("2026-01-03"),
    ) == [
        ecmwf_tcwv.parse_date("2026-01-01"),
        ecmwf_tcwv.parse_date("2026-01-02"),
        ecmwf_tcwv.parse_date("2026-01-03"),
    ]
    assert ecmwf_tcwv.latest_available_date_from_error(
        Exception(
            "invalid request None of the data you have requested is available yet. "
            "The latest date available for this dataset is: 2026-04-07 10:00"
        )
    ) == ecmwf_tcwv.parse_date("2026-04-07")
    assert ecmwf_tcwv.missing_hours_for_date(
        ecmwf_tcwv.parse_date("2026-03-20"),
        ["00", "06", "12"],
        {"2026/MARCH-20/0000", "2026/MARCH-20/1200"},
    ) == ["06"]


def test_initialise_output_file_from_input_h5():
    ecmwf_tcwv, _ = load_ecmwf_tcwv()
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        src = tmpdir / "src.h5"
        dst = tmpdir / "dst.h5"

        src.write_bytes(b"example")
        ecmwf_tcwv.initialise_output_file(dst, src)

        assert dst.read_bytes() == b"example"


def test_snapshot_helpers():
    ecmwf_tcwv, _ = load_ecmwf_tcwv()
    outdir = Path("/tmp/tcwv")
    output_path, latest_path = ecmwf_tcwv.resolve_output_paths(
        outdir,
        2026,
        snapshot=True,
        output_label="2026-04-12-00",
    )
    assert output_path == outdir / "2026" / "pr_wtr.eatm.2026.2026-04-12-00.h5"
    assert latest_path == outdir / "pr_wtr.eatm.2026.h5"
    assert (
        ecmwf_tcwv.default_output_label(
            ecmwf_tcwv.dt.datetime(
                2026, 4, 12, 0, 30, tzinfo=ecmwf_tcwv.dt.timezone.utc
            )
        )
        == "2026-04-12-00"
    )


def test_update_latest_symlink():
    ecmwf_tcwv, _ = load_ecmwf_tcwv()
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        target = tmpdir / "2026" / "pr_wtr.eatm.2026.2026-04-12-00.h5"
        target.parent.mkdir(parents=True)
        target.write_bytes(b"example")
        symlink = tmpdir / "pr_wtr.eatm.2026.h5"

        ecmwf_tcwv.update_latest_symlink(target, symlink)

        assert symlink.is_symlink()
        assert symlink.resolve() == target
        assert symlink.readlink() == Path("2026/pr_wtr.eatm.2026.2026-04-12-00.h5")


def test_convert_tcwv_zip_to_h5_with_embedded_metadata():
    ecmwf_tcwv, h5py = load_ecmwf_tcwv()
    repo = Path(__file__).resolve().parents[1]
    source_grib = repo / "tests" / "data.grib"
    if not source_grib.exists():
        raise FileNotFoundError(f"Missing fixture: {source_grib}")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        zip_path = tmpdir / "fixture.zip"
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write(source_grib, arcname="data.grib")

        out_path = tmpdir / "pr_wtr.eatm.2026.h5"
        metadata_docs, metadata_records = ecmwf_tcwv.convert_tcwv_zips_to_h5(
            [zip_path], out_path
        )

        sidecar = out_path.with_suffix(".ga-md.yaml")
        ecmwf_tcwv.write_metadata_sidecar(out_path, metadata_records)

        with h5py.File(out_path, "r") as h5:
            assert "INDEX" in h5
            assert "METADATA" in h5
            assert ".METADATA" in h5
            assert "CURRENT-LIST" in h5["METADATA"]
            layer_name = metadata_docs[0]["measurements"]["water_vapour"][
                "layer"
            ].strip("/")
            assert layer_name in h5["METADATA"]
            assert layer_name in h5[".METADATA"]

        docs = list(yaml.safe_load_all(sidecar.read_text()))
        assert len(docs) == len(metadata_docs)
        assert docs[0]["measurements"]["water_vapour"]["path"] == ""
        assert docs[0]["measurements"]["water_vapour"]["layer"].startswith("/")
        assert docs[0]["properties"]["odc:file_format"] == "HDF5"
        assert docs[0]["properties"]["tcwv:source"] == "ecmwf"
        assert (
            docs[0]["properties"]["tcwv:source_dataset"]
            == "reanalysis-era5-single-levels"
        )
        assert (
            docs[0]["properties"]["tcwv:source_variable"] == "total_column_water_vapour"
        )
