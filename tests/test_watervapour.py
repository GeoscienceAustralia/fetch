import datetime as dt
from pathlib import Path

import fetch2.watervapour as watervapour


def test_snapshot_paths_use_year_subdirectory():
    outdir = Path("/tmp/water_vapour")

    assert (
        watervapour.tcwv_output_filepath(outdir, 2026) == outdir / "pr_wtr.eatm.2026.h5"
    )
    assert watervapour.tcwv_snapshot_filepath(outdir, 2026, "2026-04-13-00") == (
        outdir / "2026" / "pr_wtr.eatm.2026.2026-04-13-00.h5"
    )


def test_update_latest_symlink_targets_year_subdirectory(tmp_path):
    outdir = tmp_path / "water_vapour"
    target = outdir / "2026" / "pr_wtr.eatm.2026.2026-04-13-00.h5"
    symlink = outdir / "pr_wtr.eatm.2026.h5"
    target.parent.mkdir(parents=True)
    target.write_text("stub", encoding="utf-8")

    watervapour.update_latest_symlink(target, symlink)

    assert symlink.is_symlink()
    assert symlink.readlink() == Path("2026/pr_wtr.eatm.2026.2026-04-13-00.h5")


def test_parser_defaults_to_simple_directory_driven_cli(tmp_path):
    parser = watervapour.build_parser()

    args = parser.parse_args([str(tmp_path / "water_vapour")])

    assert args.water_vapour_dir == str(tmp_path / "water_vapour")
    assert args.snapshot is True
    assert args.update_latest_symlink is True
    assert args.workdir is None


def test_no_new_data_does_not_create_snapshot(monkeypatch, tmp_path):
    outdir = tmp_path / "water_vapour"
    seed = outdir / "pr_wtr.eatm.2026.h5"
    seed.parent.mkdir(parents=True)
    seed.write_text("seed", encoding="utf-8")

    hours = ["00", "06", "12", "18"]
    date = dt.date(2026, 4, 13)
    existing = {
        watervapour.dataset_name_for_datetime(
            dt.datetime.combine(date, dt.time(hour=int(hour), tzinfo=dt.timezone.utc))
        )
        for hour in hours
    }

    monkeypatch.setattr(watervapour, "_require_runtime_deps", lambda: None)
    monkeypatch.setattr(
        watervapour, "existing_dataset_names", lambda _: existing.copy()
    )

    def fail_if_called(*args, **kwargs):
        raise AssertionError(
            "initialise_output_file should not run when nothing is missing"
        )

    monkeypatch.setattr(watervapour, "initialise_output_file", fail_if_called)

    watervapour.build_tcwv_dataset(
        dates=[date],
        hours=hours,
        outdir=outdir,
        workdir=tmp_path / "work",
        input_h5=seed,
        snapshot=True,
        update_latest=True,
    )

    assert not (outdir / "2026").exists()
