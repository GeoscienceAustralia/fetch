import shutil
import tempfile
import threading
import time
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
import yaml
from flask import Flask, abort

from fetch_brdf.main import BrdfConfig, run_with_config


class USGSTestServer:
    """A dummy HTTP server to mimic USGS responses."""

    def __init__(self):
        self.app = Flask(__name__)
        self.setup_routes()
        self.directory_pages = {}
        self.files = {}
        self.server = None
        self.thread = None
        self.port = None

    def add_directory_page(self, path: str, links: list):
        """Add a directory page with links."""
        self.directory_pages[path] = links

    def add_file(self, path: str, content: bytes):
        """Add a file to be served."""
        self.files[path] = content

    def setup_routes(self):
        @self.app.route("/<path:path>")
        def serve_path(path):
            full_path = "/" + path

            # Check if it's a directory page
            if full_path in self.directory_pages:
                links = self.directory_pages[full_path]
                html_content = "<html><body>"
                for link in links:
                    html_content += f'<a href="{link}">{link}</a>'
                html_content += "</body></html>"
                return html_content

            # Check if it's a file
            elif full_path in self.files:
                content = self.files[full_path]
                return content

            # Return 404 for unknown paths
            else:
                abort(404)

    def setup_typical_data(self):
        """Setup typical BRDF data structure."""
        # Add root directory with date folders
        today = date.today()
        yesterday = today - timedelta(days=1)

        self.add_directory_page(
            "/MOTA/MCD43A1.061/",
            [f"{yesterday.strftime('%Y.%m.%d')}/", f"{today.strftime('%Y.%m.%d')}/"],
        )

        # Add yesterday's directory with BRDF files
        yesterday_path = f"/MOTA/MCD43A1.061/{yesterday.strftime('%Y.%m.%d')}/"
        test_files = [
            f"MCD43A1.A{yesterday.strftime('%Y%j')}.h27v09.061.2024084123456.hdf",
            f"MCD43A1.A{yesterday.strftime('%Y%j')}.h27v09.061.2024084123456.hdf.xml",
            f"MCD43A1.A{yesterday.strftime('%Y%j')}.h28v09.061.2024084123457.hdf",
            f"MCD43A1.A{yesterday.strftime('%Y%j')}.h28v09.061.2024084123457.hdf.xml",
        ]
        self.add_directory_page(yesterday_path, test_files)

        # Add file contents
        for filename in test_files:
            file_path = yesterday_path + filename
            if filename.endswith(".hdf"):
                # Mock HDF content
                self.add_file(file_path, b"MOCK_HDF_CONTENT_" + filename.encode())
            else:
                # Mock XML content
                self.add_file(
                    file_path,
                    f'<?xml version="1.0"?><metadata>{filename}</metadata>'.encode(),
                )

    def start(self):
        """Start the test server on a random port."""
        import socket

        sock = socket.socket()
        sock.bind(("localhost", 0))
        self.port = sock.getsockname()[1]
        sock.close()

        def run_server():
            self.app.run(
                host="localhost", port=self.port, debug=False, use_reloader=False
            )

        self.thread = threading.Thread(target=run_server, daemon=True)
        self.thread.start()

        # Wait for server to start
        time.sleep(0.5)

    def stop(self):
        """Stop the test server."""
        # Flask development server doesn't have a clean shutdown method
        # In a real test environment, you might use a proper WSGI server
        pass

    def get_base_url(self):
        """Get the base URL for this test server."""
        return f"http://localhost:{self.port}"


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test outputs."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def test_usgs_server():
    """Create and start a test USGS server."""
    server = USGSTestServer()
    server.setup_typical_data()
    server.start()
    yield server
    server.stop()


@pytest.fixture
def test_config(temp_dir):
    """Create a test configuration."""
    config_data = {
        "date_range": {
            "begin": -2,  # 2 days ago
            "end": -1,  # yesterday
        },
        "download": {
            "max_retries": 2,
            "max_queue_size": 1,
            "max_workers": 1,
            "request_timeout_secs": 30,
            "min_request_period_secs": 0.01,
        },
        "logging": {"verbose": True, "log_file_pattern": None},
        "tiles": "mainland",
        "output_path": str(temp_dir),
        "clean_up": True,
        "products": {"modis": {"enabled": True, "max_downloads": 2}},
    }
    return BrdfConfig(**config_data)


def patch_brdf_client_url(test_server_url: str):
    """Patch the BrdfClient to use our test server instead of the real USGS server."""

    # Store the original __init__ method
    from fetch_brdf.main import BrdfClient

    original_init = BrdfClient.__init__

    def patched_init(self, product_offset, **kwargs):
        # Call the original __init__ method with our host
        original_init(self, product_offset, **kwargs, host_url=test_server_url)

    return patched_init


def mock_swfo_convert(cmd, **kwargs):
    """Mock the swfo-convert subprocess call."""
    # Extract input and output paths from command
    input_file = None
    output_dir = None

    for i, arg in enumerate(cmd):
        if arg == "--fname" and i + 1 < len(cmd):
            input_file = Path(cmd[i + 1])
        elif arg == "--outdir" and i + 1 < len(cmd):
            output_dir = Path(cmd[i + 1])

    if input_file and output_dir:
        # Create the expected output file
        output_file = output_dir / input_file.with_suffix(".h5").name
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # Write mock H5 content
        with open(output_file, "wb") as f:
            f.write(b"MOCK_H5_CONVERTED_CONTENT")

    # Return successful result
    result = MagicMock()
    result.returncode = 0
    result.stdout = b"Conversion successful"
    return result


def test_end_to_end_brdf_download(test_config, test_usgs_server, temp_dir, monkeypatch):
    """Do a test download."""

    # Patch BrdfClient to use our test server
    with patch(
        "fetch_brdf.main.BrdfClient.__init__",
        patch_brdf_client_url(test_usgs_server.get_base_url()),
    ):
        # Mock subprocess for swfo-convert
        with patch("fetch_brdf.main.subprocess.run", side_effect=mock_swfo_convert):
            # Mock environment variables for auth
            monkeypatch.setenv("EARTHDATA_USERNAME", "test_user")
            monkeypatch.setenv("EARTHDATA_PASSWORD", "test_pass")

            # Run the downloader
            run_with_config(test_config)

            # Verify output structure was created
            brdf_dir = temp_dir / "BRDF" / "MCD43A1.061"
            assert brdf_dir.exists(), f"BRDF directory not created: {brdf_dir}"

            # Check for date directories (YYYY.MM.DD format)
            date_dirs = list(brdf_dir.glob("????.*.*"))
            assert len(date_dirs) > 0, (
                f"No date directories created in {brdf_dir}. Contents: {list(brdf_dir.iterdir())}"
            )

            # Check for H5 files in date directories
            h5_files = list(brdf_dir.glob("*/*.h5"))
            assert len(h5_files) > 0, f"No H5 files found in {brdf_dir}"

            # Verify file content
            for h5_file in h5_files:
                with open(h5_file, "rb") as f:
                    content = f.read()
                    assert content == b"MOCK_H5_CONVERTED_CONTENT", (
                        f"Unexpected content in {h5_file}"
                    )

            # Verify we have the expected number of files (max_downloads=2)
            assert len(h5_files) == 2, f"Expected 2 H5 files, got {len(h5_files)}"


def test_config_based_execution_with_cli(temp_dir):
    """Test running the CLI with a config file."""
    config_file = temp_dir / "test_config.yaml"

    config_data = {
        "date_range": {"begin": -1, "end": -1},
        "output_path": str(temp_dir),
        "products": {"modis": {"enabled": True, "max_downloads": 1}},
    }

    with open(config_file, "w") as f:
        yaml.dump(config_data, f)

    # Mock the entire workflow since we're testing CLI integration
    with patch("fetch_brdf.main.run_with_config") as mock_run:
        with patch(
            "sys.argv", ["fetch-brdf", "run", "--config-path", str(config_file)]
        ):
            from fetch_brdf.main import cli

            # This should not raise an exception
            try:
                cli.main(standalone_mode=False)
            except SystemExit as e:
                # Click may raise SystemExit(0) on success
                assert e.code == 0

            # Verify run_with_config was called
            mock_run.assert_called_once()


def test_environment_variable_auth(temp_dir, monkeypatch):
    """Test that authentication works via environment variables."""
    monkeypatch.setenv("EARTHDATA_USERNAME", "env_user")
    monkeypatch.setenv("EARTHDATA_PASSWORD", "env_pass")

    # Create config without auth credentials
    config_data = {
        "output_path": str(temp_dir),
        "products": {"modis": {"enabled": True}},
    }
    test_config = BrdfConfig(**config_data)

    username, password = test_config.auth.get_credentials()

    assert username == "env_user"
    assert password == "env_pass"


def test_tile_resolution():
    """Test tile resolution for different configurations."""
    from fetch_brdf.main import resolve_tiles

    # Test built-in tile sets
    mainland_tiles = resolve_tiles("mainland")
    assert "h27v09" in mainland_tiles
    assert "h32v13" in mainland_tiles

    offshore_tiles = resolve_tiles("offshore")
    assert "h22v14" in offshore_tiles
    assert "h27v14" in offshore_tiles

    combined_tiles = resolve_tiles("mainland+offshore")
    assert len(combined_tiles) == len(mainland_tiles) + len(offshore_tiles)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
