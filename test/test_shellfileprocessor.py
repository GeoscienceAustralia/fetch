"""
A package for testing the code in the ShellFileProcessor class, in _core.py
"""
import os
import subprocess
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from fetch import s3
from fetch import _core
from fetch._core import ShellFileProcessor, FileProcessError

# Test Constants
command="run command on file {filename}"
upload_dir="/data/upload"
bucket="s3-bucket"
prefix="s3-prefix"
input_file="/data/staging/file.txt"

def test_apply_file_pattern_basic():
    p = ShellFileProcessor(
        command=command,
        upload_dir=upload_dir,
        bucket=bucket,
        prefix=prefix,
    )

    result = p._apply_file_pattern(command, "/tmp/something.txt")

    assert result == "run command on file something.txt"

def test_process_success():
    # Set up mocks
    with (
        patch("subprocess.call", return_value=0),
        patch("os.path.exists", return_value=True),
        patch.object(s3, "_s3") as mock_s3,
        patch.object(_core, "_log") as mock_log
    ): 
        p = ShellFileProcessor(
            command=command,
            upload_dir=upload_dir,
            bucket=bucket,
            prefix=prefix,
        )

        # assert success using return value (and no raised errors)
        assert p.process(input_file) == "/data/upload/file.h5"

        # confirm the correct messages are logged
        mock_log.info.assert_called_once_with('Running %r', 'run command on file file.txt')
        mock_log.debug.assert_called_once_with('File available %r', '/data/upload/file.h5')

def test_process_command_failed():
    # Set up mocks
    with (
        patch("subprocess.call", return_value=1),
        patch("os.path.exists", return_value=True),
        patch.object(s3, "_s3") as mock_s3,
        patch.object(_core, "_log") as mock_log
    ): 
        p = ShellFileProcessor(
            command=command,
            upload_dir=upload_dir,
            bucket=bucket,
            prefix=prefix,
        )

        # Run code and confirm we get an expection back, checking type & message
        with pytest.raises(FileProcessError, match="Return code 1 from command 'run command on file file.txt'"):
            p.process(input_file)

        # confirm the correct messages are logged
        mock_log.info.assert_called_once_with('Running %r', 'run command on file file.txt')
        mock_log.debug.assert_not_called()

def test_process_output_file_missing():
    # Set up mocks
    with (
        patch("subprocess.call", return_value=0),
        patch("os.path.exists", return_value=False),
        patch.object(s3, "_s3") as mock_s3,
        patch.object(_core, "_log") as mock_log
    ): 
        p = ShellFileProcessor(
            command=command,
            upload_dir=upload_dir,
            bucket=bucket,
            prefix=prefix,
        )

        # Run code and confirm we get an expection back, checking type & message
        with pytest.raises(FileProcessError, match="Expected output not found '/data/upload/file.h5' for command 'run command on file file.txt'"):
            p.process(input_file)

        # confirm the correct messages are logged
        mock_log.info.assert_called_once_with('Running %r', 'run command on file file.txt')
        mock_log.debug.assert_not_called()