"""
A package for testing the code in s3.py
"""
from fetch import s3
from fetch.s3 import upload, FileUploadError

import pytest
from unittest.mock import patch

# Constants for tests
filepath = "/tmp/test.txt"
bucket = "my-bucket"
prefix = "my/prefix"


def test_upload_success():
    # Set up mocks
    with (
        patch("os.path.exists", return_value=True),
        patch.object(s3, "_s3") as mock_s3,
        patch.object(s3, "_log") as mock_log,
    ):
        # Call code
        upload(filepath, bucket, prefix)

        # Expected logged messages
        mock_log.info.assert_any_call("Uploading file /tmp/test.txt to s3://my-bucket/my/prefix")
        mock_log.debug.assert_any_call("Uploading test.txt to s3://my-bucket/my/prefix/test.txt")
        mock_log.info.assert_any_call("Successfully Uploaded test.txt to s3://my-bucket/my/prefix/test.txt")

        # expected write to S3
        mock_s3.upload_file.assert_called_once_with(
            filepath,
            bucket,
            "my/prefix/test.txt",
        )


def test_given_file_not_exist():
    # Set up mocks
    with (
        patch("os.path.exists", return_value=False),
        patch.object(s3, "_s3") as mock_s3,
        patch.object(s3, "_log") as mock_log,
    ):
        # Run code and confirm we get an expection back, checking type & message
        with pytest.raises(FileUploadError, match="File does not exist: /tmp/test.txt"):
            upload(filepath, bucket, prefix)

        # Expected logged messages
        mock_log.info.assert_called_once_with("Uploading file /tmp/test.txt to s3://my-bucket/my/prefix")
        mock_s3.upload_file.assert_not_called()


def test_write_to_s3_failed():
    # Set up mocks
    with (
        patch("os.path.exists", return_value=True),
        patch.object(s3, "_s3") as mock_s3,
        patch.object(s3, "_log") as mock_log,
    ):
        mock_s3.upload_file.side_effect = Exception("poop")

        # Run code and confirm we get an expection back, checking type & message
        with pytest.raises(FileUploadError, match="Failed to upload test.txt to s3://my-bucket/my/prefix/test.txt"):
            upload(filepath, bucket, prefix)

        # Expected logged messages
        mock_log.info.assert_called_once_with("Uploading file /tmp/test.txt to s3://my-bucket/my/prefix")
        mock_log.debug.assert_called_once_with("Uploading test.txt to s3://my-bucket/my/prefix/test.txt")
