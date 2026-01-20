import logging
import os
import boto3

from pathlib import Path
from typing import Callable

from .util import rsync, Uri

_log = logging.getLogger(__name__)

class FileUploadError(Exception):
    """
    An error in file processing.
    """
    pass

def upload(filepath: str, bucket: str, prefix: str):
    """
    Upload all files in input_file_dir to S3.

    :raises: FileUploadError
    """
    _log.info(f"Uploading file {filepath} to s3://{bucket}/{prefix}")

    s3 = boto3.client("s3")

    if not os.path.exists(filepath):
        raise FileUploadError(f"File does not exist: {filepath}")

    directory, filename = os.path.split(filepath)

    s3_key = f"{prefix}/{filename}"

    try:
        _log.debug(f"Uploading {filename} to s3://{bucket}/{s3_key}")
        s3.upload_file(filepath, bucket, s3_key)
    except Exception as e:
        raise FileUploadError(f"Failed to upload {filename} to s3://{bucket}/{s3_key}") 

    _log.info(f"Successfully Uploaded {filename} to s3://{bucket}/{s3_key}")
