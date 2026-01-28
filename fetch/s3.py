import logging
import os
import boto3

_log = logging.getLogger(__name__)
_s3 = boto3.client("s3")


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
    message = f"Uploading file {filepath} to s3://{bucket}/{prefix}"
    _log.info(message)

    if not os.path.exists(filepath):
        raise FileUploadError(f"File does not exist: {filepath}")

    directory, filename = os.path.split(filepath)

    s3_key = f"{prefix}/{filename}"

    try:
        message  = f"Uploading {filename} to s3://{bucket}/{s3_key}"
        _log.debug(message)
        _s3.upload_file(filepath, bucket, s3_key)
    except Exception as e:
        message = f"Failed to upload {filename} to s3://{bucket}/{s3_key}";
        raise FileUploadError(message) from e

    message = f"Successfully Uploaded {filename} to s3://{bucket}/{s3_key}"
    _log.info(message)
