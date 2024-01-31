import os
import boto3
from botocore.exceptions import ClientError
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def download_from_s3(
    object_names: list[str], bucket_name: str, audio_dir: str, verbose: bool = False
) -> dict:
    """Downloads audio files from S3 and returns the download results."""
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=os.environ.get("AWS_SESSION_TOKEN"),
    )

    if not os.path.exists(audio_dir):
        os.makedirs(audio_dir)

    results = {"success": [], "errors": []}

    for object_name in object_names:
        local_file_path = os.path.join(audio_dir, os.path.basename(object_name))
        if not os.path.exists(local_file_path):
            if verbose:
                logging.info(f"Downloading: {os.path.basename(object_name)}")
            try:
                s3.download_file(bucket_name, object_name, local_file_path)
                results["success"].append(object_name)
            except ClientError as e:
                logging.error(f"Failed to download {object_name}: {e}")
                results["errors"].append(object_name)

    return results


def log_results(results: dict, verbose: bool):
    """Logs the results of the download operation based on verbosity."""
    if verbose:
        logging.info(f"Downloaded files: {results['success']}")
        if results["errors"]:
            logging.error(
                f"Errors occurred with the following files: {results['errors']}"
            )
        else:
            logging.info("All files downloaded successfully.")


if __name__ == "__main__":
    object_names = []
    bucket_name = ""
    audio_dir = ""  # path to local directory
    verbose = True  # True to enable verbose logging

    if not os.path.exists(audio_dir):
        os.makedirs(audio_dir)

    results = download_from_s3(object_names, bucket_name, audio_dir, verbose)
    log_results(results, verbose)
