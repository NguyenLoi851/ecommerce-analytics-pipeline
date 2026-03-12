#!/usr/bin/env python3
"""
upload_to_s3.py
---------------
Download the Olist Brazilian E-Commerce dataset from Kaggle and upload all
CSVs to the configured S3 raw prefix.

Usage:
    python scripts/upload_to_s3.py \
        --bucket <your-raw-bucket> \
        --prefix raw/olist \
        --region us-east-1 \
        --profile <aws-profile-name>

Requirements:
    pip install kaggle boto3

Kaggle credentials must be present at ~/.kaggle/kaggle.json:
    {"username": "<user>", "key": "<api-key>"}
"""

import argparse
import logging
import os
import pathlib
import tempfile
import zipfile

import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError, ProfileNotFound

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

KAGGLE_DATASET = "olistbr/brazilian-ecommerce"

# Expected CSV files in the dataset zip.
EXPECTED_FILES = [
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv",
]


def download_dataset(dest_dir: pathlib.Path) -> pathlib.Path:
    """Download Kaggle dataset zip to dest_dir and return the zip path."""
    # Import here so the rest of the script can be imported without kaggle installed.
    from kaggle import KaggleApi  # type: ignore

    api = KaggleApi()
    api.authenticate()

    log.info("Downloading dataset '%s' from Kaggle...", KAGGLE_DATASET)
    api.dataset_download_files(KAGGLE_DATASET, path=str(dest_dir), unzip=False)

    zip_name = KAGGLE_DATASET.split("/")[-1] + ".zip"
    zip_path = dest_dir / zip_name
    if not zip_path.exists():
        # Some versions save as the full dataset slug
        candidates = list(dest_dir.glob("*.zip"))
        if not candidates:
            raise FileNotFoundError(
                f"Could not find downloaded zip in {dest_dir}. "
                "Check Kaggle API authentication."
            )
        zip_path = candidates[0]

    log.info("Downloaded zip: %s (%.1f MB)", zip_path, zip_path.stat().st_size / 1e6)
    return zip_path


def extract_csvs(zip_path: pathlib.Path, extract_dir: pathlib.Path) -> list[pathlib.Path]:
    """Extract all CSVs from the zip and return their paths."""
    log.info("Extracting CSVs to %s ...", extract_dir)
    with zipfile.ZipFile(zip_path, "r") as zf:
        members = [m for m in zf.namelist() if m.endswith(".csv")]
        zf.extractall(extract_dir, members=members)
        log.info("Extracted %d CSV files.", len(members))

    csv_paths = list(extract_dir.rglob("*.csv"))
    missing = [f for f in EXPECTED_FILES if not any(p.name == f for p in csv_paths)]
    if missing:
        log.warning("Expected files not found in zip: %s", missing)

    return csv_paths


def upload_to_s3(
    csv_paths: list[pathlib.Path],
    bucket: str,
    prefix: str,
    region: str,
    profile: str | None = None,
) -> None:
    """Upload each CSV to s3://<bucket>/<prefix>/<filename>."""
    try:
        session = boto3.session.Session(profile_name=profile, region_name=region)
    except ProfileNotFound as exc:
        raise RuntimeError(
            f"AWS profile '{profile}' was not found. "
            "Run 'aws configure --profile <name>' or use a valid profile."
        ) from exc

    s3 = session.client("s3")

    # Verify bucket is accessible.
    try:
        s3.head_bucket(Bucket=bucket)
    except (NoCredentialsError, PartialCredentialsError) as exc:
        raise RuntimeError(
            "AWS credentials were not found for this project. "
            "Use '--profile <name>' or configure credentials with 'aws configure'."
        ) from exc
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        raise RuntimeError(
            f"Cannot access bucket '{bucket}' (AWS error: {code}). "
            "Check AWS profile/credentials, bucket name, and region."
        ) from exc
    except Exception as exc:
        raise RuntimeError(
            f"Cannot access bucket '{bucket}'. "
            "Check AWS profile/credentials, bucket name, and region."
        ) from exc

    for csv_path in csv_paths:
        key = f"{prefix.rstrip('/')}/{csv_path.name}"
        log.info("Uploading s3://%s/%s ...", bucket, key)
        s3.upload_file(
            Filename=str(csv_path),
            Bucket=bucket,
            Key=key,
            ExtraArgs={"ContentType": "text/csv"},
        )

    log.info("Upload complete. %d files uploaded to s3://%s/%s/", len(csv_paths), bucket, prefix)


def list_uploaded_files(bucket: str, prefix: str, region: str, profile: str | None = None) -> None:
    """Print the files that are now in S3."""
    session = boto3.session.Session(profile_name=profile, region_name=region)
    s3 = session.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    print(f"\nFiles in s3://{bucket}/{prefix}/")
    print("-" * 60)
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            size_kb = obj["Size"] / 1024
            print(f"  {obj['Key']}  ({size_kb:.1f} KB)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload Olist dataset to S3.")
    parser.add_argument("--bucket", required=True, help="S3 bucket name (raw bucket).")
    parser.add_argument("--prefix", default="raw/olist", help="S3 key prefix.")
    parser.add_argument("--region", default="us-east-1", help="AWS region.")
    parser.add_argument(
        "--profile",
        default=None,
        help="AWS CLI profile name (optional). If omitted, uses default AWS credential chain.",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip Kaggle download; use --local-dir instead.",
    )
    parser.add_argument(
        "--local-dir",
        default=None,
        help="Local directory with already-extracted CSVs (used with --skip-download).",
    )
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = pathlib.Path(tmpdir)

        if args.skip_download:
            if not args.local_dir:
                parser.error("--local-dir is required when using --skip-download.")
            local = pathlib.Path(args.local_dir)
            csv_paths = list(local.rglob("*.csv"))
            log.info("Using local CSVs from %s (%d files).", local, len(csv_paths))
        else:
            zip_path = download_dataset(tmp)
            extract_dir = tmp / "extracted"
            extract_dir.mkdir()
            csv_paths = extract_csvs(zip_path, extract_dir)

        if not csv_paths:
            log.error("No CSV files found. Aborting.")
            raise SystemExit(1)

        upload_to_s3(
            csv_paths,
            bucket=args.bucket,
            prefix=args.prefix,
            region=args.region,
            profile=args.profile,
        )
        list_uploaded_files(args.bucket, args.prefix, args.region, args.profile)


if __name__ == "__main__":
    main()
