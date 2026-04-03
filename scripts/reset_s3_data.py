#!/usr/bin/env python3
"""
reset_s3_data.py
----------------
Delete pipeline data prefixes in S3 so the project can be re-run from scratch.

Supports three reset scopes:
  - raw   : remove raw CSV landing data (default prefix: raw/olist)
  - state : remove Auto Loader schema/checkpoint state (default prefix: state/autoloader/olist)
    - delta : remove Delta table data + _delta_log (default prefix: delta/olist)
    - all   : remove raw, state, and delta prefixes

Examples:
  # Preview only (recommended first)
  python scripts/reset_s3_data.py \
    --bucket <your-raw-bucket> \
    --mode all \
    --profile <aws-profile> \
    --dry-run

  # Delete raw data only
  python scripts/reset_s3_data.py \
    --bucket <your-raw-bucket> \
    --mode raw \
    --profile <aws-profile> \
    --yes

  # Delete Auto Loader state only
  python scripts/reset_s3_data.py \
    --bucket <your-raw-bucket> \
    --mode state \
    --profile <aws-profile> \
    --yes

    # Delete Delta table storage only
    python scripts/reset_s3_data.py \
        --bucket <your-raw-bucket> \
        --mode delta \
        --profile <aws-profile> \
        --yes
"""

from __future__ import annotations

import argparse
import logging
from typing import Iterable

import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError, ProfileNotFound

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def list_keys(s3_client, bucket: str, prefix: str) -> list[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend([obj["Key"] for obj in page.get("Contents", [])])
    return keys


def chunked(items: list[str], size: int) -> Iterable[list[str]]:
    for index in range(0, len(items), size):
        yield items[index:index + size]


def delete_keys(s3_client, bucket: str, keys: list[str]) -> int:
    deleted = 0
    for batch in chunked(keys, 1000):
        response = s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True},
        )
        deleted += len(response.get("Deleted", []))
    return deleted


def normalize_prefix(prefix: str) -> str:
    return prefix.strip().strip("/") + "/"


def purge_prefix(s3_client, bucket: str, prefix: str, dry_run: bool) -> None:
    keys = list_keys(s3_client, bucket, prefix)
    if not keys:
        log.info("No objects found under s3://%s/%s", bucket, prefix)
        return

    log.info("Found %d object(s) under s3://%s/%s", len(keys), bucket, prefix)
    if dry_run:
        preview = keys[:10]
        for key in preview:
            log.info("  [dry-run] would delete: s3://%s/%s", bucket, key)
        if len(keys) > len(preview):
            log.info("  [dry-run] ... and %d more", len(keys) - len(preview))
        return

    deleted = delete_keys(s3_client, bucket, keys)
    log.info("Deleted %d object(s) under s3://%s/%s", deleted, bucket, prefix)


def main() -> None:
    parser = argparse.ArgumentParser(description="Delete raw/state pipeline data in S3.")
    parser.add_argument("--bucket", required=True, help="S3 bucket to clean.")
    parser.add_argument(
        "--mode",
        choices=["raw", "state", "delta", "all"],
        required=True,
        help="Cleanup scope: raw, state, delta, or all.",
    )
    parser.add_argument(
        "--raw-prefix",
        default="raw/olist",
        help="Raw data prefix (default: raw/olist).",
    )
    parser.add_argument(
        "--state-prefix",
        default="state/autoloader/olist",
        help="Auto Loader state prefix (default: state/autoloader/olist).",
    )
    parser.add_argument(
        "--delta-prefix",
        default="delta/olist",
        help="Delta storage prefix (default: delta/olist).",
    )
    parser.add_argument("--region", default="us-east-1", help="AWS region.")
    parser.add_argument("--profile", default=None, help="AWS profile (optional).")
    parser.add_argument("--dry-run", action="store_true", help="Preview only; no deletion.")
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Required for real deletion (ignored with --dry-run).",
    )
    args = parser.parse_args()

    if not args.dry_run and not args.yes:
        parser.error("Refusing to delete without --yes. Use --dry-run to preview.")

    try:
        session = boto3.session.Session(profile_name=args.profile, region_name=args.region)
    except ProfileNotFound as exc:
        raise RuntimeError(f"AWS profile '{args.profile}' was not found.") from exc

    s3 = session.client("s3")

    try:
        s3.head_bucket(Bucket=args.bucket)
    except (NoCredentialsError, PartialCredentialsError) as exc:
        raise RuntimeError("AWS credentials not found. Configure credentials/profile first.") from exc
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        raise RuntimeError(f"Cannot access bucket '{args.bucket}' (AWS error: {code}).") from exc

    raw_prefix = normalize_prefix(args.raw_prefix)
    state_prefix = normalize_prefix(args.state_prefix)
    delta_prefix = normalize_prefix(args.delta_prefix)

    prefixes: list[str] = []
    if args.mode in {"raw", "all"}:
        prefixes.append(raw_prefix)
    if args.mode in {"state", "all"}:
        prefixes.append(state_prefix)
    if args.mode in {"delta", "all"}:
        prefixes.append(delta_prefix)

    log.info("Bucket         : s3://%s", args.bucket)
    log.info("Mode           : %s", args.mode)
    log.info("Dry run        : %s", args.dry_run)
    log.info("Target prefixes: %s", ", ".join(prefixes))

    for prefix in prefixes:
        purge_prefix(s3, args.bucket, prefix, args.dry_run)

    log.info("Done.")


if __name__ == "__main__":
    main()
