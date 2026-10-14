"""لایه‌ی Bucket (Railway Storage Bucket یا هر S3 سازگار).

اگر متغیرهای S3_* تنظیم نباشند، enabled() برابر False است و ربات مثل قبل مستقیم در تلگرام ارسال می‌کند.
Bucket خصوصی است؛ دانلود فقط با لینک موقت (presigned URL) انجام می‌شود.
"""
import asyncio
import logging
import mimetypes
import os
import secrets
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config

log = logging.getLogger("storage")

ENDPOINT = os.getenv("S3_ENDPOINT")
BUCKET = os.getenv("S3_BUCKET")
ACCESS_KEY = os.getenv("S3_ACCESS_KEY_ID")
SECRET_KEY = os.getenv("S3_SECRET_ACCESS_KEY")
REGION = os.getenv("S3_REGION", "auto")
ADDRESSING = os.getenv("S3_ADDRESSING", "virtual")  # برای Bucketهای قدیمی‌تر: path

LINK_TTL_HOURS = max(1, min(int(os.getenv("LINK_TTL_HOURS", "24")), 168))  # سقف SigV4: ۷ روز
PREFIX = "archive-bot/"

_client = None
_transfer = TransferConfig(
    multipart_threshold=64 * 1024 * 1024,
    multipart_chunksize=32 * 1024 * 1024,
    max_concurrency=4,
)


def enabled() -> bool:
    return _client is not None


def init() -> bool:
    global _client
    if not all([ENDPOINT, BUCKET, ACCESS_KEY, SECRET_KEY]):
        log.info("متغیرهای S3_* تنظیم نشده‌اند؛ ارسال مستقیم در تلگرام.")
        return False
    try:
        c = boto3.client(
            "s3",
            endpoint_url=ENDPOINT,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            region_name=REGION,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": ADDRESSING},
                retries={"max_attempts": 5, "mode": "standard"},
            ),
        )
        c.head_bucket(Bucket=BUCKET)
        _client = c
        log.info("Bucket متصل شد: %s (لینک‌ها %s ساعت معتبرند)", BUCKET, LINK_TTL_HOURS)
        return True
    except Exception:
        log.exception("اتصال به Bucket ناموفق بود؛ ارسال مستقیم در تلگرام.")
        _client = None
        return False


def _upload_and_link(path: Path, uid: int, name: str) -> str:
    key = f"{PREFIX}{uid}/{secrets.token_urlsafe(9)}/{name}"
    extra = {"ContentType": mimetypes.guess_type(name)[0] or "application/octet-stream"}
    _client.upload_file(str(path), BUCKET, key, ExtraArgs=extra, Config=_transfer)
    return _client.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": BUCKET,
            "Key": key,
            "ResponseContentDisposition": "attachment; filename*=UTF-8''" + quote(name),
        },
        ExpiresIn=LINK_TTL_HOURS * 3600,
    )


async def upload_and_link(path: Path, uid: int, name: str) -> str:
    """آپلود فایل در Bucket و برگرداندن لینک دانلود موقت."""
    return await asyncio.to_thread(_upload_and_link, path, uid, name)


def _cleanup() -> int:
    """حذف فایل‌هایی که از زمان انقضای لینکشان گذشته؛ بدون نیاز به دیتابیس (بعد از ری‌استارت هم کار می‌کند)."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=LINK_TTL_HOURS + 1)
    deleted = 0
    batch: list[dict] = []

    def flush():
        nonlocal deleted, batch
        if batch:
            _client.delete_objects(Bucket=BUCKET, Delete={"Objects": batch, "Quiet": True})
            deleted += len(batch)
            batch = []

    for page in _client.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            if obj["LastModified"] < cutoff:
                batch.append({"Key": obj["Key"]})
                if len(batch) >= 1000:
                    flush()
    flush()
    return deleted


async def janitor(every_sec: int = 900):
    if not enabled():
        return
    while True:
        try:
            n = await asyncio.to_thread(_cleanup)
            if n:
                log.info("%s فایل منقضی از Bucket حذف شد", n)
        except Exception:
            log.exception("پاک‌سازی Bucket ناموفق بود")
        await asyncio.sleep(every_sec)
