"""لایه‌ی اختیاری دیتابیس (PostgreSQL روی Railway).

اگر DATABASE_URL تنظیم نباشد، همه‌ی توابع بدون خطا هیچ‌کاری نمی‌کنند و ربات عادی کار می‌کند.
دیتابیس فقط اطلاعات سبک نگه می‌دارد (کاربران، تنظیمات، لاگ کارها، سهمیه روزانه)؛
خود فایل‌ها هیچ‌وقت داخل دیتابیس ذخیره نمی‌شوند.
"""
import functools
import logging
import os

import asyncpg

log = logging.getLogger("db")

DATABASE_URL = os.getenv("DATABASE_URL")
_pool: asyncpg.Pool | None = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id        BIGINT PRIMARY KEY,
    username       TEXT,
    first_name     TEXT,
    default_format TEXT NOT NULL DEFAULT 'zip',
    first_seen     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS jobs (
    id         BIGSERIAL PRIMARY KEY,
    user_id    BIGINT NOT NULL,
    kind       TEXT NOT NULL,
    fmt        TEXT,
    files      INT NOT NULL DEFAULT 0,
    bytes_in   BIGINT NOT NULL DEFAULT 0,
    bytes_out  BIGINT NOT NULL DEFAULT 0,
    ok         BOOLEAN NOT NULL,
    error      TEXT,
    seconds    REAL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS jobs_user_day ON jobs (user_id, created_at);
"""


def enabled() -> bool:
    return _pool is not None


async def init() -> bool:
    global _pool
    if not DATABASE_URL:
        log.info("DATABASE_URL تنظیم نشده؛ ربات بدون دیتابیس اجرا می‌شود.")
        return False
    try:
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3, command_timeout=15)
        async with _pool.acquire() as c:
            await c.execute(SCHEMA)
        log.info("اتصال به دیتابیس برقرار شد.")
        return True
    except Exception:
        log.exception("اتصال به دیتابیس ناموفق بود؛ ادامه بدون دیتابیس.")
        _pool = None
        return False


def safe(default=None):
    """هر خطای دیتابیس نباید ربات را از کار بیندازد."""

    def deco(fn):
        @functools.wraps(fn)
        async def wrapper(*a, **kw):
            if _pool is None:
                return default
            try:
                return await fn(*a, **kw)
            except Exception:
                log.exception("db error in %s", fn.__name__)
                return default

        return wrapper

    return deco


@safe()
async def touch_user(uid: int, username: str | None, first_name: str | None):
    await _pool.execute(
        """
        INSERT INTO users (user_id, username, first_name) VALUES ($1, $2, $3)
        ON CONFLICT (user_id) DO UPDATE
           SET username = EXCLUDED.username,
               first_name = EXCLUDED.first_name,
               last_seen = now()
        """,
        uid, username, first_name,
    )


@safe("zip")
async def get_format(uid: int) -> str:
    v = await _pool.fetchval("SELECT default_format FROM users WHERE user_id = $1", uid)
    return v or "zip"


@safe()
async def set_format(uid: int, fmt: str):
    await _pool.execute(
        """
        INSERT INTO users (user_id, default_format) VALUES ($1, $2)
        ON CONFLICT (user_id) DO UPDATE SET default_format = EXCLUDED.default_format
        """,
        uid, fmt,
    )


@safe()
async def log_job(uid, kind, fmt, files, bytes_in, bytes_out, ok, error, seconds):
    await _pool.execute(
        """
        INSERT INTO jobs (user_id, kind, fmt, files, bytes_in, bytes_out, ok, error, seconds)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """,
        uid, kind, fmt, files, bytes_in, bytes_out, ok, (error or "")[:300] or None, seconds,
    )


@safe(0)
async def used_today(uid: int) -> int:
    """مجموع حجم ورودی کارهای موفق امروز (UTC)."""
    return int(
        await _pool.fetchval(
            """
            SELECT COALESCE(SUM(bytes_in), 0) FROM jobs
            WHERE user_id = $1 AND ok AND created_at >= date_trunc('day', now())
            """,
            uid,
        )
    )


@safe()
async def stats(uid: int) -> dict | None:
    row = await _pool.fetchrow(
        """
        SELECT
          COUNT(*) FILTER (WHERE ok AND kind = 'zip')     AS made,
          COUNT(*) FILTER (WHERE ok AND kind = 'extract') AS extracted,
          COALESCE(SUM(bytes_in)  FILTER (WHERE ok), 0)   AS total_in,
          COALESCE(SUM(bytes_out) FILTER (WHERE ok), 0)   AS total_out
        FROM jobs WHERE user_id = $1
        """,
        uid,
    )
    return dict(row) if row else None
