import asyncio
import logging
import os
import re
import shutil
import subprocess
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

from telethon import Button, TelegramClient, events
from telethon.sessions import StringSession

import db
import storage

# ───────────────────────── تنظیمات ─────────────────────────
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]

MB = 1024 * 1024
# اگر Volume روی /data مونت شده باشد، پیش‌فرض همان‌جا کار می‌کنیم
WORK_DIR = Path(os.getenv("WORK_DIR") or ("/data/work" if Path("/data").is_dir() else "/tmp/archive-bot"))
MAX_FILE_MB = int(os.getenv("MAX_FILE_MB", "1500"))         # حداکثر حجم هر فایل ورودی
MAX_TOTAL_MB = int(os.getenv("MAX_TOTAL_MB", "1800"))       # حداکثر مجموع حجم برای ساخت آرشیو
MAX_EXTRACT_MB = int(os.getenv("MAX_EXTRACT_MB", "3000"))   # حداکثر حجم بعد از استخراج
MAX_SEND_FILES = int(os.getenv("MAX_SEND_FILES", "50"))     # حداکثر تعداد فایل ارسالی بعد از استخراج
RESERVE_MB = int(os.getenv("RESERVE_MB", "300"))            # فضای خالی که همیشه باید دست‌نخورده بماند
MAX_PARALLEL_JOBS = int(os.getenv("MAX_PARALLEL_JOBS", "2"))  # تعداد کار همزمان (کنترل مصرف دیسک/CPU)
DAILY_LIMIT_MB = int(os.getenv("DAILY_LIMIT_MB", "0"))      # سهمیه روزانه هر کاربر؛ 0 = بدون محدودیت (نیاز به دیتابیس)
SESSION_TTL = int(os.getenv("SESSION_TTL_MIN", "60")) * 60
# اگر Bucket فعال باشد، خروجی‌ها همیشه با لینک می‌آیند؛ با این گزینه فایل خود آرشیو هم (تا ۲ گیگ) در تلگرام ارسال می‌شود
SEND_IN_TELEGRAM = os.getenv("SEND_IN_TELEGRAM", "0") == "1"
TG_LIMIT = 2000 * MB                                        # سقف آپلود تلگرام برای ربات‌ها

ALLOWED_USERS = {int(x) for x in os.getenv("ALLOWED_USERS", "").replace(" ", "").split(",") if x}

HAVE_RAR = bool(shutil.which("rar"))
HAVE_UNRAR = bool(shutil.which("unrar"))
HAVE_UNAR = bool(shutil.which("unar"))
HAVE_7Z = bool(shutil.which("7z"))

# فایل‌هایی که از قبل فشرده‌اند؛ دوباره فشرده‌شون نمی‌کنیم (سریع‌تر و بدون فایده‌ی اضافه)
STORED_EXT = {
    ".zip", ".rar", ".7z", ".gz", ".bz2", ".xz", ".jpg", ".jpeg", ".png", ".webp", ".gif",
    ".mp3", ".aac", ".ogg", ".opus", ".m4a", ".mp4", ".mkv", ".webm", ".avi", ".mov", ".apk", ".pdf",
}
ARCHIVE_EXT = (".zip", ".rar", ".7z", ".tar", ".gz", ".tgz", ".bz2", ".xz", ".cab", ".iso")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("archive-bot")

client = TelegramClient(StringSession(), API_ID, API_HASH)
JOBS = asyncio.Semaphore(MAX_PARALLEL_JOBS)


# ───────────────────────── خطاها ─────────────────────────
class JobError(Exception):
    pass


class NeedPassword(JobError):
    pass


class LimitError(JobError):
    pass


# ───────────────────────── ابزارها ─────────────────────────
def human(n: int) -> str:
    size = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def safe_name(name: str) -> str:
    name = os.path.basename(name.replace("\\", "/"))
    name = re.sub(r'[\x00-\x1f<>:"|?*]', "_", name).strip(" .")
    if not name:
        return "file"
    if name[0] in "-@":  # جلوگیری از اشتباه گرفته شدن با سوییچ یا لیست‌فایل در rar/7z
        name = "_" + name
    return name


def free_bytes() -> int:
    return shutil.disk_usage(WORK_DIR).free


def enough_space(need: int) -> bool:
    return free_bytes() - RESERVE_MB * MB >= need


def dir_size(path: Path) -> int:
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total


def allowed(uid: int) -> bool:
    return not ALLOWED_USERS or uid in ALLOWED_USERS


async def quota_ok(uid: int, size: int) -> bool:
    if not DAILY_LIMIT_MB or not db.enabled():
        return True
    return (await db.used_today(uid)) + size <= DAILY_LIMIT_MB * MB


async def safe_edit(msg, text, **kw):
    try:
        await msg.edit(text, **kw)
    except Exception as e:  # MessageNotModified و مشابه
        log.debug("edit skipped: %s", e)


@dataclass
class Session:
    uid: int
    mode: str | None = None          # "zip" | "extract"
    files: list[str] = field(default_factory=list)
    total: int = 0
    archive_name: str = "archive"
    fmt: str = "zip"                 # "zip" | "rar"
    password: str | None = None
    awaiting: str | None = None      # "name" | "password" | "extract_pass"
    pending: Path | None = None      # آرشیو منتظر رمز
    pass_tries: int = 0
    busy: bool = False
    last: float = field(default_factory=time.time)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    @property
    def root(self) -> Path:
        return WORK_DIR / str(self.uid)

    @property
    def inp(self) -> Path:
        return self.root / "in"

    @property
    def out(self) -> Path:
        return self.root / "out"

    def reset(self, mode: str | None = None):
        shutil.rmtree(self.root, ignore_errors=True)
        self.inp.mkdir(parents=True, exist_ok=True)
        self.out.mkdir(parents=True, exist_ok=True)
        self.mode = mode
        self.files = []
        self.total = 0
        self.archive_name = "archive"
        self.fmt = "zip"
        self.password = None
        self.awaiting = None
        self.pending = None
        self.pass_tries = 0
        self.busy = False
        self.last = time.time()


sessions: dict[int, Session] = {}


def get_session(uid: int) -> Session:
    s = sessions.get(uid)
    if not s:
        s = sessions[uid] = Session(uid)
        s.reset()
    s.last = time.time()
    return s


def unique_name(s: Session, name: str) -> str:
    if name not in s.files:
        return name
    stem, ext = os.path.splitext(name)
    i = 2
    while f"{stem} ({i}){ext}" in s.files:
        i += 1
    return f"{stem} ({i}){ext}"


def incoming_name(event) -> str:
    f = event.file
    return safe_name(f.name or f"file_{event.id}{f.ext or ''}")


# ───────────────────────── منوها ─────────────────────────
def main_menu():
    return [
        [Button.inline("🗜 ساخت ZIP / RAR از چند فایل", b"mode_zip")],
        [Button.inline("📂 استخراج ZIP / RAR / 7z", b"mode_extract")],
    ]


def cancel_menu():
    return [[Button.inline("❌ لغو", b"cancel")]]


def zip_menu(s: Session):
    fmt_row = [Button.inline(("✅ " if s.fmt == "zip" else "") + "ZIP", b"fmt_zip")]
    if HAVE_RAR:
        fmt_row.append(Button.inline(("✅ " if s.fmt == "rar" else "") + "RAR", b"fmt_rar"))
    return [
        fmt_row,
        [
            Button.inline("🔑 رمز" + (" ✔" if s.password else ""), b"set_pass"),
            Button.inline("✏️ اسم فایل", b"set_name"),
        ],
        [Button.inline("🗜 ساخت آرشیو", b"build")],
        [Button.inline("❌ لغو", b"cancel")],
    ]


def status_text(s: Session) -> str:
    return (
        f"📦 {len(s.files)} فایل | حجم: {human(s.total)}\n"
        f"فرمت: {s.fmt.upper()} | اسم: {s.archive_name}.{s.fmt}\n"
        f"رمز: {'دارد 🔑' if s.password else 'ندارد'}"
    )


WELCOME = "سلام 👋\nمن ربات فشرده‌ساز و استخراج‌کننده‌ام.\n\nیکی از حالت‌ها را انتخاب کن:"


# ───────────────────────── اجرای ابزارهای خارجی ─────────────────────────
def run_monitored(cmd: list[str], watch: Path | None, limit: int, timeout: int = 3600, cwd=None):
    """اجرای دستور با نظارت بر حجم خروجی و فضای دیسک (ضد زیپ‌بمب)."""
    proc = subprocess.Popen(
        cmd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, errors="replace", cwd=cwd,
    )
    start = time.time()
    try:
        while True:
            try:
                out, err = proc.communicate(timeout=1)
                return proc.returncode, (out or "") + (err or "")
            except subprocess.TimeoutExpired:
                if watch is not None and dir_size(watch) > limit:
                    raise LimitError(f"حجم بعد از استخراج از سقف مجاز ({MAX_EXTRACT_MB}MB) بیشتر شد")
                if free_bytes() < (RESERVE_MB // 2) * MB:
                    raise LimitError("فضای دیسک سرور تمام شد")
                if time.time() - start > timeout:
                    raise JobError("زمان اجرا بیش از حد طول کشید")
    except BaseException:
        proc.kill()
        proc.communicate()
        raise


def _check(rc: int, text: str, ok_codes: tuple, tool: str):
    if rc in ok_codes:
        return
    low = text.lower()
    if "password" in low or (tool == "unrar" and rc == 11):
        raise NeedPassword()
    raise JobError((text.strip()[-200:]) or f"{tool} exit code {rc}")


# ───────────────────────── استخراج ─────────────────────────
def _x_unrar(archive: Path, dest: Path, password, limit):
    cmd = ["unrar", "x", "-y", "-idq", f"-p{password}" if password else "-p-", str(archive), str(dest) + "/"]
    rc, text = run_monitored(cmd, dest, limit)
    _check(rc, text, (0, 1), "unrar")


def _x_unar(archive: Path, dest: Path, password, limit):
    cmd = ["unar", "-D", "-f", "-o", str(dest)]
    if password:
        cmd += ["-p", password]
    cmd.append(str(archive))
    rc, text = run_monitored(cmd, dest, limit)
    _check(rc, text, (0,), "unar")


def _x_7z(archive: Path, dest: Path, password, limit):
    cmd = ["7z", "x", "-y", "-bd", f"-o{dest}"]
    if password:
        cmd.append(f"-p{password}")
    cmd.append(str(archive))
    rc, text = run_monitored(cmd, dest, limit)
    _check(rc, text, (0, 1), "7z")


def _x_zipfile(archive: Path, dest: Path, password, limit):
    try:
        with zipfile.ZipFile(archive) as z:
            total = sum(i.file_size for i in z.infolist())
            if total > limit:
                raise LimitError(f"حجم بعد از استخراج ({human(total)}) خیلی زیاد است")
            z.extractall(dest, pwd=password.encode() if password else None)
    except RuntimeError as e:
        if "password" in str(e).lower():
            raise NeedPassword() from e
        raise


def extract_archive(archive: Path, dest: Path, password: str | None):
    """چند ابزار را به ترتیب امتحان می‌کند تا یکی جواب بدهد."""
    limit = MAX_EXTRACT_MB * MB
    ext = archive.suffix.lower()
    strategies = []
    if ext == ".rar" and HAVE_UNRAR:
        strategies.append(("unrar", _x_unrar))  # بهترین گزینه برای RAR و RAR5
    if HAVE_UNAR:
        strategies.append(("unar", _x_unar))
    if HAVE_7Z:
        strategies.append(("7z", _x_7z))
    if zipfile.is_zipfile(archive):
        strategies.append(("zipfile", _x_zipfile))
    if not strategies:
        raise JobError("ابزار استخراج روی سرور نصب نیست")

    last: Exception | None = None
    for name, fn in strategies:
        shutil.rmtree(dest, ignore_errors=True)
        dest.mkdir(parents=True, exist_ok=True)
        try:
            fn(archive, dest, password, limit)
        except (NeedPassword, LimitError):
            raise
        except Exception as e:
            log.warning("%s failed: %s", name, e)
            last = e
            continue
        if any(p.is_file() for p in dest.rglob("*")):
            return
        last = JobError("آرشیو خالی بود")
    raise JobError(f"استخراج ناموفق بود: {last}")


# ───────────────────────── ساخت آرشیو ─────────────────────────
def make_archive(s: Session, out_path: Path):
    names, src = s.files, s.inp
    if s.fmt == "zip" and not s.password:
        with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6, allowZip64=True) as z:
            for n in names:
                comp = zipfile.ZIP_STORED if Path(n).suffix.lower() in STORED_EXT else zipfile.ZIP_DEFLATED
                z.write(src / n, arcname=n, compress_type=comp)
        return

    if s.fmt == "zip":  # ZIP رمزدار (AES-256) با 7z
        if not HAVE_7Z:
            raise JobError("ابزار 7z برای ZIP رمزدار نصب نیست")
        cmd = ["7z", "a", "-tzip", "-mem=AES256", f"-p{s.password}", "-bd", "-y", str(out_path), *names]
        ok = (0, 1)
        tool = "7z"
    else:  # RAR
        if not HAVE_RAR:
            raise JobError("ابزار rar روی سرور نصب نیست")
        cmd = ["rar", "a", "-idq", "-ep1", "-m3", "-y"]
        if s.password:
            cmd.append(f"-hp{s.password}")  # رمز روی محتوا و نام فایل‌ها
        cmd += [str(out_path), *names]
        ok = (0, 1)
        tool = "rar"

    rc, text = run_monitored(cmd, None, 0, cwd=str(src))
    if rc not in ok:
        raise JobError(text.strip()[-200:] or f"{tool} exit code {rc}")
    if not out_path.exists():
        raise JobError("فایل خروجی ساخته نشد")


# ───────────────────────── دستورات ─────────────────────────
@client.on(events.NewMessage(pattern=r"^/(start|help|menu)", func=lambda e: e.is_private))
async def on_start(event):
    uid = event.sender_id
    if not allowed(uid):
        return await event.reply("⛔ دسترسی نداری.")
    get_session(uid).reset()
    sender = await event.get_sender()
    await db.touch_user(uid, getattr(sender, "username", None), getattr(sender, "first_name", None))
    await event.reply(WELCOME, buttons=main_menu())


@client.on(events.NewMessage(pattern=r"^/cancel", func=lambda e: e.is_private))
async def on_cancel_cmd(event):
    s = get_session(event.sender_id)
    if s.busy:
        return await event.reply("⏳ در حال انجام کار هستم، بعداً لغو کن.")
    s.reset()
    await event.reply("لغو شد. هر وقت خواستی دوباره /start بزن.", buttons=main_menu())


@client.on(events.NewMessage(pattern=r"^/stats", func=lambda e: e.is_private))
async def on_stats(event):
    if not allowed(event.sender_id):
        return
    st = await db.stats(event.sender_id)
    if not st:
        return await event.reply("آماری موجود نیست.")
    await event.reply(
        "📊 آمار تو\n"
        f"آرشیو ساخته‌شده: {st['made']}\n"
        f"آرشیو استخراج‌شده: {st['extracted']}\n"
        f"حجم ورودی کل: {human(st['total_in'])}\n"
        f"حجم خروجی کل: {human(st['total_out'])}"
    )


@client.on(events.CallbackQuery)
async def on_callback(event):
    uid = event.sender_id
    if not allowed(uid):
        return await event.answer("دسترسی نداری", alert=True)
    s = get_session(uid)
    data = event.data

    if data in (b"mode_zip", b"mode_extract", b"cancel") and s.busy:
        return await event.answer("⏳ صبر کن تا کار قبلی تموم شه", alert=True)

    if data == b"mode_zip":
        s.reset("zip")
        pref = await db.get_format(uid)
        s.fmt = "rar" if (pref == "rar" and HAVE_RAR) else "zip"
        await event.edit(
            "🗜 حالت ساخت آرشیو\n\n"
            "فایل‌هایت را یکی‌یکی (یا چندتا با هم) بفرست.\n"
            "بعد فرمت، رمز و اسم فایل را تنظیم کن و «ساخت آرشیو» را بزن.",
            buttons=cancel_menu(),
        )

    elif data == b"mode_extract":
        s.reset("extract")
        await event.edit(
            "📂 حالت استخراج\n\n"
            "فایل ZIP / RAR / 7z / TAR را بفرست.\n"
            "اگر رمز دارد، می‌توانی رمز را در کپشن بنویسی؛ وگرنه خودم می‌پرسم.",
            buttons=cancel_menu(),
        )

    elif data == b"cancel":
        s.reset()
        await event.edit("لغو شد ✅", buttons=main_menu())

    elif data in (b"fmt_zip", b"fmt_rar"):
        if s.mode != "zip" or s.busy:
            return await event.answer()
        s.fmt = "rar" if data == b"fmt_rar" else "zip"
        await db.set_format(uid, s.fmt)
        await safe_edit(event, status_text(s), buttons=zip_menu(s))

    elif data == b"set_pass":
        if s.mode != "zip" or s.busy:
            return await event.answer()
        if s.password:
            s.password = None
            await event.answer("رمز حذف شد")
            return await safe_edit(event, status_text(s), buttons=zip_menu(s))
        s.awaiting = "password"
        await event.answer()
        await event.respond("🔑 رمز مورد نظر را در یک پیام بفرست (پیامت بعد از دریافت پاک می‌شود):")

    elif data == b"set_name":
        if s.mode != "zip" or s.busy:
            return await event.answer()
        s.awaiting = "name"
        await event.answer()
        await event.respond("✏️ اسم فایل آرشیو را بفرست (بدون پسوند):")

    elif data == b"build":
        await event.answer()
        await build(event, s)


# ───────────────────────── پیام‌های متنی ─────────────────────────
@client.on(
    events.NewMessage(
        func=lambda e: e.is_private and e.file is None and e.raw_text and not e.raw_text.startswith("/")
    )
)
async def on_text(event):
    uid = event.sender_id
    if not allowed(uid):
        return
    s = get_session(uid)
    text = event.raw_text.strip()

    if s.busy:
        return await event.reply("⏳ در حال انجام کار قبلی هستم، کمی صبر کن.")

    # رمز آرشیوی که منتظرش بودیم
    if s.awaiting == "extract_pass" and s.pending:
        try:
            await event.delete()
        except Exception:
            pass
        status = await event.respond("🔑 در حال امتحان رمز ...")
        return await run_extract(event.chat_id, s, status, text)

    if s.mode == "zip":
        if s.awaiting == "password":
            s.password = text[:100]
            s.awaiting = None
            try:
                await event.delete()
            except Exception:
                pass
            return await event.respond("🔑 رمز ثبت شد.\n\n" + status_text(s), buttons=zip_menu(s))
        name = safe_name(text)
        name = re.sub(r"\.(zip|rar)$", "", name, flags=re.I) or "archive"
        s.archive_name = name[:80]
        s.awaiting = None
        return await event.reply("✏️ اسم ثبت شد.\n\n" + status_text(s), buttons=zip_menu(s))

    await event.reply("از منوی زیر یکی را انتخاب کن 👇", buttons=main_menu())


# ───────────────────────── دریافت فایل ─────────────────────────
@client.on(events.NewMessage(func=lambda e: e.is_private and e.file is not None))
async def on_file(event):
    uid = event.sender_id
    if not allowed(uid):
        return await event.reply("⛔ دسترسی نداری.")
    s = get_session(uid)

    if not s.mode:
        return await event.reply("اول یکی از حالت‌ها را انتخاب کن 👇", buttons=main_menu())
    if s.busy:
        return await event.reply("⏳ در حال انجام کار قبلی هستم، کمی صبر کن.")

    size = event.file.size or 0
    if size > MAX_FILE_MB * MB:
        return await event.reply(f"❌ حجم فایل بیشتر از {MAX_FILE_MB}MB است.")
    if not await quota_ok(uid, size):
        return await event.reply(f"❌ سهمیه روزانه‌ات ({DAILY_LIMIT_MB}MB) تمام شده؛ فردا دوباره بیا.")

    async with s.lock:
        if s.mode == "zip":
            await add_to_zip(event, s)
        else:
            await do_extract(event, s)


async def add_to_zip(event, s: Session):
    size = event.file.size or 0
    if s.total + size > MAX_TOTAL_MB * MB:
        return await event.reply(
            f"❌ مجموع حجم از {MAX_TOTAL_MB}MB بیشتر می‌شود. با همین‌هایی که دارم آرشیو بساز.",
            buttons=zip_menu(s) if s.files else None,
        )
    # ورودی‌ها + خروجی (تقریباً هم‌اندازه‌ی ورودی‌ها) باید روی دیسک جا شوند
    if not enough_space((s.total + size) * 2):
        return await event.reply("❌ فضای دیسک سرور الان کافی نیست؛ چند دقیقه دیگه امتحان کن.")

    name = unique_name(s, incoming_name(event))
    status = await event.reply(f"⬇️ در حال دریافت: {name}")
    try:
        await event.download_media(file=str(s.inp / name))
    except Exception as e:
        log.exception("download failed")
        (s.inp / name).unlink(missing_ok=True)
        return await safe_edit(status, f"❌ دانلود ناموفق بود: {e}")

    s.files.append(name)
    s.total += (s.inp / name).stat().st_size
    await safe_edit(status, "✅ " + name + " اضافه شد\n\n" + status_text(s), buttons=zip_menu(s))


async def build(event, s: Session):
    if s.mode != "zip" or not s.files:
        return await event.respond("هنوز فایلی نفرستادی 🙂")
    if s.busy:
        return
    if s.fmt == "rar" and not HAVE_RAR:
        s.fmt = "zip"

    s.busy = True
    chat, uid = event.chat_id, s.uid
    fmt, nfiles, bytes_in = s.fmt, len(s.files), s.total
    t0 = time.time()
    status = await event.respond("⏳ در صف / در حال ساخت آرشیو ...")
    ok, err, size = False, None, 0
    try:
        async with JOBS:
            await safe_edit(status, f"🗜 در حال ساخت {fmt.upper()} ...")
            out_path = s.out / f"{s.archive_name}.{fmt}"
            await asyncio.to_thread(make_archive, s, out_path)
            size = out_path.stat().st_size
            caption = f"✅ {nfiles} فایل در یک {fmt.upper()}" + (" 🔑" if s.password else "")
            if storage.enabled():
                await safe_edit(status, f"☁️ در حال آپلود ({human(size)}) ...")
                url = await storage.upload_and_link(out_path, uid, out_path.name)
                await client.send_message(
                    chat,
                    f"{caption}\n📦 حجم: {human(size)}\n⏳ لینک تا {storage.LINK_TTL_HOURS} ساعت معتبر است.",
                    buttons=[[Button.url("⬇️ دانلود", url)]],
                )
                if SEND_IN_TELEGRAM and size <= TG_LIMIT:
                    await safe_edit(status, f"⬆️ در حال ارسال در تلگرام ({human(size)}) ...")
                    await client.send_file(chat, str(out_path), force_document=True, caption=caption)
            else:
                if size > TG_LIMIT:
                    raise JobError(f"حجم آرشیو ({human(size)}) از سقف تلگرام (۲ گیگ) بیشتر است")
                await safe_edit(status, f"⬆️ در حال ارسال ({human(size)}) ...")
                await client.send_file(chat, str(out_path), force_document=True, caption=caption)
            out_path.unlink(missing_ok=True)  # فضا را فوراً آزاد کن
        ok = True
        await status.delete()
        s.reset()
        await client.send_message(chat, "کار دیگه‌ای داری؟", buttons=main_menu())
    except Exception as e:
        err = str(e)
        log.exception("build failed")
        await safe_edit(status, f"❌ خطا: {e}", buttons=zip_menu(s))
    finally:
        s.busy = False
        await db.log_job(uid, "zip", fmt, nfiles, bytes_in, size, ok, err, time.time() - t0)


async def do_extract(event, s: Session):
    name = incoming_name(event)
    if not name.lower().endswith(ARCHIVE_EXT):
        return await event.reply("❌ این فایل آرشیو نیست (ZIP / RAR / 7z / TAR).")
    size = event.file.size or 0
    if not enough_space(size * 2):
        return await event.reply("❌ فضای دیسک سرور الان کافی نیست؛ چند دقیقه دیگه امتحان کن.")

    s.reset("extract")
    s.busy = True
    status = await event.reply(f"⬇️ در حال دریافت: {name}")
    try:
        archive = s.inp / name
        await event.download_media(file=str(archive))
        s.pending = archive
    except Exception as e:
        log.exception("download failed")
        s.reset("extract")
        return await safe_edit(status, f"❌ دانلود ناموفق بود: {e}")

    password = (event.raw_text or "").strip() or None
    await run_extract(event.chat_id, s, status, password)


async def run_extract(chat, s: Session, status, password: str | None):
    s.busy = True
    t0 = time.time()
    archive = s.pending
    bytes_in = archive.stat().st_size if archive and archive.exists() else 0
    ok, err, sent, bytes_out = False, None, 0, 0
    try:
        async with JOBS:
            dest = s.out / "x"
            await safe_edit(status, "📂 در حال استخراج ...")
            try:
                await asyncio.to_thread(extract_archive, archive, dest, password)
            except NeedPassword:
                s.pass_tries += 1
                if s.pass_tries > 5:
                    s.reset("extract")
                    return await safe_edit(status, "❌ رمز چند بار اشتباه بود. دوباره فایل را بفرست.")
                s.awaiting = "extract_pass"
                msg = "🔑 رمز اشتباه است. رمز درست را بفرست:" if password else "🔑 این آرشیو رمز دارد. رمز را بفرست:"
                return await safe_edit(status, msg, buttons=cancel_menu())

            archive.unlink(missing_ok=True)  # فضای آرشیو را زود آزاد کن
            s.awaiting = None

            files = sorted(p for p in dest.rglob("*") if p.is_file() and p.stat().st_size > 0)
            if not files:
                raise JobError("آرشیو خالی بود")

            to_send = files[:MAX_SEND_FILES]
            use_bucket = storage.enabled()
            await safe_edit(status, f"{'☁️ در حال آپلود' if use_bucket else '⬆️ در حال ارسال'} {len(to_send)} فایل ...")
            skipped = 0
            links: list[tuple[str, int, str]] = []
            for p in to_send:
                fsize = p.stat().st_size
                rel = str(p.relative_to(dest))
                try:
                    if use_bucket:
                        url = await storage.upload_and_link(p, s.uid, p.name)
                        links.append((rel, fsize, url))
                        if SEND_IN_TELEGRAM and fsize <= TG_LIMIT:
                            await client.send_file(chat, str(p), force_document=True, caption=rel[:1000])
                    elif fsize > TG_LIMIT:
                        skipped += 1
                        continue
                    else:
                        await client.send_file(chat, str(p), force_document=True, caption=rel[:1000])
                    sent += 1
                    bytes_out += fsize
                finally:
                    p.unlink(missing_ok=True)  # بلافاصله بعد از ارسال/آپلود پاک کن

            # لینک‌ها در پیام‌های ۱۰تایی (دکمه‌ی URL) فرستاده می‌شوند
            for i in range(0, len(links), 10):
                chunk = links[i:i + 10]
                await client.send_message(
                    chat,
                    f"📎 فایل‌های {i + 1} تا {i + len(chunk)} از {len(links)}\n"
                    f"⏳ لینک‌ها تا {storage.LINK_TTL_HOURS} ساعت معتبرند.",
                    buttons=[[Button.url(f"{rel[:45]} ({human(sz)})", url)] for rel, sz, url in chunk],
                )

            note = f"✅ {sent} فایل {'آپلود شد' if use_bucket else 'ارسال شد'}."
            if len(files) > MAX_SEND_FILES:
                note += f"\n⚠️ {len(files) - MAX_SEND_FILES} فایل دیگر به خاطر سقف تعداد ارسال نشد."
            if skipped:
                note += f"\n⚠️ {skipped} فایل بزرگ‌تر از ۲ گیگ بود و ارسال نشد."
            await safe_edit(status, note)
            ok = True
        s.reset("extract")
        await client.send_message(chat, "کار دیگه‌ای داری؟", buttons=main_menu())
    except JobError as e:
        err = str(e)
        s.reset("extract")
        await safe_edit(status, f"❌ {e}")
    except Exception as e:
        err = str(e)
        log.exception("extract failed")
        s.reset("extract")
        await safe_edit(status, f"❌ خطا: {e}")
    finally:
        s.busy = False
        if ok or err:  # درخواست رمز، خطا حساب نمی‌شود
            await db.log_job(s.uid, "extract", archive.suffix.lstrip(".") if archive else None,
                             sent, bytes_in, bytes_out, ok, err, time.time() - t0)


# ───────────────────────── پاک‌سازی ─────────────────────────
async def janitor():
    while True:
        await asyncio.sleep(300)
        now = time.time()
        for uid, s in list(sessions.items()):
            if not s.busy and now - s.last > SESSION_TTL:
                shutil.rmtree(s.root, ignore_errors=True)
                sessions.pop(uid, None)


def startup_cleanup():
    """باقی‌مانده‌ی کارهای نیمه‌تمام (مثلاً بعد از کرش) را پاک می‌کند."""
    shutil.rmtree(WORK_DIR, ignore_errors=True)
    WORK_DIR.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    startup_cleanup()
    log.info(
        "WORK_DIR=%s free=%s | rar=%s unrar=%s unar=%s 7z=%s",
        WORK_DIR, human(free_bytes()), HAVE_RAR, HAVE_UNRAR, HAVE_UNAR, HAVE_7Z,
    )
    client.loop.run_until_complete(db.init())
    storage.init()
    client.loop.run_until_complete(client.start(bot_token=BOT_TOKEN))
    client.loop.create_task(janitor())
    client.loop.create_task(storage.janitor())
    log.info("Bot is running ...")
    client.run_until_disconnected()
