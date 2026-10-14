FROM python:3.11-slim

# unar و 7z برای استخراج ZIP/7z/TAR و ...؛ wget فقط برای دانلود rar در زمان build
RUN apt-get update \
    && apt-get install -y --no-install-recommends unar p7zip-full wget ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# rar / unrar رسمی (برای ساخت RAR و استخراج مطمئن RAR5).
# اگر دانلود نشد، build خراب نمی‌شود و فقط ساخت RAR غیرفعال می‌ماند.
# ورژن را در صورت نیاز با RAR_VERSION عوض کن (مثلاً 712 یا 720).
ARG RAR_VERSION=720
RUN if wget -q "https://www.rarlab.com/rar/rarlinux-x64-${RAR_VERSION}.tar.gz" -O /tmp/rar.tgz; then \
        (tar -xzf /tmp/rar.tgz -C /tmp \
         && install -m 755 /tmp/rar/rar /tmp/rar/unrar /usr/local/bin/) \
        || echo "WARNING: نصب rar ناموفق بود؛ ساخت RAR غیرفعال است"; \
    else \
        echo "WARNING: دانلود rar ناموفق بود؛ ساخت RAR غیرفعال است"; \
    fi; \
    rm -rf /tmp/rar /tmp/rar.tgz

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY bot.py db.py storage.py ./

CMD ["python", "bot.py"]
