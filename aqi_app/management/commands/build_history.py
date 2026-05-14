"""
aqi_app/management/commands/build_history.py

Đọc data/hanoi_aqi.csv → tính trung bình ngày → ghi data/history_daily.json

Chạy thủ công:
    python manage.py build_history
    python manage.py build_history --days 180

Tự động chạy sau run_forecast (đã tích hợp).
"""

import csv
import json
from collections import defaultdict
from pathlib import Path

from django.core.management.base import BaseCommand

BASE_DIR    = Path(__file__).resolve().parents[3]
CSV_PATH    = BASE_DIR / "data" / "hanoi_aqi.csv"
OUTPUT_PATH = BASE_DIR / "data" / "history_daily.json"

FIELDS = ["AQI", "PM25", "PM10", "NO2", "O3", "CO", "SO2"]


def safe_float(val):
    try:
        v = float(val)
        return v if v == v else None   # loại NaN
    except (ValueError, TypeError):
        return None


def avg(lst):
    vals = [v for v in lst if v is not None]
    return round(sum(vals) / len(vals), 1) if vals else None


class Command(BaseCommand):
    help = "Build history_daily.json từ hanoi_aqi.csv"

    def add_arguments(self, parser):
        parser.add_argument(
            "--days", type=int, default=9999,
            help="Số ngày gần nhất cần giữ lại (default: tất cả)"
        )

    def handle(self, *args, **options):
        if not CSV_PATH.exists():
            self.stderr.write(f"❌ Không tìm thấy: {CSV_PATH}")
            return

        self.stdout.write(f"📂 Đọc {CSV_PATH.name} ...")

        # Đọc CSV, gom theo ngày (dùng Local_Time)
        daily = defaultdict(lambda: {f: [] for f in FIELDS})
        with open(CSV_PATH, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                date_str = row.get("Local_Time", "").strip()[:10]
                if not date_str or date_str < "2000-01-01":
                    continue
                for field in FIELDS:
                    v = safe_float(row.get(field, ""))
                    if v is not None:
                        daily[date_str][field].append(v)

        # Tính trung bình, sắp xếp theo ngày, lấy N ngày gần nhất
        n_days = options["days"]
        dates  = sorted(daily.keys())[-n_days:]

        result = []
        for d in dates:
            entry = {"date": d}
            for field in FIELDS:
                entry[field] = avg(daily[d][field])
            result.append(entry)

        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False)

        self.stdout.write(
            self.style.SUCCESS(
                f"✅ Đã ghi {len(result)} ngày → {OUTPUT_PATH.name}"
            )
        )