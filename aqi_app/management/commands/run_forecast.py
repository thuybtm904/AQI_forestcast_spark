"""
aqi_app/management/commands/run_forecast.py

Chạy từ Django:
    python manage.py run_forecast

Hoặc thêm vào cron:
    0 6 * * * cd /path/to/project && python manage.py run_forecast
"""

import subprocess, sys
from pathlib import Path
from django.core.management.base import BaseCommand
from django.core.management import call_command


class Command(BaseCommand):
    help = "Chạy predict_service.py để cập nhật dự báo AQI, sau đó build history"

    def handle(self, *args, **options):
        # 1. Chạy predict_service.py
        script = Path(__file__).resolve().parents[4] / "predict_service.py"
        if not script.exists():
            self.stderr.write(f"Không tìm thấy: {script}")
            return

        self.stdout.write(f"Đang chạy: {script}")
        result = subprocess.run(
            [sys.executable, str(script)],
            capture_output=True, text=True
        )
        self.stdout.write(result.stdout)
        if result.returncode != 0:
            self.stderr.write(result.stderr)
            self.stderr.write(" Predict thất bại")
            return

        self.stdout.write(self.style.SUCCESS(" Forecast cập nhật xong"))

        # 2. Build history_daily.json từ CSV
        self.stdout.write("📊 Đang build history_daily.json ...")
        call_command("build_history", days=90)