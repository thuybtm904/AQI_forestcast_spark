from django.db import models

class ForecastRun(models.Model):
    """Mỗi lần chạy model GBT từ Colab tạo 1 record này."""
    created_at = models.DateTimeField(auto_now_add=True)
    note       = models.CharField(max_length=200, blank=True)

    class Meta:
        ordering = ['-created_at']

    def __str__(self):
        return f"Run {self.id} – {self.created_at.strftime('%d/%m/%Y %H:%M')}"


class ForecastSlot(models.Model):
    """
    Một khung giờ dự báo PM2.5.
    Model GBT output ra pm25, Django tự tính AQI khi lưu.
    """
    run         = models.ForeignKey(ForecastRun, on_delete=models.CASCADE,
                                    related_name='slots')
    forecast_dt = models.DateTimeField(help_text="Thời điểm được dự báo")
    pm25        = models.FloatField(help_text="Giá trị PM2.5 từ model (µg/m³)")
    aqi         = models.IntegerField(default=0, help_text="Tự tính khi save")

    class Meta:
        ordering = ['forecast_dt']

    # ── PM2.5 → AQI tự động khi save ──────────────────────────────────
    def save(self, *args, **kwargs):
        self.aqi = self._calc_aqi(self.pm25)
        super().save(*args, **kwargs)

    @staticmethod
    def _calc_aqi(pm):
        bps = [
            (0.0,  12.0,   0,  50),
            (12.1, 35.4,  51, 100),
            (35.5, 55.4, 101, 150),
            (55.5, 150.4,151, 200),
            (150.5,250.4,201, 300),
            (250.5,500.4,301, 500),
        ]
        for c_lo, c_hi, i_lo, i_hi in bps:
            if c_lo <= pm <= c_hi:
                return round((i_hi-i_lo)/(c_hi-c_lo)*(pm-c_lo)+i_lo)
        return 500

    def __str__(self):
        return f"{self.forecast_dt.strftime('%d/%m %Hh')} – PM2.5={self.pm25}, AQI={self.aqi}"
