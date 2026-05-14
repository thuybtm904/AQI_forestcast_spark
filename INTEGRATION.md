# Hướng dẫn tích hợp model GBT vào Django

## Cấu trúc sau khi setup

```
aqi_project/
├── predict_service.py          ← chạy độc lập, tạo ra forecast.json
├── data/
│   ├── hanoi_aqi.csv           ← CSV lịch sử của bạn (đặt vào đây)
│   └── forecast.json           ← tự động tạo sau khi chạy predict
├── models/
│   ├── gbt_k1/                 ← copy từ C:\Users\HP One\Downloads\models\
│   ├── gbt_k4/
│   ├── ... (17 folders)
│   ├── features_k1.json
│   └── ... (17 json files)
└── aqi_app/
    ├── views.py                ← đọc forecast.json, render dashboard
    └── management/commands/
        └── run_forecast.py     ← python manage.py run_forecast
```

---

## Bước 1: Copy models vào đúng chỗ

```
# Copy toàn bộ thư mục models từ Downloads vào project
# Từ: C:\Users\HP One\Downloads\models\
# Vào: aqi_project\models\

# Sau khi copy xong kiểm tra:
ls aqi_project/models/
# Phải thấy: gbt_k1/ gbt_k4/ ... features_k1.json features_k4.json ...
```

---

## Bước 2: Copy CSV vào data/

```
# Đặt file CSV lịch sử vào:
aqi_project/data/hanoi_aqi.csv

# Cột bắt buộc phải có:
Local_Time, PM25, Temperature, Wind_Speed, Relative_Humidity,
Pressure, Precipitation, BLH, Wind_Direction, NO2, PM10, Clouds
```

---

## Bước 3: Cài PySpark (nếu chưa có)

```bash
pip install pyspark==3.5.0
# hoặc đúng version bạn đã train:
pip install pyspark==<version_bạn_dùng_trên_colab>
```

Kiểm tra version trên Colab:
```python
import pyspark; print(pyspark.__version__)
```

---

## Bước 4: Chạy predict lần đầu

```bash
cd aqi_project
python predict_service.py
```

Output mong đợi:
```
[2025-04-18 06:00] Đọc CSV: data/hanoi_aqi.csv
  387 hàng, cột: [Local_Time, PM25, ...]
  Thời điểm cuối: 2025-04-18 05:00:00

Chạy predictions cho 17 horizons...
  K= 1h → PM2.5=28.3 µg/m³  AQI=84
  K= 4h → PM2.5=31.1 µg/m³  AQI=91
  ...

✅ Đã ghi 16 slots → data/forecast.json
```

---

## Bước 5: Chạy Django

```bash
python manage.py runserver
```

Mở http://127.0.0.1:8000 — dashboard hiện ra với data thật.

---

## Cập nhật dự báo mỗi ngày

### Cách 1: Chạy thủ công
```bash
python predict_service.py
# hoặc từ Django
python manage.py run_forecast
```

### Cách 2: Cron tự động (Linux/Mac)
```bash
crontab -e
# Thêm dòng này (chạy lúc 6h sáng mỗi ngày):
0 6 * * * cd /path/to/aqi_project && python predict_service.py >> logs/predict.log 2>&1
```

### Cách 3: Task Scheduler (Windows)
- Mở Task Scheduler → Create Basic Task
- Trigger: Daily, 6:00 AM
- Action: Start a program
- Program: `python`
- Arguments: `C:\path\to\aqi_project\predict_service.py`

---

## Cập nhật CSV định kỳ (crawl Open-Meteo)

Khi bạn crawl data mới từ Open-Meteo, chỉ cần:
1. Append vào `data/hanoi_aqi.csv` (giữ nguyên cấu trúc cột)
2. Chạy lại `python predict_service.py`

`predict_service.py` chỉ đọc 400 hàng cuối nên không chậm dù CSV rất dài.

---

## Xử lý lỗi thường gặp

### "Thiếu model/features cho K=X"
→ Kiểm tra folder `models/gbt_kX` và file `models/features_kX.json` đã copy đúng chưa.

### "Java not found" khi chạy PySpark
→ Cài Java 11+: https://www.java.com/download/

### Dữ liệu hiển thị là fallback (rolling mean)
→ PySpark chưa cài hoặc lỗi. Kiểm tra: `python -c "import pyspark"`

### Column mismatch
→ Tên cột CSV phải khớp chính xác. Kiểm tra: `Local_Time` (không phải `datetime`)
