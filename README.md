# 🌫️ Hanoi AQI Forecast — Django Dashboard

Hệ thống dự báo chất lượng không khí (AQI / PM2.5) tại Hà Nội sử dụng **17 mô hình Gradient Boosted Trees (GBT)** của PySpark, tích hợp vào web dashboard Django với dự báo nhiều khung giờ (1h → 49h tới).

---

## Mục lục

- [Tổng quan](#tổng-quan)
- [Cấu trúc dự án](#cấu-trúc-dự-án)
- [Yêu cầu hệ thống](#yêu-cầu-hệ-thống)
- [Cài đặt](#cài-đặt)
- [Cấu hình](#cấu-hình)
- [Chạy lần đầu](#chạy-lần-đầu)
- [Cách dùng](#cách-dùng)
- [Cập nhật dự báo tự động](#cập-nhật-dự-báo-tự-động)
- [Cấu trúc dữ liệu](#cấu-trúc-dữ-liệu)
- [Xử lý lỗi thường gặp](#xử-lý-lỗi-thường-gặp)

---

## Tổng quan

Dự án gồm 2 thành phần chính:

**`predict_service.py`** — Script độc lập (không chạy trong Django), đọc CSV lịch sử PM2.5 và chạy 17 mô hình GBT PySpark để tạo file `data/forecast.json`.

**Django App (`aqi_app`)** — Web dashboard đọc `forecast.json` và hiển thị dự báo theo từng khung giờ, kèm trang phân tích lịch sử (theo ngày/tháng/năm).

Luồng dữ liệu tổng thể:

```
Open-Meteo API → hanoi_aqi.csv → predict_service.py → forecast.json → Django Dashboard
```

---

## Cấu trúc dự án

```
aqi_prj/
├── predict_service.py          # Chạy độc lập, sinh ra forecast.json
├── manage.py
├── INTEGRATION.md              # Hướng dẫn tích hợp chi tiết
│
├── aqi_prj/                    # Django project config
│   ├── settings.py
│   ├── urls.py
│   ├── wsgi.py
│   └── asgi.py
│
├── aqi_app/                    # Django app chính
│   ├── models.py               # ForecastRun, ForecastSlot (PM2.5 → AQI)
│   ├── views.py                # Dashboard + Analytics views
│   ├── urls.py
│   ├── templates/aqi_app/
│   │   ├── dashboard.html      # Trang dự báo chính
│   │   └── analytics.html      # Trang thống kê lịch sử
│   └── management/commands/
│       ├── run_forecast.py     # python manage.py run_forecast
│       └── build_history.py   # python manage.py build_history
│
├── data/
│   ├── hanoi_aqi.csv           # Dữ liệu lịch sử (đặt thủ công)
│   ├── forecast.json           # Output của predict_service.py
│   ├── history_daily.json      # Lịch sử theo ngày (sinh tự động)
│   └── analytics_meta.json     # Thống kê theo tháng/năm
│
├── models/                     # 17 mô hình GBT PySpark
│   ├── gbt_k1/                 # Dự báo 1h tới
│   ├── gbt_k4/                 # Dự báo 4h tới
│   ├── ...                     # (k = 1, 4, 7, ..., 49)
│   ├── gbt_k49/                # Dự báo 49h tới
│   ├── features_k1.json        # Danh sách features cho horizon k=1
│   └── features_k49.json       # ...
│
└── db.sqlite3
```

---

## Yêu cầu hệ thống

| Thành phần | Phiên bản |
|---|---|
| Python | 3.11+ |
| Java | JDK 11+ (bắt buộc cho PySpark) |
| Django | 5.x |
| PySpark | 3.5.0 (khớp với version train trên Colab) |
| pandas | bất kỳ |
| requests | bất kỳ |

> **Windows:** Cần cài thêm Hadoop winutils. Xem [Cấu hình](#cấu-hình).

---

## Cài đặt

**1. Clone / giải nén project**

```bash
unzip aqi_prj.zip
cd aqi_prj
```

**2. Tạo môi trường ảo**

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux / macOS
source venv/bin/activate
```

**3. Cài dependencies**

```bash
pip install django pyspark==3.5.0 pandas requests
```

Kiểm tra PySpark version bạn đã dùng trên Colab:

```python
import pyspark; print(pyspark.__version__)
```

**4. Copy models vào project**

```bash
# Copy toàn bộ thư mục models đã train (gbt_k1 đến gbt_k49)
# vào thư mục aqi_prj/models/
```

Sau khi copy, cấu trúc phải có đủ:

```
models/
├── gbt_k1/     gbt_k4/     ... gbt_k49/
├── features_k1.json        ... features_k49.json
```

**5. Khởi tạo database**

```bash
python manage.py migrate
```

---

## Cấu hình

### Windows — Hadoop winutils

`predict_service.py` mặc định trỏ đến `D:\hadoop`. Nếu bạn đặt Hadoop ở chỗ khác, sửa các dòng đầu file:

```python
os.environ["HADOOP_HOME"] = "D:\\hadoop"          # sửa đường dẫn
os.environ["JAVA_HOME"]   = "C:\\Program Files\\Java\\jdk-11"  # sửa đường dẫn
```

### Linux / macOS

Xoá hoặc comment các dòng `os.environ["HADOOP_HOME"]` và `os.environ["JAVA_HOME"]` — PySpark tự tìm Java qua `PATH` trên Linux/macOS.

### Django settings

File `aqi_prj/settings.py`. Trước khi deploy production cần:

```python
DEBUG = False
ALLOWED_HOSTS = ['your-domain.com']
SECRET_KEY = 'thay-bang-key-ngau-nhien-khac'
```

---

## Chạy lần đầu

### Bước 1 — Đặt CSV lịch sử

Đặt file CSV vào `data/hanoi_aqi.csv`. Các cột bắt buộc:

```
Local_Time, PM25, Temperature, Wind_Speed, Relative_Humidity,
Pressure, Precipitation, BLH, Wind_Direction, NO2, PM10, Clouds
```

### Bước 2 — Chạy dự báo

```bash
python predict_service.py
```

### Bước 3 — Build lịch sử

```bash
python manage.py build_history --days 90
```

### Bước 4 — Khởi động Django

```bash
python manage.py runserver
```

Mở trình duyệt: [http://127.0.0.1:8000](http://127.0.0.1:8000)

---

## Cách dùng

### Dashboard (`/`)

Hiển thị dự báo PM2.5 và AQI cho 17 khung giờ tiếp theo (1h, 4h, 7h, … 49h), kèm màu cảnh báo theo thang AQI.

### Trang phân tích (`/analytics/`)

Biểu đồ thống kê lịch sử AQI theo ngày, tháng, năm — lấy từ `data/analytics_meta.json` và `data/history_daily.json`.

### Management commands

```bash
# Cập nhật dự báo (chạy predict_service + build_history)
python manage.py run_forecast

# Chỉ build lại lịch sử từ CSV (không cần PySpark)
python manage.py build_history --days 90
```

---

## Cập nhật dự báo tự động

### Cách 1 — Cron (Linux/macOS)

```bash
crontab -e

# Chạy lúc 6h sáng mỗi ngày
0 6 * * * cd /path/to/aqi_prj && python predict_service.py >> logs/predict.log 2>&1
```

### Cách 2 — Task Scheduler (Windows)

1. Mở **Task Scheduler** → **Create Basic Task**
2. Trigger: Daily, 6:00 AM
3. Action: `python C:\path\to\aqi_prj\predict_service.py`

### Cập nhật CSV từ Open-Meteo

`predict_service.py` có hàm `ensure_csv_fresh()` tự crawl Open-Meteo khi CSV thiếu hơn 1 giờ so với thời điểm hiện tại. Script chỉ đọc 400 hàng cuối CSV nên không chậm dù file rất dài.

---

## Cấu trúc dữ liệu

### `data/forecast.json`

```json
[
  {
    "k": 1,
    "forecast_dt": "2025-04-18T07:00:00",
    "pm25": 28.3,
    "aqi": 84
  },
  ...
]
```

### Tính AQI từ PM2.5

Dự án tự tính AQI theo breakpoint chuẩn US EPA trong `ForecastSlot.save()`:

| PM2.5 (µg/m³) | AQI |
|---|---|
| 0.0 – 12.0 | 0 – 50 (Tốt) |
| 12.1 – 35.4 | 51 – 100 (Trung bình) |
| 35.5 – 55.4 | 101 – 150 (Không tốt cho nhóm nhạy cảm) |
| 55.5 – 150.4 | 151 – 200 (Không tốt) |
| 150.5 – 250.4 | 201 – 300 (Rất không tốt) |
| 250.5 – 500.4 | 301 – 500 (Nguy hiểm) |

---

## Xử lý lỗi thường gặp

**"Thiếu model/features cho K=X"**
→ Kiểm tra `models/gbt_kX/` và `models/features_kX.json` đã copy đúng vị trí chưa.

**"Java not found" khi chạy PySpark**
→ Cài Java 11+: [https://www.java.com/download/](https://www.java.com/download/), sau đó kiểm tra `java -version`.

**Dữ liệu hiển thị là fallback (rolling mean)**
→ PySpark không chạy được. Kiểm tra: `python -c "import pyspark; print(pyspark.__version__)"`.

**Column mismatch**
→ Tên cột CSV phải khớp chính xác, ví dụ `Local_Time` (không phải `datetime` hay `time`).

**CSV không cập nhật dù đã crawl**
→ Kiểm tra kết nối mạng đến Open-Meteo API, và đảm bảo `cutoff_ts` tính đúng múi giờ địa phương.
