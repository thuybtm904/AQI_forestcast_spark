import json
import sys
from pathlib import Path
from datetime import datetime, timedelta
from django.shortcuts import render

BASE_DIR      = Path(__file__).resolve().parent.parent
FORECAST_JSON = BASE_DIR / "data" / "forecast.json"
HISTORY_JSON  = BASE_DIR / "data" / "history_daily.json"
CSV_PATH      = BASE_DIR / "data" / "hanoi_aqi.csv"

# Thêm thư mục gốc project vào sys.path để import predict_service
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# ── Cache Spark session + model k1 (khởi động 1 lần, tái dùng mọi request) ────
_spark_session = None   # SparkSession dùng chung
_model_k1      = None   # GBTRegressionModel cho K=1
_weather_cache = {"ts": None, "data": {}}   # cache weather forecast 30 phút

# ── Cache dashboard render context (tránh rebuild mỗi request) ────────────────
_dashboard_cache = {
    "ts":   None,   # thời điểm cache được tạo
    "ctx":  None,   # context dict đã build sẵn
}
DASHBOARD_CACHE_SECONDS = 300   # 5 phút — đổi sang 60 nếu muốn tươi hơn

# ── Throttle ensure_forecast_fresh (chỉ check file mỗi N giây) ───────────────
_forecast_check_cache = {"ts": None}
FORECAST_CHECK_INTERVAL = 300   # 5 phút giữa hai lần check file

def _get_spark():
    """Trả về SparkSession đang chạy, tạo mới nếu chưa có."""
    global _spark_session
    if _spark_session is not None:
        try:
            # Kiểm tra session còn sống không
            _ = _spark_session.sparkContext.statusTracker()
            return _spark_session
        except Exception:
            _spark_session = None

    try:
        from pyspark.sql import SparkSession
        _spark_session = SparkSession.builder \
            .appName("AQI_NextHour") \
            .master("local[2]") \
            .config("spark.driver.memory", "1g") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.hadoop.io.native.lib.available", "false") \
            .getOrCreate()
        _spark_session.sparkContext.setLogLevel("ERROR")
        print("🚀 Spark session khởi động (sẽ tái dùng cho các request sau)")
    except ImportError:
        _spark_session = None
    return _spark_session

def _get_model_k1():
    """Load GBT model K=1 một lần, cache vào memory."""
    global _model_k1
    if _model_k1 is not None:
        return _model_k1

    import predict_service
    spark = _get_spark()
    if spark is None:
        return None

    try:
        from pyspark.ml.regression import GBTRegressionModel
        model_path = str(predict_service.MODELS_DIR / "gbt_k1").replace("\\", "/")
        _model_k1 = GBTRegressionModel.load(model_path)
        print("📦 Model K=1 đã load vào cache")
    except Exception as e:
        print(f"⚠️  Không load được model K=1: {e}")
        _model_k1 = None
    return _model_k1

def _get_weather_forecast():
    """Cache weather forecast 30 phút — tránh gọi API mỗi request."""
    import predict_service
    now = datetime.now()
    if _weather_cache["ts"] and (now - _weather_cache["ts"]).total_seconds() < 1800:
        return _weather_cache["data"]
    data = predict_service.get_weather_forecast()
    _weather_cache["ts"]   = now
    _weather_cache["data"] = data
    return data



# ── Đọc forecast.json ─────────────────────────────────────────────────────────

def load_forecast():
    if not FORECAST_JSON.exists():
        return None, None, "Chưa có dữ liệu — hãy chạy predict_service.py"

    with open(FORECAST_JSON, encoding="utf-8") as f:
        data = json.load(f)

    generated_at = data.get("generated_at", "")
    slots = data.get("slots", [])

    for s in slots:
        if "display_dt" not in s:
            dt = datetime.fromisoformat(s["forecast_dt"])
            s["display_dt"]   = dt.strftime("%d/%m %Hh")
            s["display_full"] = dt.strftime("%A, %d/%m/%Y %H:%M")

    return slots, generated_at, None


# ── Tự động chạy predict nếu forecast.json cũ / chưa có ─────────────────────

FORECAST_RUN_HOUR = 0  # chạy lại sau 0h (midnight) mỗi ngày

def ensure_forecast_fresh():
    global _forecast_check_cache

    now = datetime.now()
    last_check = _forecast_check_cache["ts"]
    if last_check is not None:
        elapsed = (now - last_check).total_seconds()
        if elapsed < FORECAST_CHECK_INTERVAL:
            return
    _forecast_check_cache["ts"] = now

    need_run = False

    if not FORECAST_JSON.exists():
        need_run = True
    else:
        try:
            with open(FORECAST_JSON, encoding="utf-8") as f:
                data = json.load(f)
            gen_dt = datetime.fromisoformat(data.get("generated_at", ""))
            today_midnight = now.replace(hour=FORECAST_RUN_HOUR,
                                         minute=0, second=0, microsecond=0)
            if gen_dt < today_midnight:
                need_run = True
        except Exception:
            need_run = True

    if need_run:
        _dashboard_cache["ts"]  = None
        _dashboard_cache["ctx"] = None
        try:
            import predict_service
            predict_service.ensure_csv_fresh()
            predict_service.run()
        except Exception as e:
            print(f"[ensure_forecast_fresh] Lỗi: {e}")


# ── Tính AQI giờ tiếp theo ────────────────────────────────────────────────────

def get_next_hour_aqi(slots: list) -> dict:
    now       = datetime.now()
    next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    next_iso  = next_hour.isoformat()

    for slot in slots:
        slot_dt_str = slot.get("forecast_dt", "")
        try:
            slot_dt = datetime.fromisoformat(slot_dt_str)
            if slot_dt.year  == next_hour.year  and \
               slot_dt.month == next_hour.month and \
               slot_dt.day   == next_hour.day   and \
               slot_dt.hour  == next_hour.hour:
                return {
                    "aqi":      slot["aqi"],
                    "pm25":     slot["pm25"],
                    "label":    slot["label"],
                    "css_class":slot["css_class"],
                    "color":    slot["color"],
                    "hour_str": next_hour.strftime("%H:00"),
                    "source":   "slot",
                    "forecast_dt": next_iso,
                }
        except Exception:
            continue

    return _predict_next_hour_realtime(next_hour)


def _predict_next_hour_realtime(next_hour: datetime) -> dict:
    import predict_service

    try:
        import pandas as pd
        import json, math

        predict_service.ensure_csv_fresh()

        df = pd.read_csv(CSV_PATH)
        df["Local_Time"] = pd.to_datetime(df["Local_Time"])
        df = df.rename(columns={"Local_Time": "ts"})
        df = df.sort_values("ts").tail(400)

        if len(df) < 10:
            raise ValueError("Không đủ data để dự báo")

        K = 1
        feat_path = predict_service.MODELS_DIR / f"features_k{K}.json"
        with open(feat_path) as f:
            features_k1 = json.load(f)

        weather_forecast = _get_weather_forecast()

        row_pd = predict_service.build_features(df, K, features_k1, weather_forecast)
        row_pd = row_pd.fillna(0.0)

        spark = _get_spark()
        model = _get_model_k1()

        if spark is not None and model is not None:
            from pyspark.ml.feature import VectorAssembler

            row_spark = spark.createDataFrame(row_pd)
            assembler = VectorAssembler(inputCols=features_k1, outputCol="features",
                                        handleInvalid="keep")
            row_vec   = assembler.transform(row_spark)
            pred      = model.transform(row_vec)
            pm25_log  = pred.select("prediction").collect()[0][0]
            pm25_pred = max(0.0, math.expm1(pm25_log))
        else:
            pm25_pred = float(df["PM25"].iloc[-8:].mean())

        aqi_val = predict_service.pm25_to_aqi(pm25_pred)
        meta    = predict_service.aqi_meta(aqi_val)

        return {
            "aqi":         aqi_val,
            "pm25":        round(pm25_pred, 2),
            "label":       meta["label"],
            "css_class":   meta["css_class"],
            "color":       meta["color"],
            "hour_str":    next_hour.strftime("%H:00"),
            "source":      "realtime",
            "forecast_dt": next_hour.isoformat(),
        }

    except Exception as e:
        print(f"[_predict_next_hour_realtime] Lỗi: {e}")
        return {
            "aqi":       0,
            "pm25":      0.0,
            "label":     "Không có dữ liệu",
            "css_class": "unknown",
            "color":     "#9e9e9e",
            "hour_str":  next_hour.strftime("%H:00"),
            "source":    "error",
            "forecast_dt": next_hour.isoformat(),
        }


# ── Views ─────────────────────────────────────────────────────────────────────

def dashboard(request):
    global _dashboard_cache

    ensure_forecast_fresh()

    now = datetime.now()
    if (
        _dashboard_cache["ts"] is not None
        and _dashboard_cache["ctx"] is not None
        and (now - _dashboard_cache["ts"]).total_seconds() < DASHBOARD_CACHE_SECONDS
    ):
        return render(request, "aqi_app/dashboard.html", _dashboard_cache["ctx"])

    slots, generated_at, error = load_forecast()

    if error or not slots:
        return render(request, "aqi_app/dashboard.html", {
            "error": error or "Không có dữ liệu dự báo.",
        })

    next_hour_aqi = get_next_hour_aqi(slots)

    current = slots[0]
    day1    = [s for s in slots if s.get("day_index") == 0]
    day2    = [s for s in slots if s.get("day_index") == 1]

    chart_labels = json.dumps([s["display_dt"] for s in slots])
    chart_aqi    = json.dumps([s["aqi"]   for s in slots])
    chart_pm25   = json.dumps([s["pm25"]  for s in slots])
    chart_colors = json.dumps([s["color"] for s in slots])

    aqi_vals = [s["aqi"] for s in slots]
    stats = {
        "min": min(aqi_vals),
        "max": max(aqi_vals),
        "avg": round(sum(aqi_vals) / len(aqi_vals)),
    }

    try:
        run_dt   = datetime.fromisoformat(generated_at)
        run_time = run_dt.strftime("%d/%m/%Y %H:%M")
    except Exception:
        run_time = generated_at or "N/A"

    ctx = {
        "current":       current,
        "next_hour_aqi": next_hour_aqi,
        "forecast":      slots,
        "day1":          day1,
        "day2":          day2,
        "chart_labels":  chart_labels,
        "chart_aqi":     chart_aqi,
        "chart_pm25":    chart_pm25,
        "chart_colors":  chart_colors,
        "stats":         stats,
        "run_time":      run_time,
        "error":         None,
        "meta_json":     open(BASE_DIR / "data" / "analytics_meta.json").read()
                         if (BASE_DIR / "data" / "analytics_meta.json").exists() else "{}",
    }

    _dashboard_cache["ts"]  = now
    _dashboard_cache["ctx"] = ctx

    return render(request, "aqi_app/dashboard.html", ctx)


# ── Analytics ─────────────────────────────────────────────────────────────────

def _build_analytics_data():
    """
    Đọc toàn bộ hanoi_aqi.csv → daily JSON + hourly JSON.
    Trả về tuple (daily_json_str, hourly_json_str).
    """
    import csv as _csv
    from collections import defaultdict

    FIELDS = ["AQI", "PM25", "PM10", "NO2", "O3", "CO", "SO2"]

    def sf(val):
        try:
            v = float(val)
            return v if v == v else None
        except (ValueError, TypeError):
            return None

    def avg_list(lst):
        vals = [v for v in lst if v is not None]
        return round(sum(vals) / len(vals), 1) if vals else None

    if not CSV_PATH.exists():
        return "[]", "[]"

    daily  = defaultdict(lambda: {f: [] for f in FIELDS})
    hourly = defaultdict(lambda: defaultdict(lambda: {f: [] for f in FIELDS}))

    with open(CSV_PATH, encoding="utf-8-sig") as f:
        reader = _csv.DictReader(f)
        for row in reader:
            lt = row.get("Local_Time", "").strip()
            date_str = lt[:10]
            if not date_str or date_str < "2000-01-01":
                continue
            hour = int(lt[11:13]) if len(lt) > 12 else 0
            for field in FIELDS:
                v = sf(row.get(field, ""))
                if v is not None:
                    daily[date_str][field].append(v)
                    hourly[date_str][hour][field].append(v)

    daily_list = []
    for d in sorted(daily.keys()):
        entry = {"date": d}
        for field in FIELDS:
            entry[field] = avg_list(daily[d][field])
        daily_list.append(entry)

    hourly_list = []
    for d in sorted(hourly.keys()):
        for h in sorted(hourly[d].keys()):
            entry = {"date": d, "hour": h}
            for field in FIELDS:
                entry[field] = avg_list(hourly[d][h][field])
            hourly_list.append(entry)

    return (
        json.dumps(daily_list,  ensure_ascii=False),
        json.dumps(hourly_list, ensure_ascii=False),
    )


_analytics_cache = {"ts": None, "daily": "[]", "hourly": "[]"}
ANALYTICS_CACHE_SEC = 600  # 10 phút


def analytics(request):
    global _analytics_cache

    _, generated_at, _ = load_forecast()
    try:
        run_dt   = datetime.fromisoformat(generated_at) if generated_at else None
        run_time = run_dt.strftime("%d/%m/%Y %H:%M") if run_dt else "N/A"
    except Exception:
        run_time = generated_at or "N/A"

    now = datetime.now()
    if (
        _analytics_cache["ts"] is None
        or (now - _analytics_cache["ts"]).total_seconds() > ANALYTICS_CACHE_SEC
    ):
        daily_json, hourly_json = _build_analytics_data()
        _analytics_cache = {"ts": now, "daily": daily_json, "hourly": hourly_json}

    return render(request, "aqi_app/analytics.html", {
        "run_time":     run_time,
        "history_json": _analytics_cache["daily"],
        "hourly_json":  _analytics_cache["hourly"],
    })