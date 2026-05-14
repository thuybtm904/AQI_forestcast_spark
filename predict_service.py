"""
predict_service.py
──────────────────
Chạy độc lập (KHÔNG phải trong Django).
Đọc CSV lịch sử + 17 Spark GBT models → ghi forecast.json

Cách dùng:
    python predict_service.py

Hoặc đặt vào cron chạy mỗi ngày:
    0 6 * * * /usr/bin/python3 /path/to/predict_service.py
"""

import os, json, math
from datetime import datetime, timedelta
from pathlib import Path

os.environ["HADOOP_HOME"] = "D:\\hadoop"
os.environ["PATH"] += ";D:\\hadoop\\bin"
os.environ["spark.hadoop.io.native.lib.available"] = "false"
os.environ["hadoop.home.dir"] = "D:\\hadoop"
os.environ["JAVA_HOME"] = "C:\\Program Files\\Java\\jdk-11"

# ── Config ────────────────────────────────────────────────────────────────────
BASE_DIR     = Path(__file__).parent
CSV_PATH     = BASE_DIR / "data" / "hanoi_aqi.csv"
MODELS_DIR   = BASE_DIR / "models"
OUTPUT_JSON  = BASE_DIR / "data" / "forecast.json"

# 17 horizons (giờ) — khớp với lúc train
K_LIST = [1, 4, 7, 10, 13, 16, 19, 22, 25, 28, 31, 34, 37, 40, 43, 46, 49]

HANOI_LAT, HANOI_LON = 21.0285, 105.8542

# ── Thêm hàm này vào predict_service.py, ngay sau phần Config ────────────────
# Đặt sau dòng: HANOI_LAT, HANOI_LON = 21.0285, 105.8542

def ensure_csv_fresh():
    """
    Kiểm tra CSV có data đến gần giờ hiện tại chưa.
    Nếu thiếu (cách hơn 2h) → crawl Open-Meteo và append vào CSV.

    Crawl đến hôm nay nhưng cắt bỏ các giờ >= giờ hiện tại
    (vì Open-Meteo trả cả giờ tương lai dạng forecast, không phải observation).

    Gọi trước predict_service.run() và trước _predict_next_hour_realtime().
    """
    import pandas as pd

    if not CSV_PATH.exists():
        print("⚠️  CSV chưa tồn tại, bỏ qua crawl")
        return

    df = pd.read_csv(CSV_PATH)
    # Ép kiểu rõ ràng — tránh lẫn str/Timestamp khi concat sau này
    df["Local_Time"] = pd.to_datetime(df["Local_Time"])
    last_ts = df["Local_Time"].max()

    now_dt = datetime.now()
    # Giờ thực cuối cùng đã xảy ra (tròn giờ, lùi 1h để chắc chắn đã có data)
    # Open-Meteo air quality thường delay ~1h so với thực tế
    cutoff_ts = now_dt.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)

    gap_hours = (cutoff_ts - last_ts).total_seconds() / 3600
    if gap_hours < 1:
        print(f"✅ CSV đã có đến {last_ts.strftime('%Y-%m-%d %H:%M')}, không cần crawl")
        return

    start_date = last_ts.strftime("%Y-%m-%d")       # từ ngày cuối CSV (lấy lại để fill đủ)
    end_date   = now_dt.strftime("%Y-%m-%d")         # đến hôm nay — API trả cả ngày
    print(f"📥 CSV thiếu {gap_hours:.1f}h, crawl từ {start_date} → {end_date} ...")

    new_df = _crawl_openmeteo(start_date, end_date)
    if new_df is None or len(new_df) == 0:
        print("⚠️  Crawl không có data mới")
        return

    # Ép kiểu và CẮT BỎ các giờ tương lai (>= cutoff_ts)
    # Giữ lại: Local_Time <= cutoff_ts (giờ đã thực sự xảy ra)
    new_df["Local_Time"] = pd.to_datetime(new_df["Local_Time"])
    new_df = new_df[new_df["Local_Time"] <= cutoff_ts]

    if len(new_df) == 0:
        print("⚠️  Không có hàng nào trong khoảng thời gian đã xảy ra")
        return

    old_len = len(df)
    combined = pd.concat([df, new_df]).drop_duplicates("Local_Time", keep="last")
    combined = combined.sort_values("Local_Time").reset_index(drop=True)
    combined.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    print(f"✅ CSV đã cập nhật: {len(combined):,} hàng "
          f"(+{len(combined) - old_len} hàng mới, đến {cutoff_ts.strftime('%Y-%m-%d %H:%M')})")


def _crawl_openmeteo(start_date: str, end_date: str):
    """
    Crawl Air Quality + Weather từ Open-Meteo cho Hà Nội.
    Trả về DataFrame với cùng cấu trúc cột như hanoi_aqi.csv,
    hoặc None nếu lỗi.
    """
    import pandas as pd
    import numpy as np
    import requests

    TZ = "Asia/Ho_Chi_Minh"

    try:
        # ── 1. Air Quality ────────────────────────────────────────────────────
        r_aq = requests.get(
            "https://air-quality-api.open-meteo.com/v1/air-quality",
            params={
                "latitude":  HANOI_LAT,
                "longitude": HANOI_LON,
                "hourly":    "pm2_5,pm10,carbon_monoxide,nitrogen_dioxide,"
                             "sulphur_dioxide,ozone",
                "timezone":  TZ,
                "start_date": start_date,
                "end_date":   end_date,
            },
            timeout=60,
        )
        r_aq.raise_for_status()
        df_aq = pd.DataFrame(r_aq.json()["hourly"]).rename(columns={
            "time":              "Local_Time",
            "pm2_5":            "PM25",
            "pm10":             "PM10",
            "carbon_monoxide":  "CO",
            "nitrogen_dioxide": "NO2",
            "sulphur_dioxide":  "SO2",
            "ozone":            "O3",
        })

        # ── 2. Weather (forecast API — có past_days, bù gap ~7 ngày) ─────────
        r_wt = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude":  HANOI_LAT,
                "longitude": HANOI_LON,
                "hourly":    "temperature_2m,relative_humidity_2m,precipitation,"
                             "surface_pressure,cloud_cover,wind_speed_10m,"
                             "wind_direction_10m,boundary_layer_height",
                "timezone":  TZ,
                "past_days": 7,
                "forecast_days": 1,
            },
            timeout=60,
        )
        r_wt.raise_for_status()
        df_wt = pd.DataFrame(r_wt.json()["hourly"]).rename(columns={
            "time":                    "Local_Time",
            "temperature_2m":          "Temperature",
            "relative_humidity_2m":    "Relative_Humidity",
            "precipitation":           "Precipitation",
            "surface_pressure":        "Pressure",
            "cloud_cover":             "Clouds",
            "wind_speed_10m":          "Wind_Speed",
            "wind_direction_10m":      "Wind_Direction",
            "boundary_layer_height":   "BLH",
        })

        # ── 3. Merge ──────────────────────────────────────────────────────────
        df = pd.merge(df_wt, df_aq, on="Local_Time", how="inner")
        df["Local_Time"] = pd.to_datetime(df["Local_Time"])

        # Lọc chỉ lấy khoảng thời gian cần (start_date trở đi)
        df = df[df["Local_Time"] >= pd.to_datetime(start_date)]

        # ── 4. Tính AQI ───────────────────────────────────────────────────────
        def _pm25_to_aqi_raw(c):
            """Dùng hàm pm25_to_aqi có sẵn trong predict_service."""
            try:
                return pm25_to_aqi(float(c)) if c is not None and not np.isnan(c) else None
            except Exception:
                return None

        df["AQI"]           = df["PM25"].apply(_pm25_to_aqi_raw)
        df["AQI_Pollutant"] = "PM2.5"   # simplified — PM2.5 thường là dominant ở HN

        # ── 5. Giữ Local_Time là datetime (không format thành string) ──────────
        # ensure_csv_fresh() đã ép pd.to_datetime() ở cả hai phía trước khi concat
        # → không còn lẫn str/Timestamp khi drop_duplicates

        # Chỉ giữ các cột đúng thứ tự CSV gốc
        final_cols = [
            "Local_Time",
            "AQI", "AQI_Pollutant",
            "CO", "NO2", "O3", "PM10", "PM25", "SO2",
            "Clouds", "Precipitation", "Pressure",
            "Relative_Humidity", "Temperature", "Wind_Speed",
            "Wind_Direction", "BLH",
        ]
        df = df[[c for c in final_cols if c in df.columns]]

        print(f"   → Crawl được {len(df):,} hàng "
              f"({df['Local_Time'].iloc[0]} → {df['Local_Time'].iloc[-1]})")
        return df

    except Exception as e:
        print(f"❌ _crawl_openmeteo thất bại: {e}")
        return None


def get_weather_forecast():
    import requests

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": HANOI_LAT,
        "longitude": HANOI_LON,
        "hourly": ",".join([
            "temperature_2m",
            "relativehumidity_2m",
            "precipitation",
            "windspeed_10m",
            "pressure_msl",
            "boundary_layer_height",
        ]),
        "timezone": "Asia/Bangkok"
    }

    try:
        res = requests.get(url, params=params, timeout=10)
        data = res.json()
        times = data["hourly"]["time"]
        weather_dict = {}
        for i, t in enumerate(times):
            weather_dict[t] = {
                "temp":   data["hourly"]["temperature_2m"][i],
                "humid":  data["hourly"]["relativehumidity_2m"][i],
                "precip": data["hourly"]["precipitation"][i],
                "wind":   data["hourly"]["windspeed_10m"][i],
                "press":  data["hourly"]["pressure_msl"][i],
                "blh":    data["hourly"]["boundary_layer_height"][i],
            }
        print(f"🌤️ Loaded weather forecast: {len(weather_dict)} hours")
        return weather_dict
    except Exception as e:
        print(f"⚠️ Weather API failed: {e}")
        return {}


# ── PM2.5 → AQI (US EPA) ─────────────────────────────────────────────────────
def pm25_to_aqi(pm: float) -> int:
    if pm < 0: pm = 0
    pm = round(pm, 1)   # ← thêm dòng này, giống hàm gốc
    bps = [
        (0.0,   12.0,    0,  50),
        (12.1,  35.4,   51, 100),
        (35.5,  55.4,  101, 150),
        (55.5,  150.4, 151, 200),
        (150.5, 250.4, 201, 300),
        (250.5, 500.4, 301, 500),
    ]
    for c_lo, c_hi, i_lo, i_hi in bps:
        if c_lo <= pm <= c_hi:
            return round((i_hi - i_lo) / (c_hi - c_lo) * (pm - c_lo) + i_lo)
    return 500


def aqi_meta(aqi: int) -> dict:
    levels = [
        (50,  "Tốt",       "good",      "#00c853"),
        (100, "Trung bình","moderate",  "#ffca28"),
        (150, "Kém",       "poor",      "#ff6f00"),
        (200, "Xấu",       "unhealthy", "#f44336"),
        (300, "Rất xấu",   "very_bad",  "#ab47bc"),
        (999, "Nguy hại",  "hazardous", "#b71c1c"),
    ]
    for thresh, label, css, color in levels:
        if aqi <= thresh:
            return {"label": label, "css_class": css, "color": color}
    return {"label": "Nguy hại", "css_class": "hazardous", "color": "#b71c1c"}


# ── Feature engineering ───────────────────────────────────────────────────────
def build_features(df_pd, K: int, features_k: list, weather_forecast=None) -> "pd.DataFrame":
    import pandas as pd
    import numpy as np

    df = df_pd.copy().sort_values("ts").reset_index(drop=True)
    last = len(df) - 1

    def lag(col, n):
        idx = last - n
        return float(df[col].iloc[idx]) if idx >= 0 else np.nan

    def roll_mean(col, n):
        return float(df[col].iloc[max(0, last-n):last].mean())

    def roll_std(col, n):
        return float(df[col].iloc[max(0, last-n):last].std(ddof=0)) if last >= n else np.nan

    def roll_max(col, n):
        return float(df[col].iloc[max(0, last-n):last].max())

    def roll_min(col, n):
        return float(df[col].iloc[max(0, last-n):last].min())

    now_ts = pd.to_datetime(df["ts"].iloc[last])
    hour   = now_ts.hour
    month  = now_ts.month

    pm1  = lag("PM25", 1)
    pm6  = lag("PM25", 6)
    pm24 = lag("PM25", 24)
    rm24_mean = roll_mean("PM25", 24)
    rm24_std  = roll_std("PM25",  24) or 1e-6

    future_dt = (now_ts + timedelta(hours=K)).strftime("%Y-%m-%dT%H:00")
    wf = weather_forecast.get(future_dt, {}) if weather_forecast else {}

    temp_fk   = wf.get("temp",   lag("Temperature",       0))
    wind_fk   = wf.get("wind",   lag("Wind_Speed",        0))
    humid_fk  = wf.get("humid",  lag("Relative_Humidity", 0))
    press_fk  = wf.get("press",  lag("Pressure",          0))
    precip_fk = wf.get("precip", lag("Precipitation",     0))

    feat = {
        "pm25_current":      lag("PM25", 0),
        "pm25_lag1":         pm1,
        "pm25_lag6":         pm6,
        "pm25_lag24":        pm24,
        "pm25_lag48":        lag("PM25", 48),
        "pm25_lag72":        lag("PM25", 72),
        "pm25_lag168":       lag("PM25", 168),
        "pm25_roll8_mean":   roll_mean("PM25", 8),
        "pm25_roll8_std":    roll_std("PM25",  8),
        "pm25_roll8_max":    roll_max("PM25",  8),
        "pm25_roll8_min":    roll_min("PM25",  8),
        "pm25_roll24_mean":  rm24_mean,
        "pm25_roll24_std":   rm24_std,
        "pm25_roll24_max":   roll_max("PM25", 24),
        "pm25_roll48_mean":  roll_mean("PM25", 48),
        "pm25_roll48_std":   roll_std("PM25",  48),
        "pm25_roll48_max":   roll_max("PM25",  48),
        "pm25_roll72_mean":  roll_mean("PM25", 72),
        "pm25_roll168_mean": roll_mean("PM25", 168),
        "pm25_roll168_std":  roll_std("PM25",  168),
        "pm25_roll365_mean": roll_mean("PM25", 365*24),
        "pm25_diff_1_6":     pm1 - pm6,
        "pm25_diff_6_24":    pm6 - pm24,
        "pm25_accel":        (pm1 - pm6) - (pm6 - pm24),
        "pm25_trend_24h":    pm1 - pm24,
        "pm25_anomaly_score":(pm1 - rm24_mean) / (rm24_std + 1e-6),
        "is_spike":          int(abs((pm1 - rm24_mean) / (rm24_std + 1e-6)) > 2.0),
        "pm25_norm":         pm1 / (rm24_mean + 1e-6),
        "pm25_ratio_24":     pm1 / (rm24_mean + 1e-6),
        "pm25_range_24":     roll_max("PM25", 24) - rm24_mean,
        "pm25_level_ratio":  rm24_mean / (roll_mean("PM25", 365*24) + 1e-6),
        "days_since_start":  (now_ts - pd.Timestamp("2022-08-11")).days,
        "temperature_lag1":       lag("Temperature",       1),
        "wind_speed_lag1":        lag("Wind_Speed",        1),
        "relative_humidity_lag1": lag("Relative_Humidity", 1),
        "pressure_lag1":          lag("Pressure",          1),
        "precip_lag1":            lag("Precipitation",     1),
        "precip_lag6":            lag("Precipitation",     6),
        "pm10_lag1":              lag("PM10", 1),
        "no2_lag1":               lag("NO2",  1),
        "clouds_lag1":            lag("Clouds", 1),
        **{f"temp_lag{h}":     lag("Temperature",       h) for h in [1,3,6,12,24]},
        **{f"wind_lag{h}":     lag("Wind_Speed",        h) for h in [1,3,6,12,24]},
        **{f"humid_lag{h}":    lag("Relative_Humidity", h) for h in [1,3,6,12,24]},
        **{f"precip_lag{h}":   lag("Precipitation",     h) for h in [1,3,6,12,24]},
        **{f"pressure_lag{h}": lag("Pressure",          h) for h in [1,3,6,12,24]},
        "blh_lag1":          lag("BLH", 1),
        "blh_lag6":          lag("BLH", 6),
        "blh_lag24":         lag("BLH", 24),
        "blh_pm25_interact": pm1 / (lag("BLH", 1) + 1.0),
        "winddir_sin_lag1":  math.sin(math.radians(lag("Wind_Direction", 1) or 0)),
        "winddir_cos_lag1":  math.cos(math.radians(lag("Wind_Direction", 1) or 0)),
        "winddir_sin_lag6":  math.sin(math.radians(lag("Wind_Direction", 6) or 0)),
        "winddir_cos_lag6":  math.cos(math.radians(lag("Wind_Direction", 6) or 0)),
        "dispersion_idx":    lag("Wind_Speed", 1) * (100 - lag("Relative_Humidity", 1)),
        "hour_sin":          math.sin(hour  * 2 * math.pi / 24),
        "hour_cos":          math.cos(hour  * 2 * math.pi / 24),
        "month_sin":         math.sin(month * 2 * math.pi / 12),
        "month_cos":         math.cos(month * 2 * math.pi / 12),
        "is_rush_hour":      int(hour in [7,8,9,17,18,19]),
        "is_night":          int(hour >= 22 or hour <= 5),
        "is_weekend":        int(now_ts.weekday() >= 5),
        "is_burn_season":    int(month in [4,5,6,10,11,12]),
        "is_april":          int(month == 4),
        "horizon_norm":      K / 49.0,
        "pm25_lagK":         lag("PM25", K),
        "temp_fK":           temp_fk,
        "wind_fK":           wind_fk,
        "humid_fK":          humid_fk,
        "press_fK":          press_fk,
        "precip_fK":         precip_fk,
        "blh_fK":            wf.get("blh", lag("BLH", 0)),
        "dispersion_fK":     wind_fk * (100 - humid_fk),
    }

    for el in {K, max(K//2, 1), max(K//3, 1)}:
        feat[f"pm25_lag_extra_{el}"] = lag("PM25", el)

    row = {f: feat.get(f, float("nan")) for f in features_k}
    return pd.DataFrame([row])


# ── Load Spark model + predict ────────────────────────────────────────────────
def predict_all_horizons(df_pd) -> list:
    try:
        from pyspark.sql import SparkSession
        from pyspark.ml.regression import GBTRegressionModel
        from pyspark.ml.feature import VectorAssembler

        spark = SparkSession.builder \
            .appName("AQI_Predict") \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.hadoop.io.native.lib.available", "false") \
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
            .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
            .config("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs") \
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
            .config("spark.hadoop.mapred.FileInputFormat.list-status.num-threads", "1") \
            .config("spark.sql.legacy.setCommandRejectsSparkCoreConfs", "false") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
    except ImportError:
        print("⚠️  PySpark không khả dụng — dùng fallback mode")
        return _fallback_predict(df_pd)

    df_sorted = df_pd.sort_values("ts")
    now_ts    = df_sorted["ts"].iloc[-1]
    base_dt   = now_ts if isinstance(now_ts, datetime) else datetime.fromisoformat(str(now_ts))

    weather_forecast = get_weather_forecast()

    results = []
    for K in K_LIST:
        feat_path  = MODELS_DIR / f"features_k{K}.json"
        model_path = str(MODELS_DIR / f"gbt_k{K}")

        if not os.path.exists(feat_path) or not os.path.exists(model_path):
            print(f"⚠️  Thiếu model/features cho K={K}, bỏ qua")
            continue

        with open(feat_path) as f:
            features_k = json.load(f)

        row_pd = build_features(df_pd, K, features_k, weather_forecast)
        row_pd = row_pd.fillna(0.0)

        row_spark = spark.createDataFrame(row_pd)

        assembler = VectorAssembler(inputCols=features_k, outputCol="features",
                                    handleInvalid="keep")
        row_vec = assembler.transform(row_spark)

        model_path_str = str(model_path).replace("\\", "/")
        model = GBTRegressionModel.load(model_path_str)

        pred     = model.transform(row_vec)
        pm25_log = pred.select("prediction").collect()[0][0]
        pm25_pred = max(0.0, math.expm1(pm25_log))

        # ✅ forecast_dt = base_dt + đúng K giờ — mỗi K cho 1 thời điểm khác nhau
        forecast_dt = base_dt + timedelta(hours=K)
        results.append({
            "K":           K,
            "pm25":        round(pm25_pred, 2),
            "aqi":         pm25_to_aqi(pm25_pred),
            "forecast_dt": forecast_dt.isoformat(),
        })
        print(f"  K={K:2d}h → {forecast_dt.strftime('%d/%m %H:%M')}  PM2.5={pm25_pred:.1f}  AQI={pm25_to_aqi(pm25_pred)}")

    spark.stop()
    return results


def _fallback_predict(df_pd) -> list:
    """Dùng khi PySpark chưa cài — trả về giá trị naive (rolling mean)."""
    print("🔄 Fallback: dùng rolling-24h mean làm proxy")
    pm_recent = df_pd.sort_values("ts")["PM25"].iloc[-24:].mean()
    base_dt   = datetime.now()
    return [
        {
            "K":           K,
            "pm25":        round(float(pm_recent) + (K * 0.1), 2),  # thêm offset nhỏ để debug
            "aqi":         pm25_to_aqi(pm_recent),
            "forecast_dt": (base_dt + timedelta(hours=K)).isoformat(),
        }
        for K in K_LIST
    ]


# ── Chọn 16 slot từ 17 predictions ───────────────────────────────────────────
def select_16_slots(predictions: list, base_dt: datetime) -> list:
    """
    BUG CŨ: dùng min() toàn bộ list → nhiều target map về cùng 1 prediction.

    FIX: Với mỗi target slot (8 slot/ngày × 2 ngày), tính forecast_dt thực từ
    base_dt + K, rồi chọn K có forecast_dt gần target nhất — mỗi K chỉ dùng 1 lần.

    Nếu số predictions < 16, phần dư sẽ dùng nội suy tuyến tính.
    """
    # Build lookup: forecast_dt (string) → prediction dict
    preds_by_dt = {p["forecast_dt"]: p for p in predictions}
    # Cũng build sorted list để nội suy
    preds_sorted = sorted(predictions, key=lambda p: p["forecast_dt"])

    # 16 display slots: 8 slot mỗi ngày, tính từ midnight ngày mai
    now_date = base_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    target_slots = []
    for day in [1, 2]:
        day_base = now_date + timedelta(days=day)
        for h in [0, 3, 6, 9, 12, 15, 18, 21]:
            target_slots.append({
                "target_dt": day_base + timedelta(hours=h),
                "day_index": day - 1,
                "hour_str":  f"{h:02d}:00",
                "date_str":  (day_base).strftime("%d/%m"),
            })

    slots = []
    used_K = set()  # ✅ mỗi K chỉ dùng 1 lần

    for slot_info in target_slots:
        target_dt = slot_info["target_dt"]

        # Tìm prediction chưa dùng, gần target_dt nhất
        best = None
        best_diff = float("inf")
        for p in preds_sorted:
            if p["K"] in used_K:
                continue
            fdt = datetime.fromisoformat(p["forecast_dt"])
            diff = abs((fdt - target_dt).total_seconds())
            if diff < best_diff:
                best_diff = diff
                best = p

        if best is None:
            # Fallback: dùng prediction cuối cùng nếu hết
            best = preds_sorted[-1]

        used_K.add(best["K"])

        slot = {
            "K":           best["K"],
            "pm25":        best["pm25"],
            "aqi":         best["aqi"],
            "forecast_dt": best["forecast_dt"],
            "display_dt":  slot_info["target_dt"].strftime("%d/%m %Hh"),
            "display_full":slot_info["target_dt"].strftime("%A, %d/%m/%Y %H:%M"),
            "hour_str":    slot_info["hour_str"],
            "date_str":    slot_info["date_str"],
            "day_index":   slot_info["day_index"],
        }
        slot.update(aqi_meta(slot["aqi"]))
        slots.append(slot)

        print(f"  Slot {slot['display_dt']} ← K={best['K']}h  "
              f"PM2.5={best['pm25']}  AQI={best['aqi']}")

    return slots


# ── Main ──────────────────────────────────────────────────────────────────────
def run():
    import pandas as pd

    print(f"[{datetime.now():%Y-%m-%d %H:%M}] Đọc CSV: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH, parse_dates=["Local_Time"])
    df = df.rename(columns={"Local_Time": "ts"})
    df = df.sort_values("ts").tail(400)

    print(f"  {len(df)} hàng, cột: {list(df.columns)}")
    print(f"  Thời điểm cuối: {df['ts'].iloc[-1]}")

    base_dt = df["ts"].iloc[-1]
    base_dt = base_dt if isinstance(base_dt, datetime) else datetime.fromisoformat(str(base_dt))

    print("\nChạy predictions cho 17 horizons...")
    predictions = predict_all_horizons(df)

    if not predictions:
        print("❌ Không có prediction nào, dừng lại.")
        return

    print(f"\nChọn 16 display slots...")
    slots = select_16_slots(predictions, base_dt)

    output = {
        "generated_at": datetime.now().isoformat(),
        "base_ts":      str(df["ts"].iloc[-1]),
        "slots":        slots,
        # Raw predictions để debug
        "raw_predictions": predictions,
    }

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n✅ Đã ghi {len(slots)} slots → {OUTPUT_JSON}")
    print("\n--- Tóm tắt ---")
    for s in slots:
        print(f"  {s['display_dt']:12s}  PM2.5={s['pm25']:6.1f}  AQI={s['aqi']:3d}  {s['label']}")


if __name__ == "__main__":
    run()