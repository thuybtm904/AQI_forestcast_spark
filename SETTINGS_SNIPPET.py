# ─── Thêm vào settings.py của bạn ────────────────────────────────────────────
#
# 1. Thêm 'aqi_app' vào INSTALLED_APPS:
#
# INSTALLED_APPS = [
#     ...
#     'aqi_app',
# ]
#
# 2. Đảm bảo APP_DIRS = True trong TEMPLATES:
#
# TEMPLATES = [{
#     ...
#     'APP_DIRS': True,
#     ...
# }]
#
# 3. Thêm vào urls.py (project level):
#
# from django.urls import path, include
# urlpatterns = [
#     path('admin/', admin.site.urls),
#     path('', include('aqi_app.urls')),
# ]
#
# ─────────────────────────────────────────────────────────────────────────────
