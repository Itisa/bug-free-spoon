from django.shortcuts import render

from django.http import HttpResponse, JsonResponse
from .models import BikeUsageData
from django.conf import settings

def index(request):
    hint = settings.HINT_TEXT
    render_data = {
        'hint': hint,
    }
    rsp = render(request, 'bikes/bikes.html', context=render_data)
    return rsp

# views.py
from typing import Any, Dict, List
from django.http import JsonResponse, HttpRequest
from django.db.models import QuerySet
from .models import BikeUsageData

# --- 小工具 ---
def _round_sec(x: Any) -> int:
    """把可能是 None/str/float 的时长安全地转为整数秒"""
    if x is None:
        return 0
    try:
        return int(round(float(x)))
    except (TypeError, ValueError):
        return 0

def _ensure_len_24(arr: Any, cast=float, default=0) -> List:
    """确保数组长度为 24（不足补 0 / 多余截断）；并按需要做类型转换"""
    if not isinstance(arr, (list, tuple)):
        return [default] * 24
    out = list(arr[:24]) + [default] * max(0, 24 - len(arr))
    # 转换类型
    return [cast(v) if v is not None else default for v in out]

def _row_to_v2(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    v2 压缩 schema：
      { d: 'YYYY-MM-DD', c: int[24], s: int[24], w: [avg,min,max,precip,wind,snow,pressure] }
    其中 s 为“秒”的整数。
    """
    counts = _ensure_len_24(row.get("hourly_counts"), cast=int, default=0)
    durations_sec = _ensure_len_24(row.get("hourly_durations"), cast=float, default=0.0)
    durations_sec = [_round_sec(x) for x in durations_sec]

    return {
        "d": row.get("date").isoformat() if hasattr(row.get("date"), "isoformat") else row.get("date"),
        "c": counts,
        "s": durations_sec,
        "w": [
            row.get("avg_temperature"),
            row.get("min_temperature"),
            row.get("max_temperature"),
            row.get("precipitation"),
            row.get("windspeed"),
            row.get("snow"),
            row.get("pressure"),
        ],
    }

def _row_to_v1(row: Dict[str, Any]) -> Dict[str, Any]:
    """保持你当前的 v1 输出（原样字段名），但只选择必要字段并做基础清洗。"""
    return {
        "id": row.get("id"),
        "date": row.get("date").isoformat() if hasattr(row.get("date"), "isoformat") else row.get("date"),
        "year": row.get("year"),
        "month": row.get("month"),
        "day": row.get("day"),
        "hourly_counts": _ensure_len_24(row.get("hourly_counts"), cast=int, default=0),
        "hourly_durations": _ensure_len_24(row.get("hourly_durations"), cast=float, default=0.0),
        "avg_temperature": row.get("avg_temperature"),
        "min_temperature": row.get("min_temperature"),
        "max_temperature": row.get("max_temperature"),
        "precipitation": row.get("precipitation"),
        "windspeed": row.get("windspeed"),
        "snow": row.get("snow"),
        "pressure": row.get("pressure"),
    }

def api(request: HttpRequest) -> JsonResponse:
    """
    /bikes/api          -> v1（向后兼容）
    /bikes/api?v=2      -> v2 压缩 schema
    """
    want_v2 = request.GET.get("v") == "2"

    # 只取用得到的字段，减少 ORM 反序列化开销
    qs: QuerySet = BikeUsageData.objects.order_by("date").values(
        "id", "date", "year", "month", "day",
        "hourly_counts", "hourly_durations",
        "avg_temperature", "min_temperature", "max_temperature",
        "precipitation", "windspeed", "snow", "pressure",
    )

    rows: List[Dict[str, Any]] = list(qs)

    if want_v2:
        payload = [_row_to_v2(row) for row in rows]
    else:
        payload = [_row_to_v1(row) for row in rows]

    # 紧凑 JSON（去空白）以进一步减小体积；配合 GZipMiddleware 效果更好
    return JsonResponse(
        payload,
        safe=False,
        json_dumps_params={
            "separators": (",", ":"),   # 紧凑
            "ensure_ascii": False,      # 允许中文
        },
    )
