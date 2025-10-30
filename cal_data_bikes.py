# -*- coding: utf-8 -*-
from __future__ import annotations
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed
from collections import defaultdict
from typing import Dict, Tuple, List
import csv, glob, json, os, sys

def parse_mmddyyyy_hhmm(s: str) -> Tuple[str, int]:
    #  0123456789012345
    #  YYYY/MM/DD HH:MM
    y = s[0:4]
    m = s[5:7]
    d = s[8:10]
    hour = int(s[11:13])
    return f"{y}-{m}-{d}", hour

def fast_parse_iso_dt(s: str) -> datetime:
    year  = int(s[0:4])
    month = int(s[5:7])
    day   = int(s[8:10])
    hour  = int(s[11:13])
    minute= int(s[14:16])
    sec   = int(s[17:19])
    if len(s) > 19 and s[19] == '.':
        micro = int((s[20:]).ljust(6, '0')[:6])  # 把毫秒/微秒统一成6位微秒
    else:
        micro = 0
    return datetime(year, month, day, hour, minute, sec, micro)

def process_one_csv(csv_path: str) -> Tuple[
    Dict[str, Dict[str, List[float]]],
    str, str,                    # min_date, max_date（字符串 YYYY-MM-DD）
    int, int, int, int           # 总记录、异常记录、max_dur、min_dur（为监控而返）
]:
    """
    返回：
    - per-day 累计：{ date: { 'hourly_counts':[24], 'hourly_durations_sum':[24], 'daily_count':int, 'daily_durations_sum':float } }
    - 全局最小日期、最大日期（字符串）
    - 统计：总记录数、异常数、最大时长、最小时长
    """
    # 仅做累加，最后在主进程求平均
    # day_acc = defaultdict(lambda: {
    #     'hourly_counts':        [0]*24,
    #     'hourly_durations_sum': [0.0]*24,
    #     'daily_count':          0,
    #     'daily_durations_sum':  0.0
    # })
    print(f"处理文件: {csv_path}")
    # numbers = ''.join(filter(str.isdigit,csv_path))
    # if int(numbers[:4]) < 2025:
    #     return {}, None, None, 0, 0, 0, 86400
    day_acc: dict[str, dict] = {}

    total = 0
    bad   = 0
    maxdur = 0
    mindur = 86400

    min_date = None
    max_date = None

    with open(csv_path, 'r', encoding='utf-8', newline='') as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None:
            return {}, None, None, 0, 0, 0, 86400

        # 建列索引（两种模式）
        # 模式A：有 "Start date" + "Duration"
        # 模式B：有 "started_at"/"ended_at"（需计算 Duration）
        name2idx = {h: i for i, h in enumerate(header)}
        has_mode_a = ("Start date" in name2idx) and ("Duration" in name2idx)
        has_mode_b = ("started_at" in name2idx) and ("ended_at" in name2idx)

        if not (has_mode_a or has_mode_b):
            # 该文件没有可用列，直接跳过
            return {}, None, None, 0, 0, 0, 86400

        if has_mode_a:
            i_startA = name2idx["Start date"]
            i_dur    = name2idx["Duration"]
        if has_mode_b:
            i_startB = name2idx["started_at"]
            i_endB   = name2idx["ended_at"]

        for row in reader:
            total += 1
            try:
                if has_mode_a:
                    start = row[i_startA]
                    # 时长直接转 int；空字符串或非数字会触发 ValueError
                    duration = int(row[i_dur])
                    # 按题意，这条模式下 start 格式为 "MM/DD/YYYY HH:MM"
                    date_str, hour = parse_mmddyyyy_hhmm(start)
                else:
                    # 模式B：ISO 起止时间，需计算 duration
                    start = row[i_startB]
                    end   = row[i_endB]
                    dt_s = fast_parse_iso_dt(start)
                    dt_e = fast_parse_iso_dt(end)
                    duration = int((dt_e - dt_s).total_seconds())
                    date_str = f"{dt_s.year:04d}-{dt_s.month:02d}-{dt_s.day:02d}"
                    hour = dt_s.hour

                # 过滤异常
                # if duration < 0 or duration > 86400:
                if duration < 0 or duration > 43200:
                # if duration < 0 or duration > 21600:
                # if duration < 0 or duration > 10800:
                    bad += 1
                    continue

                # 累计
                acc = day_acc.get(date_str)
                if acc is None:
                    acc = {
                        'hourly_counts':        [0]*24,
                        'hourly_durations_sum': [0.0]*24,
                        'daily_count':          0,
                        'daily_durations_sum':  0.0
                    }
                    day_acc[date_str] = acc
                acc['hourly_counts'][hour]        += 1
                acc['hourly_durations_sum'][hour] += duration
                acc['daily_count']                += 1
                acc['daily_durations_sum']        += duration

                # 统计范围
                if (min_date is None) or (date_str < min_date):
                    min_date = date_str
                if (max_date is None) or (date_str > max_date):
                    max_date = date_str

                if duration > maxdur: maxdur = duration
                if duration < mindur: mindur = duration
            
            except Exception:
                # 任一条解析失败，视为异常记录
                bad += 1
                continue
    print(f"完成文件: {csv_path}，总记录: {total}，异常/丢弃: {bad}，最大时长: {maxdur}，最小时长: {mindur}")
    return day_acc, min_date, max_date, total, bad, maxdur, mindur

# ---------- 主函数：并行处理并汇总 ----------

def calculate_daily_bike_data(source_folder: str, output_json: str, workers: int | None = None):
    csv_files = glob.glob(os.path.join(source_folder, "*.csv"))
    print(f"发现 {len(csv_files)} 个 CSV。")

    # 并行跑
    merged = defaultdict(lambda: {
        'hourly_counts':        [0]*24,
        'hourly_durations_sum': [0.0]*24,
        'daily_count':          0,
        'daily_durations_sum':  0.0
    })

    global_min, global_max = None, None
    total_all, bad_all = 0, 0
    maxdur_all, mindur_all = 0, 86400

    with ProcessPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(process_one_csv, p): p for p in csv_files}
        for fut in as_completed(futs):
            p = futs[fut]
            try:
                part, dmin, dmax, total, bad, md, nd = fut.result()
            except Exception as e:
                print(f"[ERROR] 子任务失败: {p}: {e}", file=sys.stderr)
                continue

            # 汇总字典
            for date_str, acc in part.items():
                m = merged[date_str]
                hc, hs = m['hourly_counts'], m['hourly_durations_sum']
                pc, ps = acc['hourly_counts'], acc['hourly_durations_sum']
                for h in range(24):
                    hc[h] += pc[h]
                    hs[h] += ps[h]
                m['daily_count']         += acc['daily_count']
                m['daily_durations_sum'] += acc['daily_durations_sum']

            # 汇总日期范围
            if dmin:
                print(f"filename={p}")
                print(f"{dmin=}")
                if (global_min is None) or (dmin < global_min):
                    global_min = dmin
            if dmax:
                print(f"filename={p}")
                print(f"{dmax=}")
                if (global_max is None) or (dmax > global_max):
                    global_max = dmax

            # 汇总统计
            total_all += total
            bad_all   += bad
            if md > maxdur_all: maxdur_all = md
            if nd < mindur_all: mindur_all = nd

    print(f"总记录: {total_all}，异常/丢弃: {bad_all}，最大时长: {maxdur_all}，最小时长: {mindur_all}")

    # 按日期补齐空天
    print(f"数据日期范围: {global_min} ~ {global_max}")
    if global_min and global_max:
        d0 = datetime.strptime(global_min, "%Y-%m-%d").date()
        d1 = datetime.strptime(global_max, "%Y-%m-%d").date()
        cur = d0
        while cur <= d1:
            _ = merged[cur.strftime("%Y-%m-%d")]  # 触发初始化
            cur += timedelta(days=1)

    # 计算“每小时平均时长”与“每天加权平均时长”
    result = {}
    for date_str, acc in merged.items():
        hourly_avg = []
        for h in range(24):
            cnt = acc['hourly_counts'][h]
            if cnt:
                hourly_avg.append(acc['hourly_durations_sum'][h] / cnt)
            else:
                hourly_avg.append(0.0)

        daily_cnt = acc['daily_count']
        daily_avg = (acc['daily_durations_sum'] / daily_cnt) if daily_cnt else 0.0

        result[date_str] = {
            "hourly_counts":    acc['hourly_counts'],
            "hourly_durations": hourly_avg,     # 平均值（秒）
            "daily_count":      daily_cnt,
            "daily_avg_dur":    daily_avg       # 平均值（秒）
        }

    # 排序 & 落盘
    result_sorted = dict(sorted(result.items(), key=lambda kv: kv[0]))
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(result_sorted, f, ensure_ascii=False, indent=4)

    return result_sorted

if __name__ == "__main__":
    src = "./unzip"
    out = "daily_bike_data.json"
    calculate_daily_bike_data(src, out, workers=None)
