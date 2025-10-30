from datetime import datetime, timedelta
import os
import csv
import glob
import json
from collections import defaultdict
from pathlib import Path
def calculate_daily_weather_data(source_folder: str, output_json: str):
    """
    计算指定文件夹下所有CSV文件中每天的天气数据。

    参数:
    source_folder (str): 包含CSV文件的源文件夹路径。
    output_json (str): 输出JSON文件路径，用于存储每天的记录次数。
    """

    # {
    #     "tavg": "2.7",
    #     "tmin": "-1.1",
    #     "tmax": "6.7",
    #     "prcp": "",
    #     "snow": "",
    #     "wdir": "",
    #     "wspd": "12.7",
    #     "wpgt": "",
    #     "pres": "1014.8",
    #     "tsun": ""
    # }
    daily_weather_data = defaultdict(lambda : {'tavg': "", 'tmin': "", 'tmax': "", 'prcp': "", 'snow': "", 'wdir': "", 'wspd': "", 'wpgt': "", 'pres': "", 'tsun': ""})
    csv_files = glob.glob(os.path.join(source_folder, '*.csv'))
    print(f"找到 {len(csv_files)} 个CSV文件。")
    datename = '\ufeffdate'
    for csv_file in csv_files:
        if len(csv_file.strip()) != 8:
            continue
        try:
            with open(csv_file, mode='r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    # print(row)
                    date = row[datename]  # 取日期部分
                    # 不在daily_weather_data中存储日期
                    row.pop(datename, None)
                    daily_weather_data[date.split(" ")[0]] = row
        except Exception as e:
            print(f"读取 {csv_file} 时发生错误: {e}")

    # 从第一天到最后一天，确保每天都有数据，即使某些天没有天气记录，也要初始化为0
    if daily_weather_data:
        all_dates = sorted(daily_weather_data.keys())
        start_date_str = all_dates[0]
        end_date_str = all_dates[-1]
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        current_date = start_date
        print(f"初始化日期范围从 {start_date} 到 {end_date}")
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            if date_str not in daily_weather_data:
                print("初始化缺失日期数据:", date_str)
                daily_weather_data[date_str]
            current_date += timedelta(days=1)

    # 计算每天的记录次数
    daily_record_counts = defaultdict(int)
    for date, records in daily_weather_data.items():
        daily_record_counts[date] = len(records)

    # 保存记录次数和天气数据到JSON文件
    with open("daily_weather_data.json", 'w', encoding='utf-8') as json_file:
        json.dump(daily_weather_data, json_file, ensure_ascii=False, indent=4)

    return daily_weather_data
if __name__ == "__main__":
    source_dir = ""
    output_json_path = "daily_record_counts.json"
    daily_weather = calculate_daily_weather_data(source_dir, output_json_path)
    