from django.core.management.base import BaseCommand
from pathlib import Path
from django.conf import settings
from django.db import transaction, connections
from bikes.models import BikeUsageData
import csv

CHUNK_SIZE = 5000  # 可按机器内存和DB调大/调小


class Command(BaseCommand):
    help = '从 bikes/merged_data.json 读取数据并批量写入数据库'

    def _get_arg(self,record, field_name, default=None):
        value = record.get(field_name, default)
        if value == "":
            value = default
        return value

    def handle(self, *args, **kwargs):
        file_path = Path(settings.BASE_DIR) / 'bikes' / 'merged_data.json'
        objs = []
        total = 0
        with open(file_path, 'r', encoding='utf-8') as f:
            import json
            data = json.load(f)
            for date_str, record in data.items():
                prcp_cnt = self._get_arg(record, 'prcp', 0.0)
                wspd_cnt = self._get_arg(record, 'wspd', 0.0)
                snow_cnt = self._get_arg(record, 'snow', 0.0)
                pres_cnt = self._get_arg(record, 'pres', 0.0)
                tmin_val = self._get_arg(record, 'tmin', 0.0)
                tmax_val = self._get_arg(record, 'tmax', 0.0)
                obj = BikeUsageData(
                    date=date_str,
                    year=int(date_str[:4]),
                    month=int(date_str[5:7]),
                    day=int(date_str[8:10]),
                    hourly_counts=record.get('hourly_counts', []),
                    hourly_durations=record.get('hourly_durations', []),
                    avg_temperature=record.get('tavg', 0.0),
                    min_temperature=tmin_val,
                    max_temperature=tmax_val,
                    precipitation=prcp_cnt,
                    windspeed=wspd_cnt,
                    snow=snow_cnt,
                    pressure=pres_cnt,
                )
                objs.append(obj)
                total += 1

                if len(objs) >= CHUNK_SIZE:
                    BikeUsageData.objects.bulk_create(objs)
                    self.stdout.write(f'已插入 {total} 条记录')
                    objs = []

            if objs:
                BikeUsageData.objects.bulk_create(objs)
                self.stdout.write(f'已插入 {total} 条记录')
        
        