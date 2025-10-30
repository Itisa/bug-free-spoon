from django.db import models

# Create your models here.

class BikeUsageData(models.Model):
    date = models.DateField()
    year = models.IntegerField()
    month = models.IntegerField()
    day = models.IntegerField()
    hourly_counts = models.JSONField()
    hourly_durations = models.JSONField()
    avg_temperature = models.FloatField()
    min_temperature = models.FloatField()
    max_temperature = models.FloatField()
    precipitation = models.FloatField()
    windspeed = models.FloatField()
    snow = models.FloatField()
    pressure = models.FloatField()
    def __str__(self):
        return f"{self.date}"