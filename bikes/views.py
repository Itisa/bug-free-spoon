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

def api(request):
    data = BikeUsageData.objects.all().order_by('date').values()
    data_list = list(data)
    return JsonResponse(data_list, safe=False)