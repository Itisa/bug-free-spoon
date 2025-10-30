from django.shortcuts import render

from django.http import HttpResponse, JsonResponse
from .models import BikeUsageData

def index(request):
    rsp = render(request, 'bikes/bikes.html')
    return rsp

def api(request):
    data = BikeUsageData.objects.all().order_by('date').values()
    data_list = list(data)
    return JsonResponse(data_list, safe=False)