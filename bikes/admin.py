from django.contrib import admin
from django import forms
from django.shortcuts import render
from django.http import HttpResponseRedirect
from django.urls import path
from django.contrib import messages

from .models import BikeUsageData
from django.contrib.admin import DateFieldListFilter

class BikeUsageDataAdmin(admin.ModelAdmin):
    list_filter = ('year','month','day')
    
admin.site.register(BikeUsageData, BikeUsageDataAdmin)