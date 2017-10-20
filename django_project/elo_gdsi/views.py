from django.shortcuts import render
from django.http import HttpResponse
from elo_gdsi_module import get_ratings as gr
import csv
import unicodecsv

r_data = []

def index(request):
    r_data = gr()
    response = HttpResponse(mimetype='text/plain; charset=utf-8')
#    response['Content-Disposition'] = 'attachment;filename=export.csv'
    writer = unicodecsv.writer(response)
    writer.writerows(r_data)


    return response
