from django.shortcuts import render, HttpResponse

import json

from DjangoTest_01 import newsearch


def search(request):
    if request.GET:
        handler = newsearch.MedicalGraph()
        number = request.GET['number']
        name = request.GET['name']
        print(name, number)
        if len(name) != 0:
            search_result = handler.query_onestart_relationship(name, number)
            return HttpResponse(json.dumps(search_result, ensure_ascii=False))
        else:
            search_result = handler.query_all_relationship(number)
            return HttpResponse(json.dumps(search_result, ensure_ascii=False))


