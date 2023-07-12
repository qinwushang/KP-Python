from django.urls import re_path as url

from DjangoTest_01 import views

urlpatterns = [
    url(r'^search/$', views.search),
]
