"""
URL configuration for compta project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from django.urls import re_path
from django.contrib import staticfiles
from pdf import views

urlpatterns = [
    path('admin/', admin.site.urls),
    path("banque", views.banque_list, name="banque_list"),
    path("banque/<int:banque_id>", views.banque_associate_file, name="piece_associate_file"),
    path("pieces", views.piece_list, name="pieces_list"),
    path("piece/<str:md5>", views.piece_associate_banque, name="piece_associate_banque"),
    path("files", views.file_list, name="files_list"),
    path("check", views.file_list, name="files_check"),
    path("update", views.file_update, name="files_update"),
    path("pdf/<str:md5>", views.pdf_edit, name="pdf_edit"),
    path("associate/<str:id>", views.piece_pre_associate, name="piece_pre_associate"),

    re_path(r"^static/(?P<path>.*)$", staticfiles.views.serve),
]
