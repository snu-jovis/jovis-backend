from django.urls import path

from web.views import QueryView

urlpatterns = [
    path('query/', QueryView.as_view()),
]