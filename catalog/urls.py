"""
URL configuration for Zasqua Catalog API.
"""

from django.urls import path, include
from rest_framework.routers import DefaultRouter

from .views import (
    RepositoryViewSet, DescriptionViewSet, EntityViewSet, PlaceViewSet,
)

router = DefaultRouter()
router.register(r'repositories', RepositoryViewSet, basename='repository')
router.register(r'descriptions', DescriptionViewSet, basename='description')
router.register(r'entities', EntityViewSet, basename='entity')
router.register(r'places', PlaceViewSet, basename='place')

urlpatterns = [
    path('', include(router.urls)),
]
