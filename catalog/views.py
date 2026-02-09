"""
REST API Views for Zasqua Catalog.
"""

from django.db.models import Q, Count
from rest_framework import viewsets, filters, status
from rest_framework.decorators import action, api_view
from rest_framework.response import Response

from .models import Repository, Description, Entity, Place
from .serializers import (
    RepositoryListSerializer, RepositoryDetailSerializer,
    DescriptionListSerializer, DescriptionDetailSerializer,
    DescriptionTreeSerializer, EntitySerializer, PlaceSerializer,
    SearchResultSerializer
)
from . import search as search_service


class RepositoryViewSet(viewsets.ReadOnlyModelViewSet):
    """
    API endpoint for repositories.

    list: Get all repositories
    retrieve: Get a single repository with its root descriptions
    """
    queryset = Repository.objects.filter(enabled=True).order_by('name')
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['name', 'code', 'city']
    ordering_fields = ['name', 'created_at']

    def get_serializer_class(self):
        if self.action == 'retrieve':
            return RepositoryDetailSerializer
        return RepositoryListSerializer


class DescriptionViewSet(viewsets.ReadOnlyModelViewSet):
    """
    API endpoint for archival descriptions.

    list: Get descriptions (filterable by repository, level, etc.)
    retrieve: Get a single description with its children and linked entities/places
    tree: Get hierarchical tree structure
    search: Full-text search across descriptions
    """
    queryset = Description.objects.select_related('repository', 'parent').order_by('tree_id', 'lft')
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['title', 'scope_content', 'local_identifier', 'creator_display']
    ordering_fields = ['title', 'date_start', 'created_at']

    def get_serializer_class(self):
        if self.action == 'retrieve':
            return DescriptionDetailSerializer
        if self.action == 'tree':
            return DescriptionTreeSerializer
        if self.action == 'search':
            return SearchResultSerializer
        return DescriptionListSerializer

    def get_queryset(self):
        queryset = super().get_queryset()

        # Annotate child_count for list views (avoids N+1 queries)
        # This counts direct children using parent_id foreign key
        if self.action == 'list':
            queryset = queryset.annotate(
                _child_count=Count('children')
            )

        # Filter by repository
        repository_id = self.request.query_params.get('repository')
        if repository_id:
            queryset = queryset.filter(repository_id=repository_id)

        # Filter by level
        description_level = self.request.query_params.get('level')
        if description_level:
            queryset = queryset.filter(description_level=description_level)

        # Filter by parent (for browsing hierarchy)
        parent_id = self.request.query_params.get('parent')
        if parent_id:
            if parent_id == 'null' or parent_id == 'root':
                queryset = queryset.filter(parent__isnull=True)
            else:
                queryset = queryset.filter(parent_id=parent_id)

        # Filter to only root units
        root_only = self.request.query_params.get('root_only')
        if root_only and root_only.lower() in ('true', '1', 'yes'):
            queryset = queryset.filter(parent__isnull=True)

        return queryset

    @action(detail=False, methods=['get'])
    def search(self, request):
        """
        Full-text search across descriptions.

        Query params:
            q: Search query
            repository: Filter by repository ID
            level: Filter by level
            date_from: Filter by date range start (YYYY)
            date_to: Filter by date range end (YYYY)
        """
        query = request.query_params.get('q', '').strip()
        if not query:
            return Response({'results': [], 'count': 0})

        queryset = self.get_queryset()

        # Basic text search
        queryset = queryset.filter(
            Q(title__icontains=query) |
            Q(scope_content__icontains=query) |
            Q(local_identifier__icontains=query) |
            Q(creator_display__icontains=query)
        )

        # Date range filters
        date_from = request.query_params.get('date_from')
        date_to = request.query_params.get('date_to')
        if date_from:
            try:
                year = int(date_from)
                queryset = queryset.filter(
                    Q(date_end__year__gte=year) | Q(date_end__isnull=True, date_start__year__gte=year)
                )
            except ValueError:
                pass
        if date_to:
            try:
                year = int(date_to)
                queryset = queryset.filter(
                    Q(date_start__year__lte=year) | Q(date_start__isnull=True)
                )
            except ValueError:
                pass

        # Paginate
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = SearchResultSerializer(page, many=True)
            return self.get_paginated_response(serializer.data)

        serializer = SearchResultSerializer(queryset[:100], many=True)
        return Response({'results': serializer.data, 'count': queryset.count()})

    @action(detail=True, methods=['get'])
    def tree(self, request, pk=None):
        """
        Get hierarchical tree starting from this description.

        Query params:
            depth: How many levels to include (default 2, max 5)
        """
        description = self.get_object()
        depth = min(int(request.query_params.get('depth', 2)), 5)
        serializer = DescriptionTreeSerializer(description, context={'depth': depth})
        return Response(serializer.data)

    @action(detail=True, methods=['get'])
    def children(self, request, pk=None):
        """
        Get direct children of this description.

        Query params:
            all: If 'true', return all children without pagination (default: true)
        """
        description = self.get_object()
        # Annotate child_count and sort by reference_code for consistent ordering
        children = description.get_children().annotate(_child_count=Count('children')).order_by('reference_code')

        # By default, return all children for tree navigation
        # Use ?all=false to get paginated results
        return_all = request.query_params.get('all', 'true').lower() in ('true', '1', 'yes')

        if return_all:
            serializer = DescriptionListSerializer(children, many=True)
            return Response({
                'count': children.count(),
                'results': serializer.data
            })

        page = self.paginate_queryset(children)
        if page is not None:
            serializer = DescriptionListSerializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        serializer = DescriptionListSerializer(children, many=True)
        return Response(serializer.data)

    @action(detail=True, methods=['get'])
    def ancestors(self, request, pk=None):
        """Get all ancestors (breadcrumb) of this description."""
        description = self.get_object()
        ancestors = description.get_ancestors(include_self=False)
        serializer = DescriptionListSerializer(ancestors, many=True)
        return Response(serializer.data)


class EntityViewSet(viewsets.ReadOnlyModelViewSet):
    """
    API endpoint for entities (persons, organizations).

    list: Get all entities
    retrieve: Get a single entity with linked descriptions (by ID or entity_code)
    """
    queryset = Entity.objects.filter(merged_into__isnull=True).order_by('sort_name')
    serializer_class = EntitySerializer
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['display_name', 'sort_name', 'entity_code']
    ordering_fields = ['sort_name', 'created_at']

    def get_object(self):
        """Allow lookup by entity_code (ne-xxxxx) or numeric ID."""
        pk = self.kwargs.get('pk')
        if pk and pk.startswith('ne-'):
            return self.queryset.get(entity_code=pk)
        return super().get_object()

    def get_queryset(self):
        queryset = super().get_queryset()

        # Filter by entity type
        entity_type = self.request.query_params.get('type')
        if entity_type:
            queryset = queryset.filter(entity_type=entity_type)

        return queryset

    @action(detail=True, methods=['get'])
    def descriptions(self, request, pk=None):
        """Get descriptions linked to this entity."""
        entity = self.get_object()
        links = entity.description_links.select_related(
            'description', 'description__repository'
        )
        descriptions = [link.description for link in links]
        serializer = DescriptionListSerializer(descriptions, many=True)
        return Response(serializer.data)


class PlaceViewSet(viewsets.ReadOnlyModelViewSet):
    """
    API endpoint for places (gazetteer).

    list: Get all places
    retrieve: Get a single place with linked descriptions
    """
    queryset = Place.objects.filter(merged_into__isnull=True).order_by('label')
    serializer_class = PlaceSerializer
    filter_backends = [filters.SearchFilter, filters.OrderingFilter]
    search_fields = ['label', 'display_name']
    ordering_fields = ['label', 'created_at']

    def get_queryset(self):
        queryset = super().get_queryset()

        # Filter by place type
        place_type = self.request.query_params.get('type')
        if place_type:
            queryset = queryset.filter(place_type=place_type)

        # Filter to geocoded only
        geocoded = self.request.query_params.get('geocoded')
        if geocoded and geocoded.lower() in ('true', '1', 'yes'):
            queryset = queryset.filter(latitude__isnull=False, longitude__isnull=False)

        return queryset

    @action(detail=True, methods=['get'])
    def descriptions(self, request, pk=None):
        """Get descriptions linked to this place."""
        place = self.get_object()
        links = place.description_links.select_related(
            'description', 'description__repository'
        )
        descriptions = [link.description for link in links]
        serializer = DescriptionListSerializer(descriptions, many=True)
        return Response(serializer.data)


@api_view(['GET'])
def meilisearch_search(request):
    """
    Full-text search using Meilisearch.

    Query params:
        q: Search query (optional, empty returns all documents)
        repository: Filter by repository code (repeatable for multi-select)
        level: Filter by description level (repeatable for multi-select)
        has_digital: Filter by digitised status (true/false)
        date_from: Filter by date range start (year)
        date_to: Filter by date range end (year)
        sort: Sort field (date_start_year:asc, date_start_year:desc, title:asc)
        page: Page number (default 1)
        per_page: Results per page (default 50, max 100)

    Returns:
        hits: List of matching documents with highlights
        facets: Facet counts for filtering
        total: Total number of matches
        page: Current page
        total_pages: Total number of pages
    """
    query = request.query_params.get('q', '').strip()

    # Build filters
    filters = {}
    repositories = request.query_params.getlist('repository')
    if repositories:
        filters['repository_code'] = repositories if len(repositories) > 1 else repositories[0]
    levels = request.query_params.getlist('level')
    if levels:
        filters['description_level'] = levels if len(levels) > 1 else levels[0]
    if request.query_params.get('has_digital') == 'true':
        filters['has_digital'] = 'true'
    if request.query_params.get('date_from'):
        try:
            filters['date_from'] = int(request.query_params.get('date_from'))
        except ValueError:
            pass
    if request.query_params.get('date_to'):
        try:
            filters['date_to'] = int(request.query_params.get('date_to'))
        except ValueError:
            pass

    # Pagination
    try:
        page = max(1, int(request.query_params.get('page', 1)))
    except ValueError:
        page = 1
    try:
        per_page = min(100, max(1, int(request.query_params.get('per_page', 50))))
    except ValueError:
        per_page = 50

    # Sort
    sort_param = request.query_params.get('sort')
    sort = [sort_param] if sort_param else None

    try:
        results = search_service.search(
            query=query,
            filters=filters,
            sort=sort,
            page=page,
            per_page=per_page
        )

        # Format response
        total = results.get('estimatedTotalHits', 0)
        total_pages = (total + per_page - 1) // per_page if total > 0 else 0

        return Response({
            'query': query,
            'hits': results.get('hits', []),
            'facets': results.get('facetDistribution', {}),
            'total': total,
            'page': page,
            'per_page': per_page,
            'total_pages': total_pages,
            'processing_time_ms': results.get('processingTimeMs', 0),
        })
    except Exception as e:
        return Response(
            {'error': f'Search error: {str(e)}'},
            status=status.HTTP_503_SERVICE_UNAVAILABLE
        )
