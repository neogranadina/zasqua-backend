"""
Meilisearch integration for Zasqua catalog.

Provides indexing and search functionality for archival descriptions.
"""

import re
import meilisearch
from django.conf import settings


def strip_html(text):
    """Remove HTML tags from text."""
    if not text:
        return ''
    return re.sub(r'<[^>]+>', '', text).strip()


def get_client():
    """Get configured Meilisearch client."""
    return meilisearch.Client(
        settings.MEILISEARCH_URL,
        settings.MEILISEARCH_API_KEY or None
    )


def get_index():
    """Get or create the descriptions index."""
    client = get_client()
    return client.index(settings.MEILISEARCH_INDEX)


def configure_index():
    """
    Configure index settings for optimal search.
    Call this once during setup or when settings change.
    """
    index = get_index()

    # Searchable attributes (order matters for ranking)
    index.update_searchable_attributes([
        'title',
        'scope_content',
        'creator_display',
        'place_display',
        'local_identifier',
        'reference_code',
    ])

    # Filterable attributes (for faceting)
    index.update_filterable_attributes([
        'repository_code',
        'repository_name',
        'description_level',
        'resource_type',
        'date_start_year',
        'date_end_year',
        'has_digital',
    ])

    # Sortable attributes
    index.update_sortable_attributes([
        'title',
        'date_start_year',
        'reference_code',
        'created_at',
    ])

    # Displayed attributes (what's returned in results)
    index.update_displayed_attributes([
        'id',
        'reference_code',
        'local_identifier',
        'title',
        'description_level',
        'date_expression',
        'date_start_year',
        'date_end_year',
        'scope_content',
        'creator_display',
        'place_display',
        'repository_code',
        'repository_name',
        'path_cache',
        'has_digital',
    ])

    # Ranking rules (default + date preference)
    index.update_ranking_rules([
        'words',
        'typo',
        'proximity',
        'attribute',
        'sort',
        'exactness',
    ])

    return index


def description_to_document(description):
    """
    Convert a Description instance to a Meilisearch document.
    """
    scope = strip_html(description.scope_content)
    return {
        'id': description.id,
        'reference_code': description.reference_code,
        'local_identifier': description.local_identifier,
        'title': strip_html(description.title),
        'description_level': description.description_level,
        'resource_type': description.resource_type or '',
        'date_expression': description.date_expression,
        'date_start_year': description.date_start.year if description.date_start else None,
        'date_end_year': description.date_end.year if description.date_end else None,
        'scope_content': scope[:5000] if scope else '',
        'creator_display': description.creator_display,
        'place_display': description.place_display,
        'repository_code': description.repository.code,
        'repository_name': description.repository.name,
        'path_cache': description.path_cache,
        'has_digital': description.has_digital,
        'created_at': description.created_at.isoformat() if description.created_at else None,
    }


def index_descriptions(descriptions, batch_size=1000):
    """
    Index a queryset of descriptions.
    Returns (indexed_count, errors).
    """
    index = get_index()
    indexed = 0
    errors = []

    batch = []
    for desc in descriptions.select_related('repository').iterator():
        try:
            batch.append(description_to_document(desc))
            if len(batch) >= batch_size:
                task = index.add_documents(batch)
                indexed += len(batch)
                batch = []
        except Exception as e:
            errors.append((desc.id, str(e)))

    # Index remaining
    if batch:
        task = index.add_documents(batch)
        indexed += len(batch)

    return indexed, errors


def delete_from_index(description_ids):
    """Remove descriptions from index by ID."""
    index = get_index()
    if isinstance(description_ids, int):
        description_ids = [description_ids]
    index.delete_documents(description_ids)


def search(query, filters=None, sort=None, page=1, per_page=50):
    """
    Search descriptions.

    Args:
        query: Search query string
        filters: Dict of filter conditions, e.g.:
            {'repository_code': 'co-cihjml', 'description_level': 'item'}
        sort: List of sort criteria, e.g. ['date_start_year:asc']
        page: Page number (1-indexed)
        per_page: Results per page

    Returns:
        Meilisearch search results dict with hits, facets, etc.
    """
    index = get_index()

    # Build filter string
    filter_parts = []
    if filters:
        for key, value in filters.items():
            if value is None:
                continue
            if key == 'date_from' and value:
                filter_parts.append(f'date_end_year >= {value}')
            elif key == 'date_to' and value:
                filter_parts.append(f'date_start_year <= {value}')
            elif key == 'has_digital':
                filter_parts.append(f'has_digital = true')
            elif isinstance(value, list):
                # OR within same field
                conditions = ' OR '.join(f'{key} = "{v}"' for v in value)
                filter_parts.append(f'({conditions})')
            else:
                filter_parts.append(f'{key} = "{value}"')

    search_params = {
        'limit': per_page,
        'offset': (page - 1) * per_page,
        'facets': ['repository_code', 'description_level', 'has_digital'],
        'attributesToHighlight': ['title', 'scope_content'],
        'highlightPreTag': '<mark>',
        'highlightPostTag': '</mark>',
    }

    if filter_parts:
        search_params['filter'] = ' AND '.join(filter_parts)

    if sort:
        search_params['sort'] = sort

    return index.search(query, search_params)


def get_stats():
    """Get index statistics."""
    index = get_index()
    return index.get_stats()
