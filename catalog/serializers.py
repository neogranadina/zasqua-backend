"""
REST API Serializers for Zasqua Catalog.
"""

from rest_framework import serializers
from .models import (
    Repository, Description, Entity, Place,
    DescriptionEntity, DescriptionPlace
)


class RepositoryListSerializer(serializers.ModelSerializer):
    """Compact serializer for repository listings."""
    description_count = serializers.SerializerMethodField()

    class Meta:
        model = Repository
        fields = ['id', 'code', 'name', 'city', 'country_code', 'description_count']

    def get_description_count(self, obj):
        return obj.descriptions.count()


class RepositoryDetailSerializer(serializers.ModelSerializer):
    """Full serializer for repository detail view."""
    description_count = serializers.SerializerMethodField()
    root_descriptions = serializers.SerializerMethodField()

    class Meta:
        model = Repository
        fields = '__all__'

    def get_description_count(self, obj):
        return obj.descriptions.count()

    def get_root_descriptions(self, obj):
        """Return top-level descriptions for this repository."""
        roots = obj.descriptions.filter(parent__isnull=True)[:100]  # Increased limit
        return DescriptionListSerializer(roots, many=True).data


class EntitySerializer(serializers.ModelSerializer):
    """Serializer for Entity model."""

    class Meta:
        model = Entity
        fields = [
            'id', 'entity_code', 'display_name', 'sort_name', 'entity_type',
            'honorific', 'primary_function', 'dates_of_existence', 'history'
        ]


class PlaceSerializer(serializers.ModelSerializer):
    """Serializer for Place model."""

    class Meta:
        model = Place
        fields = [
            'id', 'label', 'display_name', 'place_type',
            'latitude', 'longitude', 'country_code',
            'admin_level_1', 'admin_level_2',
            'colonial_gobernacion', 'colonial_partido', 'colonial_region'
        ]


class DescriptionEntitySerializer(serializers.ModelSerializer):
    """Serializer for entity links."""
    entity = EntitySerializer(read_only=True)

    class Meta:
        model = DescriptionEntity
        fields = [
            'entity', 'role', 'role_note', 'sequence',
            'honorific', 'function', 'name_as_recorded'
        ]


class DescriptionPlaceSerializer(serializers.ModelSerializer):
    """Serializer for place links."""
    place = PlaceSerializer(read_only=True)

    class Meta:
        model = DescriptionPlace
        fields = ['place', 'role', 'role_note']


class DescriptionListSerializer(serializers.ModelSerializer):
    """Compact serializer for description listings."""
    repository_code = serializers.CharField(source='repository.code', read_only=True)
    parent_id = serializers.IntegerField(source='parent.id', read_only=True, allow_null=True)
    parent_reference_code = serializers.CharField(source='parent.reference_code', read_only=True, allow_null=True)
    has_children = serializers.SerializerMethodField()
    child_count = serializers.SerializerMethodField()
    children_level = serializers.SerializerMethodField()

    class Meta:
        model = Description
        fields = [
            'id', 'repository_code', 'reference_code', 'local_identifier',
            'title', 'description_level', 'date_expression',
            'parent_id', 'parent_reference_code',
            'has_children', 'child_count', 'children_level', 'has_digital',
            # Include key metadata for description pages
            'scope_content', 'extent', 'arrangement', 'access_conditions',
            'language', 'notes', 'creator_display', 'place_display'
        ]

    def get_has_children(self, obj):
        return obj.get_descendant_count() > 0

    def get_child_count(self, obj):
        return obj.get_children().count()

    def get_children_level(self, obj):
        """Return the description_level of the first child, or infer from title patterns."""
        first_child = obj.get_children().first()
        if first_child:
            # Check if title suggests a container type (Caja, Carpeta, Legajo, Tomo)
            title_lower = first_child.title.lower()
            if title_lower.startswith('caja '):
                return 'caja'
            elif title_lower.startswith('carpeta '):
                return 'carpeta'
            elif title_lower.startswith('legajo '):
                return 'legajo'
            elif title_lower.startswith('tomo '):
                return 'tomo'
            return first_child.description_level
        return None


class DescriptionDetailSerializer(serializers.ModelSerializer):
    """Full serializer for description detail view."""
    repository = RepositoryListSerializer(read_only=True)
    breadcrumb = serializers.SerializerMethodField()
    children = serializers.SerializerMethodField()
    entities = serializers.SerializerMethodField()
    places = serializers.SerializerMethodField()

    class Meta:
        model = Description
        fields = '__all__'

    def get_breadcrumb(self, obj):
        """Return ancestor chain for navigation."""
        ancestors = obj.get_ancestors(include_self=False)
        return [
            {
                'id': a.id,
                'title': a.title[:80] + '...' if len(a.title) > 80 else a.title,
                'description_level': a.description_level
            }
            for a in ancestors
        ]

    def get_children(self, obj):
        """Return direct children."""
        children = obj.get_children()[:50]
        return DescriptionListSerializer(children, many=True).data

    def get_entities(self, obj):
        """Return linked entities."""
        links = obj.entity_links.select_related('entity').all()
        return DescriptionEntitySerializer(links, many=True).data

    def get_places(self, obj):
        """Return linked places."""
        links = obj.place_links.select_related('place').all()
        return DescriptionPlaceSerializer(links, many=True).data


class DescriptionTreeSerializer(serializers.ModelSerializer):
    """Serializer for tree/hierarchy view."""
    children = serializers.SerializerMethodField()

    class Meta:
        model = Description
        fields = ['id', 'title', 'reference_code', 'description_level', 'children']

    def get_children(self, obj):
        """Recursively serialize children (with depth limit)."""
        depth = self.context.get('depth', 2)
        if depth <= 0:
            return []
        children = obj.get_children()
        return DescriptionTreeSerializer(
            children, many=True,
            context={'depth': depth - 1}
        ).data


class SearchResultSerializer(serializers.ModelSerializer):
    """Serializer for search results."""
    repository_code = serializers.CharField(source='repository.code', read_only=True)
    breadcrumb_text = serializers.SerializerMethodField()

    class Meta:
        model = Description
        fields = [
            'id', 'repository_code', 'reference_code', 'local_identifier',
            'title', 'description_level', 'date_expression', 'scope_content',
            'creator_display', 'breadcrumb_text'
        ]

    def get_breadcrumb_text(self, obj):
        """Return breadcrumb as text string."""
        ancestors = obj.get_ancestors(include_self=False)
        if not ancestors:
            return obj.repository.code
        parts = [obj.repository.code]
        for a in ancestors[:3]:
            parts.append(a.title[:30])
        return ' > '.join(parts)
