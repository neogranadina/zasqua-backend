"""
Fisqua Catalog Admin Configuration

Admin interfaces with intelligent tiered field display:
- ~20 essential fields visible by default
- Everything else in collapsed sections organized by ISAD(G) area
- Designed not to overwhelm catalogers
"""

from django import forms
from django.contrib import admin
from mptt.admin import MPTTModelAdmin

from .models import Repository, CatalogUnit, Place, CatalogUnitPlace
from .countries import COUNTRY_CHOICES
from .departments import COLOMBIA_DEPARTMENTS


class RepositoryAdminForm(forms.ModelForm):
    """Custom form with country dropdown and conditional department field."""
    country_code = forms.ChoiceField(
        choices=[('', '---------')] + COUNTRY_CHOICES,
        required=False,
        initial='COL',
    )

    class Meta:
        model = Repository
        fields = '__all__'
        widgets = {
            'region': forms.TextInput(attrs={'class': 'region-text'}),
        }

    class Media:
        js = ('admin/js/country_region.js',)


class CatalogUnitPlaceInline(admin.TabularInline):
    """Inline for editing place associations on CatalogUnit."""
    model = CatalogUnitPlace
    extra = 1
    autocomplete_fields = ['place']


@admin.register(Repository)
class RepositoryAdmin(admin.ModelAdmin):
    form = RepositoryAdminForm
    list_display = ['name', 'abbreviation', 'institution_type', 'city',
                    'country_code', 'enabled']
    list_filter = ['enabled', 'institution_type', 'country_code']
    search_fields = ['name', 'abbreviation', 'repository_code', 'city']
    ordering = ['name']

    fieldsets = (
        ('Basic Information', {
            'fields': ('name', 'name_translations', 'abbreviation', 'repository_code')
        }),
        ('Location & Contact', {
            'fields': ('institution_type', 'country_code', 'region', 'city',
                       'address', 'website_url', 'contact_email', 'contact_phone')
        }),
        ('Settings', {
            'fields': ('default_metadata_standard', 'default_language')
        }),
        ('Administrative', {
            'fields': ('enabled', 'notes')
        }),
    )


@admin.register(CatalogUnit)
class CatalogUnitAdmin(MPTTModelAdmin):
    """
    Tiered admin interface for CatalogUnit.

    Design principles:
    - ~20 most common fields visible by default
    - Collapsed sections for specialized fields
    - Organized by ISAD(G) areas
    """

    list_display = ['title_short', 'reference_code', 'repository', 'level_type',
                    'date_expression', 'is_published']
    list_filter = ['repository', 'metadata_standard', 'level_type', 'is_published',
                   'access_conditions', 'resource_type', 'has_digital_files']
    search_fields = ['title', 'local_identifier', 'description', 'creator_string']
    autocomplete_fields = ['repository', 'parent', 'created_by', 'updated_by']
    readonly_fields = ['reference_code']
    ordering = ['tree_id', 'lft']
    date_hierarchy = 'date_start'

    inlines = [CatalogUnitPlaceInline]

    fieldsets = (
        # =====================================================================
        # ESSENTIAL FIELDS (~20) - Always visible
        # =====================================================================
        ('Essential Information', {
            'description': 'Core fields for every catalog record',
            'fields': (
                'repository',
                'parent',
                'level_type',
                'local_identifier',
                'reference_code',
                'title',
                'translated_title',
                ('date_expression', 'date_start', 'date_end'),
                'extent_expression',
                'creator_string',
                'description',
                'language_codes',
                ('access_conditions', 'resource_type'),
                ('is_published', 'has_digital_files'),
            )
        }),

        # =====================================================================
        # COLLAPSED SECTIONS - Organized by ISAD(G) area
        # =====================================================================

        # ISAD(G) 3.1 Identity Statement Area (additional fields)
        ('Identity Details', {
            'classes': ('collapse',),
            'description': 'ISAD(G) 3.1 - Additional identity fields',
            'fields': (
                'metadata_standard',
                ('neogranadina_pid', 'original_reference'),
                'uniform_title',
                ('date_type', 'date_note'),
                ('date_start_approximation', 'date_end_approximation'),
                ('extent_quantity', 'extent_unit'),
                'extent_note',
                ('dimensions', 'medium'),
                ('duration', 'condition'),
            )
        }),

        # ISAD(G) 3.2 Context Area
        ('Context', {
            'classes': ('collapse',),
            'description': 'ISAD(G) 3.2 - Provenance and history',
            'fields': (
                'administrative_history',
                'biographical_history',
                'archival_history',
                'acquisition_info',
            )
        }),

        # ISAD(G) 3.3 Content and Structure Area
        ('Content & Structure', {
            'classes': ('collapse',),
            'description': 'ISAD(G) 3.3 - Additional content description',
            'fields': (
                'description_translations',
                'appraisal_destruction',
                'accruals',
                'system_of_arrangement',
            )
        }),

        # ISAD(G) 3.4 Access and Use Area
        ('Access & Rights', {
            'classes': ('collapse',),
            'description': 'ISAD(G) 3.4 - Conditions of access and use',
            'fields': (
                'access_restrictions_note',
                ('access_restriction_type', 'access_restriction_end_date'),
                ('contains_sensitive_data', 'sensitive_data_nature'),
                'reproduction_conditions',
                ('rights_copyright_status', 'rights_holder_name'),
                'rights_statement',
                ('rights_license', 'rights_note'),
                'language_note',
                'physical_characteristics',
                'technical_requirements',
                ('finding_aids', 'finding_aid_url'),
            )
        }),

        # ISAD(G) 3.5 Allied Materials Area
        ('Allied Materials & Physical Location', {
            'classes': ('collapse',),
            'description': 'ISAD(G) 3.5 - Related materials',
            'fields': (
                'location_of_originals',
                'physical_location',
                'physical_collection',
                ('physical_box', 'physical_folder'),
                'physical_location_note',
                'location_of_copies',
                'related_units',
                'publication_note',
            )
        }),

        # ISAD(G) 3.6 & 3.7 Notes and Description Control
        ('Notes & Description Control', {
            'classes': ('collapse',),
            'description': 'ISAD(G) 3.6/3.7 - Notes and cataloging info',
            'fields': (
                'notes',
                'internal_notes',
                ('cataloger_name', 'description_status'),
                'rules_conventions',
                ('description_date', 'description_revision_date'),
                'statement_of_responsibility',
            )
        }),

        # Provenance Details (for books, periodicals, photos)
        ('Provenance Details', {
            'classes': ('collapse',),
            'description': 'For books, periodicals, photographs, and AV materials',
            'fields': (
                ('author', 'editor'),
                'scribe',
                ('publisher', 'publisher_location'),
                ('photographer', 'artist'),
                ('composer', 'director'),
                ('volume_number', 'issue_number', 'page_number'),
            )
        }),

        # Subjects & Keywords
        ('Subjects & Keywords', {
            'classes': ('collapse',),
            'description': 'Subject access points',
            'fields': (
                'subjects_topic',
                'subjects_geographic',
                'subjects_temporal',
                'subjects_name_string',
            )
        }),

        # Digital
        ('Digital Files & Links', {
            'classes': ('collapse',),
            'description': 'Digital attachments and IIIF',
            'fields': (
                ('external_url', 'external_url_label'),
                ('iiif_manifest_url', 'iiif_manifest_version'),
                ('digital_folder_name', 'digital_file_count'),
                ('digital_file_format', 'digitization_date'),
                'digitization_notes',
                'has_external_link',
            )
        }),

        # Display & Sorting
        ('Display & Sorting', {
            'classes': ('collapse',),
            'description': 'Control display order and visibility',
            'fields': (
                ('sequence_number', 'sort_key'),
                'descendant_count',
                ('publication_date', 'featured'),
            )
        }),

        # Metadata (who created/updated)
        ('Record Metadata', {
            'classes': ('collapse',),
            'description': 'Record creation and modification info',
            'fields': (
                ('created_by', 'updated_by'),
            )
        }),
    )

    def title_short(self, obj):
        """Truncated title for list display."""
        return obj.title[:80] + '...' if len(obj.title) > 80 else obj.title
    title_short.short_description = 'Title'


@admin.register(Place)
class PlaceAdmin(admin.ModelAdmin):
    list_display = ['label', 'historical_name', 'place_type', 'latitude',
                    'longitude', 'historical_admin_1', 'is_active']
    list_filter = ['place_type', 'is_active', 'country_code', 'historical_region',
                   'historical_admin_1']
    search_fields = ['label', 'historical_name', 'gazetteer_id']
    ordering = ['label']

    fieldsets = (
        ('Identification', {
            'fields': ('gazetteer_id', 'gazetteer_source', 'label', 'historical_name',
                       'place_type')
        }),
        ('Geocoding', {
            'fields': ('latitude', 'longitude', 'coordinate_precision',
                       'coordinate_source')
        }),
        ('Modern Administrative', {
            'fields': ('country_code', 'admin_level_1', 'admin_level_2',
                       'admin_level_3')
        }),
        ('Historical Administrative (Colonial)', {
            'fields': ('historical_admin_1', 'historical_admin_2', 'historical_region')
        }),
        ('Hierarchy & Metadata', {
            'fields': ('parent_place', 'notes', 'is_active')
        }),
    )


@admin.register(CatalogUnitPlace)
class CatalogUnitPlaceAdmin(admin.ModelAdmin):
    list_display = ['catalog_unit', 'place', 'place_role', 'sequence_number']
    list_filter = ['place_role']
    search_fields = ['catalog_unit__title', 'place__label']
    autocomplete_fields = ['catalog_unit', 'place']
    ordering = ['catalog_unit', 'sequence_number']
