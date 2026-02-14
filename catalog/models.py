"""
Zasqua Catalog Models

6-table schema optimized for CA migration:
- Repository: Archival institutions
- Description: Unified hierarchy (MPTT) for collections + items
- Entity: Authority records for persons/organizations
- Place: Geographic authority with coordinates
- DescriptionEntity: Junction with typed roles
- DescriptionPlace: Junction with typed roles
"""

import secrets
import string

from django.db import models
from django.contrib.auth.models import User
from mptt.models import MPTTModel, TreeForeignKey


def generate_neogranadina_code(prefix='ne', length=5):
    """
    Generate a unique Neogranadina identifier code.

    Format: {prefix}-{alphanumeric}
    - ne-xxxxx for entities (entidad)
    - nl-xxxxx for places (lugar)
    - nd-xxxxx for descriptions (documento)
    """
    # Lowercase + digits, removing ambiguous chars (0/o, 1/l)
    alphabet = 'abcdefghijkmnpqrstuvwxyz23456789'  # 32 chars
    code = ''.join(secrets.choice(alphabet) for _ in range(length))
    return f'{prefix}-{code}'


class Repository(models.Model):
    """
    An archival institution holding materials.
    Maps to CA 'institucion' type collections.
    """
    code = models.CharField(max_length=20, unique=True)
    name = models.CharField(max_length=255)
    short_name = models.CharField(max_length=200, blank=True)
    country_code = models.CharField(max_length=3, default='COL')
    city = models.CharField(max_length=100, blank=True)

    # Optional
    address = models.TextField(blank=True)
    website = models.URLField(blank=True)
    notes = models.TextField(blank=True)

    # Administrative
    enabled = models.BooleanField(default=True)

    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name_plural = 'repositories'
        ordering = ['name']

    def __str__(self):
        return f"{self.name} ({self.code})"


class Description(MPTTModel):
    """
    A unit of archival description at any level.

    Combines CA's collections + objects into a single ISAD(G)-compliant
    hierarchy. Everything from fonds to individual items lives here.
    Items CAN have children (enables splitting workflow).
    """

    class Level(models.TextChoices):
        FONDS = 'fonds', 'Fondo'
        SUBFONDS = 'subfonds', 'Subfondo'
        SERIES = 'series', 'Serie'
        SUBSERIES = 'subseries', 'Subserie'
        FILE = 'file', 'Expediente'
        ITEM = 'item', 'Unidad documental'
        # Flexible
        COLLECTION = 'collection', 'Coleccion'
        SECTION = 'section', 'Seccion'
        VOLUME = 'volume', 'Tomo'

    class ResourceType(models.TextChoices):
        TEXT = 'text', 'Text'
        STILL_IMAGE = 'still_image', 'Still Image'
        CARTOGRAPHIC = 'cartographic', 'Cartographic'
        MIXED = 'mixed', 'Mixed Materials'

    # --- Hierarchy ---
    repository = models.ForeignKey(Repository, on_delete=models.PROTECT,
                                   related_name='descriptions')
    parent = TreeForeignKey('self', on_delete=models.CASCADE,
                            null=True, blank=True, related_name='children')

    # --- Classification ---
    description_level = models.CharField(max_length=20, choices=Level.choices)
    resource_type = models.CharField(max_length=20, choices=ResourceType.choices,
                                     blank=True)
    genre = models.JSONField(default=list, blank=True)  # Getty AAT terms

    # --- Identity (ISAD 3.1) ---
    reference_code = models.CharField(max_length=100, unique=True, db_index=True)
    local_identifier = models.CharField(max_length=100, db_index=True)
    title = models.CharField(max_length=2000)
    translated_title = models.CharField(max_length=2000, blank=True)
    uniform_title = models.CharField(max_length=500, blank=True)

    # --- Dates ---
    date_expression = models.CharField(max_length=255, blank=True)
    date_start = models.DateField(null=True, blank=True)
    date_end = models.DateField(null=True, blank=True)
    date_certainty = models.CharField(max_length=20, blank=True)

    # --- Physical Description ---
    extent = models.CharField(max_length=1000, blank=True)
    dimensions = models.CharField(max_length=100, blank=True)
    medium = models.CharField(max_length=255, blank=True)

    # --- Bibliographic (for printed materials) ---
    imprint = models.CharField(max_length=500, blank=True)
    edition_statement = models.CharField(max_length=500, blank=True)
    series_statement = models.CharField(max_length=500, blank=True)
    volume_number = models.CharField(max_length=50, blank=True)
    issue_number = models.CharField(max_length=50, blank=True)
    pages = models.CharField(max_length=100, blank=True)

    # --- Context (ISAD 3.2) ---
    provenance = models.TextField(blank=True)

    # --- Content (ISAD 3.3) ---
    scope_content = models.TextField(blank=True)
    ocr_text = models.TextField(blank=True, default='')
    arrangement = models.TextField(blank=True)

    # --- Access (ISAD 3.4) ---
    access_conditions = models.TextField(blank=True)
    reproduction_conditions = models.TextField(blank=True)
    language = models.CharField(max_length=100, blank=True)

    # --- Rights ---
    rights_status = models.CharField(max_length=50, blank=True)
    rights_holder = models.CharField(max_length=255, blank=True)
    rights_statement = models.TextField(blank=True)

    # --- Allied Materials (ISAD 3.5) ---
    location_of_originals = models.TextField(blank=True)
    related_materials = models.TextField(blank=True)

    # --- Notes (ISAD 3.6) ---
    notes = models.TextField(blank=True)
    internal_notes = models.TextField(blank=True)

    # --- Denormalized for Display/Search ---
    creator_display = models.CharField(max_length=500, blank=True)
    place_display = models.CharField(max_length=500, blank=True)

    # --- Performance ---
    path_cache = models.CharField(max_length=500, blank=True, db_index=True)

    # --- Digital ---
    iiif_manifest_url = models.URLField(blank=True)
    has_digital = models.BooleanField(default=False)

    # --- Workflow ---
    is_published = models.BooleanField(default=True)
    needs_review = models.BooleanField(default=False)
    review_note = models.TextField(blank=True)

    # --- Provenance (CA migration) ---
    ca_object_id = models.IntegerField(null=True, blank=True, db_index=True)
    ca_collection_id = models.IntegerField(null=True, blank=True, db_index=True)

    # --- Timestamps ---
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    created_by = models.ForeignKey(User, null=True, blank=True,
                                   on_delete=models.SET_NULL, related_name='+')
    updated_by = models.ForeignKey(User, null=True, blank=True,
                                   on_delete=models.SET_NULL, related_name='+')

    class MPTTMeta:
        order_insertion_by = ['local_identifier']

    class Meta:
        verbose_name_plural = 'descriptions'
        ordering = ['tree_id', 'lft']

    def __str__(self):
        return self.title[:100] if len(self.title) > 100 else self.title


class Entity(models.Model):
    """
    Authority record for persons, families, or corporate bodies.
    ISAAR(CPF) inspired but simplified.
    """

    class EntityType(models.TextChoices):
        PERSON = 'person', 'Persona'
        FAMILY = 'family', 'Familia'
        CORPORATE = 'corporate', 'Entidad corporativa'

    # --- Identity (ISAAR 5.1) ---
    entity_code = models.CharField(max_length=8, blank=True, null=True, db_index=True,
                                   help_text='Unique identifier (ne-xxxxx) for URLs and citations')
    display_name = models.CharField(max_length=500, db_index=True)
    sort_name = models.CharField(max_length=500, db_index=True)
    surname = models.CharField(max_length=200, blank=True, db_index=True,
                               help_text='Family name (without particles like de/del)')
    given_name = models.CharField(max_length=200, blank=True,
                                  help_text='Given name(s), including particles (e.g., "Agustina del")')
    entity_type = models.CharField(max_length=20, choices=EntityType.choices)
    honorific = models.CharField(max_length=100, blank=True,
                                 help_text='Primary form of address (Don, Fray, Dr.)')
    primary_function = models.CharField(max_length=300, blank=True,
                                        help_text='Most notable office/role (Gobernador de Popayán)')

    # --- Variants ---
    name_variants = models.JSONField(default=list, blank=True)

    # --- Description (ISAAR 5.2) ---
    dates_of_existence = models.CharField(max_length=100, blank=True)
    date_start = models.DateField(null=True, blank=True)
    date_end = models.DateField(null=True, blank=True)
    history = models.TextField(blank=True)

    # For corporate bodies
    legal_status = models.CharField(max_length=100, blank=True)
    functions = models.TextField(blank=True)

    # --- Control (ISAAR 5.4) ---
    sources = models.TextField(blank=True)

    # --- Workflow ---
    needs_review = models.BooleanField(default=False)
    review_note = models.TextField(blank=True)

    # For deduplication
    merged_into = models.ForeignKey('self', null=True, blank=True,
                                    on_delete=models.SET_NULL,
                                    related_name='merged_records')

    # --- Provenance (CA migration) ---
    ca_entity_id = models.IntegerField(null=True, unique=True, db_index=True)

    # --- Timestamps ---
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name_plural = 'entities'
        ordering = ['sort_name']

    def save(self, *args, **kwargs):
        if not self.entity_code:
            # Generate unique ne-xxxxx code, retry if collision
            for _ in range(10):
                code = generate_neogranadina_code(prefix='ne', length=5)
                if not Entity.objects.filter(entity_code=code).exists():
                    self.entity_code = code
                    break
            else:
                raise ValueError("Could not generate unique entity_code")
        super().save(*args, **kwargs)

    def __str__(self):
        return self.display_name


class EntityFunction(models.Model):
    """
    Known functions/offices held by an entity over time.
    Interpretation layer - synthesized from documents or external sources.
    """

    class Certainty(models.TextChoices):
        CERTAIN = 'certain', 'Cierto'
        PROBABLE = 'probable', 'Probable'
        POSSIBLE = 'possible', 'Posible'

    entity = models.ForeignKey(Entity, on_delete=models.CASCADE,
                               related_name='known_functions')

    # The function held
    honorific = models.CharField(max_length=100, blank=True,
                                 help_text='Associated honorific if known')
    function = models.CharField(max_length=300,
                                help_text='The office, rank, or role held')

    # Temporal bounds
    date_start = models.DateField(null=True, blank=True)
    date_end = models.DateField(null=True, blank=True)
    date_note = models.CharField(max_length=100, blank=True,
                                 help_text='For uncertain dates: "ca. 1815", "before 1820"')

    # Provenance
    certainty = models.CharField(max_length=20, choices=Certainty.choices,
                                 default='probable')
    source = models.TextField(blank=True,
                              help_text='How we know this: documents, external authority, etc.')
    notes = models.TextField(blank=True)

    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = 'Known Function'
        verbose_name_plural = 'Known Functions'
        ordering = ['date_start', 'function']

    def __str__(self):
        if self.date_start:
            return f"{self.entity.display_name}: {self.function} ({self.date_start.year})"
        return f"{self.entity.display_name}: {self.function}"


class Place(models.Model):
    """
    Geographic authority record with optional coordinates.
    Canonical places - CA duplicates are merged here.
    """

    class PlaceType(models.TextChoices):
        COUNTRY = 'country', 'Pais'
        REGION = 'region', 'Region/Audiencia'
        DEPARTMENT = 'department', 'Departamento'
        PROVINCE = 'province', 'Provincia'
        PARTIDO = 'partido', 'Partido (colonial)'
        CITY = 'city', 'Ciudad'
        TOWN = 'town', 'Villa/Pueblo'
        PARISH = 'parish', 'Parroquia'
        HACIENDA = 'hacienda', 'Hacienda'
        MINE = 'mine', 'Real de minas'
        RIVER = 'river', 'Rio'
        OTHER = 'other', 'Otro'

    # --- Identity ---
    label = models.CharField(max_length=255, db_index=True)
    display_name = models.CharField(max_length=500)
    place_type = models.CharField(max_length=50, choices=PlaceType.choices,
                                  blank=True)

    # --- Variants ---
    name_variants = models.JSONField(default=list, blank=True)

    # --- Hierarchy ---
    parent = models.ForeignKey('self', on_delete=models.SET_NULL,
                               null=True, blank=True, related_name='children')

    # --- Geography ---
    latitude = models.DecimalField(max_digits=9, decimal_places=6,
                                   null=True, blank=True)
    longitude = models.DecimalField(max_digits=9, decimal_places=6,
                                    null=True, blank=True)
    coordinate_precision = models.CharField(max_length=20, blank=True)

    # Colonial context (for CIHJML gazetteer matching)
    colonial_gobernacion = models.CharField(max_length=100, blank=True)
    colonial_partido = models.CharField(max_length=100, blank=True)
    colonial_region = models.CharField(max_length=10, blank=True)

    # Modern context
    country_code = models.CharField(max_length=3, blank=True)
    admin_level_1 = models.CharField(max_length=100, blank=True)
    admin_level_2 = models.CharField(max_length=100, blank=True)

    # --- Workflow ---
    needs_geocoding = models.BooleanField(default=True)
    needs_review = models.BooleanField(default=False)
    review_note = models.TextField(blank=True)

    # For deduplication
    merged_into = models.ForeignKey('self', null=True, blank=True,
                                    on_delete=models.SET_NULL,
                                    related_name='merged_places')

    # --- Provenance (CA migration) ---
    ca_place_ids = models.JSONField(default=list, blank=True)

    # --- Timestamps ---
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name_plural = 'places'
        ordering = ['label']

    def __str__(self):
        return self.display_name


class DescriptionEntity(models.Model):
    """
    Links archival descriptions to entities with typed roles.
    Replaces CA's ca_objects_x_entities.
    """

    class Role(models.TextChoices):
        # General
        CREATOR = 'creator', 'Creador'
        AUTHOR = 'author', 'Autor'
        EDITOR = 'editor', 'Editor'
        PUBLISHER = 'publisher', 'Impresor/Editorial'
        # Correspondence
        SENDER = 'sender', 'Remitente'
        RECIPIENT = 'recipient', 'Destinatario'
        # Mentions
        MENTIONED = 'mentioned', 'Mencionado'
        SUBJECT = 'subject', 'Tema'
        # Notarial/legal
        SCRIBE = 'scribe', 'Escribano'
        WITNESS = 'witness', 'Testigo'
        NOTARY = 'notary', 'Notario'
        # Visual materials
        PHOTOGRAPHER = 'photographer', 'Fotografo'
        ARTIST = 'artist', 'Artista'

    description = models.ForeignKey(Description, on_delete=models.CASCADE,
                                    related_name='entity_links')
    entity = models.ForeignKey(Entity, on_delete=models.PROTECT,
                               related_name='description_links')
    role = models.CharField(max_length=20, choices=Role.choices)

    role_note = models.TextField(blank=True)
    sequence = models.PositiveIntegerField(default=0)

    # Documentary styling (evidence layer)
    honorific = models.CharField(max_length=100, blank=True,
                                 help_text='Honorific as recorded in this document (Don, Fray)')
    function = models.CharField(max_length=300, blank=True,
                                help_text='Function/office as recorded (Gobernador de Popayán)')
    name_as_recorded = models.CharField(max_length=500, blank=True,
                                        help_text='Full name string as appears in document')

    # Workflow
    needs_review = models.BooleanField(default=False)

    # Provenance
    ca_relationship_id = models.IntegerField(null=True, blank=True)

    class Meta:
        verbose_name = 'description-entity link'
        verbose_name_plural = 'description-entity links'
        unique_together = ['description', 'entity', 'role']
        ordering = ['sequence', 'entity__sort_name']
        indexes = [
            models.Index(fields=['entity', 'role']),
        ]

    def __str__(self):
        return f"{self.description} - {self.entity} ({self.get_role_display()})"


class DescriptionPlace(models.Model):
    """
    Links archival descriptions to places with typed roles.
    Replaces CA's ca_objects_x_places.
    """

    class Role(models.TextChoices):
        CREATED = 'created', 'Lugar de creacion'
        SUBJECT = 'subject', 'Tema/Asunto'
        MENTIONED = 'mentioned', 'Mencionado'
        SENT_FROM = 'sent_from', 'Enviado desde'
        SENT_TO = 'sent_to', 'Enviado a'
        PUBLISHED = 'published', 'Publicado en'

    description = models.ForeignKey(Description, on_delete=models.CASCADE,
                                    related_name='place_links')
    place = models.ForeignKey(Place, on_delete=models.PROTECT,
                              related_name='description_links')
    role = models.CharField(max_length=20, choices=Role.choices)

    role_note = models.TextField(blank=True)

    # Workflow
    needs_review = models.BooleanField(default=False)

    # Provenance
    ca_relationship_id = models.IntegerField(null=True, blank=True)

    class Meta:
        verbose_name = 'description-place link'
        verbose_name_plural = 'description-place links'
        unique_together = ['description', 'place', 'role']
        indexes = [
            models.Index(fields=['place', 'role']),
        ]

    def __str__(self):
        return f"{self.description} - {self.place} ({self.get_role_display()})"
