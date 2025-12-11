"""
Import data from CollectiveAccess MySQL database.

Usage:
    python manage.py import_ca --phase repositories
    python manage.py import_ca --phase collections
    python manage.py import_ca --phase objects
    python manage.py import_ca --phase entities
    python manage.py import_ca --phase entity_links
    python manage.py import_ca --phase places
    python manage.py import_ca --phase place_links
    python manage.py import_ca --phase denormalize
    python manage.py import_ca --phase all
"""

import mysql.connector
import re
from datetime import date
from django.core.management.base import BaseCommand
from django.db import transaction
from catalog.models import (
    Repository, Description, Entity, Place,
    DescriptionEntity, DescriptionPlace
)


def parse_date_expression(date_str):
    """
    Parse CA date expression into structured date fields.

    Returns dict with: date_start, date_end, date_certainty

    Handles formats:
    - Year only: "1875" -> start=1875-01-01, end=1875-12-31
    - Year-month: "1875-03" -> start=1875-03-01, end=1875-03-31
    - ISO date: "1824-10-16" -> start=end=1824-10-16
    - European date: "13-02-1815" (DD-MM-YYYY) -> 1815-02-13
    - Spanish text: "29 Marzo 1815" -> 1815-03-29
    - Date range: "1825-01-01 .. 1825-12-31" -> start, end
    - Mixed range: "1830-05-14 .. 1831-12" -> start=1830-05-14, end=1831-12-31
    - Year range: "1864 - 1930" -> start=1864-01-01, end=1930-12-31
    - End only: "- 1878-12-01" or "- 1878-03" -> duplicate as start
    - Start only: ".. 1823-04-06" -> duplicate as end
    - Uncertain: "189?" or "ca. 1750" -> left unparsed
    """
    if not date_str or not date_str.strip():
        return None

    date_str = date_str.strip()

    # Remove leading punctuation (e.g., ",1824-01-02")
    date_str = date_str.lstrip(',;. ')

    result = {
        'date_start': None,
        'date_end': None,
        'date_certainty': '',
    }

    # Skip invalid dates (like "152")
    if len(date_str) < 4:
        return None

    # Skip uncertain dates - leave for manual review
    if '?' in date_str or 'ca.' in date_str.lower() or 'circa' in date_str.lower() or ' ca ' in date_str.lower():
        return None

    # Try Spanish text date range first: "7 Diciembre 1780 - 29 Junio 1781"
    spanish_range = _parse_spanish_date_range(date_str)
    if spanish_range:
        return spanish_range

    # Try single Spanish text date: "29 Marzo 1815"
    spanish_single = _parse_spanish_date(date_str)
    if spanish_single:
        result['date_start'] = spanish_single
        result['date_end'] = spanish_single
        return result

    # Try European date range: "01-02-1820 .. 29-02-1820" or mixed "1815-05-07 .. 26-08-1815"
    euro_range = _parse_european_date_range(date_str)
    if euro_range:
        return euro_range

    # Try single European date: "13-02-1815" (DD-MM-YYYY)
    euro_single = _parse_european_date(date_str)
    if euro_single:
        result['date_start'] = euro_single
        result['date_end'] = euro_single
        return result

    # Pattern for partial dates: YYYY, YYYY-MM, or YYYY-MM-DD
    date_pattern = r'\d{4}(?:-\d{2})?(?:-\d{2})?'

    # Try date range patterns (with .. or -)
    # Handles: "1825-01-01 .. 1825-12-31", "1864 - 1930", "1830-05-14 .. 1831-12"
    range_match = re.match(rf'^({date_pattern})\s*(?:\.\.|-)\s*({date_pattern})$', date_str)
    if range_match:
        start_str, end_str = range_match.groups()
        result['date_start'] = _parse_single_date(start_str, is_start=True)
        result['date_end'] = _parse_single_date(end_str, is_start=False)
        return result if result['date_start'] or result['date_end'] else None

    # Pattern: "- 1878-12-01" or "- 1878-03" (end only) - duplicate as start
    end_only_match = re.match(rf'^-\s*({date_pattern})$', date_str)
    if end_only_match:
        end_date = _parse_single_date(end_only_match.group(1), is_start=False)
        result['date_end'] = end_date
        result['date_start'] = _parse_single_date(end_only_match.group(1), is_start=True)
        return result if result['date_end'] else None

    # Pattern: ".. 1823-04-06" (start only notation) - duplicate as end
    start_only_match = re.match(rf'^\.\.\s*({date_pattern})$', date_str)
    if start_only_match:
        start_date = _parse_single_date(start_only_match.group(1), is_start=True)
        result['date_start'] = start_date
        result['date_end'] = _parse_single_date(start_only_match.group(1), is_start=False)
        return result if result['date_start'] else None

    # Single date: YYYY, YYYY-MM, or YYYY-MM-DD
    single_match = re.match(r'^(\d{4})(?:-(\d{2}))?(?:-(\d{2}))?$', date_str)
    if single_match:
        year, month, day = single_match.groups()
        year = int(year)
        if year < 1000 or year > 2100:
            return None

        if day:
            # Full date
            d = _safe_date(year, int(month), int(day))
            result['date_start'] = d
            result['date_end'] = d
        elif month:
            # Year-month: start at 1st, end at last day of month
            result['date_start'] = _safe_date(year, int(month), 1)
            result['date_end'] = _last_day_of_month(year, int(month))
        else:
            # Year only
            result['date_start'] = _safe_date(year, 1, 1)
            result['date_end'] = _safe_date(year, 12, 31)
        return result if result['date_start'] else None

    return None


# Spanish month names to numbers (including Peruvian spelling variants)
SPANISH_MONTHS = {
    'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
    'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
    'septiembre': 9, 'setiembre': 9,  # Peruvian spelling
    'octubre': 10, 'noviembre': 11, 'diciembre': 12,
}


def _parse_spanish_date(date_str):
    """Parse Spanish text date like '29 Marzo 1815' -> date(1815, 3, 29)."""
    # Pattern: "29 Marzo 1815" or "7 Diciembre 1780"
    match = re.match(r'^(\d{1,2})\s+([A-Za-záéíóú]+)\s+(\d{4})$', date_str.strip(), re.IGNORECASE)
    if not match:
        return None

    day, month_name, year = match.groups()
    month = SPANISH_MONTHS.get(month_name.lower())
    if not month:
        return None

    return _safe_date(int(year), month, int(day))


def _parse_spanish_date_range(date_str):
    """Parse Spanish text date range like '7 Diciembre 1780 - 29 Junio 1781'."""
    # Pattern: "7 Diciembre 1780 - 29 Junio 1781"
    match = re.match(
        r'^(\d{1,2})\s+([A-Za-záéíóú]+)\s+(\d{4})\s*-\s*(\d{1,2})\s+([A-Za-záéíóú]+)\s+(\d{4})$',
        date_str.strip(), re.IGNORECASE
    )
    if not match:
        return None

    day1, month1_name, year1, day2, month2_name, year2 = match.groups()
    month1 = SPANISH_MONTHS.get(month1_name.lower())
    month2 = SPANISH_MONTHS.get(month2_name.lower())
    if not month1 or not month2:
        return None

    return {
        'date_start': _safe_date(int(year1), month1, int(day1)),
        'date_end': _safe_date(int(year2), month2, int(day2)),
        'date_certainty': '',
    }


def _parse_european_date(date_str):
    """Parse European DD-MM-YYYY date like '13-02-1815' -> date(1815, 2, 13)."""
    match = re.match(r'^(\d{1,2})-(\d{2})-(\d{4})$', date_str.strip())
    if not match:
        return None

    day, month, year = match.groups()
    year = int(year)
    if year < 1000 or year > 2100:
        return None

    return _safe_date(year, int(month), int(day))


def _parse_european_date_range(date_str):
    """Parse European date ranges like '01-02-1820 .. 29-02-1820' or mixed formats."""
    # Pattern for European date: DD-MM-YYYY
    euro_pattern = r'(\d{1,2})-(\d{2})-(\d{4})'
    # Pattern for ISO date: YYYY-MM-DD
    iso_pattern = r'(\d{4})-(\d{2})-(\d{2})'

    # Try Euro .. Euro
    match = re.match(rf'^{euro_pattern}\s*(?:\.\.|-)\s*{euro_pattern}$', date_str)
    if match:
        d1, m1, y1, d2, m2, y2 = match.groups()
        return {
            'date_start': _safe_date(int(y1), int(m1), int(d1)),
            'date_end': _safe_date(int(y2), int(m2), int(d2)),
            'date_certainty': '',
        }

    # Try ISO .. Euro (mixed)
    match = re.match(rf'^{iso_pattern}\s*(?:\.\.|-)\s*{euro_pattern}$', date_str)
    if match:
        y1, m1, d1, d2, m2, y2 = match.groups()
        return {
            'date_start': _safe_date(int(y1), int(m1), int(d1)),
            'date_end': _safe_date(int(y2), int(m2), int(d2)),
            'date_certainty': '',
        }

    # Try Euro .. ISO (mixed)
    match = re.match(rf'^{euro_pattern}\s*(?:\.\.|-)\s*{iso_pattern}$', date_str)
    if match:
        d1, m1, y1, y2, m2, d2 = match.groups()
        return {
            'date_start': _safe_date(int(y1), int(m1), int(d1)),
            'date_end': _safe_date(int(y2), int(m2), int(d2)),
            'date_certainty': '',
        }

    return None


def _parse_single_date(date_str, is_start=True):
    """Parse a single date string like '1875', '1875-03', or '1824-10-16'."""
    match = re.match(r'^(\d{4})(?:-(\d{2}))?(?:-(\d{2}))?$', date_str.strip())
    if not match:
        return None

    year, month, day = match.groups()
    year = int(year)
    if year < 1000 or year > 2100:
        return None

    if day:
        return _safe_date(year, int(month), int(day))
    elif month:
        month = int(month)
        if is_start:
            return _safe_date(year, month, 1)
        else:
            return _last_day_of_month(year, month)
    else:
        if is_start:
            return _safe_date(year, 1, 1)
        else:
            return _safe_date(year, 12, 31)


def _last_day_of_month(year, month):
    """Get the last day of a given month."""
    import calendar
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, last_day)


def _safe_date(year, month, day):
    """Create date safely, handling invalid day/month combinations."""
    try:
        return date(year, month, day)
    except ValueError:
        # Handle invalid dates (e.g., Feb 30) - use last day of month
        try:
            import calendar
            last_day = calendar.monthrange(year, month)[1]
            return date(year, month, min(day, last_day))
        except ValueError:
            return None


# CA MySQL connection settings
CA_DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'abcneogranadina',
    'charset': 'utf8mb4',
}

# Repository mappings from CA collection IDs
# Codes normalized to: {country}-{repo} format, lowercase
REPOSITORY_MAP = {
    712: {'code': 'co-cihjml', 'name': 'Centro de Investigaciones Históricas José María Arboleda Llorente, Universidad del Cauca', 'city': 'Popayán', 'country_code': 'COL'},
    360: {'code': 'pe-bn', 'name': 'Biblioteca Nacional del Perú', 'city': 'Lima', 'country_code': 'PER'},
    14805: {'code': 'co-ahrb', 'name': 'Archivo Histórico Regional de Boyacá', 'city': 'Tunja', 'country_code': 'COL'},
    14940: {'code': 'co-ahr', 'name': 'Archivo Histórico de Rionegro', 'city': 'Rionegro', 'country_code': 'COL'},
    16479: {'code': 'co-ahjci', 'name': 'Archivo Histórico del Juzgado de Istmina', 'city': 'Istmina', 'country_code': 'COL'},
}

# Old code -> new code mapping (for migrations)
REPO_CODE_MIGRATION = {
    'CIHJML': 'co-cihjml',
    'PE-BN': 'pe-bn',
    'AHRB': 'co-ahrb',
    'CO.AHR': 'co-ahr',
    'AHJCI': 'co-ahjci',
}

# CA collection type to Zasqua level mapping
COLLECTION_TYPE_MAP = {
    'institucion': 'fonds',
    'fondo': 'fonds',
    'subfondo': 'subfonds',
    'serie': 'series',
    'subserie': 'subseries',
    'subsubserie': 'subseries',
    'coleccion': 'collection',
    'tomo': 'volume',
    'caja': 'file',
    'carpeta': 'file',
    'legajo': 'file',
    'proyecto': 'collection',  # Legacy containers
}

# CA relationship type to Zasqua role mapping
ENTITY_ROLE_MAP = {
    'creator': 'creator',
    'author': 'author',
    'publisher': 'publisher',
    'mencion': 'mentioned',
    'destinatario': 'recipient',
    'remitente': 'sender',
    'scribe': 'scribe',
    'testigo': 'witness',
    'photographer': 'photographer',
    'artist': 'artist',
}

PLACE_ROLE_MAP = {
    'place_of_creation': 'created',
    'subject': 'subject',
    'mentioned': 'mentioned',
    'sent_from': 'sent_from',
    'sent_to': 'sent_to',
    'published': 'published',
}

# CA EAV attribute element_code -> Zasqua field
# Based on actual CA data (see mysql query for element_code counts)
CA_ATTRIBUTE_MAP = {
    # Content fields (ISAD 3.3)
    'description': 'scope_content',
    'arrangement': 'arrangement',

    # Dates - stored as text in unitdate
    'unitdate': 'date_expression',

    # Access fields (ISAD 3.4)
    'accessrestrict': 'access_conditions',
    'reproduction': 'reproduction_conditions',
    'langmaterial': 'language',

    # Physical description
    'extent_text': 'extent',

    # Allied materials (ISAD 3.5)
    'originalsloc': 'location_of_originals',
    'otherfindingaid': 'related_materials',

    # Bibliographic (PE-BN printed materials)
    'narra_imprenta': 'imprint',
    'pages': 'pages',

    # Notes
    'note': 'notes',
}

# CA table_num values
CA_TABLE_OBJECTS = 57
CA_TABLE_COLLECTIONS = 13


class Command(BaseCommand):
    help = 'Import data from CollectiveAccess MySQL database'

    def add_arguments(self, parser):
        parser.add_argument(
            '--phase',
            type=str,
            required=True,
            choices=['repositories', 'collections', 'objects', 'entities',
                     'entity_links', 'places', 'place_links', 'denormalize',
                     'attributes', 'all'],
            help='Which phase to run'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Print what would be done without making changes'
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=0,
            help='Limit number of records (for testing)'
        )

    def handle(self, *args, **options):
        self.dry_run = options['dry_run']
        self.limit = options['limit']
        self.verbosity = options['verbosity']

        phase = options['phase']

        if phase == 'all':
            phases = ['repositories', 'collections', 'objects', 'entities',
                      'entity_links', 'places', 'place_links', 'denormalize',
                      'attributes']
        else:
            phases = [phase]

        for p in phases:
            self.stdout.write(f"\n{'='*60}")
            self.stdout.write(f"Running phase: {p}")
            self.stdout.write(f"{'='*60}\n")

            method = getattr(self, f'import_{p}')
            method()

    def get_ca_connection(self):
        """Get MySQL connection to CA database."""
        return mysql.connector.connect(**CA_DB_CONFIG)

    def import_repositories(self):
        """Phase 1: Create repository records."""
        self.stdout.write("Creating repositories...")

        for ca_id, data in REPOSITORY_MAP.items():
            if self.dry_run:
                self.stdout.write(f"  Would create: {data['code']} - {data['name']}")
            else:
                repo, created = Repository.objects.update_or_create(
                    code=data['code'],
                    defaults={
                        'name': data['name'],
                        'city': data['city'],
                        'country_code': data['country_code'],
                    }
                )
                status = 'created' if created else 'updated'
                self.stdout.write(f"  {status}: {repo.code} - {repo.name}")

        self.stdout.write(self.style.SUCCESS(f"Repositories: {len(REPOSITORY_MAP)} done"))

    def import_collections(self):
        """Phase 2: Import CA collections as Description hierarchy."""
        self.stdout.write("Importing collections from CA...")

        conn = self.get_ca_connection()
        cursor = conn.cursor(dictionary=True)

        # Get all collections with their labels and types
        query = """
            SELECT
                c.collection_id,
                c.parent_id,
                c.idno,
                c.type_id,
                cl.name as title,
                lit.idno as type_code
            FROM ca_collections c
            JOIN ca_collection_labels cl ON c.collection_id = cl.collection_id
                AND cl.is_preferred = 1
            LEFT JOIN ca_list_items lit ON c.type_id = lit.item_id
            WHERE c.deleted = 0
            ORDER BY c.parent_id, c.collection_id
        """
        if self.limit:
            query += f" LIMIT {self.limit}"

        cursor.execute(query)
        collections = cursor.fetchall()

        self.stdout.write(f"  Found {len(collections)} collections")

        # Build parent mapping and track which repo each collection belongs to
        ca_to_zasqua = {}  # ca_collection_id -> Description.id
        repo_membership = {}  # ca_collection_id -> repo_code

        # First pass: identify repository membership
        for coll in collections:
            if coll['collection_id'] in REPOSITORY_MAP:
                repo_membership[coll['collection_id']] = REPOSITORY_MAP[coll['collection_id']]['code']

        # Propagate repository membership down the tree
        changed = True
        while changed:
            changed = False
            for coll in collections:
                if coll['collection_id'] not in repo_membership and coll['parent_id'] in repo_membership:
                    repo_membership[coll['collection_id']] = repo_membership[coll['parent_id']]
                    changed = True

        # Second pass: create descriptions
        created_count = 0
        skipped_count = 0

        for coll in collections:
            ca_id = coll['collection_id']

            # Skip repository-level collections (they're not descriptions)
            if ca_id in REPOSITORY_MAP:
                continue

            # Skip if we can't determine repository
            if ca_id not in repo_membership:
                skipped_count += 1
                continue

            repo_code = repo_membership[ca_id]
            type_code = (coll['type_code'] or '').lower()
            level = COLLECTION_TYPE_MAP.get(type_code, 'collection')

            # Get parent Description if exists
            parent = None
            if coll['parent_id'] and coll['parent_id'] not in REPOSITORY_MAP:
                parent_id = ca_to_zasqua.get(coll['parent_id'])
                if parent_id:
                    parent = Description.objects.filter(id=parent_id).first()

            if self.dry_run:
                self.stdout.write(f"  Would create: [{repo_code}] {coll['idno']} - {coll['title'][:50]}")
            else:
                try:
                    repo = Repository.objects.get(code=repo_code)
                    idno = str(coll['idno']) if coll['idno'] else str(ca_id)
                    desc, created = Description.objects.update_or_create(
                        ca_collection_id=ca_id,
                        defaults={
                            'repository': repo,
                            'parent': parent,
                            'description_level': level,
                            'reference_code': f"{repo_code}-{idno}",
                            'local_identifier': idno,
                            'title': coll['title'] or f"Collection {ca_id}",
                            'is_published': True,
                        }
                    )
                    ca_to_zasqua[ca_id] = desc.id
                    if created:
                        created_count += 1
                except Exception as e:
                    import traceback
                    self.stdout.write(self.style.ERROR(f"  Error on {ca_id}: {e}"))
                    if self.verbosity > 1:
                        traceback.print_exc()

        cursor.close()
        conn.close()

        self.stdout.write(self.style.SUCCESS(
            f"Collections: {created_count} created, {skipped_count} skipped"
        ))

    def import_objects(self):
        """Phase 3: Import CA objects as item-level Descriptions."""
        self.stdout.write("Importing objects from CA...")

        conn = self.get_ca_connection()
        cursor = conn.cursor(dictionary=True)

        # Get objects with their collection membership
        query = """
            SELECT
                o.object_id,
                o.idno,
                ol.name as title,
                oxc.collection_id
            FROM ca_objects o
            JOIN ca_object_labels ol ON o.object_id = ol.object_id
                AND ol.is_preferred = 1
            LEFT JOIN ca_objects_x_collections oxc ON o.object_id = oxc.object_id
            WHERE o.deleted = 0
            ORDER BY o.object_id
        """
        if self.limit:
            query += f" LIMIT {self.limit}"

        cursor.execute(query)
        objects = cursor.fetchall()

        self.stdout.write(f"  Found {len(objects)} objects")

        # Build collection -> Description mapping
        collection_to_desc = {}
        for desc in Description.objects.filter(ca_collection_id__isnull=False):
            collection_to_desc[desc.ca_collection_id] = desc

        created_count = 0
        skipped_count = 0
        batch = []
        batch_size = 1000

        for obj in objects:
            ca_id = obj['object_id']
            collection_id = obj['collection_id']

            # Find parent Description
            parent = collection_to_desc.get(collection_id) if collection_id else None
            if not parent:
                skipped_count += 1
                continue

            idno = str(obj['idno']) if obj['idno'] else str(ca_id)
            if self.dry_run:
                self.stdout.write(f"  Would create: {idno} - {obj['title'][:50]}")
                created_count += 1
            else:
                batch.append(Description(
                    repository=parent.repository,
                    parent=parent,
                    description_level='item',
                    reference_code=f"{parent.repository.code}-{idno}",
                    local_identifier=idno,
                    title=obj['title'] or f"Object {ca_id}",
                    ca_object_id=ca_id,
                    is_published=True,
                ))

                if len(batch) >= batch_size:
                    Description.objects.bulk_create(batch, ignore_conflicts=True)
                    created_count += len(batch)
                    self.stdout.write(f"  Created {created_count} objects...")
                    batch = []

        # Final batch
        if batch and not self.dry_run:
            Description.objects.bulk_create(batch, ignore_conflicts=True)
            created_count += len(batch)

        cursor.close()
        conn.close()

        self.stdout.write(self.style.SUCCESS(
            f"Objects: {created_count} created, {skipped_count} skipped"
        ))

    def import_entities(self):
        """Phase 4: Import CA entities as Entity records."""
        self.stdout.write("Importing entities from CA...")

        conn = self.get_ca_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT
                e.entity_id,
                e.idno,
                e.type_id,
                el.displayname,
                el.surname,
                el.forename,
                lit.idno as type_code
            FROM ca_entities e
            JOIN ca_entity_labels el ON e.entity_id = el.entity_id
                AND el.is_preferred = 1
            LEFT JOIN ca_list_items lit ON e.type_id = lit.item_id
            WHERE e.deleted = 0
            ORDER BY e.entity_id
        """
        if self.limit:
            query += f" LIMIT {self.limit}"

        cursor.execute(query)
        entities = cursor.fetchall()

        self.stdout.write(f"  Found {len(entities)} entities")

        created_count = 0
        batch = []
        batch_size = 100  # Smaller batch for SQLite

        for ent in entities:
            ca_id = ent['entity_id']
            type_code = (ent['type_code'] or 'ind').lower()
            entity_type = 'corporate' if type_code == 'org' else 'person'

            display_name = ent['displayname'] or f"Entity {ca_id}"
            # Create sort name: surname, forename
            if ent['surname'] and ent['forename']:
                sort_name = f"{ent['surname']}, {ent['forename']}"
            elif ent['surname']:
                sort_name = ent['surname']
            else:
                sort_name = display_name

            if self.dry_run:
                self.stdout.write(f"  Would create: {display_name}")
                created_count += 1
            else:
                batch.append(Entity(
                    display_name=display_name,
                    sort_name=sort_name,
                    entity_type=entity_type,
                    ca_entity_id=ca_id,
                ))

                if len(batch) >= batch_size:
                    Entity.objects.bulk_create(batch, ignore_conflicts=True)
                    created_count += len(batch)
                    self.stdout.write(f"  Created {created_count} entities...")
                    batch = []

        # Final batch
        if batch and not self.dry_run:
            Entity.objects.bulk_create(batch, ignore_conflicts=True)
            created_count += len(batch)

        cursor.close()
        conn.close()

        self.stdout.write(self.style.SUCCESS(f"Entities: {created_count} created"))

    def import_entity_links(self):
        """Phase 5: Import CA object-entity relationships."""
        self.stdout.write("Importing entity links from CA...")

        conn = self.get_ca_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT
                oxe.relation_id,
                oxe.object_id,
                oxe.entity_id,
                oxe.type_id,
                rt.type_code
            FROM ca_objects_x_entities oxe
            JOIN ca_relationship_types rt ON oxe.type_id = rt.type_id
            ORDER BY oxe.object_id
        """
        if self.limit:
            query += f" LIMIT {self.limit}"

        cursor.execute(query)
        links = cursor.fetchall()

        self.stdout.write(f"  Found {len(links)} entity links")

        # Build mappings
        obj_to_desc = {}
        for desc in Description.objects.filter(ca_object_id__isnull=False).values('id', 'ca_object_id'):
            obj_to_desc[desc['ca_object_id']] = desc['id']

        entity_map = {}
        for ent in Entity.objects.filter(ca_entity_id__isnull=False).values('id', 'ca_entity_id'):
            entity_map[ent['ca_entity_id']] = ent['id']

        created_count = 0
        skipped_count = 0
        batch = []
        batch_size = 1000

        for link in links:
            desc_id = obj_to_desc.get(link['object_id'])
            entity_id = entity_map.get(link['entity_id'])

            if not desc_id or not entity_id:
                skipped_count += 1
                continue

            type_code = (link['type_code'] or 'creator').lower()
            role = ENTITY_ROLE_MAP.get(type_code, 'creator')

            if self.dry_run:
                created_count += 1
            else:
                batch.append(DescriptionEntity(
                    description_id=desc_id,
                    entity_id=entity_id,
                    role=role,
                    ca_relationship_id=link['relation_id'],
                ))

                if len(batch) >= batch_size:
                    DescriptionEntity.objects.bulk_create(batch, ignore_conflicts=True)
                    created_count += len(batch)
                    self.stdout.write(f"  Created {created_count} links...")
                    batch = []

        # Final batch
        if batch and not self.dry_run:
            DescriptionEntity.objects.bulk_create(batch, ignore_conflicts=True)
            created_count += len(batch)

        cursor.close()
        conn.close()

        self.stdout.write(self.style.SUCCESS(
            f"Entity links: {created_count} created, {skipped_count} skipped"
        ))

    def import_places(self):
        """Phase 6: Import CA places (deduplicated)."""
        self.stdout.write("Importing places from CA...")

        conn = self.get_ca_connection()
        cursor = conn.cursor(dictionary=True)

        # Only get places that have object links and aren't garbage
        query = """
            SELECT DISTINCT
                p.place_id,
                pl.name
            FROM ca_places p
            JOIN ca_place_labels pl ON p.place_id = pl.place_id
                AND pl.is_preferred = 1
            JOIN ca_objects_x_places oxp ON p.place_id = oxp.place_id
            WHERE p.deleted = 0
                AND pl.name NOT LIKE '%|%'
                AND pl.name != ''
            ORDER BY pl.name
        """
        if self.limit:
            query += f" LIMIT {self.limit}"

        cursor.execute(query)
        places = cursor.fetchall()

        self.stdout.write(f"  Found {len(places)} usable places")

        # Deduplicate by name
        seen_names = {}
        created_count = 0

        for place in places:
            name = place['name'].strip()
            ca_id = place['place_id']

            if name in seen_names:
                # Add this CA ID to existing place's ca_place_ids
                if not self.dry_run:
                    existing = Place.objects.filter(label=name).first()
                    if existing and ca_id not in existing.ca_place_ids:
                        existing.ca_place_ids.append(ca_id)
                        existing.save()
                continue

            seen_names[name] = ca_id

            if self.dry_run:
                self.stdout.write(f"  Would create: {name}")
                created_count += 1
            else:
                Place.objects.update_or_create(
                    label=name,
                    defaults={
                        'display_name': name,
                        'ca_place_ids': [ca_id],
                        'needs_geocoding': True,
                    }
                )
                created_count += 1

        cursor.close()
        conn.close()

        self.stdout.write(self.style.SUCCESS(f"Places: {created_count} created (deduplicated)"))

    def import_place_links(self):
        """Phase 7: Import CA object-place relationships."""
        self.stdout.write("Importing place links from CA...")

        conn = self.get_ca_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT
                oxp.relation_id,
                oxp.object_id,
                oxp.place_id,
                pl.name as place_name
            FROM ca_objects_x_places oxp
            JOIN ca_place_labels pl ON oxp.place_id = pl.place_id
                AND pl.is_preferred = 1
            WHERE pl.name NOT LIKE '%|%'
                AND pl.name != ''
            ORDER BY oxp.object_id
        """
        if self.limit:
            query += f" LIMIT {self.limit}"

        cursor.execute(query)
        links = cursor.fetchall()

        self.stdout.write(f"  Found {len(links)} place links")

        # Build mappings
        obj_to_desc = {}
        for desc in Description.objects.filter(ca_object_id__isnull=False).values('id', 'ca_object_id'):
            obj_to_desc[desc['ca_object_id']] = desc['id']

        place_by_name = {}
        for place in Place.objects.values('id', 'label'):
            place_by_name[place['label']] = place['id']

        created_count = 0
        skipped_count = 0
        batch = []
        batch_size = 1000

        for link in links:
            desc_id = obj_to_desc.get(link['object_id'])
            place_name = link['place_name'].strip()
            place_id = place_by_name.get(place_name)

            if not desc_id or not place_id:
                skipped_count += 1
                continue

            if self.dry_run:
                created_count += 1
            else:
                batch.append(DescriptionPlace(
                    description_id=desc_id,
                    place_id=place_id,
                    role='mentioned',  # Default role, CA doesn't have typed place relationships
                    ca_relationship_id=link['relation_id'],
                ))

                if len(batch) >= batch_size:
                    DescriptionPlace.objects.bulk_create(batch, ignore_conflicts=True)
                    created_count += len(batch)
                    self.stdout.write(f"  Created {created_count} links...")
                    batch = []

        # Final batch
        if batch and not self.dry_run:
            DescriptionPlace.objects.bulk_create(batch, ignore_conflicts=True)
            created_count += len(batch)

        cursor.close()
        conn.close()

        self.stdout.write(self.style.SUCCESS(
            f"Place links: {created_count} created, {skipped_count} skipped"
        ))

    def import_denormalize(self):
        """Phase 8: Compute denormalized fields."""
        import time
        import sys

        self.stdout.write("Computing denormalized fields...")

        if self.dry_run:
            self.stdout.write("  Would update creator_display and place_display for all descriptions")
            return

        total_count = Description.objects.count()
        self.stdout.write(f"  Total descriptions to process: {total_count}")

        # Update creator_display
        self.stdout.write("  Updating creator_display...")
        updated = 0
        processed = 0
        start_time = time.time()
        for desc in Description.objects.prefetch_related('entity_links__entity').iterator(chunk_size=1000):
            processed += 1
            creators = desc.entity_links.filter(
                role__in=['creator', 'author']
            ).select_related('entity')[:3]

            if creators:
                creator_names = [c.entity.display_name for c in creators]
                desc.creator_display = '; '.join(creator_names)
                if len(creators) == 3:
                    desc.creator_display += ' et al.'
                desc.save(update_fields=['creator_display'])
                updated += 1

            if processed % 10000 == 0:
                elapsed = time.time() - start_time
                rate = processed / elapsed if elapsed > 0 else 0
                self.stdout.write(f"    Progress: {processed}/{total_count} ({rate:.0f}/sec)")
                sys.stdout.flush()

        self.stdout.write(f"    Updated {updated} descriptions with creator_display")

        # Update place_display
        self.stdout.write("  Updating place_display...")
        updated = 0
        processed = 0
        start_time = time.time()
        for desc in Description.objects.prefetch_related('place_links__place').iterator(chunk_size=1000):
            processed += 1
            places = desc.place_links.select_related('place')[:3]

            if places:
                place_names = [p.place.display_name for p in places]
                desc.place_display = '; '.join(place_names)
                desc.save(update_fields=['place_display'])
                updated += 1

            if processed % 10000 == 0:
                elapsed = time.time() - start_time
                rate = processed / elapsed if elapsed > 0 else 0
                self.stdout.write(f"    Progress: {processed}/{total_count} ({rate:.0f}/sec)")
                sys.stdout.flush()

        self.stdout.write(f"    Updated {updated} descriptions with place_display")

        # Update path_cache (optimized: in-memory parent chain + bulk_update)
        self.stdout.write("  Updating path_cache...")
        start_time = time.time()

        # Step 1: Load all (id, parent_id) into memory - single query
        self.stdout.write("    Loading parent mappings...")
        parent_map = dict(Description.objects.values_list('id', 'parent_id'))
        self.stdout.write(f"    Loaded {len(parent_map)} mappings in {time.time() - start_time:.1f}s")

        # Step 2: Build path for each ID by walking parent chain
        def build_path(desc_id):
            path_ids = [desc_id]
            current = desc_id
            while parent_map.get(current):
                current = parent_map[current]
                path_ids.insert(0, current)
            return '/' + '/'.join(str(x) for x in path_ids) + '/'

        # Step 3: Update in batches using bulk_update
        self.stdout.write("    Computing and saving paths...")
        batch = []
        updated = 0
        truncated = 0
        max_path_len = 0
        max_depth = 0
        batch_size = 1000

        for desc in Description.objects.only('id', 'path_cache').iterator():
            full_path = build_path(desc.id)
            path_len = len(full_path)
            depth = full_path.count('/') - 1

            if path_len > max_path_len:
                max_path_len = path_len
                max_depth = depth

            if path_len > 500:
                self.stdout.write(self.style.WARNING(
                    f"    TRUNCATE: ID {desc.id}, {path_len} chars, depth {depth}"
                ))
                full_path = full_path[:500]
                truncated += 1

            desc.path_cache = full_path
            batch.append(desc)

            if len(batch) >= batch_size:
                Description.objects.bulk_update(batch, ['path_cache'])
                updated += len(batch)
                elapsed = time.time() - start_time
                rate = updated / elapsed if elapsed > 0 else 0
                self.stdout.write(f"    Progress: {updated}/{total_count} ({rate:.0f}/sec)")
                sys.stdout.flush()
                batch = []

        # Final batch
        if batch:
            Description.objects.bulk_update(batch, ['path_cache'])
            updated += len(batch)

        elapsed = time.time() - start_time
        self.stdout.write(f"    Updated {updated} descriptions with path_cache in {elapsed:.1f}s")
        self.stdout.write(f"    Max path: {max_path_len} chars (depth {max_depth})")
        if truncated:
            self.stdout.write(self.style.WARNING(f"    {truncated} paths were truncated"))

        self.stdout.write(self.style.SUCCESS("Denormalization complete"))

    def import_attributes(self):
        """Phase 9: Import EAV attributes from CA into Description fields."""
        import time
        import sys

        self.stdout.write("Importing EAV attributes from CA...")

        conn = self.get_ca_connection()
        cursor = conn.cursor(dictionary=True)

        # Get element_codes we care about
        element_codes = list(CA_ATTRIBUTE_MAP.keys())

        # Build query for EAV data
        placeholders = ', '.join(['%s'] * len(element_codes))

        def fetch_attributes(table_num):
            """Fetch attributes for a table type, grouped by row_id."""
            query = f"""
                SELECT
                    a.row_id,
                    me.element_code,
                    av.value_longtext1
                FROM ca_attributes a
                JOIN ca_attribute_values av ON a.attribute_id = av.attribute_id
                JOIN ca_metadata_elements me ON a.element_id = me.element_id
                WHERE a.table_num = %s
                    AND me.element_code IN ({placeholders})
                    AND av.value_longtext1 IS NOT NULL
                    AND av.value_longtext1 != ''
            """
            cursor.execute(query, [table_num] + element_codes)

            # Group by row_id
            attrs_by_id = {}
            for row in cursor.fetchall():
                row_id = row['row_id']
                if row_id not in attrs_by_id:
                    attrs_by_id[row_id] = {}

                elem = row['element_code']
                value = row['value_longtext1']

                # For unitdate, prefer valid-looking dates over garbage like "152"
                if elem == 'unitdate':
                    existing = attrs_by_id[row_id].get(elem)
                    if existing is None:
                        attrs_by_id[row_id][elem] = value
                    elif len(value) >= 4 and value[:4].isdigit() and int(value[:4]) >= 1000:
                        # This looks like a real date (starts with 4-digit year >= 1000)
                        if not (len(existing) >= 4 and existing[:4].isdigit() and int(existing[:4]) >= 1000):
                            # Existing doesn't look like a real date, replace it
                            attrs_by_id[row_id][elem] = value
                        elif len(value) > len(existing):
                            # Both look valid, prefer longer (more specific) date
                            attrs_by_id[row_id][elem] = value
                else:
                    # For other elements, store first non-empty value
                    if elem not in attrs_by_id[row_id]:
                        attrs_by_id[row_id][elem] = value
            return attrs_by_id

        # Fetch object attributes (table_num = 57)
        self.stdout.write("  Fetching object attributes...")
        start = time.time()
        object_attrs = fetch_attributes(CA_TABLE_OBJECTS)
        self.stdout.write(f"    Found attributes for {len(object_attrs)} objects ({time.time()-start:.1f}s)")

        # Fetch collection attributes (table_num = 13)
        self.stdout.write("  Fetching collection attributes...")
        start = time.time()
        collection_attrs = fetch_attributes(CA_TABLE_COLLECTIONS)
        self.stdout.write(f"    Found attributes for {len(collection_attrs)} collections ({time.time()-start:.1f}s)")

        cursor.close()
        conn.close()

        # Update descriptions in batches
        self.stdout.write("  Updating descriptions...")

        fields_to_update = list(set(CA_ATTRIBUTE_MAP.values())) + ['date_start', 'date_end', 'date_certainty']

        batch_size = 1000
        total_updated = 0
        start_time = time.time()

        def apply_attributes(desc, attrs):
            """Apply CA attributes to a Description object."""
            changed = False

            for element_code, zasqua_field in CA_ATTRIBUTE_MAP.items():
                if element_code in attrs:
                    value = attrs[element_code]
                    if value and not getattr(desc, zasqua_field):
                        # Truncate if too long
                        if len(value) > 2000:
                            value = value[:2000]
                        setattr(desc, zasqua_field, value)
                        changed = True

            # Parse dates from date_expression
            if desc.date_expression and not desc.date_start:
                parsed = parse_date_expression(desc.date_expression)
                if parsed:
                    if parsed['date_start']:
                        desc.date_start = parsed['date_start']
                        changed = True
                    if parsed['date_end']:
                        desc.date_end = parsed['date_end']
                        changed = True
                    if parsed['date_certainty'] and not desc.date_certainty:
                        desc.date_certainty = parsed['date_certainty']
                        changed = True

            return changed

        # Process objects
        self.stdout.write("    Processing objects...")
        batch = []
        for desc in Description.objects.filter(ca_object_id__isnull=False).iterator(chunk_size=1000):
            ca_id = desc.ca_object_id
            if ca_id in object_attrs:
                if apply_attributes(desc, object_attrs[ca_id]):
                    batch.append(desc)

            if len(batch) >= batch_size:
                Description.objects.bulk_update(batch, fields_to_update)
                total_updated += len(batch)
                elapsed = time.time() - start_time
                rate = total_updated / elapsed if elapsed > 0 else 0
                self.stdout.write(f"      Progress: {total_updated} updated ({rate:.0f}/sec)")
                sys.stdout.flush()
                batch = []

        if batch:
            Description.objects.bulk_update(batch, fields_to_update)
            total_updated += len(batch)

        # Process collections
        self.stdout.write("    Processing collections...")
        batch = []
        for desc in Description.objects.filter(ca_collection_id__isnull=False).iterator(chunk_size=1000):
            ca_id = desc.ca_collection_id
            if ca_id in collection_attrs:
                if apply_attributes(desc, collection_attrs[ca_id]):
                    batch.append(desc)

            if len(batch) >= batch_size:
                Description.objects.bulk_update(batch, fields_to_update)
                total_updated += len(batch)
                batch = []

        if batch:
            Description.objects.bulk_update(batch, fields_to_update)
            total_updated += len(batch)

        elapsed = time.time() - start_time
        self.stdout.write(f"    Updated {total_updated} descriptions in {elapsed:.1f}s")

        # Print field population stats
        self.stdout.write("\n  Field population stats:")
        total = Description.objects.count()
        for field in ['scope_content', 'date_expression', 'date_start', 'extent', 'arrangement']:
            if field == 'date_start':
                count = Description.objects.filter(date_start__isnull=False).count()
            else:
                count = Description.objects.exclude(**{field: ''}).count()
            pct = count / total * 100 if total > 0 else 0
            self.stdout.write(f"    {field}: {count} ({pct:.1f}%)")

        self.stdout.write(self.style.SUCCESS("Attributes import complete"))
