"""
Import AHR data with correct hierarchy from clean CSVs.

This command:
1. Clears existing AHR descriptions and related entity links
2. Imports containers with proper parent hierarchy and all metadata
3. Imports items linked to parent containers
4. Imports entities with structured name fields and name_variants
5. Creates DescriptionEntity links

CSV Sources (from /Users/juancobo/Databases/zasqua/catalogues/ahr/):
- ahr_containers.csv: 2,619 containers (fondos, tomos, cajas, carpetas)
- ahr_items.csv: 52,779 individual documents
- ahr_entities.csv: 23,450 deduplicated entities with name_variants
- ahr_entity_links.csv: 88,659 item-to-entity links

Usage:
    python manage.py import_ahr_hierarchy --dry-run
    python manage.py import_ahr_hierarchy
    python manage.py import_ahr_hierarchy --only-entities  # Just entities/links
"""

import csv
import json
import sys
import time
from datetime import date
from django.core.management.base import BaseCommand
from django.db import transaction
from catalog.models import Description, Repository, Entity, DescriptionEntity


DATA_DIR = '/Users/juancobo/Databases/zasqua/catalogues/ahr'
CONTAINERS_CSV = f'{DATA_DIR}/ahr_containers.csv'
ITEMS_CSV = f'{DATA_DIR}/ahr_items.csv'
ENTITIES_CSV = f'{DATA_DIR}/ahr_entities.csv'
ENTITY_LINKS_CSV = f'{DATA_DIR}/ahr_entity_links.csv'


class Command(BaseCommand):
    help = 'Import AHR data with correct hierarchy from clean CSVs'

    def log(self, message, style=None, newline=True):
        """Write message with immediate flush."""
        if style:
            message = style(message)
        if newline:
            self.stdout.write(message)
        else:
            self.stdout.write(message, ending='')
        sys.stdout.flush()

    def log_phase(self, phase_name):
        """Log a new phase with clear formatting."""
        self.log(f'\n{"="*60}')
        self.log(f'  {phase_name}')
        self.log(f'{"="*60}')
        self.start_time = time.time()

    def log_elapsed(self):
        """Log elapsed time since phase start."""
        elapsed = time.time() - self.start_time
        self.log(f'  Elapsed: {elapsed:.1f}s')

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be done without making changes'
        )
        parser.add_argument(
            '--skip-clear',
            action='store_true',
            help='Skip clearing existing AHR data'
        )
        parser.add_argument(
            '--only-entities',
            action='store_true',
            help='Only import entities and links (skip descriptions)'
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        skip_clear = options['skip_clear']
        only_entities = options['only_entities']

        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN - no changes will be made'))

        # Get AHR repository
        try:
            self.repo = Repository.objects.get(code='co-ahr')
        except Repository.DoesNotExist:
            self.stdout.write(self.style.ERROR('Repository co-ahr not found'))
            return

        self.ref_to_id = {}
        self.entity_code_to_id = {}
        self.dry_run = dry_run

        if not only_entities:
            # Clear existing AHR data
            if not skip_clear:
                self.clear_ahr_data(dry_run)

            # Import containers
            self.import_containers(dry_run)

            # Import items
            self.import_items(dry_run)

        # Import entities
        self.import_entities(dry_run)

        # Import entity links
        self.import_entity_links(dry_run)

        if not dry_run and not only_entities:
            self.log_phase('PHASE 6: Rebuilding MPTT tree')
            self.log('  This may take a few minutes...')
            Description.objects.rebuild()
            self.log_elapsed()

            self.log_phase('PHASE 7: Updating path_cache')
            self.update_path_cache()

        if not dry_run:
            self.log('\n' + '='*60)
            self.log(self.style.SUCCESS('  IMPORT COMPLETE!'))
            self.log('='*60)
            self.log(self.style.NOTICE(
                '\nRemember to rebuild the search index:\n'
                '  python manage.py rebuild_search_index --clear'
            ))

    def clear_ahr_data(self, dry_run):
        """Clear existing AHR descriptions and related entity links."""
        self.log_phase('PHASE 1: Clearing existing AHR data')

        link_count = DescriptionEntity.objects.filter(
            description__repository=self.repo
        ).count()
        desc_count = Description.objects.filter(repository=self.repo).count()

        self.log(f'  Found {desc_count:,} descriptions and {link_count:,} entity links to clear')

        if not dry_run:
            # Clear entity links first (foreign key constraint)
            # Delete in batches to avoid lock timeouts
            self.log(f'  Deleting entity links...', newline=False)
            deleted_links = 0
            batch_size = 10000
            while True:
                # Get batch of IDs to delete
                ids = list(
                    DescriptionEntity.objects.filter(description__repository=self.repo)
                    .values_list('id', flat=True)[:batch_size]
                )
                if not ids:
                    break
                DescriptionEntity.objects.filter(id__in=ids).delete()
                deleted_links += len(ids)
                self.log(f' {deleted_links:,}...', newline=False)
            self.log(' done')

            # Then descriptions - also in batches
            self.log(f'  Deleting descriptions...', newline=False)
            deleted_descs = 0
            while True:
                ids = list(
                    Description.objects.filter(repository=self.repo)
                    .values_list('id', flat=True)[:batch_size]
                )
                if not ids:
                    break
                Description.objects.filter(id__in=ids).delete()
                deleted_descs += len(ids)
                self.log(f' {deleted_descs:,}...', newline=False)
            self.log(' done')

        self.log(f'  Cleared {desc_count:,} descriptions, {link_count:,} links')
        self.log_elapsed()

    def import_containers(self, dry_run):
        """Import containers from CSV with all metadata.

        Note: Containers must be created one-by-one (not bulk) because we need
        parent IDs to be available for child containers in the same batch.
        """
        self.log_phase('PHASE 2: Importing containers')
        self.log(f'  Source: {CONTAINERS_CSV}')

        rows = []
        self.log('  Loading CSV...', newline=False)
        with open(CONTAINERS_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
        self.log(f' {len(rows):,} rows')

        # Sort by hierarchy depth (parents before children)
        rows.sort(key=lambda r: r['reference_code'].count('-'))
        self.log(f'  Sorted by hierarchy depth')

        created = 0
        total = len(rows)

        for row in rows:
            ref_code = row['reference_code']
            parent_ref = row.get('parent_reference_code', '').strip()

            parent_id = None
            if parent_ref and parent_ref in self.ref_to_id:
                parent_id = self.ref_to_id[parent_ref]

            # Map description level
            level = row.get('description_level', 'file').lower()
            if level == 'fonds':
                level = 'fonds'
            else:
                level = 'file'  # tomos, cajas, carpetas are all 'file' level

            # Parse dates
            date_start = self.parse_date(row.get('date_start'), start=True)
            date_end = self.parse_date(row.get('date_end'), start=False)

            # Build notes from physical_characteristics and document_type
            notes_parts = []
            if row.get('physical_characteristics'):
                notes_parts.append(f"Physical: {row['physical_characteristics']}")
            if row.get('document_type'):
                notes_parts.append(f"Document types: {row['document_type']}")
            notes = '\n'.join(notes_parts)

            if not dry_run:
                desc = Description.objects.create(
                    repository=self.repo,
                    parent_id=parent_id,
                    reference_code=ref_code,
                    local_identifier=row.get('local_identifier', ''),
                    title=row.get('title', ''),
                    description_level=level,
                    scope_content=row.get('scope_content', ''),
                    extent=row.get('extent', ''),
                    access_conditions=row.get('access_conditions', ''),
                    reproduction_conditions=row.get('reproduction_conditions', ''),
                    notes=notes,
                    date_start=date_start,
                    date_end=date_end,
                    date_expression=self.build_date_expression(row),
                )
                self.ref_to_id[ref_code] = desc.id
            else:
                self.ref_to_id[ref_code] = -created  # Placeholder for dry run

            created += 1
            if created % 500 == 0:
                pct = 100 * created / total
                self.log(f'  Created {created:,}/{total:,} containers ({pct:.0f}%)')

        self.log(f'  Created {created:,} containers')
        self.log_elapsed()

    def import_items(self, dry_run):
        """Import items from CSV with all metadata using batch creation."""
        self.log_phase('PHASE 3: Importing items')
        self.log(f'  Source: {ITEMS_CSV}')

        # Count total rows first
        self.log('  Counting rows...', newline=False)
        with open(ITEMS_CSV, 'r', encoding='utf-8') as f:
            total = sum(1 for _ in f) - 1  # Subtract header
        self.log(f' {total:,} items')

        created = 0
        skipped = 0
        batch = []
        batch_refs = []  # Track ref codes for ID lookup after bulk_create
        BATCH_SIZE = 1000

        with open(ITEMS_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row in reader:
                ref_code = row['reference_code']
                parent_ref = row.get('parent_reference_code', '').strip()

                parent_id = None
                if parent_ref:
                    if parent_ref in self.ref_to_id:
                        parent_id = self.ref_to_id[parent_ref]
                    else:
                        # Parent not found - skip
                        skipped += 1
                        if skipped <= 5:
                            self.log(f'  WARNING: Missing parent: {parent_ref} for {ref_code}',
                                     style=self.style.WARNING)
                        continue

                # Parse dates
                date_start = self.parse_date(row.get('date_start'), start=True)
                date_end = self.parse_date(row.get('date_end'), start=False)

                # Format places for display (pipe-separated -> comma-separated)
                places_raw = row.get('places', '')
                place_display = ', '.join(p.strip() for p in places_raw.split('|') if p.strip())

                if not dry_run:
                    batch.append(Description(
                        repository=self.repo,
                        parent_id=parent_id,
                        reference_code=ref_code,
                        local_identifier=row.get('local_identifier', ''),
                        title=row.get('title', ''),
                        description_level='item',
                        scope_content=row.get('scope_content', ''),
                        extent=row.get('extent', ''),  # folio ranges like "f. 001r-016v"
                        access_conditions=row.get('access_conditions', ''),
                        date_start=date_start,
                        date_end=date_end,
                        date_expression=self.build_date_expression(row),
                        place_display=place_display,
                    ))
                    batch_refs.append(ref_code)

                    if len(batch) >= BATCH_SIZE:
                        Description.objects.bulk_create(batch)
                        # Update ref_to_id lookup with created IDs
                        for desc in batch:
                            self.ref_to_id[desc.reference_code] = desc.id
                        created += len(batch)
                        batch = []
                        batch_refs = []
                        pct = 100 * created / total
                        self.log(f'  Created {created:,}/{total:,} items ({pct:.0f}%)')
                else:
                    created += 1

            # Create remaining batch
            if batch and not dry_run:
                Description.objects.bulk_create(batch)
                for desc in batch:
                    self.ref_to_id[desc.reference_code] = desc.id
                created += len(batch)

        self.log(f'  Created {created:,} items, skipped {skipped} (missing parent)')
        self.log_elapsed()

    def import_entities(self, dry_run):
        """Import entities from CSV with all metadata including name_variants."""
        self.log_phase('PHASE 4: Importing entities')
        self.log(f'  Source: {ENTITIES_CSV}')

        # Count total rows first
        self.log('  Counting rows...', newline=False)
        with open(ENTITIES_CSV, 'r', encoding='utf-8') as f:
            total = sum(1 for _ in f) - 1
        self.log(f' {total:,} entities')

        # Load existing entity codes to avoid duplicates
        self.log('  Loading existing entity codes...', newline=False)
        existing_codes = set(
            Entity.objects.values_list('entity_code', flat=True)
        )
        # Also load their IDs for later linking
        existing_ids = dict(
            Entity.objects.filter(entity_code__in=existing_codes)
            .values_list('entity_code', 'id')
        )
        self.log(f' {len(existing_codes):,} existing')

        created = 0
        skipped = 0
        batch = []
        BATCH_SIZE = 1000

        with open(ENTITIES_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row in reader:
                entity_code = row['entity_code']

                # Check if entity already exists
                if entity_code in existing_codes:
                    self.entity_code_to_id[entity_code] = existing_ids.get(entity_code)
                    skipped += 1
                    continue

                # Parse name_variants JSON
                name_variants = []
                if row.get('name_variants'):
                    try:
                        name_variants = json.loads(row['name_variants'])
                    except json.JSONDecodeError:
                        # If not valid JSON, treat as single variant
                        name_variants = [row['name_variants']]

                needs_review = row.get('needs_review', '').upper() == 'TRUE'

                # Build review note with original form and count
                review_note = ''
                if needs_review:
                    parts = []
                    if row.get('original'):
                        parts.append(f"Original: {row['original']}")
                    if row.get('count'):
                        parts.append(f"Mentions: {row['count']}")
                    review_note = '; '.join(parts)

                if not dry_run:
                    batch.append(Entity(
                        entity_code=entity_code,
                        display_name=row.get('display_name', ''),
                        sort_name=row.get('sort_name', ''),
                        surname=row.get('surname', ''),
                        given_name=row.get('given_name', ''),
                        entity_type=row.get('entity_type', 'person'),
                        honorific=row.get('honorific', ''),
                        primary_function=row.get('primary_function', ''),
                        name_variants=name_variants,
                        needs_review=needs_review,
                        review_note=review_note,
                    ))

                    if len(batch) >= BATCH_SIZE:
                        Entity.objects.bulk_create(batch)
                        # Update lookup with created IDs
                        for entity in batch:
                            self.entity_code_to_id[entity.entity_code] = entity.id
                        created += len(batch)
                        batch = []
                        pct = 100 * (created + skipped) / total
                        self.log(f'  Processed {created + skipped:,}/{total:,} ({pct:.0f}%) - created {created:,}')
                else:
                    self.entity_code_to_id[entity_code] = -created
                    created += 1

            # Create remaining batch
            if batch and not dry_run:
                Entity.objects.bulk_create(batch)
                for entity in batch:
                    self.entity_code_to_id[entity.entity_code] = entity.id
                created += len(batch)

        self.log(f'  Created {created:,} entities, skipped {skipped:,} (already exist)')
        self.log_elapsed()

    def import_entity_links(self, dry_run):
        """Import entity-description links from CSV."""
        self.log_phase('PHASE 5: Importing entity links')
        self.log(f'  Source: {ENTITY_LINKS_CSV}')

        # Count total rows first
        self.log('  Counting rows...', newline=False)
        with open(ENTITY_LINKS_CSV, 'r', encoding='utf-8') as f:
            total = sum(1 for _ in f) - 1
        self.log(f' {total:,} links')

        # Build lookup for description IDs if not already built
        if not self.ref_to_id:
            self.log('  Building description lookup...', newline=False)
            self.ref_to_id = dict(
                Description.objects.filter(repository=self.repo)
                .values_list('reference_code', 'id')
            )
            self.log(f' {len(self.ref_to_id):,} descriptions')

        # Build lookup for entity IDs if not already built
        if not self.entity_code_to_id:
            self.log('  Building entity lookup...', newline=False)
            self.entity_code_to_id = dict(
                Entity.objects.filter(entity_code__startswith='ne-')
                .values_list('entity_code', 'id')
            )
            self.log(f' {len(self.entity_code_to_id):,} entities')

        created = 0
        skipped_no_desc = 0
        skipped_no_entity = 0

        batch = []

        with open(ENTITY_LINKS_CSV, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)

            for row in reader:
                item_ref = row['item_reference_code']
                entity_code = row['entity_code']

                desc_id = self.ref_to_id.get(item_ref)
                entity_id = self.entity_code_to_id.get(entity_code)

                if not desc_id:
                    skipped_no_desc += 1
                    continue
                if not entity_id:
                    skipped_no_entity += 1
                    continue

                if not dry_run:
                    batch.append(DescriptionEntity(
                        description_id=desc_id,
                        entity_id=entity_id,
                        role='mentioned',  # Default role for name_access_points
                    ))

                    if len(batch) >= 1000:
                        DescriptionEntity.objects.bulk_create(
                            batch, ignore_conflicts=True
                        )
                        batch = []

                created += 1
                processed = created + skipped_no_desc + skipped_no_entity
                if processed % 10000 == 0:
                    pct = 100 * processed / total
                    self.log(f'  Processed {processed:,}/{total:,} ({pct:.0f}%) - created {created:,}')

            # Create remaining batch
            if batch and not dry_run:
                DescriptionEntity.objects.bulk_create(batch, ignore_conflicts=True)

        self.log(
            f'  Created {created:,} links, '
            f'skipped {skipped_no_desc:,} (no desc), {skipped_no_entity:,} (no entity)'
        )
        self.log_elapsed()

    def parse_date(self, value, start=True):
        """Parse date value to date object."""
        if not value:
            return None
        try:
            year = int(str(value).strip())
            if 1500 <= year <= 2100:
                if start:
                    return date(year, 1, 1)
                else:
                    return date(year, 12, 31)
        except (ValueError, TypeError):
            pass
        return None

    def build_date_expression(self, row):
        """Build date expression string."""
        start = str(row.get('date_start', '')).strip()
        end = str(row.get('date_end', '')).strip()
        if start and end:
            if start == end:
                return start
            return f"{start}-{end}"
        return start or end or ''

    def update_path_cache(self):
        """Update path_cache for AHR descriptions."""
        self.stdout.write('  Building parent map...')
        parent_map = dict(Description.objects.filter(
            repository=self.repo
        ).values_list('id', 'parent_id'))

        def build_path(desc_id):
            path_ids = [desc_id]
            current = desc_id
            while parent_map.get(current):
                current = parent_map[current]
                path_ids.insert(0, current)
            return '/' + '/'.join(str(x) for x in path_ids) + '/'

        self.stdout.write('  Updating path_cache...')
        batch = []
        count = 0
        for desc in Description.objects.filter(repository=self.repo).only('id', 'path_cache').iterator():
            desc.path_cache = build_path(desc.id)
            batch.append(desc)
            count += 1
            if len(batch) >= 1000:
                Description.objects.bulk_update(batch, ['path_cache'])
                batch = []
                if count % 10000 == 0:
                    self.stdout.write(f'    Updated {count} paths...')
        if batch:
            Description.objects.bulk_update(batch, ['path_cache'])
        self.stdout.write(f'  Updated {count} path_cache entries')
