"""
Import/update AHR containers from CSV with metadata.

This command:
1. Updates existing AHR containers with metadata from CSV
2. Creates missing AHR containers with correct hierarchy and metadata

Usage:
    python manage.py import_ahr_from_csv --dry-run
    python manage.py import_ahr_from_csv
"""

import csv
import re
from datetime import date
from django.core.management.base import BaseCommand
from django.db import transaction
from catalog.models import Description, Repository


CSV_PATH = '/Users/juancobo/Databases/zasqua/catalogues/archivo-historico-rionegro/data/csv/rionegro_fondos.csv'

# Fondo name to code mapping
FONDO_CODES = {
    'gobierno': 'gob',
    'concejo': 'con',
    'notarial': 'not',
    'judicial': 'jud',
}


class Command(BaseCommand):
    help = 'Import/update AHR containers from CSV with metadata'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be changed without making changes'
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']

        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN - no changes will be made'))

        # Get AHR repository
        try:
            self.repo = Repository.objects.get(code='co-ahr')
        except Repository.DoesNotExist:
            self.stdout.write(self.style.ERROR('Repository co-ahr not found'))
            return

        # Load CSV
        rows = self.load_csv()
        self.stdout.write(f'Loaded {len(rows)} rows from CSV')

        # Sort by hierarchy depth (parents before children)
        rows.sort(key=lambda r: r['unidad_documental_compuesta'].count(','))

        # Build reference code lookup for existing records
        self.existing_refs = {
            d.reference_code: d.id
            for d in Description.objects.filter(repository=self.repo)
            .only('id', 'reference_code')
        }
        self.stdout.write(f'Found {len(self.existing_refs)} existing AHR records')

        # Track new records for parent lookup during import
        self.new_refs = {}

        # Process rows
        updated = 0
        created = 0
        skipped = 0
        errors = []

        import sys

        with transaction.atomic():
            for i, row in enumerate(rows):
                path = row['unidad_documental_compuesta']

                if (i + 1) % 50 == 0:
                    self.stdout.write(f'  [{i + 1}/{len(rows)}] Updated: {updated}, Created: {created}')
                    sys.stdout.flush()

                result = self.process_row(row, dry_run)

                if result == 'updated':
                    updated += 1
                    if updated <= 5:
                        self.stdout.write(f'    Updated: {path[:60]}')
                        sys.stdout.flush()
                elif result == 'created':
                    created += 1
                    if created <= 5:
                        self.stdout.write(f'    Created: {path[:60]}')
                        sys.stdout.flush()
                elif result == 'skipped':
                    skipped += 1
                elif result.startswith('error'):
                    errors.append(result)
                    self.stdout.write(self.style.ERROR(f'    Error: {result}'))
                    sys.stdout.flush()

            if dry_run:
                transaction.set_rollback(True)

        # Summary
        self.stdout.write('')
        self.stdout.write(self.style.SUCCESS(f'Summary:'))
        self.stdout.write(f'  Updated: {updated} existing containers')
        self.stdout.write(f'  Created: {created} new containers')
        self.stdout.write(f'  Skipped: {skipped} (repository level)')
        self.stdout.write(f'  Errors: {len(errors)}')

        if errors:
            self.stdout.write(self.style.WARNING('Errors:'))
            for err in errors[:10]:
                self.stdout.write(f'  {err}')
            if len(errors) > 10:
                self.stdout.write(f'  ... and {len(errors) - 10} more')

        if not dry_run and (updated > 0 or created > 0):
            self.stdout.write('')
            self.stdout.write('Rebuilding MPTT tree...')
            Description.objects.rebuild()
            self.stdout.write('Updating path_cache...')
            self.update_path_cache()
            self.stdout.write(self.style.SUCCESS('Done'))
            self.stdout.write('')
            self.stdout.write(self.style.NOTICE(
                'Remember to rebuild the search index: '
                'python manage.py rebuild_search_index --clear'
            ))

    def load_csv(self):
        """Load CSV file."""
        rows = []
        with open(CSV_PATH, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
        return rows

    def process_row(self, row, dry_run):
        """Process a single CSV row."""
        path = row['unidad_documental_compuesta']
        parts = [p.strip() for p in path.split(',')]

        # Skip repository-level record
        if len(parts) == 1:
            return 'skipped'

        # Build local_identifier and reference_code
        local_id = self.build_local_identifier(parts)
        if not local_id:
            return f'error: could not build identifier for {path}'

        ref_code = f'co-ahr-{local_id}'

        # Find parent
        parent_id = self.find_parent(parts)

        # Prepare metadata
        metadata = self.extract_metadata(row)

        # Check if exists
        if ref_code in self.existing_refs:
            # Update existing - use raw queryset to avoid MPTT ORDER BY in UPDATE
            desc_id = self.existing_refs[ref_code]
            if not dry_run:
                from django.db.models import QuerySet
                QuerySet(model=Description).filter(id=desc_id).update(**metadata)
            return 'updated'
        elif ref_code in self.new_refs:
            # Already created in this run
            return 'updated'
        else:
            # Create new
            if not dry_run:
                desc = Description.objects.create(
                    repository=self.repo,
                    parent_id=parent_id,
                    reference_code=ref_code,
                    local_identifier=local_id,
                    title=row['título'],
                    description_level=self.map_level(row['nivel_de_descripción']),
                    **metadata
                )
                self.new_refs[ref_code] = desc.id
            else:
                self.new_refs[ref_code] = -1  # Placeholder for dry run
            return 'created'

    def build_local_identifier(self, parts):
        """Build local_identifier from path parts."""
        # parts[0] = "Archivo Histórico Rionegro"
        # parts[1] = "Gobierno" / "Concejo" / etc.
        # parts[2] = "Tomo 001" / "Caja 255" / etc.
        # parts[3] = "Carpeta 001" (if present)

        if len(parts) < 2:
            return None

        # Get fondo code
        fondo_name = parts[1].lower()
        fondo_code = FONDO_CODES.get(fondo_name)
        if not fondo_code:
            return None

        if len(parts) == 2:
            # Fondo level
            return fondo_code

        # Container level
        container_parts = [fondo_code]

        for part in parts[2:]:
            normalized = self.normalize_container(part)
            if normalized:
                container_parts.append(normalized)

        return '-'.join(container_parts)

    def normalize_container(self, text):
        """Normalize container name to code."""
        text = text.strip().lower()

        # Tomo NNN -> tNNN
        match = re.match(r'^tomo\s+0*(\d+)$', text)
        if match:
            return f't{match.group(1).zfill(3)}'

        # Caja NNN -> cajNNN
        match = re.match(r'^caja\s+0*(\d+)$', text)
        if match:
            return f'caj{match.group(1).zfill(3)}'

        # Carpeta NNN -> carNNN
        match = re.match(r'^carpeta\s+0*(\d+)$', text)
        if match:
            return f'car{match.group(1).zfill(3)}'

        return None

    def find_parent(self, parts):
        """Find parent ID from path parts."""
        if len(parts) <= 2:
            # Fondo level - no parent
            return None

        # Build parent path
        parent_parts = parts[:-1]
        parent_local = self.build_local_identifier(parent_parts)
        if not parent_local:
            return None

        parent_ref = f'co-ahr-{parent_local}'

        # Check existing records first, then new records
        if parent_ref in self.existing_refs:
            return self.existing_refs[parent_ref]
        elif parent_ref in self.new_refs:
            return self.new_refs[parent_ref]

        return None

    def extract_metadata(self, row):
        """Extract metadata fields from CSV row."""
        metadata = {}

        # Scope and content
        if row.get('alcance_y_contenido'):
            metadata['scope_content'] = row['alcance_y_contenido']

        # Dates
        if row.get('fecha_inicial'):
            try:
                year = int(row['fecha_inicial'])
                metadata['date_start'] = date(year, 1, 1)
            except (ValueError, TypeError):
                pass

        if row.get('fecha_final'):
            try:
                year = int(row['fecha_final'])
                metadata['date_end'] = date(year, 12, 31)
            except (ValueError, TypeError):
                pass

        # Build date expression
        if row.get('fecha_inicial') and row.get('fecha_final'):
            if row['fecha_inicial'] == row['fecha_final']:
                metadata['date_expression'] = row['fecha_inicial']
            else:
                metadata['date_expression'] = f"{row['fecha_inicial']}-{row['fecha_final']}"
        elif row.get('fecha_inicial'):
            metadata['date_expression'] = row['fecha_inicial']

        # Access conditions
        if row.get('condiciones_de_acceso'):
            metadata['access_conditions'] = row['condiciones_de_acceso']

        # Reproduction conditions
        if row.get('condiciones_de_reproducción'):
            metadata['reproduction_conditions'] = row['condiciones_de_reproducción']

        # Physical characteristics -> notes
        if row.get('caracterísitcas_físicas'):
            metadata['notes'] = f"Physical: {row['caracterísitcas_físicas']}"

        # Location of originals
        if row.get('ubicación_de_los_originales'):
            metadata['location_of_originals'] = row['ubicación_de_los_originales']

        return metadata

    def map_level(self, nivel):
        """Map CSV nivel_de_descripción to model description_level."""
        nivel_lower = (nivel or '').lower().strip()

        if nivel_lower == 'fondo':
            return 'fonds'
        elif nivel_lower in ('tomo', 'caja', 'carpeta'):
            return 'file'
        else:
            return 'file'

    def update_path_cache(self):
        """Update path_cache for AHR descriptions."""
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

        batch = []
        for desc in Description.objects.filter(repository=self.repo).only('id', 'path_cache').iterator():
            desc.path_cache = build_path(desc.id)
            batch.append(desc)
            if len(batch) >= 1000:
                Description.objects.bulk_update(batch, ['path_cache'])
                batch = []
        if batch:
            Description.objects.bulk_update(batch, ['path_cache'])
