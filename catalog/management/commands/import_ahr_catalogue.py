"""
Import AHR (Archivo Historico de Rionegro) container metadata from source CSV.

This command reads the rionegro_fondos.csv file and:
1. Creates/updates container descriptions (Fondo, Tomo, Caja, Carpeta)
2. Sets proper parent-child relationships
3. Populates ISAD(G) metadata fields from the CSV

Usage:
    python manage.py import_ahr_catalogue --dry-run
    python manage.py import_ahr_catalogue
"""

import csv
import re
from datetime import date
from django.core.management.base import BaseCommand
from django.db import transaction
from catalog.models import Repository, Description


# Map CSV nivel_de_descripcion to Description.Level
LEVEL_MAP = {
    'fondo': 'fonds',
    'subfondo': 'subfonds',
    'serie': 'series',
    'tomo': 'volume',
    'caja': 'file',
    'carpeta': 'file',
    'legajo': 'file',
    'unidad documental': 'item',
    'item': 'item',
}


class Command(BaseCommand):
    help = 'Import AHR container metadata from source catalogue CSV'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be changed without making changes'
        )
        parser.add_argument(
            '--csv-path',
            type=str,
            default='/Users/juancobo/Databases/zasqua/catalogues/archivo-historico-rionegro/data/csv/rionegro_fondos.csv',
            help='Path to the CSV file'
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        csv_path = options['csv_path']

        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN - no changes will be made'))

        # Get or create the repository
        repo, _ = Repository.objects.get_or_create(
            code='co-ahr',
            defaults={
                'name': 'Archivo Historico de Rionegro',
                'city': 'Rionegro',
                'country_code': 'COL',
            }
        )

        self.stdout.write(f'Processing CSV: {csv_path}')

        # Parse CSV and build hierarchy
        rows = self.parse_csv(csv_path)
        self.stdout.write(f'Found {len(rows)} rows in CSV')

        # Process rows and create/update descriptions
        created = 0
        updated = 0
        errors = []

        # Build reference_code -> row mapping for hierarchy lookup
        row_by_path = {}
        for row in rows:
            path = row['unidad_documental_compuesta']
            row_by_path[path] = row

        with transaction.atomic():
            for row in rows:
                try:
                    result = self.process_row(row, row_by_path, repo, dry_run)
                    if result == 'created':
                        created += 1
                    elif result == 'updated':
                        updated += 1
                except Exception as e:
                    errors.append((row['identificador'], str(e)))
                    if len(errors) <= 10:
                        self.stdout.write(self.style.ERROR(
                            f"  Error on {row['identificador']}: {e}"
                        ))

            if dry_run:
                transaction.set_rollback(True)

        self.stdout.write('')
        self.stdout.write(self.style.SUCCESS(f'Created: {created}'))
        self.stdout.write(self.style.SUCCESS(f'Updated: {updated}'))
        if errors:
            self.stdout.write(self.style.ERROR(f'Errors: {len(errors)}'))

    def parse_csv(self, csv_path):
        """Parse CSV file with proper encoding."""
        rows = []
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Skip empty rows
                if not row.get('identificador'):
                    continue
                rows.append(row)
        return rows

    def process_row(self, row, row_by_path, repo, dry_run):
        """Create or update a Description from a CSV row."""
        path = row['unidad_documental_compuesta']
        identifier = row['identificador']
        title = row['titulo'] or row.get('título') or identifier
        level_raw = (row.get('nivel_de_descripcion') or row.get('nivel_de_descripción') or '').lower().strip()
        level = LEVEL_MAP.get(level_raw, 'file')

        # Build reference_code: co-ahr-{fondo}-{identifier}
        reference_code = self.build_reference_code(path, identifier, repo)

        # Find parent
        parent = self.find_parent(path, row_by_path, repo)

        # Parse dates
        date_start = self.parse_date(row.get('fecha_inicial'))
        date_end = self.parse_date(row.get('fecha_final'))

        # Build date expression
        date_expression = ''
        if date_start and date_end:
            if date_start.year == date_end.year:
                date_expression = str(date_start.year)
            else:
                date_expression = f'{date_start.year} .. {date_end.year}'
        elif date_start:
            date_expression = str(date_start.year)
        elif date_end:
            date_expression = str(date_end.year)

        # Build extent from folio info
        folio_start = row.get('folio_inicial_del_documento', '')
        folio_end = row.get('folio_final_del_documento', '')
        extent = row.get('medio_y_extension') or row.get('medio_y_extensión', '')
        if folio_start and folio_end:
            if extent:
                extent = f"{extent} (ff. {folio_start}-{folio_end})"
            else:
                extent = f"ff. {folio_start}-{folio_end}"

        # Get metadata fields
        scope_content = row.get('alcance_y_contenido', '')
        access_conditions = row.get('condiciones_de_acceso', '')
        reproduction_conditions = row.get('condiciones_de_reproduccion') or row.get('condiciones_de_reproducción', '')
        physical_chars = row.get('caracterisitcas_fisicas') or row.get('caracterísitcas_físicas', '')

        if dry_run:
            action = 'Would create' if not Description.objects.filter(reference_code=reference_code).exists() else 'Would update'
            self.stdout.write(f"  {action}: {reference_code} ({level}) - {title[:50]}")
            parent_ref = parent.reference_code if parent else 'None'
            self.stdout.write(f"    Parent: {parent_ref}")
            return 'created' if 'create' in action.lower() else 'updated'

        # Create or update
        desc, created = Description.objects.update_or_create(
            reference_code=reference_code,
            defaults={
                'repository': repo,
                'parent': parent,
                'description_level': level,
                'local_identifier': identifier,
                'title': title,
                'date_expression': date_expression,
                'date_start': date_start,
                'date_end': date_end,
                'extent': extent,
                'scope_content': scope_content,
                'access_conditions': access_conditions,
                'reproduction_conditions': reproduction_conditions,
                'notes': physical_chars,
                'is_published': True,
            }
        )

        return 'created' if created else 'updated'

    def build_reference_code(self, path, identifier, repo):
        """
        Build reference code from path and identifier.

        Path examples:
        - "Archivo Historico Rionegro" -> co-ahr
        - "Archivo Historico Rionegro, Gobierno" -> co-ahr-gob
        - "Archivo Historico Rionegro, Gobierno, Tomo 001" -> co-ahr-gob-t001
        - "Archivo Historico Rionegro, Concejo, Caja 001, Carpeta 001" -> co-ahr-con-caj001-car001
        """
        parts = [p.strip() for p in path.split(',')]

        code_parts = [repo.code]  # Start with co-ahr

        for i, part in enumerate(parts[1:], 1):  # Skip "Archivo Historico Rionegro"
            # Convert part to code segment
            part_lower = part.lower()

            if 'gobierno' in part_lower:
                code_parts.append('gob')
            elif 'concejo' in part_lower:
                code_parts.append('con')
            elif 'notarial' in part_lower:
                code_parts.append('not')
            elif 'judicial' in part_lower:
                code_parts.append('jud')
            elif 'tomo' in part_lower:
                # Extract tomo number: "Tomo 001" -> "t001"
                match = re.search(r'tomo\s*(\d+)', part_lower)
                if match:
                    code_parts.append(f"t{match.group(1).zfill(3)}")
                else:
                    code_parts.append(identifier.lower())
            elif 'caja' in part_lower:
                # Extract caja number: "Caja 001" -> "caj001"
                match = re.search(r'caja\s*(\d+)', part_lower)
                if match:
                    code_parts.append(f"caj{match.group(1).zfill(3)}")
                else:
                    code_parts.append(identifier.lower())
            elif 'carpeta' in part_lower:
                # Extract carpeta number: "Carpeta 001" -> "car001"
                match = re.search(r'carpeta\s*(\d+)', part_lower)
                if match:
                    code_parts.append(f"car{match.group(1).zfill(3)}")
                else:
                    code_parts.append(identifier.lower())
            else:
                # Use the identifier for unknown types
                code_parts.append(identifier.lower().replace(' ', ''))

        return '-'.join(code_parts)

    def find_parent(self, path, row_by_path, repo):
        """Find the parent Description based on the path hierarchy."""
        parts = [p.strip() for p in path.split(',')]

        if len(parts) <= 1:
            # This is the repository level, no parent
            return None

        # Build parent path by removing the last part
        parent_path = ', '.join(parts[:-1])

        if parent_path not in row_by_path:
            # Parent not in CSV, might be repository level
            if len(parts) == 2:
                # Direct child of repository (fondo level)
                return None
            # Try to find existing parent in database
            parent_ref = self.build_reference_code(parent_path, parts[-2], repo)
            return Description.objects.filter(reference_code=parent_ref).first()

        # Get parent row and build its reference code
        parent_row = row_by_path[parent_path]
        parent_ref = self.build_reference_code(
            parent_path,
            parent_row['identificador'],
            repo
        )

        return Description.objects.filter(reference_code=parent_ref).first()

    def parse_date(self, date_str):
        """Parse a year string into a date."""
        if not date_str:
            return None

        date_str = str(date_str).strip()

        # Handle year only
        if re.match(r'^\d{4}$', date_str):
            year = int(date_str)
            if 1000 <= year <= 2100:
                return date(year, 1, 1)

        return None
