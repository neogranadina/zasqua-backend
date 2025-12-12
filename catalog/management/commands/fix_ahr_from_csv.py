"""
Fix AHR hierarchy and reference codes using original CSV as source of truth.

The CA import had issues with both hierarchy and reference codes for AHR.
This command uses the original CSV to:
1. Fix parent-child relationships based on unidad_documental_compuesta
2. Fix reference_code and local_identifier based on identificador column

Usage:
    python manage.py fix_ahr_from_csv --dry-run
    python manage.py fix_ahr_from_csv
"""

import csv
import re
from django.core.management.base import BaseCommand
from django.db import transaction
from catalog.models import Description, Repository

CSV_PATH = '/Users/juancobo/Databases/zasqua/catalogues/archivo-historico-rionegro/data/csv/rionegro_fondos.csv'


class Command(BaseCommand):
    help = 'Fix AHR hierarchy and reference codes from original CSV'

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

        # Load CSV data
        self.stdout.write('Loading CSV data...')
        csv_data = self.load_csv()
        self.stdout.write(f'  Loaded {len(csv_data)} rows from CSV')

        # Build hierarchy map from CSV: title path -> (identificador, parent_path)
        hierarchy = self.build_hierarchy_map(csv_data)
        self.stdout.write(f'  Built hierarchy for {len(hierarchy)} containers')

        # Get AHR repository
        try:
            repo = Repository.objects.get(code='co-ahr')
        except Repository.DoesNotExist:
            self.stdout.write(self.style.ERROR('Repository co-ahr not found'))
            return

        # Get all AHR descriptions
        ahr_descs = list(Description.objects.filter(
            repository=repo
        ).select_related('parent'))

        self.stdout.write(f'Found {len(ahr_descs)} AHR descriptions in database')

        # Build reference_code -> description mapping
        refcode_to_desc = {d.reference_code: d for d in ahr_descs if d.reference_code}

        # Also build a map of normalized path -> expected old reference_code
        # This helps us find records based on their current (incorrect) codes
        path_to_old_ref = self.build_path_to_old_ref_map(csv_data)
        self.stdout.write(f'  Built {len(path_to_old_ref)} path to old refcode mappings')

        # Fix hierarchy and reference codes
        hierarchy_fixed = 0
        refcode_fixed = 0
        not_found = 0
        not_found_by_fondo = {'gob': 0, 'con': 0, 'not': 0, 'jud': 0, 'other': 0}
        not_found_examples = []

        # Also build the expected new refcodes to find correct parents
        path_to_new_ref = {}
        for path, (identificador, parent_path, fondo) in hierarchy.items():
            norm_id = self.normalize_id(identificador)
            new_local, new_ref = self.build_codes(path, norm_id, hierarchy, fondo)
            path_to_new_ref[path] = new_ref

        with transaction.atomic():
            for path, (identificador, parent_path, fondo) in hierarchy.items():
                # Find description by current reference_code
                old_ref = path_to_old_ref.get(path)
                desc = refcode_to_desc.get(old_ref) if old_ref else None

                if not desc:
                    not_found += 1
                    fondo_key = fondo if fondo in not_found_by_fondo else 'other'
                    not_found_by_fondo[fondo_key] += 1
                    if len(not_found_examples) < 10:
                        not_found_examples.append((path, old_ref, fondo))
                    continue

                # Normalize identificador
                norm_id = self.normalize_id(identificador)

                # Build correct local_identifier and reference_code
                new_local, new_ref = self.build_codes(path, norm_id, hierarchy, fondo)

                # Find correct parent by looking up parent path
                new_parent = None
                if parent_path:
                    parent_old_ref = path_to_old_ref.get(parent_path)
                    new_parent = refcode_to_desc.get(parent_old_ref) if parent_old_ref else None

                # Check if hierarchy needs fixing
                current_parent_id = desc.parent_id
                new_parent_id = new_parent.id if new_parent else None

                if current_parent_id != new_parent_id:
                    if hierarchy_fixed < 20:
                        old_parent = desc.parent.reference_code if desc.parent else 'ROOT'
                        new_parent_ref = new_parent.reference_code if new_parent else 'ROOT'
                        self.stdout.write(f'  HIERARCHY: {desc.title} | {old_parent} -> {new_parent_ref}')

                    if not dry_run:
                        desc.parent = new_parent
                        desc.save(update_fields=['parent'])

                    hierarchy_fixed += 1

                # Check if reference_code needs fixing
                if new_ref != desc.reference_code:
                    if refcode_fixed < 20:
                        self.stdout.write(f'  REFCODE: {desc.reference_code} -> {new_ref}')

                    if not dry_run:
                        desc.reference_code = new_ref
                        desc.local_identifier = new_local
                        desc.save(update_fields=['reference_code', 'local_identifier'])

                    refcode_fixed += 1

            if dry_run:
                transaction.set_rollback(True)

        self.stdout.write('')
        if hierarchy_fixed > 20:
            self.stdout.write(f'  ... and {hierarchy_fixed - 20} more hierarchy fixes')
        if refcode_fixed > 20:
            self.stdout.write(f'  ... and {refcode_fixed - 20} more refcode fixes')

        self.stdout.write('')
        self.stdout.write(self.style.SUCCESS(f'Hierarchy fixed: {hierarchy_fixed}'))
        self.stdout.write(self.style.SUCCESS(f'Reference codes fixed: {refcode_fixed}'))
        if not_found > 0:
            self.stdout.write(self.style.WARNING(f'Not found in database: {not_found}'))
            self.stdout.write('  By fondo:')
            for fondo, count in not_found_by_fondo.items():
                if count > 0:
                    self.stdout.write(f'    {fondo}: {count}')
            self.stdout.write('  Examples:')
            for path, old_ref, fondo in not_found_examples:
                self.stdout.write(f'    {fondo} | {old_ref} | {path}')

        if not dry_run and (hierarchy_fixed > 0 or refcode_fixed > 0):
            self.stdout.write('')
            self.stdout.write('Rebuilding MPTT tree...')
            Description.objects.rebuild()
            self.stdout.write(self.style.SUCCESS('MPTT tree rebuilt'))

            self.stdout.write('Updating path_cache...')
            self.update_path_cache(repo)
            self.stdout.write(self.style.SUCCESS('path_cache updated'))

            self.stdout.write('')
            self.stdout.write(self.style.NOTICE(
                'Remember to rebuild the search index: '
                'python manage.py rebuild_search_index --clear'
            ))

    def load_csv(self):
        """Load and parse the CSV file."""
        rows = []
        with open(CSV_PATH, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
        return rows

    def build_path_to_old_ref_map(self, csv_data):
        """
        Build mapping from CSV path to Zasqua reference_code.

        The CA import used T-numbers based on the title numbers (e.g., "Caja 001" -> T001).
        We extract numbers from titles to reconstruct the mapping.
        """
        path_to_ref = {}  # CSV path -> Zasqua reference_code
        path_to_parent_ref = {}  # CSV path -> parent reference_code (for building nested refs)

        for row in csv_data:
            path = row.get('unidad_documental_compuesta', '').strip()
            nivel = row.get('nivel_de_descripción', '').strip().lower()

            if not path:
                continue

            parts = [p.strip() for p in path.split(',')]

            # Skip repository level
            if len(parts) < 2:
                continue

            # Determine fondo
            fondo_name = parts[1].strip()
            fondo = self.get_fondo_code(fondo_name)

            # Fondo level
            if len(parts) == 2 and nivel == 'fondo':
                path_to_ref[path] = f'co-ahr-{fondo}'
                continue

            # Extract number from title (last part of path)
            title = parts[-1]
            num = self.extract_number(title)
            if not num:
                continue

            if len(parts) == 3:
                # First level container (Caja or Tomo) - T-number from title
                old_ref = f'co-ahr-{fondo}-t{num:03d}'
                path_to_ref[path] = old_ref
                path_to_parent_ref[path] = old_ref

            elif len(parts) == 4:
                # Second level container (Carpeta) - need parent's T-number
                parent_path = ', '.join(parts[:3])
                parent_ref = path_to_parent_ref.get(parent_path)
                if parent_ref:
                    old_ref = f'{parent_ref}-t{num:03d}'
                    path_to_ref[path] = old_ref

        return path_to_ref

    def extract_number(self, title):
        """Extract number from title like 'Caja 001' or 'Tomo 003'."""
        match = re.search(r'\d+', title)
        if match:
            return int(match.group())
        return None

    def build_hierarchy_map(self, csv_data):
        """
        Build mapping of path -> (identificador, parent_path, fondo).

        Uses unidad_documental_compuesta to determine hierarchy.
        """
        hierarchy = {}

        for row in csv_data:
            path = row.get('unidad_documental_compuesta', '').strip()
            identificador = row.get('identificador', '').strip()
            nivel = row.get('nivel_de_descripción', '').strip().lower()

            if not path or not identificador:
                continue

            # Skip repository-level records
            if path == 'Archivo Histórico Rionegro':
                continue

            # Parse path to determine parent
            parts = [p.strip() for p in path.split(',')]

            # Determine fondo from path
            fondo = None
            if len(parts) >= 2:
                fondo_name = parts[1].strip()
                fondo = self.get_fondo_code(fondo_name)

            # Parent is everything except the last segment
            if len(parts) > 2:
                parent_path = ', '.join(parts[:-1])
            else:
                parent_path = None  # Direct child of fondo

            hierarchy[path] = (identificador, parent_path, fondo)

        return hierarchy

    def get_fondo_code(self, fondo_name):
        """Convert fondo name to code."""
        mapping = {
            'Gobierno': 'gob',
            'Concejo': 'con',
            'Notarial': 'not',
            'Judicial': 'jud',
        }
        return mapping.get(fondo_name, fondo_name.lower()[:3])

    def normalize_id(self, identificador):
        """
        Normalize identifier to consistent format.

        Handles variations like: Caj001, Caj.001, car001, car.001, T001
        """
        identificador = identificador.strip()

        # Caja variations -> cajNNN
        match = re.match(r'^Caj\.?0*(\d+)$', identificador, re.IGNORECASE)
        if match:
            return f'caj{match.group(1).zfill(3)}'

        # Carpeta variations -> carNNN
        match = re.match(r'^car\.?0*(\d+)$', identificador, re.IGNORECASE)
        if match:
            return f'car{match.group(1).zfill(3)}'

        # Tomo -> tNNN
        match = re.match(r'^T0*(\d+)$', identificador, re.IGNORECASE)
        if match:
            return f't{match.group(1).zfill(3)}'

        # Fondo codes
        if identificador.upper() in ('GOB', 'CON', 'NOT', 'JUD'):
            return identificador.lower()

        return identificador.lower()

    def build_codes(self, path, norm_id, hierarchy, fondo):
        """Build local_identifier and reference_code from path."""
        parts = [p.strip() for p in path.split(',')]

        # Build local_identifier by walking up the path
        id_parts = []

        if fondo:
            id_parts.append(fondo)

        # Add identifiers for each level (skip repo and fondo name)
        for i in range(2, len(parts)):
            sub_path = ', '.join(parts[:i+1])
            if sub_path in hierarchy:
                sub_id, _, _ = hierarchy[sub_path]
                id_parts.append(self.normalize_id(sub_id))

        new_local = '-'.join(id_parts)
        new_ref = f'co-ahr-{new_local}'

        return new_local, new_ref

    def find_desc_by_path(self, ahr_descs, path, fondo):
        """Find description by matching path components."""
        parts = [p.strip() for p in path.split(',')]

        # Get the title (last segment)
        title = parts[-1]

        for desc in ahr_descs:
            if desc.title and desc.title.strip() == title:
                # Verify it's in the right fondo
                if fondo and desc.reference_code:
                    if f'-{fondo}-' in desc.reference_code:
                        return desc
                else:
                    return desc

        return None

    def update_path_cache(self, repo):
        """Update path_cache for AHR descriptions."""
        parent_map = dict(Description.objects.filter(
            repository=repo
        ).values_list('id', 'parent_id'))

        def build_path(desc_id):
            path_ids = [desc_id]
            current = desc_id
            while parent_map.get(current):
                current = parent_map[current]
                path_ids.insert(0, current)
            return '/' + '/'.join(str(x) for x in path_ids) + '/'

        batch = []
        batch_size = 1000

        for desc in Description.objects.filter(repository=repo).only('id', 'path_cache').iterator():
            desc.path_cache = build_path(desc.id)
            batch.append(desc)

            if len(batch) >= batch_size:
                Description.objects.bulk_update(batch, ['path_cache'])
                batch = []

        if batch:
            Description.objects.bulk_update(batch, ['path_cache'])
