"""
Restructure PE-BN CDIP items into section-level hierarchy.

This command:
1. Creates section-level Description records under each sub-tomo
2. Re-parents items from sub-tomos to their appropriate sections
3. Optionally updates item reference codes with element numbers

Usage:
    python manage.py restructure_pebn_sections --cleaning-csv /path/to/section_title_mappings.csv --dry-run
    python manage.py restructure_pebn_sections --cleaning-csv /path/to/section_title_mappings.csv
"""

import csv
import re
from collections import defaultdict
from django.core.management.base import BaseCommand
from django.db import transaction
from catalog.models import Description, Repository
import mysql.connector


CA_DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'abcneogranadina',
    'charset': 'utf8mb4',
}

class Command(BaseCommand):
    help = 'Restructure PE-BN CDIP items into section-level hierarchy'

    def add_arguments(self, parser):
        parser.add_argument(
            '--cleaning-csv',
            required=True,
            help='Path to the section title mappings CSV file'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be changed without making changes'
        )

    def handle(self, *args, **options):
        self.cleaning_csv = options['cleaning_csv']
        dry_run = options['dry_run']

        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN - no changes will be made'))

        # Load cleaning mappings
        self.load_cleaning_mappings()

        # Get PE-BN repository
        try:
            self.repo = Repository.objects.get(code='pe-bn')
        except Repository.DoesNotExist:
            self.stdout.write(self.style.ERROR('Repository pe-bn not found'))
            return

        # Fetch CA attributes
        self.fetch_ca_attributes()

        # Build section structure
        self.build_section_structure()

        # Execute restructuring
        with transaction.atomic():
            self.create_sections(dry_run)
            self.reparent_items(dry_run)

            if dry_run:
                transaction.set_rollback(True)

        # Summary
        self.stdout.write('')
        self.stdout.write(self.style.SUCCESS('Summary:'))
        self.stdout.write(f'  Sections created: {self.sections_created}')
        self.stdout.write(f'  Items re-parented: {self.items_reparented}')

        if not dry_run and self.sections_created > 0:
            self.stdout.write('')
            self.stdout.write('Rebuilding MPTT tree...')
            Description.objects.rebuild()
            self.stdout.write(self.style.SUCCESS('Done'))
            self.stdout.write('')
            self.stdout.write(self.style.NOTICE(
                'Remember to rebuild the search index: '
                'python manage.py rebuild_search_index --clear'
            ))

    def load_cleaning_mappings(self):
        """Load section title cleaning mappings from CSV."""
        self.cleaning_map = {}
        with open(self.cleaning_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                orig = row['original_title']
                cleaned = row['cleaned_title']
                self.cleaning_map[orig] = cleaned

        self.stdout.write(f'Loaded {len(self.cleaning_map)} cleaning mappings')

    def fetch_ca_attributes(self):
        """Fetch section titles and element numbers from CA."""
        conn = mysql.connector.connect(**CA_DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        # Fetch section titles
        cursor.execute('''
            SELECT a.row_id as object_id, av.value_longtext1 as value
            FROM ca_attributes a
            JOIN ca_attribute_values av ON a.attribute_id = av.attribute_id
            JOIN ca_metadata_elements me ON a.element_id = me.element_id
            WHERE a.table_num = 57 AND me.element_code = 'narra_secc_titulo'
            AND av.value_longtext1 IS NOT NULL AND av.value_longtext1 != ''
        ''')
        self.section_by_obj = {row['object_id']: row['value'].strip()
                               for row in cursor.fetchall()}

        # Fetch element numbers
        cursor.execute('''
            SELECT a.row_id as object_id, av.value_longtext1 as value
            FROM ca_attributes a
            JOIN ca_attribute_values av ON a.attribute_id = av.attribute_id
            JOIN ca_metadata_elements me ON a.element_id = me.element_id
            WHERE a.table_num = 57 AND me.element_code = 'narra_num_elemento'
            AND av.value_longtext1 IS NOT NULL AND av.value_longtext1 != ''
        ''')
        self.element_by_obj = {row['object_id']: row['value'].strip()
                               for row in cursor.fetchall()}

        cursor.close()
        conn.close()

        self.stdout.write(f'Fetched {len(self.section_by_obj)} section titles from CA')
        self.stdout.write(f'Fetched {len(self.element_by_obj)} element numbers from CA')

    def build_section_structure(self):
        """Build mapping of parent -> section -> items."""
        self.parent_sections = defaultdict(lambda: defaultdict(list))

        # Get all items with section titles
        descs = Description.objects.filter(
            ca_object_id__in=self.section_by_obj.keys()
        ).select_related('parent')

        for desc in descs:
            section_orig = self.section_by_obj.get(desc.ca_object_id, '')
            if not section_orig or not desc.parent:
                continue

            # Apply cleaning
            section_clean = self.cleaning_map.get(section_orig, section_orig)

            parent_id = desc.parent_id
            element_num = self.element_by_obj.get(desc.ca_object_id, '')

            self.parent_sections[parent_id][section_clean].append({
                'desc_id': desc.id,
                'element_num': element_num,
            })

        total_pairs = sum(len(s) for s in self.parent_sections.values())
        self.stdout.write(f'Built structure: {len(self.parent_sections)} parents, {total_pairs} sections')

    def create_sections(self, dry_run):
        """Create section-level Description records."""
        self.sections_created = 0
        self.section_lookup = {}  # (parent_id, section_title) -> section Description

        for parent_id, sections in self.parent_sections.items():
            parent = Description.objects.get(id=parent_id)

            # Extract volume and tomo numbers from parent reference code
            # pe-bn-cdip-01-01 -> v01, t01
            ref_parts = parent.reference_code.split('-')
            if len(ref_parts) >= 5:
                vol_num = ref_parts[3]  # 01
                tomo_num = ref_parts[4]  # 01
            else:
                vol_num = '00'
                tomo_num = '00'

            # Generate section reference codes
            section_num = 1
            for section_title in sorted(sections.keys()):
                items = sections[section_title]

                # Extract original Roman numeral from title (if present)
                original_section_num = self.extract_roman_numeral(section_title)
                clean_title = self.remove_roman_numeral(section_title)

                # Build reference code: pe-bn-cdip-01-01-s01
                section_ref = f'{parent.reference_code}-s{section_num:02d}'

                # Build local identifier with prefixes: cdip-v01-t01-s01
                section_local = f'cdip-v{vol_num}-t{tomo_num}-s{section_num:02d}'

                # Build notes with original section number
                notes = ''
                if original_section_num:
                    notes = f'Sección {original_section_num} en la edición impresa del CDIP'

                if not dry_run:
                    section_desc = Description.objects.create(
                        repository=self.repo,
                        parent=parent,
                        reference_code=section_ref,
                        local_identifier=section_local,
                        title=clean_title,
                        description_level='subseries',
                        notes=notes,
                    )
                    self.section_lookup[(parent_id, section_title)] = section_desc
                else:
                    # Create placeholder for dry run
                    self.section_lookup[(parent_id, section_title)] = type('obj', (object,), {
                        'id': None,
                        'reference_code': section_ref,
                        'local_identifier': section_local,
                        'title': clean_title,
                    })()

                self.sections_created += 1
                section_num += 1

                if self.sections_created <= 5:
                    self.stdout.write(f'  Created: {section_local} - {clean_title[:50]}')
                    if notes:
                        self.stdout.write(f'           ({notes})')

        if self.sections_created > 5:
            self.stdout.write(f'  ... and {self.sections_created - 5} more sections')

    def extract_roman_numeral(self, title):
        """Extract leading Roman numeral from title."""
        match = re.match(r'^([IVXL]+)[\s.\-—]+', title)
        if match:
            return match.group(1)
        # Also check for numbered sections like "1. Medicina"
        match = re.match(r'^(\d+)[\s.\-]+', title)
        if match:
            return match.group(1)
        return None

    def remove_roman_numeral(self, title):
        """Remove leading Roman numeral or number from title."""
        # Remove patterns like "I. ", "IX. ", "1. ", "XII - "
        cleaned = re.sub(r'^[IVXL]+[\s.\-—]+', '', title)
        cleaned = re.sub(r'^\d+[\s.\-]+', '', cleaned)
        # Capitalize first letter if needed
        if cleaned and cleaned[0].islower():
            cleaned = cleaned[0].upper() + cleaned[1:]
        return cleaned.strip()

    def reparent_items(self, dry_run):
        """Re-parent items from sub-tomos to their sections using bulk update."""
        self.items_reparented = 0

        if dry_run:
            # Just count for dry run
            for parent_id, sections in self.parent_sections.items():
                for section_title, items in sections.items():
                    self.items_reparented += len(items)
            self.stdout.write(f'Re-parented {self.items_reparented} items')
            return

        # Collect all updates: desc_id -> new_parent_id
        updates = []
        for parent_id, sections in self.parent_sections.items():
            for section_title, items in sections.items():
                section_desc = self.section_lookup.get((parent_id, section_title))
                if not section_desc:
                    continue

                for item_info in items:
                    updates.append({
                        'desc_id': item_info['desc_id'],
                        'new_parent_id': section_desc.id,
                    })

        # Bulk update in batches
        batch_size = 500
        total = len(updates)
        self.stdout.write(f'Re-parenting {total} items in batches of {batch_size}...')

        for i in range(0, total, batch_size):
            batch = updates[i:i + batch_size]

            # Group by new parent for efficiency
            by_parent = {}
            for u in batch:
                pid = u['new_parent_id']
                if pid not in by_parent:
                    by_parent[pid] = []
                by_parent[pid].append(u['desc_id'])

            # Update each group
            for new_parent_id, desc_ids in by_parent.items():
                Description.objects.filter(id__in=desc_ids).update(parent_id=new_parent_id)

            self.items_reparented += len(batch)
            if (i + batch_size) % 2000 == 0 or i + batch_size >= total:
                self.stdout.write(f'  Progress: {self.items_reparented}/{total}')

        self.stdout.write(f'Re-parented {self.items_reparented} items')
