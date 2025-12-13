"""
Analyze PE-BN section titles from CA database.

This command extracts narra_secc_titulo and narra_num_elemento attributes
from CollectiveAccess to prepare for hierarchy restructuring.

Usage:
    python manage.py analyze_pebn_sections
    python manage.py analyze_pebn_sections --export
"""

import csv
import re
from collections import defaultdict
from django.core.management.base import BaseCommand
from catalog.models import Description, Repository
import mysql.connector


CA_DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'abcneogranadina',
    'charset': 'utf8mb4',
}

CA_TABLE_OBJECTS = 57


class Command(BaseCommand):
    help = 'Analyze PE-BN section titles from CA database'

    def add_arguments(self, parser):
        parser.add_argument(
            '--export',
            action='store_true',
            help='Export section titles to CSV for cleaning'
        )

    def handle(self, *args, **options):
        export = options['export']

        # Connect to CA database
        conn = mysql.connector.connect(**CA_DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        # Query section titles and element numbers
        self.stdout.write("Fetching section titles from CA...")
        query = """
            SELECT
                a.row_id as object_id,
                me.element_code,
                av.value_longtext1 as value
            FROM ca_attributes a
            JOIN ca_attribute_values av ON a.attribute_id = av.attribute_id
            JOIN ca_metadata_elements me ON a.element_id = me.element_id
            WHERE a.table_num = %s
                AND me.element_code IN ('narra_secc_titulo', 'narra_num_elemento', 'narra_tomo_titulo')
                AND av.value_longtext1 IS NOT NULL
                AND av.value_longtext1 != ''
        """
        cursor.execute(query, [CA_TABLE_OBJECTS])

        # Group by object_id
        object_attrs = defaultdict(dict)
        for row in cursor.fetchall():
            object_attrs[row['object_id']][row['element_code']] = row['value']

        cursor.close()
        conn.close()

        self.stdout.write(f"Found attributes for {len(object_attrs)} objects")

        # Build ca_object_id -> Description lookup
        self.stdout.write("Building Zasqua lookup...")
        desc_lookup = {}
        for desc in Description.objects.filter(
            ca_object_id__isnull=False
        ).select_related('parent').only('id', 'ca_object_id', 'title', 'parent'):
            desc_lookup[desc.ca_object_id] = desc

        self.stdout.write(f"Found {len(desc_lookup)} descriptions with CA object IDs")

        # Analyze section titles
        section_data = defaultdict(lambda: {
            'count': 0,
            'tomos': set(),
            'tomo_titles_from_attr': set(),
            'element_numbers': set(),
            'sample_ids': []
        })

        for obj_id, attrs in object_attrs.items():
            section_title = attrs.get('narra_secc_titulo', '').strip()
            if not section_title:
                continue

            elem_num = attrs.get('narra_num_elemento', '').strip()
            tomo_attr = attrs.get('narra_tomo_titulo', '').strip()

            data = section_data[section_title]
            data['count'] += 1

            if elem_num:
                data['element_numbers'].add(elem_num)
            if tomo_attr:
                data['tomo_titles_from_attr'].add(tomo_attr)

            # Get parent tomo from Zasqua hierarchy
            if obj_id in desc_lookup:
                desc = desc_lookup[obj_id]
                if desc.parent:
                    data['tomos'].add(desc.parent.title or '(untitled)')
                if len(data['sample_ids']) < 5:
                    data['sample_ids'].append(obj_id)

        # Sort by count descending
        sorted_sections = sorted(section_data.items(), key=lambda x: -x[1]['count'])

        self.stdout.write(f"\n{'='*80}")
        self.stdout.write(f"Total unique section titles: {len(sorted_sections)}")
        self.stdout.write(f"Total items with section titles: {sum(d['count'] for _, d in sorted_sections)}")
        self.stdout.write(f"{'='*80}\n")

        # Display all sections
        for i, (title, data) in enumerate(sorted_sections, 1):
            tomos = sorted(data['tomos'])
            tomo_attrs = sorted(data['tomo_titles_from_attr'])
            elem_nums = sorted(data['element_numbers'])[:5]  # First 5 element numbers

            self.stdout.write(f"\n[{i:3d}] ({data['count']:4d} items) {title}")

            if tomos:
                self.stdout.write(f"      Parent tomos: {'; '.join(tomos[:3])}")
                if len(tomos) > 3:
                    self.stdout.write(f"                    ... and {len(tomos)-3} more")

            if tomo_attrs:
                self.stdout.write(f"      Tomo attrs:   {'; '.join(tomo_attrs[:3])}")

            if elem_nums:
                self.stdout.write(f"      Element nums: {', '.join(elem_nums)}")

        # Look for potential duplicates/typos
        self.stdout.write(f"\n{'='*80}")
        self.stdout.write("Potential duplicates/typos (similar titles):")
        self.stdout.write(f"{'='*80}\n")

        titles = [t for t, _ in sorted_sections]
        for i, title1 in enumerate(titles):
            normalized1 = self.normalize_title(title1)
            for title2 in titles[i+1:]:
                normalized2 = self.normalize_title(title2)
                if normalized1 == normalized2 and title1 != title2:
                    d1 = section_data[title1]
                    d2 = section_data[title2]
                    self.stdout.write(f"  [{d1['count']:3d}] {title1}")
                    self.stdout.write(f"  [{d2['count']:3d}] {title2}")
                    self.stdout.write("")

        if export:
            self.export_csv(sorted_sections, section_data)

    def normalize_title(self, title):
        """Normalize title for comparison."""
        # Lowercase, remove extra spaces, strip punctuation
        t = title.lower().strip()
        t = re.sub(r'\s+', ' ', t)
        t = re.sub(r'[.,;:!?()"\']', '', t)
        return t

    def export_csv(self, sorted_sections, section_data):
        """Export section titles to CSV for cleaning."""
        output_path = '/Users/juancobo/Databases/zasqua/catalogues/pebn/section_titles_for_cleaning.csv'

        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                'original_title',
                'cleaned_title',
                'item_count',
                'parent_tomos',
                'tomo_attrs',
                'sample_element_nums',
                'notes'
            ])

            for title, data in sorted_sections:
                tomos = '; '.join(sorted(data['tomos'])[:3])
                tomo_attrs = '; '.join(sorted(data['tomo_titles_from_attr'])[:3])
                elem_nums = ', '.join(sorted(data['element_numbers'])[:5])

                writer.writerow([
                    title,
                    '',  # cleaned_title - to be filled manually
                    data['count'],
                    tomos,
                    tomo_attrs,
                    elem_nums,
                    ''  # notes
                ])

        self.stdout.write(f"\nExported to: {output_path}")
