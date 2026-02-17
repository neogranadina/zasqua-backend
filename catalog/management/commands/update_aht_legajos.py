"""
Update AHT legajo containers with metadata from cleaned Pilar CSV.

This command updates the 134 legajo containers with:
- extent (e.g., "394 tomas")
- dates (eventStartDates, eventEndDates)
- physical characteristics / notes
- clears needs_review flag for legajos that now have metadata

Usage:
    python manage.py update_aht_legajos --csv-path /path/to/AHT_items_clean.csv --dry-run
    python manage.py update_aht_legajos --csv-path /path/to/AHT_items_clean.csv
"""

import csv
import re
from datetime import date
from django.core.management.base import BaseCommand
from django.db import transaction
from catalog.models import Description, Repository


class Command(BaseCommand):
    help = 'Update AHT legajo containers with metadata from Pilar CSV'

    def add_arguments(self, parser):
        parser.add_argument(
            '--csv-path',
            required=True,
            help='Path to the cleaned AHT items CSV file'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be changed without making changes'
        )

    def handle(self, *args, **options):
        csv_path = options['csv_path']
        dry_run = options['dry_run']

        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN - no changes will be made'))

        # Get AHRB repository
        try:
            repo = Repository.objects.get(code='co-ahrb')
        except Repository.DoesNotExist:
            self.stdout.write(self.style.ERROR('Repository co-ahrb not found'))
            return

        # Build legajo lookup by identifier (L001 -> Description)
        legajo_lookup = {}
        legajos = Description.objects.filter(
            repository=repo,
            reference_code__startswith='co-ahrb-aht-',
            description_level__in=['file', 'series']
        )

        for leg in legajos:
            # Extract number from reference code (co-ahrb-aht-003 -> L003)
            match = re.search(r'aht-(\d+)$', leg.reference_code)
            if match:
                num = match.group(1)
                key = f'L{num}'
                legajo_lookup[key] = leg

        self.stdout.write(f'Found {len(legajo_lookup)} legajo containers in database')

        # Load CSV and filter for File-level (legajo) rows
        legajo_rows = []
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('levelOfDescription', '').strip() == 'File':
                    legajo_rows.append(row)

        self.stdout.write(f'Found {len(legajo_rows)} legajo rows in CSV')

        # Process updates
        updated = 0
        not_found = []
        errors = []

        with transaction.atomic():
            for row in legajo_rows:
                identifier = row.get('identifier', '').strip()
                if not identifier:
                    continue

                # Find matching legajo
                legajo = legajo_lookup.get(identifier)
                if not legajo:
                    not_found.append(identifier)
                    continue

                # Extract and apply metadata
                try:
                    changes = self.update_legajo(legajo, row, dry_run)
                    if changes:
                        updated += 1
                        if updated <= 5:
                            self.stdout.write(f'  Updated {identifier}: {", ".join(changes)}')
                except Exception as e:
                    errors.append(f'{identifier}: {e}')

            if dry_run:
                transaction.set_rollback(True)

        # Summary
        self.stdout.write('')
        self.stdout.write(self.style.SUCCESS('Summary:'))
        self.stdout.write(f'  Updated: {updated} legajos')
        self.stdout.write(f'  Not found: {len(not_found)}')
        self.stdout.write(f'  Errors: {len(errors)}')

        if not_found:
            self.stdout.write(self.style.WARNING(f'\nNot found in database: {", ".join(not_found[:10])}'))

        if errors:
            self.stdout.write(self.style.WARNING('\nErrors:'))
            for err in errors[:10]:
                self.stdout.write(f'  {err}')

        if not dry_run and updated > 0:
            self.stdout.write(self.style.SUCCESS('\nDone!'))

    def update_legajo(self, legajo, row, dry_run):
        """Update a legajo with metadata from CSV row. Returns list of changed fields."""
        changes = []

        # Extent (e.g., "394 tomas")
        extent = row.get('extentAndMedium', '').strip()
        if extent and legajo.extent != extent:
            if not dry_run:
                legajo.extent = extent
            changes.append('extent')

        # Physical characteristics -> notes
        phys = row.get('physicalCharacteristics', '').strip()
        if phys and legajo.notes != phys:
            if not dry_run:
                legajo.notes = phys
            changes.append('notes')

        # Dates
        date_start_str = row.get('eventStartDates', '').strip()
        date_end_str = row.get('eventEndDates', '').strip()

        if date_start_str:
            parsed = self.parse_year(date_start_str)
            if parsed and legajo.date_start != parsed:
                if not dry_run:
                    legajo.date_start = parsed
                changes.append('date_start')

        if date_end_str:
            parsed = self.parse_year(date_end_str, end=True)
            if parsed and legajo.date_end != parsed:
                if not dry_run:
                    legajo.date_end = parsed
                changes.append('date_end')

        # Date expression
        if date_start_str and date_end_str:
            if date_start_str == date_end_str:
                expr = date_start_str
            else:
                expr = f'{date_start_str}-{date_end_str}'
            if legajo.date_expression != expr:
                if not dry_run:
                    legajo.date_expression = expr
                changes.append('date_expression')
        elif date_start_str and legajo.date_expression != date_start_str:
            if not dry_run:
                legajo.date_expression = date_start_str
            changes.append('date_expression')

        # Clear needs_review if we have metadata now
        if changes and legajo.needs_review:
            if not dry_run:
                legajo.needs_review = False
                legajo.review_note = ''
            changes.append('cleared needs_review')

        # Save if changes were made
        if changes and not dry_run:
            legajo.save()

        return changes

    def parse_year(self, date_str, end=False):
        """Parse year string to date object."""
        if not date_str:
            return None

        date_str = date_str.strip()

        # Try YYYY format
        if re.match(r'^\d{4}$', date_str):
            year = int(date_str)
            if 1400 <= year <= 2100:
                if end:
                    return date(year, 12, 31)
                return date(year, 1, 1)

        return None
