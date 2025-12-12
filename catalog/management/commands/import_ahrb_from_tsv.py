"""
Import/update AHRB-AHT containers from AtoM TSV export.

This command:
1. Updates existing AHRB-AHT containers with metadata from TSV
2. Creates missing AHRB-AHT containers with metadata

Usage:
    python manage.py import_ahrb_from_tsv --dry-run
    python manage.py import_ahrb_from_tsv
"""

import csv
import re
from datetime import date
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import QuerySet
from catalog.models import Description, Repository


TSV_PATH = '/Users/juancobo/Databases/zasqua/catalogues/ahrb-aht_containers.tsv'


class Command(BaseCommand):
    help = 'Import/update AHRB-AHT containers from AtoM TSV'

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

        # Get AHRB repository
        try:
            self.repo = Repository.objects.get(code='co-ahrb')
        except Repository.DoesNotExist:
            self.stdout.write(self.style.ERROR('Repository co-ahrb not found'))
            return

        # Find the AHT parent (Archivo Histórico de Tunja)
        self.aht_parent = Description.objects.filter(
            repository=self.repo,
            reference_code='co-ahrb-aht'
        ).first()

        if not self.aht_parent:
            self.stdout.write(self.style.ERROR('Parent co-ahrb-aht not found'))
            return

        self.stdout.write(f'Found AHT parent: {self.aht_parent.reference_code} (id={self.aht_parent.id})')

        # Load TSV
        rows = self.load_tsv()
        self.stdout.write(f'Loaded {len(rows)} rows from TSV')

        # Build reference code lookup for existing records
        self.existing_refs = {
            d.reference_code: d.id
            for d in Description.objects.filter(
                repository=self.repo,
                reference_code__startswith='co-ahrb-aht-'
            ).only('id', 'reference_code')
        }
        self.stdout.write(f'Found {len(self.existing_refs)} existing AHRB-AHT records')

        # Process rows
        updated = 0
        created = 0
        skipped = 0
        errors = []

        import sys

        with transaction.atomic():
            for i, row in enumerate(rows):
                identifier = row.get('identifier', '')

                if (i + 1) % 20 == 0:
                    self.stdout.write(f'  [{i + 1}/{len(rows)}] Updated: {updated}, Created: {created}')
                    sys.stdout.flush()

                result = self.process_row(row, dry_run)

                if result == 'updated':
                    updated += 1
                    if updated <= 5:
                        self.stdout.write(f'    Updated: {identifier}')
                elif result == 'created':
                    created += 1
                    if created <= 5:
                        self.stdout.write(f'    Created: {identifier}')
                elif result == 'skipped':
                    skipped += 1
                elif result and result.startswith('error'):
                    errors.append(result)
                    self.stdout.write(self.style.ERROR(f'    {result}'))

            if dry_run:
                transaction.set_rollback(True)

        # Summary
        self.stdout.write('')
        self.stdout.write(self.style.SUCCESS('Summary:'))
        self.stdout.write(f'  Updated: {updated} existing containers')
        self.stdout.write(f'  Created: {created} new containers')
        self.stdout.write(f'  Skipped: {skipped}')
        self.stdout.write(f'  Errors: {len(errors)}')

        if errors:
            self.stdout.write(self.style.WARNING('Errors:'))
            for err in errors[:10]:
                self.stdout.write(f'  {err}')

        if not dry_run and (updated > 0 or created > 0):
            self.stdout.write('')
            self.stdout.write('Rebuilding MPTT tree...')
            Description.objects.rebuild()
            self.stdout.write(self.style.SUCCESS('Done'))
            self.stdout.write('')
            self.stdout.write(self.style.NOTICE(
                'Remember to rebuild the search index: '
                'python manage.py rebuild_search_index --clear'
            ))

    def load_tsv(self):
        """Load TSV file."""
        rows = []
        with open(TSV_PATH, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            for row in reader:
                rows.append(row)
        return rows

    def process_row(self, row, dry_run):
        """Process a single TSV row."""
        identifier = row.get('identifier', '').strip()

        if not identifier:
            return 'skipped'

        # Convert L001 -> 001
        match = re.match(r'^L0*(\d+)$', identifier)
        if not match:
            return f'error: invalid identifier {identifier}'

        num = match.group(1).zfill(3)
        local_id = f'aht-{num}'
        ref_code = f'co-ahrb-{local_id}'

        # Prepare metadata
        metadata = self.extract_metadata(row)

        # Build title from identifier
        title = f'Legajo {num}'

        # Check if exists
        if ref_code in self.existing_refs:
            # Update existing - use raw queryset to avoid MPTT ORDER BY in UPDATE
            desc_id = self.existing_refs[ref_code]
            if not dry_run:
                # Check if this legajo has children (items)
                has_children = Description.objects.filter(parent_id=desc_id).exists()
                if not has_children:
                    metadata['needs_review'] = True
                    metadata['review_note'] = 'Empty container - no items cataloged yet'
                QuerySet(model=Description).filter(id=desc_id).update(**metadata)
            return 'updated'
        else:
            # Create new - these are always empty (no items yet)
            if not dry_run:
                Description.objects.create(
                    repository=self.repo,
                    parent=self.aht_parent,
                    reference_code=ref_code,
                    local_identifier=local_id,
                    title=title,
                    description_level='file',
                    needs_review=True,
                    review_note='Empty container - no items cataloged yet',
                    **metadata
                )
            return 'created'

    def extract_metadata(self, row):
        """Extract metadata fields from TSV row."""
        metadata = {}

        # Extent and medium
        if row.get('extentAndMedium'):
            metadata['extent'] = row['extentAndMedium']

        # Physical characteristics -> notes
        if row.get('physicalCharacteristics'):
            metadata['notes'] = row['physicalCharacteristics']

        # Dates from eventStartDates / eventEndDates
        if row.get('eventStartDates'):
            try:
                year = int(row['eventStartDates'])
                metadata['date_start'] = date(year, 1, 1)
            except (ValueError, TypeError):
                pass

        if row.get('eventEndDates'):
            try:
                year = int(row['eventEndDates'])
                metadata['date_end'] = date(year, 12, 31)
            except (ValueError, TypeError):
                pass

        # Build date expression
        start = row.get('eventStartDates', '')
        end = row.get('eventEndDates', '')
        if start and end:
            if start == end:
                metadata['date_expression'] = start
            else:
                metadata['date_expression'] = f'{start}-{end}'
        elif start:
            metadata['date_expression'] = start

        # Access conditions
        if row.get('accessConditions'):
            metadata['access_conditions'] = row['accessConditions']

        # Reproduction conditions
        if row.get('reproductionConditions'):
            metadata['reproduction_conditions'] = row['reproductionConditions']

        # Scope and content
        if row.get('scopeAndContent'):
            metadata['scope_content'] = row['scopeAndContent']

        # Sources -> internal_notes
        if row.get('sources'):
            metadata['internal_notes'] = row['sources']

        return metadata
