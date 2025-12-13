"""
Import AHT item-level records from Pilar CSV.

This command imports item-level records from the Pilar processing CSV into
the existing AHT legajo containers. It handles the inherited parent field
pattern where blank "Unidad Documental Compuesta" values inherit from the
previous row.

Rows without identifiers are skipped and exported to a separate CSV for
manual review.

Usage:
    python manage.py import_aht_items --dry-run
    python manage.py import_aht_items
    python manage.py import_aht_items --skip-existing  # Skip legajos that already have items
"""

import csv
import re
from datetime import date
from django.core.management.base import BaseCommand
from django.db import transaction
from catalog.models import Description, Repository


# Use the cleaned CSV (original was split into clean + needs_review)
CSV_PATH = '/Users/juancobo/Databases/zasqua/zasqua-dev-notes/reference/catalogues/arhb/AHT_items_clean.csv'


class Command(BaseCommand):
    help = 'Import AHT item-level records from Pilar CSV'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be changed without making changes'
        )
        parser.add_argument(
            '--skip-existing',
            action='store_true',
            help='Skip legajos that already have items (avoid duplicates)'
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        skip_existing = options['skip_existing']

        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN - no changes will be made'))

        # Get AHRB repository
        try:
            self.repo = Repository.objects.get(code='co-ahrb')
        except Repository.DoesNotExist:
            self.stdout.write(self.style.ERROR('Repository co-ahrb not found'))
            return

        # Build legajo lookup (Legajo_003 -> Description object)
        # Note: CA-migrated legajos have level='series', new ones have level='file'
        self.legajo_lookup = {}
        legajos = Description.objects.filter(
            repository=self.repo,
            reference_code__startswith='co-ahrb-aht-',
            description_level__in=['file', 'series']
        ).select_related('parent')

        for leg in legajos:
            # Extract number from reference code (co-ahrb-aht-003 -> 003)
            match = re.search(r'aht-(\d+)$', leg.reference_code)
            if match:
                num = match.group(1)
                key = f'Legajo_{num}'
                self.legajo_lookup[key] = leg

        self.stdout.write(f'Found {len(self.legajo_lookup)} legajo containers')

        # Track which legajos already have items
        if skip_existing:
            self.populated_legajos = set()
            for key, leg in self.legajo_lookup.items():
                if leg.get_children().exists():
                    self.populated_legajos.add(key)
            self.stdout.write(f'Skipping {len(self.populated_legajos)} legajos that already have items')

        # Load and process CSV
        rows, fieldnames = self.load_csv()
        self.stdout.write(f'Loaded {len(rows)} rows from CSV')

        # Process items
        created = 0
        skipped_existing = 0
        needs_review = []
        errors = []

        with transaction.atomic():
            current_legajo = None

            for i, row in enumerate(rows):
                level = row.get('levelOfDescription', '').strip()

                # Skip File-level records (legajo metadata)
                if level == 'File':
                    continue

                # Track current legajo (inherit from previous if blank)
                parent_field = row.get('Unidad Documental Compuesta (Legajo, volumen)', '').strip()
                if parent_field:
                    current_legajo = parent_field

                identifier = row.get('identifier', '').strip()
                scope = row.get('scopeAndContent', '').strip()
                title = row.get('title', '').strip()

                # Skip completely blank rows
                if not identifier and not scope and not title:
                    continue

                # Rows with content but no identifier -> needs review
                if not identifier:
                    row['_inferred_legajo'] = current_legajo or ''
                    row['_row_number'] = i + 2
                    needs_review.append(row)
                    continue

                # Skip if no current legajo context
                if not current_legajo:
                    errors.append(f'Row {i+2}: No legajo context for {identifier}')
                    continue

                # Skip if legajo already has items and --skip-existing is set
                if skip_existing and current_legajo in self.populated_legajos:
                    skipped_existing += 1
                    continue

                # Get parent legajo
                parent = self.legajo_lookup.get(current_legajo)
                if not parent:
                    errors.append(f'Row {i+2}: Legajo not found: {current_legajo}')
                    continue

                # Create item
                result = self.create_item(parent, row, identifier, dry_run)
                if result == 'created':
                    created += 1
                elif result and result.startswith('error'):
                    errors.append(f'Row {i+2}: {result}')

                if (i + 1) % 200 == 0:
                    self.stdout.write(f'  [{i + 1}] Created: {created}, Skipped: {skipped_existing}')

            if dry_run:
                transaction.set_rollback(True)

        # Export rows needing review
        if needs_review:
            self.export_review_csv(needs_review, fieldnames)

        # Summary
        self.stdout.write('')
        self.stdout.write(self.style.SUCCESS('Summary:'))
        self.stdout.write(f'  Created: {created} items')
        self.stdout.write(f'  Skipped (existing legajos): {skipped_existing}')
        self.stdout.write(f'  Needs review (no identifier): {len(needs_review)}')
        self.stdout.write(f'  Errors: {len(errors)}')

        if needs_review:
            self.stdout.write(self.style.WARNING(f'\nExported {len(needs_review)} rows to:'))
            self.stdout.write(f'  {REVIEW_CSV_PATH}')

        if errors:
            self.stdout.write(self.style.WARNING('\nFirst 20 errors:'))
            for err in errors[:20]:
                self.stdout.write(f'  {err}')

        if not dry_run and created > 0:
            self.stdout.write('')
            self.stdout.write('Rebuilding MPTT tree...')
            Description.objects.rebuild()
            self.stdout.write(self.style.SUCCESS('Done'))
            self.stdout.write('')
            self.stdout.write(self.style.NOTICE(
                'Remember to rebuild the search index: '
                'python manage.py rebuild_search_index --clear'
            ))

    def load_csv(self):
        """Load CSV file and return rows with fieldnames."""
        rows = []
        with open(CSV_PATH, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            for row in reader:
                rows.append(row)
        return rows, fieldnames

    def export_review_csv(self, rows, fieldnames):
        """Export rows needing review to a separate CSV."""
        # Add our custom columns at the beginning
        review_fieldnames = ['_row_number', '_inferred_legajo'] + list(fieldnames)

        with open(REVIEW_CSV_PATH, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=review_fieldnames, extrasaction='ignore')
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    def create_item(self, parent, row, identifier, dry_run):
        """Create an item record under the given legajo."""
        # Build reference code: co-ahrb-aht-003-d001
        parent_num = parent.reference_code.split('-')[-1]  # 003
        item_id = identifier.lower().replace(' ', '')  # D001 -> d001
        ref_code = f'co-ahrb-aht-{parent_num}-{item_id}'
        local_id = f'aht-{parent_num}-{item_id}'

        # Extract title
        title = row.get('title', '').strip()
        if not title:
            # Use scope_content as title if no title
            title = row.get('scopeAndContent', '').strip()
        if not title:
            title = f'Documento {identifier}'

        # Truncate title if too long (keep first 500 chars)
        if len(title) > 500:
            title = title[:497] + '...'

        # Extract metadata
        metadata = self.extract_metadata(row)

        if not dry_run:
            Description.objects.create(
                repository=self.repo,
                parent=parent,
                reference_code=ref_code,
                local_identifier=local_id,
                title=title,
                description_level='item',
                **metadata
            )

        return 'created'

    def extract_metadata(self, row):
        """Extract metadata fields from CSV row."""
        metadata = {}

        # Scope and content
        scope = row.get('scopeAndContent', '').strip()
        if scope:
            metadata['scope_content'] = scope

        # Extent (folio range)
        folio_start = row.get('Folio inicial', '').strip()
        folio_end = row.get('Folio final', '').strip()
        if folio_start and folio_end:
            metadata['extent'] = f'ff. {folio_start}-{folio_end}'
        elif folio_start:
            metadata['extent'] = f'f. {folio_start}'

        # Physical characteristics -> notes
        phys = row.get('physicalCharacteristics', '').strip()
        if phys:
            metadata['notes'] = phys

        # Dates
        date_start_str = row.get('eventStartDates', '').strip()
        date_end_str = row.get('eventEndDates', '').strip()

        if date_start_str:
            parsed_start = self.parse_date(date_start_str)
            if parsed_start:
                metadata['date_start'] = parsed_start

        if date_end_str:
            parsed_end = self.parse_date(date_end_str, end=True)
            if parsed_end:
                metadata['date_end'] = parsed_end

        # Date expression
        if date_start_str and date_end_str:
            if date_start_str == date_end_str:
                metadata['date_expression'] = date_start_str
            else:
                metadata['date_expression'] = f'{date_start_str} - {date_end_str}'
        elif date_start_str:
            metadata['date_expression'] = date_start_str

        # Place access points -> place_display (pipe-separated in CSV)
        places = row.get('placeAccessPoints', '').strip()
        if places:
            # Convert "Tunja | Vélez | Moniquirá" to "Tunja, Vélez, Moniquirá"
            place_list = [p.strip() for p in places.split('|') if p.strip()]
            if place_list:
                metadata['place_display'] = ', '.join(place_list)

        # Name access points -> creator_display (pipe-separated)
        names = row.get('nameAccessPoints', '').strip()
        if names:
            name_list = [n.strip() for n in names.split('|') if n.strip()]
            if name_list:
                # Store as mentioned names (these aren't necessarily creators)
                metadata['creator_display'] = ', '.join(name_list[:5])  # Limit to 5

        # Genre/document type -> arrangement (or notes)
        genre = row.get('genreAccessPoints', '').strip()
        if genre:
            # Convert "Mortuoria | Testamento" to "Mortuoria, Testamento"
            genre_list = [g.strip() for g in genre.split('|') if g.strip()]
            if genre_list:
                # Append to notes if exists, otherwise set arrangement
                if 'notes' in metadata:
                    metadata['notes'] += f'\nTipo documental: {", ".join(genre_list)}'
                else:
                    metadata['arrangement'] = ', '.join(genre_list)

        # Subject access points
        subjects = row.get('subjectAccessPoints', '').strip()
        if subjects:
            subject_list = [s.strip() for s in subjects.split('|') if s.strip()]
            if subject_list:
                if 'notes' in metadata:
                    metadata['notes'] += f'\nTemas: {", ".join(subject_list)}'
                else:
                    metadata['notes'] = f'Temas: {", ".join(subject_list)}'

        # Language
        lang = row.get('language', '').strip()
        if lang:
            metadata['language'] = lang

        # Revision history -> internal_notes (cataloging provenance)
        revision = row.get('revisionHistory', '').strip()
        if revision:
            metadata['internal_notes'] = revision

        # Sources / finding aids
        sources = row.get('sources', '').strip()
        finding_aids = row.get('findingAids', '').strip()
        if sources or finding_aids:
            combined = ' | '.join(filter(None, [sources, finding_aids]))
            if 'internal_notes' in metadata:
                metadata['internal_notes'] += f'\nFuentes: {combined}'
            else:
                metadata['internal_notes'] = f'Fuentes: {combined}'

        # Archival history (rare but capture it)
        arch_hist = row.get('archivalHistory', '').strip()
        if arch_hist:
            metadata['provenance'] = arch_hist

        # Location of originals
        loc_orig = row.get('locationOfOriginals', '').strip()
        if loc_orig:
            metadata['location_of_originals'] = loc_orig

        return metadata

    def parse_date(self, date_str, end=False):
        """Parse date string to date object.

        Handles formats:
        - YYYY -> date(YYYY, 1, 1) or date(YYYY, 12, 31) if end
        - YYYY-MM-DD -> date(YYYY, MM, DD)
        - DD/MM/YYYY -> date(YYYY, MM, DD)
        """
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

        # Try YYYY-MM-DD format
        match = re.match(r'^(\d{4})-(\d{2})-(\d{2})$', date_str)
        if match:
            try:
                return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
            except ValueError:
                return None

        # Try DD/MM/YYYY format
        match = re.match(r'^(\d{2})/(\d{2})/(\d{4})$', date_str)
        if match:
            try:
                return date(int(match.group(3)), int(match.group(2)), int(match.group(1)))
            except ValueError:
                return None

        return None
