"""
Fix specific AHR reference code issues identified in the data.

This fix handles:
1. Containers where title type != refcode type (e.g., title "Caja 081" but refcode has "t081")
2. Children whose parent was renamed (cascade parent prefix)

Usage:
    python manage.py fix_ahr_targeted --dry-run
    python manage.py fix_ahr_targeted
"""

import re
from django.core.management.base import BaseCommand
from django.db import transaction
from catalog.models import Description


class Command(BaseCommand):
    help = 'Fix specific AHR reference code issues'

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

        # Step 1: Find type mismatches (title type != refcode type)
        self.stdout.write('Finding type mismatches...')
        type_mismatches = self.find_type_mismatches()
        self.stdout.write(f'  Found {len(type_mismatches)} type mismatches')

        # Step 2: Find children with wrong parent prefix
        self.stdout.write('Finding children with wrong parent prefix...')
        prefix_mismatches = self.find_prefix_mismatches()
        self.stdout.write(f'  Found {len(prefix_mismatches)} prefix mismatches')

        # Combine and deduplicate
        all_to_fix = {}
        all_to_fix.update(type_mismatches)
        all_to_fix.update(prefix_mismatches)
        self.stdout.write(f'Total records to fix: {len(all_to_fix)}')

        # Check for duplicates
        existing_refs = set(Description.objects.filter(
            repository__code='co-ahr'
        ).values_list('reference_code', flat=True))

        skipped = []
        to_apply = []

        for desc_id, (old_ref, new_ref, new_local, title) in all_to_fix.items():
            if new_ref in existing_refs and new_ref != old_ref:
                skipped.append((old_ref, new_ref, title))
            else:
                to_apply.append((desc_id, old_ref, new_ref, new_local, title))
                # Update existing_refs for chain detection
                if old_ref in existing_refs:
                    existing_refs.remove(old_ref)
                existing_refs.add(new_ref)

        self.stdout.write(f'Will fix: {len(to_apply)}, Skip (duplicate): {len(skipped)}')

        # Apply fixes
        fixed = 0
        with transaction.atomic():
            for desc_id, old_ref, new_ref, new_local, title in to_apply:
                if fixed < 30:
                    self.stdout.write(f'  {old_ref} -> {new_ref}')

                if not dry_run:
                    Description.objects.filter(id=desc_id).update(
                        reference_code=new_ref,
                        local_identifier=new_local
                    )
                fixed += 1

            if dry_run:
                transaction.set_rollback(True)

        if fixed > 30:
            self.stdout.write(f'  ... and {fixed - 30} more')

        self.stdout.write('')
        self.stdout.write(self.style.SUCCESS(f'Fixed: {fixed} reference codes'))

        if skipped:
            self.stdout.write(self.style.WARNING(f'Skipped {len(skipped)} duplicates:'))
            for old, new, title in skipped[:5]:
                self.stdout.write(f'  {old} -> {new} ({title})')

        if not dry_run and fixed > 0:
            self.stdout.write('')
            self.stdout.write('Updating path_cache...')
            self.update_path_cache()
            self.stdout.write(self.style.SUCCESS('Done'))

    def find_type_mismatches(self):
        """Find records where title type != refcode type."""
        mismatches = {}

        # Query only containers (Caja/Carpeta/Tomo titles)
        for desc in Description.objects.filter(
            repository__code='co-ahr'
        ).filter(
            title__iregex=r'^(Caja|Carpeta|Tomo)\s+\d+$'
        ).only('id', 'reference_code', 'local_identifier', 'title'):

            title_type, title_num = self.get_type_from_title(desc.title)
            ref_type, ref_num = self.get_type_from_refcode(desc.reference_code)

            if title_type and ref_type and title_type != ref_type:
                # Build new codes: keep refcode number, use title type
                old_suffix = f"{ref_type}{ref_num}"
                new_suffix = f"{title_type}{ref_num}"

                new_ref = desc.reference_code.replace(f"-{old_suffix}", f"-{new_suffix}", 1)
                new_local = desc.local_identifier.replace(f"-{old_suffix}", f"-{new_suffix}", 1)

                if new_ref == desc.reference_code:
                    # Try end replacement
                    if desc.reference_code.endswith(old_suffix):
                        new_ref = desc.reference_code[:-len(old_suffix)] + new_suffix
                        new_local = desc.local_identifier[:-len(old_suffix)] + new_suffix

                if new_ref != desc.reference_code:
                    mismatches[desc.id] = (desc.reference_code, new_ref, new_local, desc.title)

        return mismatches

    def find_prefix_mismatches(self):
        """Find children whose refcode doesn't match parent's local_identifier."""
        mismatches = {}

        # Get all AHR records with parents
        records = Description.objects.filter(
            repository__code='co-ahr',
            parent__isnull=False
        ).select_related('parent').only(
            'id', 'reference_code', 'local_identifier', 'title',
            'parent__local_identifier'
        )

        for desc in records:
            parent_local = desc.parent.local_identifier
            expected_prefix = f"co-ahr-{parent_local}-"

            if not desc.reference_code.startswith(expected_prefix):
                # Extract our suffix
                parts = desc.reference_code.split('-')
                our_suffix = parts[-1]

                new_local = f"{parent_local}-{our_suffix}"
                new_ref = f"co-ahr-{new_local}"

                if new_ref != desc.reference_code:
                    mismatches[desc.id] = (desc.reference_code, new_ref, new_local, desc.title)

        return mismatches

    def get_type_from_title(self, title):
        """Get container type and number from title."""
        if not title:
            return None, None
        title_lower = title.lower().strip()

        match = re.match(r'^caja\s+0*(\d+)$', title_lower)
        if match:
            return 'caj', match.group(1).zfill(3)

        match = re.match(r'^carpeta\s+0*(\d+)$', title_lower)
        if match:
            return 'car', match.group(1).zfill(3)

        match = re.match(r'^tomo\s+0*(\d+)$', title_lower)
        if match:
            return 't', match.group(1).zfill(3)

        return None, None

    def get_type_from_refcode(self, refcode):
        """Get container type and number from refcode."""
        if not refcode:
            return None, None

        parts = refcode.split('-')
        for part in reversed(parts):
            if part.startswith('d') and part[1:].isdigit():
                continue
            if part.startswith('caj'):
                match = re.match(r'^caj(\d+)$', part)
                if match:
                    return 'caj', match.group(1).zfill(3)
            if part.startswith('car'):
                match = re.match(r'^car(\d+)$', part)
                if match:
                    return 'car', match.group(1).zfill(3)
            if part.startswith('t') and len(part) > 1:
                match = re.match(r'^t(\d+)$', part)
                if match:
                    return 't', match.group(1).zfill(3)

        return None, None

    def update_path_cache(self):
        """Update path_cache for AHR descriptions."""
        parent_map = dict(Description.objects.filter(
            repository__code='co-ahr'
        ).values_list('id', 'parent_id'))

        def build_path(desc_id):
            path_ids = [desc_id]
            current = desc_id
            while parent_map.get(current):
                current = parent_map[current]
                path_ids.insert(0, current)
            return '/' + '/'.join(str(x) for x in path_ids) + '/'

        batch = []
        for desc in Description.objects.filter(repository__code='co-ahr').only('id', 'path_cache').iterator():
            desc.path_cache = build_path(desc.id)
            batch.append(desc)
            if len(batch) >= 1000:
                Description.objects.bulk_update(batch, ['path_cache'])
                batch = []
        if batch:
            Description.objects.bulk_update(batch, ['path_cache'])
