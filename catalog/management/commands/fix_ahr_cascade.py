"""
Fix AHR reference codes by cascading from parent to children.

After running fix_ahr_from_csv, some children still have old parent prefixes
in their reference_code. This command:
1. Walks the tree top-down
2. For each record, builds reference_code from parent's code + correct suffix
3. Suffix is determined by title (Caja -> caj, Carpeta -> car, Tomo -> t)

Usage:
    python manage.py fix_ahr_cascade --dry-run
    python manage.py fix_ahr_cascade
"""

import re
from django.core.management.base import BaseCommand
from django.db import transaction
from catalog.models import Description


class Command(BaseCommand):
    help = 'Fix AHR reference codes by cascading from parent to children'

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

        # Get all AHR descriptions ordered by level (parents first)
        ahr_descs = list(Description.objects.filter(
            repository__code='co-ahr'
        ).select_related('parent', 'repository').order_by('level', 'reference_code'))

        self.stdout.write(f'Found {len(ahr_descs)} AHR descriptions')

        # Build ID -> description map
        id_to_desc = {d.id: d for d in ahr_descs}

        # Process in level order (parents before children)
        fixed = 0

        with transaction.atomic():
            for desc in ahr_descs:
                new_local, new_ref = self.build_correct_codes(desc, id_to_desc)

                if new_ref and new_ref != desc.reference_code:
                    if fixed < 50:
                        self.stdout.write(f'  {desc.reference_code} -> {new_ref}')

                    if not dry_run:
                        desc.reference_code = new_ref
                        desc.local_identifier = new_local
                        desc.save(update_fields=['reference_code', 'local_identifier'])
                        # Update in-memory for children
                        id_to_desc[desc.id] = desc

                    fixed += 1

            if dry_run:
                transaction.set_rollback(True)

        if fixed > 50:
            self.stdout.write(f'  ... and {fixed - 50} more')

        self.stdout.write('')
        self.stdout.write(self.style.SUCCESS(f'Fixed: {fixed} reference codes'))

        if not dry_run and fixed > 0:
            self.stdout.write('')
            self.stdout.write('Updating path_cache...')
            self.update_path_cache()
            self.stdout.write(self.style.SUCCESS('path_cache updated'))

            self.stdout.write('')
            self.stdout.write(self.style.NOTICE(
                'Remember to rebuild the search index: '
                'python manage.py rebuild_search_index --clear'
            ))

    def build_correct_codes(self, desc, id_to_desc):
        """
        Build correct local_identifier and reference_code from parent + title.

        Rules:
        - If no parent (fondo level): keep as-is
        - If parent exists: parent_local + '-' + this_suffix
        - Suffix comes from title: Caja -> cajNNN, Carpeta -> carNNN, Tomo -> tNNN
        - If title doesn't match pattern, use current suffix
        """
        if not desc.parent_id:
            # Fondo level - keep as-is
            return desc.local_identifier, desc.reference_code

        parent = id_to_desc.get(desc.parent_id)
        if not parent:
            return desc.local_identifier, desc.reference_code

        # Determine suffix from title
        suffix = self.get_suffix_from_title(desc.title)

        if not suffix:
            # Not a container with recognizable pattern
            # Keep the item ID part (dXXXX)
            old_parts = desc.local_identifier.split('-')
            suffix = old_parts[-1] if old_parts else None

        if not suffix:
            return desc.local_identifier, desc.reference_code

        # Build new codes from parent
        new_local = f'{parent.local_identifier}-{suffix}'
        new_ref = f'co-ahr-{new_local}'

        return new_local, new_ref

    def get_suffix_from_title(self, title):
        """Get correct suffix based on title."""
        if not title:
            return None

        title_lower = title.lower().strip()

        # Caja NNN -> cajNNN
        match = re.match(r'^caja\s+0*(\d+)$', title_lower)
        if match:
            return f'caj{match.group(1).zfill(3)}'

        # Carpeta NNN -> carNNN
        match = re.match(r'^carpeta\s+0*(\d+)$', title_lower)
        if match:
            return f'car{match.group(1).zfill(3)}'

        # Tomo NNN -> tNNN
        match = re.match(r'^tomo\s+0*(\d+)$', title_lower)
        if match:
            return f't{match.group(1).zfill(3)}'

        return None

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
        batch_size = 1000

        for desc in Description.objects.filter(repository__code='co-ahr').only('id', 'path_cache').iterator():
            desc.path_cache = build_path(desc.id)
            batch.append(desc)

            if len(batch) >= batch_size:
                Description.objects.bulk_update(batch, ['path_cache'])
                batch = []

        if batch:
            Description.objects.bulk_update(batch, ['path_cache'])
