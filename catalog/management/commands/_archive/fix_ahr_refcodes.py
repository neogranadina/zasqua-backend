"""
Fix AHR reference codes to use proper container type prefixes.

The CA import used 'T' (tomo) for all container types, but the actual structure uses:
- Caja -> cajNNN
- Carpeta -> carNNN
- Tomo -> tNNN

This command fixes reference_code and local_identifier based on title patterns.

Usage:
    python manage.py fix_ahr_refcodes --dry-run
    python manage.py fix_ahr_refcodes
"""

import re
from django.core.management.base import BaseCommand
from django.db import transaction
from catalog.models import Description


class Command(BaseCommand):
    help = 'Fix AHR reference codes to use proper container type prefixes'

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

        # Get all AHR descriptions
        ahr_descs = list(Description.objects.filter(
            repository__code='co-ahr'
        ).select_related('parent', 'repository').order_by('level', 'reference_code'))

        self.stdout.write(f'Found {len(ahr_descs)} AHR descriptions')

        # Build a mapping of description ID -> correct suffix based on title
        # This tells us what each container's suffix SHOULD be
        id_to_suffix = {}
        for desc in ahr_descs:
            suffix = self.get_correct_suffix(desc.title)
            if suffix:
                id_to_suffix[desc.id] = suffix

        self.stdout.write(f'Found {len(id_to_suffix)} containers with correctable suffixes')

        # Build ID -> description map for parent lookups
        id_to_desc = {d.id: d for d in ahr_descs}

        # Fix reference codes
        fixed = 0
        with transaction.atomic():
            for desc in ahr_descs:
                new_local, new_ref = self.build_new_codes(desc, id_to_suffix, id_to_desc)

                if new_ref != desc.reference_code:
                    if fixed < 50:  # Only print first 50
                        self.stdout.write(f'  {desc.reference_code} -> {new_ref}')

                    if not dry_run:
                        desc.reference_code = new_ref
                        desc.local_identifier = new_local
                        desc.save(update_fields=['reference_code', 'local_identifier'])

                    fixed += 1

            if fixed > 50:
                self.stdout.write(f'  ... and {fixed - 50} more')

            if dry_run:
                transaction.set_rollback(True)

        self.stdout.write('')
        self.stdout.write(self.style.SUCCESS(f'Fixed: {fixed} reference codes'))

        # Rebuild path_cache and search index reminder
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

    def get_correct_suffix(self, title):
        """
        Determine correct suffix based on title.

        Returns new suffix (like 'caj001') or None if not a container.
        """
        if not title:
            return None

        title_lower = title.lower().strip()

        # Caja NNN -> cajNNN
        match = re.match(r'^caja\s+0*(\d+)$', title_lower)
        if match:
            num = match.group(1).zfill(3)
            return f'caj{num}'

        # Carpeta NNN -> carNNN
        match = re.match(r'^carpeta\s+0*(\d+)$', title_lower)
        if match:
            num = match.group(1).zfill(3)
            return f'car{num}'

        # Tomo NNN -> tNNN
        match = re.match(r'^tomo\s+0*(\d+)$', title_lower)
        if match:
            num = match.group(1).zfill(3)
            return f't{num}'

        return None

    def build_new_codes(self, desc, id_to_suffix, id_to_desc):
        """
        Build new local_identifier and reference_code for a description.

        Walks up the hierarchy to get correct suffixes for each ancestor.
        """
        # Build path from root to this description
        path = []
        current = desc
        while current:
            path.insert(0, current)
            current = id_to_desc.get(current.parent_id) if current.parent_id else None

        # Build new local_identifier from path
        # Format: {fondo}-{container1}-{container2}-...-{item_id}
        new_parts = []

        for node in path:
            # Get the last segment of current local_identifier
            old_parts = node.local_identifier.split('-')
            old_suffix = old_parts[-1]

            # Check if this node has a corrected suffix
            if node.id in id_to_suffix:
                new_parts.append(id_to_suffix[node.id])
            else:
                new_parts.append(old_suffix)

        new_local = '-'.join(new_parts)
        new_ref = f'co-ahr-{new_local}'

        return new_local, new_ref

    def update_path_cache(self):
        """Update path_cache for AHR descriptions."""
        # Build parent mapping in memory
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

        # Update in batches
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
