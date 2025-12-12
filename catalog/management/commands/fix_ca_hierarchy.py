"""
Fix description hierarchy using CA's parent_id relationships.

The original import_ca.py had a bug: it built the ca_to_zasqua mapping
during import, but on re-runs the mapping was empty so parents weren't found.

This command:
1. Queries CA database for collection parent_id relationships
2. Matches them to existing Zasqua descriptions via ca_collection_id
3. Sets the correct parent for each description

Usage:
    python manage.py fix_ca_hierarchy --dry-run
    python manage.py fix_ca_hierarchy
"""

import mysql.connector
from django.core.management.base import BaseCommand
from django.db import transaction
from catalog.models import Description, Repository


# CA MySQL connection settings (same as import_ca.py)
CA_DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'abcneogranadina',
    'charset': 'utf8mb4',
}

# Repository CA collection IDs
REPOSITORY_IDS = {712, 360, 14805, 14940, 16479}


class Command(BaseCommand):
    help = 'Fix description hierarchy using CA parent_id relationships'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be changed without making changes'
        )
        parser.add_argument(
            '--repo',
            type=str,
            help='Only process specific repository (by code)'
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        repo_code = options.get('repo')

        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN - no changes will be made'))

        # Step 1: Get CA parent_id relationships
        self.stdout.write('Fetching parent relationships from CA...')
        ca_parents = self.get_ca_parent_map()
        self.stdout.write(f'  Found {len(ca_parents)} collection parent relationships')

        # Step 2: Build mapping of ca_collection_id -> Description
        self.stdout.write('Building CA ID to Description mapping...')
        queryset = Description.objects.filter(ca_collection_id__isnull=False)
        if repo_code:
            queryset = queryset.filter(repository__code=repo_code)

        ca_to_desc = {}
        for desc in queryset.select_related('repository'):
            ca_to_desc[desc.ca_collection_id] = desc

        self.stdout.write(f'  Found {len(ca_to_desc)} descriptions with ca_collection_id')

        # Step 3: Fix parent relationships
        fixed = 0
        skipped = 0
        already_correct = 0
        orphaned = 0

        with transaction.atomic():
            for ca_id, desc in ca_to_desc.items():
                ca_parent_id = ca_parents.get(ca_id)

                if not ca_parent_id:
                    # No parent in CA (shouldn't happen for non-repo collections)
                    skipped += 1
                    continue

                if ca_parent_id in REPOSITORY_IDS:
                    # Parent is a repository - this should be a root description
                    if desc.parent is None:
                        already_correct += 1
                    else:
                        if not dry_run:
                            desc.parent = None
                            desc.save(update_fields=['parent'])
                        self.stdout.write(f'  {desc.reference_code} -> ROOT (was under {desc.parent})')
                        fixed += 1
                    continue

                # Find parent Description
                parent_desc = ca_to_desc.get(ca_parent_id)

                if not parent_desc:
                    # Parent not found - might be missing from import
                    orphaned += 1
                    if orphaned <= 10:
                        self.stdout.write(self.style.WARNING(
                            f'  {desc.reference_code}: parent CA ID {ca_parent_id} not found'
                        ))
                    continue

                # Check if already correct
                if desc.parent_id == parent_desc.id:
                    already_correct += 1
                    continue

                # Fix the parent
                if not dry_run:
                    desc.parent = parent_desc
                    desc.save(update_fields=['parent'])

                self.stdout.write(f'  {desc.reference_code} -> {parent_desc.reference_code}')
                fixed += 1

            if dry_run:
                transaction.set_rollback(True)

        # Summary
        self.stdout.write('')
        self.stdout.write(self.style.SUCCESS(f'Fixed: {fixed}'))
        self.stdout.write(f'Already correct: {already_correct}')
        self.stdout.write(f'Skipped (no CA parent): {skipped}')
        if orphaned:
            self.stdout.write(self.style.WARNING(f'Orphaned (parent not found): {orphaned}'))

        # Step 4: Rebuild MPTT tree and path_cache
        if not dry_run and fixed > 0:
            self.stdout.write('')
            self.stdout.write('Rebuilding MPTT tree...')
            Description.objects.rebuild()
            self.stdout.write(self.style.SUCCESS('MPTT tree rebuilt'))

            self.stdout.write('Updating path_cache...')
            self.update_path_cache()
            self.stdout.write(self.style.SUCCESS('path_cache updated'))

    def get_ca_parent_map(self):
        """Get parent_id for all collections from CA database."""
        conn = mysql.connector.connect(**CA_DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT collection_id, parent_id
            FROM ca_collections
            WHERE deleted = 0 AND parent_id IS NOT NULL
        """)

        parent_map = {}
        for row in cursor.fetchall():
            parent_map[row[0]] = row[1]

        cursor.close()
        conn.close()

        return parent_map

    def update_path_cache(self):
        """Update path_cache for all descriptions."""
        # Build parent mapping in memory
        parent_map = dict(Description.objects.values_list('id', 'parent_id'))

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

        for desc in Description.objects.only('id', 'path_cache').iterator():
            desc.path_cache = build_path(desc.id)
            batch.append(desc)

            if len(batch) >= batch_size:
                Description.objects.bulk_update(batch, ['path_cache'])
                batch = []

        if batch:
            Description.objects.bulk_update(batch, ['path_cache'])
