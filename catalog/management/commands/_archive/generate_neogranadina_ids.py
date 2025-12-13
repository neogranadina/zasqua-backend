"""
Generate Neogranadina IDs for entities missing them.

Usage:
    ./manage.py generate_neogranadina_ids
    ./manage.py generate_neogranadina_ids --dry-run
"""

from django.core.management.base import BaseCommand
from catalog.models import Entity, generate_neogranadina_code


class Command(BaseCommand):
    help = 'Generate ne-xxxxx codes for entities missing entity_code'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be done without making changes',
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=1000,
            help='Number of entities to update per batch (default: 1000)',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        batch_size = options['batch_size']

        # Find entities without codes
        entities_without_code = Entity.objects.filter(entity_code__isnull=True)
        total = entities_without_code.count()

        if total == 0:
            self.stdout.write(self.style.SUCCESS('All entities already have codes.'))
            return

        self.stdout.write(f'Found {total} entities without codes.')

        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN - no changes will be made.'))
            # Show sample codes that would be generated
            for entity in entities_without_code[:5]:
                code = generate_neogranadina_code(prefix='ne', length=5)
                self.stdout.write(f'  Would assign: {code} -> {entity.display_name[:50]}')
            return

        # Generate codes in batches
        generated = 0
        existing_codes = set(
            Entity.objects.exclude(entity_code__isnull=True)
            .values_list('entity_code', flat=True)
        )

        entities_to_update = []
        for entity in entities_without_code.iterator():
            # Generate unique code
            for _ in range(10):
                code = generate_neogranadina_code(prefix='ne', length=5)
                if code not in existing_codes:
                    entity.entity_code = code
                    existing_codes.add(code)
                    entities_to_update.append(entity)
                    generated += 1
                    break
            else:
                self.stderr.write(f'Could not generate unique code for {entity.id}')
                continue

            # Batch update
            if len(entities_to_update) >= batch_size:
                Entity.objects.bulk_update(entities_to_update, ['entity_code'])
                self.stdout.write(f'  Updated {generated}/{total} entities...')
                entities_to_update = []

        # Final batch
        if entities_to_update:
            Entity.objects.bulk_update(entities_to_update, ['entity_code'])

        self.stdout.write(self.style.SUCCESS(
            f'Generated {generated} Neogranadina IDs for entities.'
        ))

        # Verify
        remaining = Entity.objects.filter(entity_code__isnull=True).count()
        if remaining > 0:
            self.stdout.write(self.style.WARNING(
                f'{remaining} entities still without codes.'
            ))
        else:
            self.stdout.write(self.style.SUCCESS('All entities now have codes.'))
