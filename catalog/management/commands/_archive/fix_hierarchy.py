"""
Fix description hierarchy based on reference_code patterns.

The CA import didn't properly establish parent-child relationships.
This command parses reference_codes to infer and fix the hierarchy.

Examples:
- pe-bn-cdip-01 should be child of pe-bn-cdip
- co-ahr-con-t001-t004 should be child of co-ahr-con-t001 (or co-ahr-con if intermediate doesn't exist)
"""

from django.core.management.base import BaseCommand
from django.db import transaction
from catalog.models import Description, Repository


class Command(BaseCommand):
    help = 'Fix description hierarchy based on reference_code patterns'

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
        parser.add_argument(
            '--create-missing',
            action='store_true',
            help='Create missing intermediate containers'
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        repo_code = options.get('repo')
        create_missing = options['create_missing']

        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN - no changes will be made'))

        # Get descriptions to process
        queryset = Description.objects.filter(parent__isnull=True)
        if repo_code:
            queryset = queryset.filter(repository__code=repo_code)

        orphans = list(queryset.select_related('repository'))
        self.stdout.write(f'Found {len(orphans)} root-level descriptions to check')

        fixed = 0
        created = 0
        errors = []

        with transaction.atomic():
            for desc in orphans:
                result = self.fix_parent(desc, dry_run, create_missing)
                if result == 'fixed':
                    fixed += 1
                elif result == 'created':
                    created += 1
                    fixed += 1
                elif result and result.startswith('error'):
                    errors.append((desc.reference_code, result))

            if dry_run:
                # Rollback in dry run
                transaction.set_rollback(True)

        self.stdout.write('')
        self.stdout.write(self.style.SUCCESS(f'Fixed: {fixed} descriptions'))
        if create_missing:
            self.stdout.write(self.style.SUCCESS(f'Created: {created} intermediate containers'))
        if errors:
            self.stdout.write(self.style.ERROR(f'Errors: {len(errors)}'))
            for ref, err in errors[:10]:
                self.stdout.write(f'  {ref}: {err}')

    def fix_parent(self, desc, dry_run, create_missing):
        """
        Try to find and set the correct parent for a description.
        Returns: 'fixed', 'created', 'skipped', or 'error:...'
        """
        ref = desc.reference_code
        repo = desc.repository

        # Parse reference_code to find expected parent
        expected_parent_code = self.get_expected_parent_code(ref)

        if not expected_parent_code:
            # This is a true root (e.g., pe-bn-cdip with no further parent)
            return 'skipped'

        # Try to find the parent
        parent = Description.objects.filter(
            reference_code=expected_parent_code,
            repository=repo
        ).first()

        if parent:
            if not dry_run:
                desc.parent = parent
                desc.save()
            self.stdout.write(f'  {ref} -> {expected_parent_code}')
            return 'fixed'
        else:
            # Parent doesn't exist
            if create_missing:
                # Try to create intermediate container
                parent = self.create_intermediate(expected_parent_code, repo, desc, dry_run)
                if parent:
                    if not dry_run:
                        desc.parent = parent
                        desc.save()
                    self.stdout.write(f'  {ref} -> {expected_parent_code} (created)')
                    return 'created'

            # Try grandparent
            grandparent_code = self.get_expected_parent_code(expected_parent_code)
            if grandparent_code:
                grandparent = Description.objects.filter(
                    reference_code=grandparent_code,
                    repository=repo
                ).first()
                if grandparent:
                    if not dry_run:
                        desc.parent = grandparent
                        desc.save()
                    self.stdout.write(f'  {ref} -> {grandparent_code} (skipped missing {expected_parent_code})')
                    return 'fixed'

            self.stdout.write(self.style.WARNING(
                f'  {ref}: parent {expected_parent_code} not found'
            ))
            return 'error: parent not found'

    def get_expected_parent_code(self, ref_code):
        """
        Parse reference_code to determine expected parent.

        Examples:
        - pe-bn-cdip-01 -> pe-bn-cdip
        - co-ahr-con-t001-t004 -> co-ahr-con-t001
        - co-ahr-con -> None (true root within repo)
        """
        parts = ref_code.split('-')

        # Minimum: repo-code-collection (3 parts)
        # e.g., pe-bn-cdip, co-ahr-con
        if len(parts) <= 3:
            return None

        # Remove last part to get parent
        parent_code = '-'.join(parts[:-1])
        return parent_code

    def create_intermediate(self, ref_code, repo, child_desc, dry_run):
        """
        Create a missing intermediate container.
        """
        # Infer properties from reference_code and child
        parts = ref_code.split('-')
        last_part = parts[-1]

        # Try to guess title from reference code pattern
        if last_part.startswith('t') and last_part[1:].isdigit():
            title = f'Tomo {int(last_part[1:])}'
            level = 'file'
        elif last_part.isdigit():
            title = f'Volumen {int(last_part)}'
            level = 'file'
        else:
            title = last_part.title()
            level = 'series'

        # Find parent for this intermediate
        parent_code = self.get_expected_parent_code(ref_code)
        parent = None
        if parent_code:
            parent = Description.objects.filter(
                reference_code=parent_code,
                repository=repo
            ).first()

        self.stdout.write(self.style.NOTICE(
            f'    Creating: {ref_code} ({level}: {title})'
        ))

        if dry_run:
            return True  # Indicate we would create it

        intermediate = Description.objects.create(
            repository=repo,
            reference_code=ref_code,
            local_identifier=parts[-1],
            title=title,
            description_level=level,
            parent=parent,
            is_published=True
        )

        return intermediate
