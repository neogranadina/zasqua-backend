"""
Export all data needed by the frontend build as static JSON files.

Replaces the API-based data fetching during the 11ty build with a
pre-computed JSON export.  This eliminates the need for a running
Django server during the frontend build.

Outputs:
  descriptions.json  — all descriptions with metadata + OCR text
  repositories.json  — all repositories with root descriptions
  children/{id}.json — tree children per parent (for lazy-loaded trees)

Usage:
    python manage.py export_frontend_data
    python manage.py export_frontend_data --output-dir /tmp/zasqua-export
"""

import json
import os
import re
import sys
import time

from django.core.management.base import BaseCommand

from catalog.models import Description, Repository


# ---------------------------------------------------------------------------
# children_level inference — ported from DescriptionListSerializer
# ---------------------------------------------------------------------------

_LEVEL_HIERARCHY = {
    'fonds': 'caja',
    'collection': 'file',
    'subfonds': 'series',
    'series': 'subseries',
    'subseries': 'file',
    'file': 'item',
}

# Map CA list-item IDs and legacy strings to display names.
# All materials in the archive are in Spanish; the DB stores raw CA IDs
# (e.g. '192') which we translate here to preserve DB provenance.
_LANGUAGE_MAP = {
    '192': 'Español',
    '173': 'Español',
    '195': 'Español',
    'Spanish': 'Español',
}


def _children_level(ref, level, child_refs):
    """Infer the archival level of a description's children."""
    ref = ref or ''

    if re.search(r'-caj\d+$', ref):
        return 'carpeta'
    if re.search(r'-car\d+$', ref):
        return 'item'
    if re.search(r'-leg\d+$', ref):
        return 'item'
    if re.search(r'-tom\d+$', ref):
        return 'item'
    if re.search(r'-t\d+$', ref):
        return 'item'
    if re.search(r'-aht-\d+$', ref):
        return 'item'
    if re.search(r'-cab-\d+$', ref):
        return 'item'

    if level == 'fonds' and child_refs:
        sample = child_refs[:20]
        has_caja = any('-caj' in r for r in sample)
        has_tomo = any('-tom' in r or '-t0' in r for r in sample)
        has_carpeta = any('-car' in r for r in sample)
        has_legajo = any(
            re.search(r'-aht-\d+$', r) or re.search(r'-leg\d+$', r)
            or re.search(r'-cab-\d+$', r)
            for r in sample
        )
        types = sum([has_caja, has_tomo, has_carpeta, has_legajo])
        if types > 1:
            return None
        if has_caja:
            return 'caja'
        if has_tomo:
            return 'tomo'
        if has_carpeta:
            return 'carpeta'
        if has_legajo:
            return 'legajo'

    return _LEVEL_HIERARCHY.get(level)


class Command(BaseCommand):
    help = 'Export frontend build data as static JSON files'

    def add_arguments(self, parser):
        parser.add_argument(
            '--output-dir',
            default='export',
            help='Output directory (default: ./export)',
        )

    def log(self, message, style=None, newline=True):
        if style:
            message = style(message)
        if newline:
            self.stdout.write(message)
        else:
            self.stdout.write(message, ending='')
        sys.stdout.flush()

    def log_phase(self, phase_name):
        self.log(f'\n{"="*60}')
        self.log(f'  {phase_name}')
        self.log(f'{"="*60}')
        self.phase_start = time.time()

    def log_elapsed(self):
        elapsed = time.time() - self.phase_start
        self.log(f'  Elapsed: {elapsed:.1f}s')

    def handle(self, *args, **options):
        output_dir = options['output_dir']
        start_time = time.time()

        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(os.path.join(output_dir, 'children'), exist_ok=True)

        # ------------------------------------------------------------------
        # Phase 1: Load all descriptions
        # ------------------------------------------------------------------
        self.log_phase('Phase 1: Loading descriptions')

        rows = list(
            Description.objects
            .select_related('repository', 'parent')
            .order_by('id')
            .values(
                'id', 'repository__code', 'reference_code', 'local_identifier',
                'title', 'description_level', 'date_expression', 'date_start',
                'parent_id', 'parent__reference_code',
                'scope_content', 'ocr_text', 'extent', 'arrangement',
                'access_conditions', 'language', 'notes',
                'creator_display', 'place_display',
                'has_digital', 'iiif_manifest_url', 'lft', 'rght',
            )
        )

        self.log(f'  Loaded {len(rows):,} descriptions')
        self.log_elapsed()

        # ------------------------------------------------------------------
        # Phase 2: Compute derived fields
        # ------------------------------------------------------------------
        self.log_phase('Phase 2: Computing derived fields')

        # Group children by parent_id, sorted by reference_code
        children_by_parent = {}
        for d in rows:
            pid = d['parent_id']
            if pid:
                children_by_parent.setdefault(pid, []).append(d)

        for children in children_by_parent.values():
            children.sort(key=lambda x: x['reference_code'] or '')

        self.log(f'  Parents with children: {len(children_by_parent):,}')

        # Compute has_children, child_count, children_level
        for d in rows:
            d['has_children'] = (d['rght'] - d['lft']) > 1
            kids = children_by_parent.get(d['id'], [])
            d['child_count'] = len(kids)
            if d['has_children']:
                child_refs = [c['reference_code'] or '' for c in kids]
                d['children_level'] = _children_level(
                    d['reference_code'], d['description_level'], child_refs
                )
            else:
                d['children_level'] = None

        self.log_elapsed()

        # ------------------------------------------------------------------
        # Phase 3: Write descriptions.json
        # ------------------------------------------------------------------
        self.log_phase('Phase 3: Writing descriptions.json')

        desc_records = []
        for d in rows:
            desc_records.append({
                'id': d['id'],
                'repository_code': d['repository__code'],
                'reference_code': d['reference_code'],
                'local_identifier': d['local_identifier'],
                'title': d['title'],
                'description_level': d['description_level'],
                'date_expression': d['date_expression'],
                'date_start': (
                    d['date_start'].isoformat() if d['date_start'] else None
                ),
                'parent_id': d['parent_id'],
                'parent_reference_code': d['parent__reference_code'],
                'has_children': d['has_children'],
                'child_count': d['child_count'],
                'children_level': d['children_level'],
                'has_digital': d['has_digital'],
                'iiif_manifest_url': d['iiif_manifest_url'] or '',
                'scope_content': d['scope_content'],
                'ocr_text': d['ocr_text'],
                'extent': d['extent'],
                'arrangement': d['arrangement'],
                'access_conditions': d['access_conditions'],
                'language': _LANGUAGE_MAP.get(d['language'], d['language']),
                'notes': d['notes'],
                'creator_display': d['creator_display'],
                'place_display': d['place_display'],
            })

        desc_path = os.path.join(output_dir, 'descriptions.json')
        with open(desc_path, 'w') as f:
            json.dump(desc_records, f, ensure_ascii=False)

        size_mb = os.path.getsize(desc_path) / 1024 / 1024
        self.log(f'  Wrote {len(desc_records):,} descriptions ({size_mb:.1f} MB)')
        self.log_elapsed()

        # ------------------------------------------------------------------
        # Phase 4: Write repositories.json
        # ------------------------------------------------------------------
        self.log_phase('Phase 4: Writing repositories.json')

        repos = list(Repository.objects.filter(enabled=True).order_by('name'))

        # Count descriptions per repo
        desc_counts = {}
        roots_by_repo = {}
        for d in desc_records:
            code = d['repository_code']
            desc_counts[code] = desc_counts.get(code, 0) + 1
            if d['parent_id'] is None:
                roots_by_repo.setdefault(code, []).append(d)

        repo_records = []
        for repo in repos:
            # root_descriptions without ocr_text (not needed at repo level)
            roots = []
            for r in roots_by_repo.get(repo.code, []):
                roots.append({k: v for k, v in r.items() if k != 'ocr_text'})

            repo_records.append({
                'id': repo.id,
                'code': repo.code,
                'name': repo.name,
                'short_name': repo.short_name,
                'country_code': repo.country_code,
                'city': repo.city,
                'address': repo.address,
                'website': repo.website,
                'notes': repo.notes,
                'enabled': repo.enabled,
                'created_at': repo.created_at.isoformat(),
                'updated_at': repo.updated_at.isoformat(),
                'description_count': desc_counts.get(repo.code, 0),
                'root_descriptions': roots,
            })

        repo_path = os.path.join(output_dir, 'repositories.json')
        with open(repo_path, 'w') as f:
            json.dump(repo_records, f, ensure_ascii=False)

        size_kb = os.path.getsize(repo_path) / 1024
        self.log(f'  Wrote {len(repo_records)} repositories ({size_kb:.1f} KB)')
        self.log_elapsed()

        # ------------------------------------------------------------------
        # Phase 5: Write children JSON files
        # ------------------------------------------------------------------
        self.log_phase('Phase 5: Writing children JSON files')

        children_dir = os.path.join(output_dir, 'children')
        total_size = 0
        files_written = 0

        for parent_id, kids in children_by_parent.items():
            results = []
            for child in kids:
                sc = child['scope_content'] or ''
                if len(sc) > 150:
                    sc = sc[:150] + '...'
                results.append({
                    'id': child['id'],
                    'reference_code': child['reference_code'],
                    'title': child['title'],
                    'description_level': child['description_level'],
                    'date_expression': child['date_expression'] or '',
                    'scope_content': sc,
                    'child_count': child['child_count'],
                    'children_level': child['children_level'],
                    'has_digital': child['has_digital'] or False,
                })

            output = {'count': len(results), 'results': results}
            data = json.dumps(output, ensure_ascii=False)
            file_path = os.path.join(children_dir, f'{parent_id}.json')
            with open(file_path, 'w') as f:
                f.write(data)

            total_size += len(data)
            files_written += 1

        size_mb = total_size / 1024 / 1024
        self.log(f'  Wrote {files_written:,} children files ({size_mb:.1f} MB)')
        self.log_elapsed()

        # ------------------------------------------------------------------
        # Summary
        # ------------------------------------------------------------------
        self.log_phase('Summary')

        elapsed = time.time() - start_time
        self.log(f'  Descriptions: {len(desc_records):,}')
        self.log(f'  Repositories: {len(repo_records)}')
        self.log(f'  Children files: {files_written:,}')
        self.log(f'  Output directory: {os.path.abspath(output_dir)}')
        self.log(f'  Total time: {elapsed:.1f}s')
