"""
Generate IIIF Presentation API v3 manifests for all digitised descriptions.

Reads the ca-image-manifest.csv for image data (filenames, dimensions),
queries the database for metadata, builds manifests with iiif_prezi3,
writes JSON files, and optionally updates iiif_manifest_url in the DB.

Manifests are uploaded to R2 separately via rclone.

Usage:
    python manage.py generate_iiif_manifests \\
      --csv /path/to/ca-image-manifest.csv \\
      --output-dir /tmp/manifests \\
      --originals-dir /mnt/zasqua-originals/files

    python manage.py generate_iiif_manifests \\
      --csv ca-image-manifest.csv \\
      --output-dir manifests/ \\
      --repository co-cihjml \\
      --limit 10 \\
      --dry-run
"""

import csv
import json
import os
import re
import subprocess
import sys
import time

from django.core.management.base import BaseCommand

from catalog.models import Description

from iiif_prezi3 import KeyValueString, Manifest


# ---------------------------------------------------------------------------
# Language mapping (from export_frontend_data)
# ---------------------------------------------------------------------------

_LANGUAGE_MAP = {
    '192': 'Español',
    '173': 'Español',
    '195': 'Español',
    'Spanish': 'Español',
}


# ---------------------------------------------------------------------------
# Image name extraction (shared with generate_tiles_production.py)
# ---------------------------------------------------------------------------

IMAGE_NAME_PATTERNS = [
    # ACC: ACC_00001-Civil_I_H-img_0004.jpg -> img_0004
    re.compile(r'ACC_.*-(img_\d+)'),
    # AHRB: AHRB_AHT_003-img_0073.jpg -> img_0073
    re.compile(r'AHRB_.*-(img_\d+)'),
    # AHJCI: EAP1477_MFC_B01_Doc02_MurillovsMoreno_IMG_001.jpg -> IMG_001
    re.compile(r'.*_(IMG_\d+)'),
    # PDF pages: page_001.jpg -> page_001
    re.compile(r'(page_\d+)'),
]


def extract_image_name(filename):
    """Extract a short image identifier from a filename."""
    from pathlib import Path
    stem = Path(filename).stem
    for pattern in IMAGE_NAME_PATTERNS:
        match = pattern.match(stem)
        if match:
            return match.group(1)
    raise ValueError(f"Cannot extract image name from '{filename}'")


def derive_doc_slug(object_idno):
    """Derive a URL-safe doc-slug from a CA object_idno."""
    return object_idno.lower().replace('.', '-').replace('_', '-')


# ---------------------------------------------------------------------------
# CSV loading
# ---------------------------------------------------------------------------

def load_csv(csv_path):
    """Load ca-image-manifest.csv grouped by ca_object_id.

    Returns:
        dict mapping ca_object_id (int) -> {
            'object_idno': str,
            'doc_slug': str,
            'images': list of dicts with name, width, height, rank
        }
    """
    documents = {}
    with open(csv_path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row['ca_object_id']:
                continue
            ca_id = int(row['ca_object_id'])
            if ca_id not in documents:
                documents[ca_id] = {
                    'object_idno': row['object_idno'],
                    'doc_slug': derive_doc_slug(row['object_idno']),
                    'images': [],
                }

            mimetype = row['mimetype']
            if mimetype == 'application/pdf':
                # PDFs: image names are page_001, page_002, etc.
                # CSV dimensions are at 72 DPI (PDF points); tiles are
                # generated at 300 DPI.  Convert to match actual tiles.
                csv_w = int(row['width'])
                csv_h = int(row['height'])
                dpi_scale = 300 / 72
                documents[ca_id]['images'].append({
                    'original_filename': row['original_filename'],
                    'representation_id': row['representation_id'],
                    'mimetype': mimetype,
                    'width': round(csv_w * dpi_scale),
                    'height': round(csv_h * dpi_scale),
                    'rank': int(row['rank']),
                    'is_primary': row.get('is_primary', '0'),
                    'is_pdf': True,
                })
            else:
                try:
                    image_name = extract_image_name(row['original_filename'])
                except ValueError:
                    continue
                documents[ca_id]['images'].append({
                    'name': image_name,
                    'original_filename': row['original_filename'],
                    'mimetype': mimetype,
                    'width': int(row['width']),
                    'height': int(row['height']),
                    'rank': int(row['rank']),
                    'is_pdf': False,
                })

    # Sort images by rank, deduplicate PDFs, resolve PDF page names
    for doc in documents.values():
        doc['images'].sort(key=lambda r: r['rank'])

        # Deduplicate PDFs: keep only the primary representation
        pdf_rows = [r for r in doc['images'] if r.get('is_pdf')]
        if len(pdf_rows) > 1:
            primary = [r for r in pdf_rows
                       if r.get('is_primary') == '1']
            if primary:
                non_primary_fns = {
                    r['original_filename'] for r in pdf_rows
                    if r.get('is_primary') != '1'
                }
                doc['images'] = [
                    r for r in doc['images']
                    if r['original_filename'] not in non_primary_fns
                    or not r.get('is_pdf')
                ]

        # For PDFs, set a placeholder — actual pages resolved from tiles
        for img in doc['images']:
            if img.get('is_pdf'):
                img['name'] = '__pdf__'

    return documents


def resolve_pdf_pages(csv_docs, tiles_dir):
    """Replace PDF placeholder entries with actual page data from tiles.

    Scans the tiles directory for page_NNN subdirectories and reads
    info.json to get actual dimensions.  PDFs are multi-page, but the
    CSV has only one row per PDF — the tiles directory tells us how
    many pages there are.
    """
    for doc in csv_docs.values():
        doc_slug = doc['doc_slug']
        has_pdfs = any(img.get('is_pdf') for img in doc['images'])
        if not has_pdfs:
            continue

        # Look for page_NNN directories in the tiles dir
        doc_tiles = os.path.join(tiles_dir, doc_slug)
        if not os.path.isdir(doc_tiles):
            continue

        page_dirs = sorted(
            d for d in os.listdir(doc_tiles)
            if d.startswith('page_') and os.path.isdir(
                os.path.join(doc_tiles, d)
            )
        )

        if not page_dirs:
            continue

        # Build page entries from info.json
        page_entries = []
        for page_name in page_dirs:
            info_path = os.path.join(
                doc_tiles, page_name, 'info.json'
            )
            if os.path.exists(info_path):
                with open(info_path) as f:
                    info = json.load(f)
                page_entries.append({
                    'name': page_name,
                    'width': info.get('width', 1891),
                    'height': info.get('height', 2776),
                    'is_pdf': False,  # Now resolved to images
                })
            else:
                # Fallback to estimated 300 DPI dimensions
                pdf_imgs = [
                    img for img in doc['images']
                    if img.get('is_pdf')
                ]
                if pdf_imgs:
                    page_entries.append({
                        'name': page_name,
                        'width': pdf_imgs[0]['width'],
                        'height': pdf_imgs[0]['height'],
                        'is_pdf': False,
                    })

        # Replace PDF entries with resolved page entries
        if page_entries:
            doc['images'] = [
                img for img in doc['images']
                if not img.get('is_pdf')
            ] + page_entries


def resolve_pdf_pages_from_originals(csv_docs, originals_dir):
    """Replace PDF placeholder entries with page data from original PDFs.

    Uses pdfinfo (poppler-utils) to get page count and per-page dimensions,
    then scales from 72 DPI (PDF points) to 300 DPI to match generated tiles.
    """
    dpi_scale = 300 / 72

    for doc in csv_docs.values():
        pdf_imgs = [img for img in doc['images'] if img.get('is_pdf')]
        if not pdf_imgs:
            continue

        pdf_img = pdf_imgs[0]
        fn = pdf_img.get('original_filename', '')
        rep_id = pdf_img.get('representation_id', '')

        # Try flat layout: {rep_id}_{filename}
        if rep_id:
            pdf_path = os.path.join(originals_dir, f"{rep_id}_{fn}")
        else:
            pdf_path = os.path.join(originals_dir, fn)

        if not os.path.exists(pdf_path):
            continue

        try:
            result = subprocess.run(
                ['pdfinfo', pdf_path],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode != 0:
                continue

            page_count = 0
            for line in result.stdout.splitlines():
                if line.startswith('Pages:'):
                    page_count = int(line.split(':')[1].strip())
                    break

            if page_count == 0:
                continue

        except (subprocess.TimeoutExpired, ValueError):
            continue

        # All pages use the same DPI-scaled dimensions from the CSV
        base_w = pdf_img['width']
        base_h = pdf_img['height']

        page_entries = []
        for p in range(1, page_count + 1):
            page_entries.append({
                'name': f'page_{p:03d}',
                'width': base_w,
                'height': base_h,
                'is_pdf': False,
            })

        doc['images'] = [
            img for img in doc['images']
            if not img.get('is_pdf')
        ] + page_entries


# ---------------------------------------------------------------------------
# Manifest building (ported from generate_manifests.py)
# ---------------------------------------------------------------------------

def build_manifest(description, images, base_url, doc_slug):
    """Build a IIIF Presentation API v3 manifest.

    Args:
        description: Description model instance (with repository).
        images: List of image dicts with 'name', 'width', 'height'.
        base_url: Base URL for IIIF resources.
        doc_slug: URL-safe document identifier.

    Returns:
        iiif_prezi3.Manifest object.
    """
    manifest_id = f"{base_url}/{doc_slug}/manifest.json"

    manifest = Manifest(
        id=manifest_id,
        label={"es": [description.title]},
    )
    manifest.behavior = ["paged"]

    # Homepage — link to Zasqua description page
    manifest.homepage = [{
        "id": f"https://zasqua.org/descriptions/{description.reference_code}/",
        "type": "Text",
        "label": {"es": [description.title]},
        "format": "text/html",
    }]

    # Summary from scope_content
    if description.scope_content:
        manifest.summary = {"es": [description.scope_content]}

    # Metadata fields with bilingual labels
    repo = description.repository
    repo_display = repo.name
    if repo.city:
        repo_display += f", {repo.city}"

    language = _LANGUAGE_MAP.get(
        description.language, description.language
    )

    metadata_pairs = [
        ({"en": ["Date"], "es": ["Fecha"]},
         description.date_expression),
        ({"en": ["Extent"], "es": ["Extensión"]},
         description.extent),
        ({"en": ["Language"], "es": ["Idioma"]},
         language),
        ({"en": ["Access conditions"], "es": ["Condiciones de acceso"]},
         description.access_conditions),
        ({"en": ["Reference"], "es": ["Signatura"]},
         description.reference_code),
        ({"en": ["Repository"], "es": ["Repositorio"]},
         repo_display),
    ]

    manifest.metadata = []
    for label, value in metadata_pairs:
        if value:
            # Split pipe-separated values into separate list items
            if "|" in value:
                values = [v.strip() for v in value.split("|")]
            else:
                values = [value]
            manifest.metadata.append(
                KeyValueString(label=label, value={"es": values})
            )

    # Canvases — one per image
    for i, img in enumerate(images, 1):
        image_name = img['name']
        width = img['width']
        height = img['height']

        canvas = manifest.make_canvas(
            id=f"{base_url}/{doc_slug}/canvas/{i}",
            label={"none": [f"img {i}"]},
            height=height,
            width=width,
        )

        # Thumbnail — use the 200px generated thumbnail
        thumb_w = 200
        thumb_h = round(height * thumb_w / width) if width > 0 else 200
        canvas.thumbnail = [{
            "id": (
                f"{base_url}/{doc_slug}/{image_name}"
                f"/full/{thumb_w},{thumb_h}/0/default.jpg"
            ),
            "type": "Image",
            "format": "image/jpeg",
            "width": thumb_w,
            "height": thumb_h,
        }]

        # Add painting annotation with image and ImageService3
        anno_page = canvas.add_image(
            image_url=(
                f"{base_url}/{doc_slug}/{image_name}"
                f"/full/max/0/default.jpg"
            ),
            anno_id=f"{base_url}/{doc_slug}/canvas/{i}/annotation",
            anno_page_id=f"{base_url}/{doc_slug}/canvas/{i}/page",
            format="image/jpeg",
            height=height,
            width=width,
        )
        # Attach Level 0 ImageService3 to the image body
        anno_page.items[0].body.make_service(
            id=f"{base_url}/{doc_slug}/{image_name}",
            type="ImageService3",
            profile="level0",
        )

    return manifest


# ---------------------------------------------------------------------------
# Command
# ---------------------------------------------------------------------------

class Command(BaseCommand):
    help = 'Generate IIIF manifests for digitised descriptions'

    def add_arguments(self, parser):
        parser.add_argument(
            '--csv', required=True,
            help='Path to ca-image-manifest.csv',
        )
        parser.add_argument(
            '--output-dir', required=True,
            help='Output directory for manifest JSON files',
        )
        parser.add_argument(
            '--base-url', default='https://iiif.zasqua.org',
            help='Base URL for IIIF (default: https://iiif.zasqua.org)',
        )
        parser.add_argument(
            '--repository', default='',
            help='Filter by repository code (e.g. co-cihjml)',
        )
        parser.add_argument(
            '--limit', type=int, default=0,
            help='Limit number of descriptions to process',
        )
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Print what would be done without writing',
        )
        parser.add_argument(
            '--skip-db-update', action='store_true',
            help='Skip updating iiif_manifest_url in the database',
        )
        parser.add_argument(
            '--tiles-dir', default='',
            help='Tiles directory to read PDF page counts and dimensions',
        )
        parser.add_argument(
            '--originals-dir', default='',
            help='Originals directory to read PDF page counts via pdfinfo',
        )

    def log(self, message, style=None):
        if style:
            message = style(message)
        self.stdout.write(message)
        sys.stdout.flush()

    def handle(self, *args, **options):
        csv_path = options['csv']
        output_dir = options['output_dir']
        base_url = options['base_url'].rstrip('/')
        repo_filter = options['repository']
        limit = options['limit']
        dry_run = options['dry_run']
        skip_db = options['skip_db_update']
        tiles_dir = options['tiles_dir']
        originals_dir = options['originals_dir']

        start_time = time.time()

        # Load CSV
        self.log(f"Loading CSV: {csv_path}")
        csv_docs = load_csv(csv_path)
        self.log(f"  {len(csv_docs)} documents in CSV")

        # Resolve PDF pages — prefer tiles-dir (exact dimensions),
        # fall back to originals-dir (pdfinfo page counts + DPI scaling)
        if tiles_dir:
            self.log(f"  Reading PDF page data from tiles: {tiles_dir}")
            resolve_pdf_pages(csv_docs, tiles_dir)
        elif originals_dir:
            self.log(
                f"  Reading PDF page counts from originals: "
                f"{originals_dir}"
            )
            resolve_pdf_pages_from_originals(csv_docs, originals_dir)
        else:
            # Without either, PDFs won't have page data
            pdf_count = sum(
                1 for doc in csv_docs.values()
                if any(img.get('is_pdf') for img in doc['images'])
            )
            if pdf_count:
                self.log(
                    f"  Warning: {pdf_count} PDF documents found. "
                    f"Use --originals-dir or --tiles-dir to resolve "
                    f"page data."
                )

        # Query descriptions
        qs = Description.objects.filter(
            has_digital=True,
            ca_object_id__isnull=False,
        ).select_related('repository')

        if repo_filter:
            qs = qs.filter(repository__code=repo_filter)

        if limit:
            qs = qs[:limit]

        descriptions = list(qs)
        self.log(f"  {len(descriptions)} descriptions from database")

        os.makedirs(output_dir, exist_ok=True)

        # Process descriptions
        generated = 0
        skipped = 0
        errors = []
        db_updates = []

        for desc in descriptions:
            ca_id = desc.ca_object_id
            if ca_id not in csv_docs:
                skipped += 1
                continue

            doc_data = csv_docs[ca_id]
            doc_slug = doc_data['doc_slug']
            images = [
                img for img in doc_data['images']
                if 'name' in img and img['name'] != '__pdf__'
            ]

            if not images:
                skipped += 1
                continue

            if dry_run:
                self.log(
                    f"  [DRY RUN] {doc_slug}: "
                    f"{len(images)} canvases"
                )
                generated += 1
                continue

            try:
                manifest = build_manifest(
                    desc, images, base_url, doc_slug
                )

                # Write manifest JSON
                manifest_dir = os.path.join(output_dir, doc_slug)
                os.makedirs(manifest_dir, exist_ok=True)
                manifest_path = os.path.join(manifest_dir, 'manifest.json')

                manifest_json = manifest.json(indent=2)
                with open(manifest_path, 'w') as f:
                    f.write(manifest_json)

                manifest_url = f"{base_url}/{doc_slug}/manifest.json"
                db_updates.append((desc.pk, manifest_url))

                self.log(
                    f"  {doc_slug}: {len(images)} canvases -> "
                    f"{manifest_path}"
                )
                generated += 1

            except Exception as e:
                errors.append(f"{doc_slug}: {e}")

        # Bulk update iiif_manifest_url in database
        if db_updates and not skip_db and not dry_run:
            self.log(f"\nUpdating {len(db_updates)} descriptions in DB...")
            updated = 0
            for pk, url in db_updates:
                Description.objects.filter(pk=pk).update(
                    iiif_manifest_url=url
                )
                updated += 1
            self.log(f"  Updated {updated} iiif_manifest_url values")

        # Summary
        elapsed = time.time() - start_time
        self.log(f"\nDone in {elapsed:.1f}s")
        self.log(f"  Generated: {generated}")
        self.log(f"  Skipped: {skipped} (no images in CSV)")
        if db_updates and not skip_db:
            self.log(f"  DB updates: {len(db_updates)}")

        if errors:
            self.log(f"\n{len(errors)} errors:")
            for err in errors:
                self.log(f"  {err}")
