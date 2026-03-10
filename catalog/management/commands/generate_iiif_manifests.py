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
      --pdf-pages /path/to/pdf-page-counts.csv

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
import sys
import time

from django.core.management.base import BaseCommand

from catalog.models import Description

from iiif_prezi3 import KeyValueString, Manifest

# Import shared image name extraction from the IIIF tiling module
sys.path.insert(0, os.path.join(
    os.path.dirname(__file__), '..', '..', '..', 'scripts', 'iiif'
))
from iiif_tiling import extract_image_name  # noqa: E402


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
# Per-repository attribution
# ---------------------------------------------------------------------------

_ATTRIBUTION = {
    'co-cihjml': {
        'en': ('{repo}, {country_en}. Digitized by Neogranadina '
               'and hosted on Zasqua (zasqua.org).'),
        'es': ('{repo}, {country}. Digitalizado por Neogranadina '
               'y alojado en Zasqua (zasqua.org).'),
    },
    'co-ahrb': {
        'en': ('{repo}, {country_en}. Digitized by Neogranadina '
               'and hosted on Zasqua (zasqua.org).'),
        'es': ('{repo}, {country}. Digitalizado por Neogranadina '
               'y alojado en Zasqua (zasqua.org).'),
    },
    'pe-bn': {
        'en': ('{repo}, {country_en}. Processed by Neogranadina '
               'and hosted on Zasqua (zasqua.org).'),
        'es': ('{repo}, {country}. Procesado por Neogranadina '
               'y alojado en Zasqua (zasqua.org).'),
    },
    'co-ahjci': {
        'en': ('{repo}, {country_en}. Digitized under the Endangered '
               'Archives Programme (EAP 1477), funded by Arcadia '
               'and held at the British Library. CC BY-NC 4.0. '
               'Processed by Neogranadina and hosted on '
               'Zasqua (zasqua.org).'),
        'es': ('{repo}, {country}. Digitalizado en el marco del '
               'Endangered Archives Programme (EAP 1477), financiado '
               'por Arcadia y custodiado en la British Library. '
               'CC BY-NC 4.0. Procesado por Neogranadina y alojado '
               'en Zasqua (zasqua.org).'),
    },
}

# Fallback for any repository not listed above
_ATTRIBUTION_DEFAULT = {
    'en': ('{repo}, {country_en}. Hosted on Zasqua (zasqua.org).'),
    'es': ('{repo}, {country}. Alojado en Zasqua (zasqua.org).'),
}

# Country display names (Spanish → English)
_COUNTRY_EN = {
    'Colombia': 'Colombia',
    'Perú': 'Peru',
}


def derive_doc_slug(object_idno):
    """Derive a URL-safe doc-slug from a CA object_idno.

    Used for tile paths (backward compatibility).  For new tile generation,
    prefer manifest_slug_from_ref() which uses the full reference_code.
    """
    return object_idno.lower().replace('.', '-').replace('_', '-')


def manifest_slug_from_ref(reference_code):
    """Derive manifest path slug from reference_code.

    Strips characters that are unsafe in URLs (? and #).  Matches the
    frontend safeSlug filter and METS URL computation.
    """
    return reference_code.replace('?', '').replace('#', '')


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


def resolve_pdf_pages_from_counts(csv_docs, pdf_pages_path):
    """Replace PDF placeholder entries using a pre-computed page-counts file.

    The file is a simple CSV with representation_id,page_count per line,
    generated by running pdfinfo on the originals (on the droplet).
    """
    # Load page counts keyed by representation_id
    page_counts = {}
    with open(pdf_pages_path, newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) >= 2 and row[0] and row[1]:
                page_counts[row[0]] = int(row[1])

    for doc in csv_docs.values():
        pdf_imgs = [img for img in doc['images'] if img.get('is_pdf')]
        if not pdf_imgs:
            continue

        pdf_img = pdf_imgs[0]
        rep_id = pdf_img.get('representation_id', '')
        page_count = page_counts.get(rep_id, 0)

        if page_count == 0:
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

def build_manifest(description, images, base_url, manifest_slug, tile_slug):
    """Build a IIIF Presentation API v3 manifest.

    Args:
        description: Description model instance (with repository).
        images: List of image dicts with 'name', 'width', 'height'.
        base_url: Base URL for IIIF resources.
        manifest_slug: Slug for manifest paths/IDs (from reference_code).
        tile_slug: Slug for tile image URLs (from object_idno).

    Returns:
        iiif_prezi3.Manifest object.
    """
    manifest_id = f"{base_url}/{manifest_slug}/manifest.json"

    manifest = Manifest(
        id=manifest_id,
        label={"es": [description.title]},
    )
    manifest.behavior = ["paged"]

    # Rights — CC BY-NC 4.0 for all repositories
    manifest.rights = "http://creativecommons.org/licenses/by-nc/4.0/"

    # Required statement — per-repository attribution
    repo = description.repository
    repo_display = repo.name
    if repo.city:
        repo_display += f", {repo.city}"

    country = repo.country or ''
    country_en = _COUNTRY_EN.get(country, country)

    attr = _ATTRIBUTION.get(repo.code, _ATTRIBUTION_DEFAULT)
    manifest.requiredStatement = KeyValueString(
        label={"en": ["Attribution"], "es": ["Atribución"]},
        value={
            "en": [attr['en'].format(repo=repo_display, country=country,
                                     country_en=country_en)],
            "es": [attr['es'].format(repo=repo_display, country=country,
                                     country_en=country_en)],
        },
    )

    # Provider — Neogranadina
    manifest.provider = [{
        "id": "https://neogranadina.org",
        "type": "Agent",
        "label": {"en": ["Neogranadina"], "es": ["Neogranadina"]},
        "homepage": [{
            "id": "https://zasqua.org",
            "type": "Text",
            "format": "text/html",
            "label": {"en": ["Zasqua"], "es": ["Zasqua"]},
        }],
    }]

    # Homepage — link to Zasqua description page
    manifest.homepage = [{
        "id": f"https://zasqua.org/{description.reference_code}/",
        "type": "Text",
        "label": {"es": [description.title]},
        "format": "text/html",
    }]

    # seeAlso — METS metadata
    mets_slug = description.reference_code.replace('?', '').replace('#', '')
    manifest.seeAlso = [{
        "id": f"https://mets.zasqua.org/{mets_slug}.xml",
        "type": "Dataset",
        "label": {"en": ["METS metadata"], "es": ["Metadatos METS"]},
        "format": "application/xml",
        "profile": "http://www.loc.gov/METS/",
    }]

    # Summary from scope_content
    if description.scope_content:
        manifest.summary = {"es": [description.scope_content]}

    # Metadata fields with bilingual labels
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
            id=f"{base_url}/{manifest_slug}/canvas/{i}",
            label={"none": [f"img {i}"]},
            height=height,
            width=width,
        )

        # Thumbnail — use the 200px generated thumbnail
        thumb_w = 200
        thumb_h = round(height * thumb_w / width) if width > 0 else 200
        canvas.thumbnail = [{
            "id": (
                f"{base_url}/{tile_slug}/{image_name}"
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
                f"{base_url}/{tile_slug}/{image_name}"
                f"/full/max/0/default.jpg"
            ),
            anno_id=f"{base_url}/{manifest_slug}/canvas/{i}/annotation",
            anno_page_id=f"{base_url}/{manifest_slug}/canvas/{i}/page",
            format="image/jpeg",
            height=height,
            width=width,
        )
        # Attach Level 0 ImageService3 to the image body
        anno_page.items[0].body.make_service(
            id=f"{base_url}/{tile_slug}/{image_name}",
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
            '--pdf-pages', default='',
            help='CSV file with representation_id,page_count (from pdfinfo)',
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
        pdf_pages = options['pdf_pages']

        start_time = time.time()

        # Load CSV
        self.log(f"Loading CSV: {csv_path}")
        csv_docs = load_csv(csv_path)
        self.log(f"  {len(csv_docs)} documents in CSV")

        # Resolve PDF pages — prefer tiles-dir (exact dimensions),
        # fall back to pdf-pages CSV (pre-computed page counts)
        if tiles_dir:
            self.log(f"  Reading PDF page data from tiles: {tiles_dir}")
            resolve_pdf_pages(csv_docs, tiles_dir)
        elif pdf_pages:
            self.log(f"  Reading PDF page counts from: {pdf_pages}")
            resolve_pdf_pages_from_counts(csv_docs, pdf_pages)
        else:
            # Without either, PDFs won't have page data
            pdf_count = sum(
                1 for doc in csv_docs.values()
                if any(img.get('is_pdf') for img in doc['images'])
            )
            if pdf_count:
                self.log(
                    f"  Warning: {pdf_count} PDF documents found. "
                    f"Use --pdf-pages or --tiles-dir to resolve "
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
            tile_slug = doc_data['doc_slug']
            m_slug = manifest_slug_from_ref(desc.reference_code)
            images = [
                img for img in doc_data['images']
                if 'name' in img and img['name'] != '__pdf__'
            ]

            if not images:
                skipped += 1
                continue

            if dry_run:
                self.log(
                    f"  [DRY RUN] {m_slug}: "
                    f"{len(images)} canvases"
                )
                generated += 1
                continue

            try:
                manifest = build_manifest(
                    desc, images, base_url, m_slug, tile_slug
                )

                # Write manifest JSON
                manifest_dir = os.path.join(output_dir, m_slug)
                os.makedirs(manifest_dir, exist_ok=True)
                manifest_path = os.path.join(manifest_dir, 'manifest.json')

                manifest_json = manifest.json(indent=2)
                with open(manifest_path, 'w') as f:
                    f.write(manifest_json)

                manifest_url = f"{base_url}/{m_slug}/manifest.json"
                db_updates.append((desc.pk, manifest_url))

                self.log(
                    f"  {m_slug}: {len(images)} canvases -> "
                    f"{manifest_path}"
                )
                generated += 1

            except Exception as e:
                errors.append(f"{m_slug}: {e}")

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
