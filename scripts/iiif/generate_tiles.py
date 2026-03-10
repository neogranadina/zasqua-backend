#!/usr/bin/env python3
"""
Production tile generation for the Zasqua IIIF pipeline.

Reads ca-image-manifest.csv, generates IIIF Level 0 static tiles using
libvips, creates thumbnails, and uploads per-document directories to R2.
Supports multiprocessing and resume-after-interruption via a progress log.

Files on the droplet are stored flat:
    /mnt/originals/files/{representation_id}_{original_filename}

Usage:
    python generate_tiles.py \\
      --csv ca-image-manifest.csv \\
      --originals /mnt/originals/files \\
      --output /mnt/originals/tiles \\
      --base-url https://iiif.zasqua.org \\
      --r2-remote r2:zasqua-iiif-tiles \\
      --workers 16 \\
      --progress progress.log
"""

import argparse
import csv
import os
import sys
import tempfile
import time
from multiprocessing import Pool
from pathlib import Path

from iiif_tiling import (
    extract_image_name,
    generate_full_max,
    generate_thumbnails,
    generate_tiles_vips,
    patch_info_json,
    preprocess_image,
    upload_to_r2,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def derive_doc_slug(object_idno):
    """Derive a URL-safe doc-slug from a CA object_idno.

    Lowercase, dots and underscores replaced with hyphens.
    """
    return object_idno.lower().replace('.', '-').replace('_', '-')


def load_csv(csv_path):
    """Load ca-image-manifest.csv and group rows by ca_object_id.

    Returns:
        dict mapping ca_object_id (int) -> {
            'object_idno': str,
            'doc_slug': str,
            'images': list of row dicts sorted by rank
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
            documents[ca_id]['images'].append(row)

    # Sort images within each document by rank
    for doc in documents.values():
        doc['images'].sort(key=lambda r: int(r['rank']))

        # Deduplicate PDFs: if multiple PDF representations exist for
        # the same object, keep only the primary one (is_primary=1).
        # For images, every representation is a separate page -- keep all.
        pdf_rows = [r for r in doc['images']
                    if r['mimetype'] == 'application/pdf']
        if len(pdf_rows) > 1:
            primary = [r for r in pdf_rows if r['is_primary'] == '1']
            if primary:
                non_primary_ids = {
                    r['representation_id'] for r in pdf_rows
                    if r['is_primary'] != '1'
                }
                doc['images'] = [
                    r for r in doc['images']
                    if r['representation_id'] not in non_primary_ids
                ]

    return documents


def load_progress(progress_path):
    """Load set of completed doc-slugs from the progress log."""
    completed = set()
    if progress_path and os.path.exists(progress_path):
        with open(progress_path) as f:
            for line in f:
                slug = line.strip()
                if slug:
                    completed.add(slug)
    return completed


def log_progress(progress_path, doc_slug):
    """Append a completed doc-slug to the progress log."""
    if progress_path:
        with open(progress_path, 'a') as f:
            f.write(doc_slug + '\n')


def find_file(originals_dir, representation_id, original_filename, doc_slug):
    """Locate a file on disk.

    Tries the production flat structure first, then falls back to
    subdirectory organisation (for local testing).
    """
    # Production: {originals}/{repr_id}_{filename}
    flat_path = originals_dir / f"{representation_id}_{original_filename}"
    if flat_path.exists():
        return flat_path

    # Local test fallback: {originals}/{doc_slug}/{filename}
    subdir_path = originals_dir / doc_slug / original_filename
    if subdir_path.exists():
        return subdir_path

    return None


# ---------------------------------------------------------------------------
# PDF handling
# ---------------------------------------------------------------------------

def extract_pdf_pages(pdf_path, temp_dir, dpi=300):
    """Extract PDF pages as temporary JPEG files.

    Returns:
        List of (page_path, image_name) tuples.
    """
    from pdf2image import convert_from_path

    pages = convert_from_path(str(pdf_path), dpi=dpi)
    result = []

    for i, page in enumerate(pages, 1):
        image_name = f"page_{i:03d}"
        page_path = Path(temp_dir) / f"{image_name}.jpg"
        page.save(str(page_path), 'JPEG', quality=95)
        result.append((page_path, image_name))

    return result


# ---------------------------------------------------------------------------
# Per-document processing
# ---------------------------------------------------------------------------

def process_document(args):
    """Process a single document: tile all images, upload, clean up.

    This is the worker function for the multiprocessing pool.

    Args:
        args: Tuple of (doc_info, config) where:
            doc_info: dict with 'doc_slug', 'object_idno', 'images'
            config: dict with runtime configuration
    """
    doc_info, config = args
    doc_slug = doc_info['doc_slug']
    images = doc_info['images']
    originals_dir = Path(config['originals_dir'])
    output_dir = Path(config['output_dir'])
    base_url = config['base_url']
    dry_run = config['dry_run']
    skip_upload = config['skip_upload']
    r2_remote = config['r2_remote']
    progress_path = config['progress_path']

    doc_output = output_dir / doc_slug
    start = time.time()
    image_count = 0
    errors = []

    try:
        if dry_run:
            print(f"[DRY RUN] {doc_slug}: {len(images)} images")
            return doc_slug, len(images), 0, []

        doc_output.mkdir(parents=True, exist_ok=True)

        for i, row in enumerate(images, 1):
            repr_id = row['representation_id']
            filename = row['original_filename']
            mimetype = row['mimetype']

            try:
                if mimetype == 'application/pdf':
                    # PDF: extract pages, tile each
                    file_path = find_file(
                        originals_dir, repr_id, filename, doc_slug
                    )
                    if file_path is None:
                        errors.append(
                            f"{doc_slug}: file not found: "
                            f"{repr_id}_{filename}"
                        )
                        continue

                    with tempfile.TemporaryDirectory() as tmpdir:
                        pages = extract_pdf_pages(file_path, tmpdir)
                        for page_path, image_name in pages:
                            image_output = doc_output / image_name
                            generate_tiles_vips(page_path, image_output)
                            generate_thumbnails(page_path, image_output)
                            generate_full_max(page_path, image_output)
                            patch_info_json(
                                image_output, base_url, doc_slug, image_name
                            )
                            image_count += 1

                else:
                    # Image file: tile directly
                    file_path = find_file(
                        originals_dir, repr_id, filename, doc_slug
                    )
                    if file_path is None:
                        errors.append(
                            f"{doc_slug}: file not found: "
                            f"{repr_id}_{filename}"
                        )
                        continue

                    image_name = extract_image_name(filename)
                    processed_path, temp = preprocess_image(file_path)
                    try:
                        image_output = doc_output / image_name
                        generate_tiles_vips(processed_path, image_output)
                        generate_thumbnails(processed_path, image_output)
                        generate_full_max(processed_path, image_output)
                        patch_info_json(
                            image_output, base_url, doc_slug, image_name
                        )
                        image_count += 1
                    finally:
                        if temp and Path(temp.name).exists():
                            Path(temp.name).unlink()

            except Exception as e:
                errors.append(f"{doc_slug}/{filename}: {e}")

        # Clean up vips-properties.xml (vips metadata, not needed in output)
        vips_xml = doc_output / 'vips-properties.xml'
        if vips_xml.exists():
            vips_xml.unlink()

        # Upload to R2
        if not skip_upload and r2_remote and doc_output.exists():
            upload_to_r2(doc_output, r2_remote, doc_slug)

        # Clean up local tiles (unless skipping upload for testing)
        if not skip_upload and doc_output.exists():
            import shutil
            shutil.rmtree(doc_output)

        # Log completion
        log_progress(progress_path, doc_slug)

        elapsed = time.time() - start
        return doc_slug, image_count, elapsed, errors

    except Exception as e:
        elapsed = time.time() - start
        errors.append(f"{doc_slug}: {e}")
        return doc_slug, image_count, elapsed, errors


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Production IIIF tile generation for Zasqua"
    )
    parser.add_argument(
        '--csv', required=True,
        help='Path to ca-image-manifest.csv',
    )
    parser.add_argument(
        '--originals', required=True,
        help='Directory containing original files',
    )
    parser.add_argument(
        '--output', required=True,
        help='Output directory for tiles',
    )
    parser.add_argument(
        '--base-url', default='https://iiif.zasqua.org',
        help='Base URL for IIIF (default: https://iiif.zasqua.org)',
    )
    parser.add_argument(
        '--r2-remote', default='',
        help='rclone remote for R2 (e.g. r2:zasqua-iiif-tiles)',
    )
    parser.add_argument(
        '--workers', type=int, default=16,
        help='Number of parallel workers (default: 16)',
    )
    parser.add_argument(
        '--progress', default='',
        help='Path to progress log file (for resume)',
    )
    parser.add_argument(
        '--repository', default='',
        help='Filter by object_idno prefix (e.g. acc, ahrb)',
    )
    parser.add_argument(
        '--limit', type=int, default=0,
        help='Limit number of documents to process',
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Print what would be done without processing',
    )
    parser.add_argument(
        '--skip-upload', action='store_true',
        help='Skip R2 upload (for local testing)',
    )

    args = parser.parse_args()

    # Load CSV
    print(f"Loading CSV: {args.csv}")
    documents = load_csv(args.csv)
    print(f"  {len(documents)} documents found")

    # Filter by repository prefix
    if args.repository:
        prefix = args.repository.lower()
        documents = {
            k: v for k, v in documents.items()
            if v['doc_slug'].startswith(prefix)
        }
        print(f"  {len(documents)} documents after --repository {prefix}")

    # Load progress for resume
    completed = load_progress(args.progress or None)
    if completed:
        before = len(documents)
        documents = {
            k: v for k, v in documents.items()
            if v['doc_slug'] not in completed
        }
        print(f"  {before - len(documents)} already completed, "
              f"{len(documents)} remaining")

    # Apply limit
    doc_list = sorted(documents.values(), key=lambda d: d['doc_slug'])
    if args.limit:
        doc_list = doc_list[:args.limit]
        print(f"  Limited to {len(doc_list)} documents")

    total_images = sum(len(d['images']) for d in doc_list)
    print(f"\nProcessing {len(doc_list)} documents ({total_images} images)")
    print(f"Output: {args.output}")
    print(f"Base URL: {args.base_url}")
    print(f"Workers: {args.workers}")
    if args.r2_remote:
        print(f"R2 remote: {args.r2_remote}")
    print()

    # Prepare output directory
    Path(args.output).mkdir(parents=True, exist_ok=True)

    # Build config dict for workers
    config = {
        'originals_dir': args.originals,
        'output_dir': args.output,
        'base_url': args.base_url,
        'r2_remote': args.r2_remote,
        'dry_run': args.dry_run,
        'skip_upload': args.skip_upload,
        'progress_path': args.progress or None,
    }

    # Process documents
    start_time = time.time()
    total_processed = 0
    total_errors = []

    worker_args = [(doc, config) for doc in doc_list]

    if args.workers <= 1:
        # Sequential processing (easier to debug)
        for wa in worker_args:
            slug, count, elapsed, errs = process_document(wa)
            total_processed += count
            total_errors.extend(errs)
            status = f"  {slug}: {count} images in {elapsed:.1f}s"
            if errs:
                status += f" ({len(errs)} errors)"
            print(status)
    else:
        # Parallel processing
        with Pool(processes=args.workers) as pool:
            for slug, count, elapsed, errs in pool.imap_unordered(
                process_document, worker_args
            ):
                total_processed += count
                total_errors.extend(errs)
                status = f"  {slug}: {count} images in {elapsed:.1f}s"
                if errs:
                    status += f" ({len(errs)} errors)"
                print(status)

    total_elapsed = time.time() - start_time
    print(f"\nDone -- {total_processed} images tiled in {total_elapsed:.1f}s")
    if total_processed > 0:
        print(f"Average: {total_elapsed / total_processed:.2f}s per image")

    if total_errors:
        print(f"\n{len(total_errors)} errors:")
        for err in total_errors:
            print(f"  {err}")
        sys.exit(1)


if __name__ == '__main__':
    main()
