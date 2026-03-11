#!/usr/bin/env python3
"""
Ingest AHRB volumes from Dropbox into the Zasqua IIIF pipeline.

Processes volumes directly from Dropbox (via rclone) without CA dependency.
Standalone Python -- no Django required.

For each volume:
  1. Pull images from Dropbox via rclone
  2. Discover and sort image files
  3. Tile all images (preprocess -> vips dzsave -> thumbnails -> full/max
     -> patch info.json)
  4. Generate a minimal IIIF Presentation v3 manifest
  5. Upload tiles + manifest to R2 via rclone
  6. Clean up local files
  7. Log completion to progress file

Input: a volume manifest CSV with columns: fond,volume,image_dir

Usage:
    python ingest_dropbox_volumes.py \\
      --manifest volumes.csv \\
      --dropbox-root "dropbox:/path/to/AHRB" \\
      --work-dir /mnt/work \\
      --base-url https://iiif.zasqua.org \\
      --r2-remote r2:zasqua-iiif-tiles \\
      --workers 8 \\
      --progress progress.log
"""

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from iiif_tiling import (
    THUMBNAIL_WIDTHS,
    extract_image_name,
    generate_full_max,
    generate_thumbnails,
    generate_tiles_vips,
    patch_info_json,
    preprocess_image,
    upload_to_r2,
)


def log(msg):
    """Print a timestamped log message and flush immediately."""
    ts = datetime.now(timezone.utc).strftime('%H:%M:%S')
    print(f"[{ts}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Per-repository attribution (matching generate_iiif_manifests.py)
# ---------------------------------------------------------------------------

_ATTRIBUTION = {
    'co-ahrb': {
        'en': ('Archivo Histórico Regional de Boyacá, Tunja, Colombia. '
               'Digitized by Neogranadina and hosted on '
               'Zasqua (zasqua.org).'),
        'es': ('Archivo Histórico Regional de Boyacá, Tunja, Colombia. '
               'Digitalizado por Neogranadina y alojado en '
               'Zasqua (zasqua.org).'),
    },
}


# ---------------------------------------------------------------------------
# Volume slug derivation
# ---------------------------------------------------------------------------

def derive_volume_slug(fond, volume):
    """Derive a URL-safe slug from fond code and volume number.

    All slugs carry the co- repository prefix to match the Zasqua
    collection URL pattern (co-ahrb-...).

    Examples:
        ('AHRB_AHT', '003') -> 'co-ahrb-aht-003'
        ('AHRB_N1', '001')  -> 'co-ahrb-n1-001'
        ('AHRB_NVL', '067') -> 'co-ahrb-nvl-067'
    """
    return f"co-{fond.lower().replace('_', '-')}-{volume}"


def derive_volume_label(fond, volume):
    """Derive a human-readable label from fond code and volume number.

    Examples:
        ('AHRB_AHT', '003') -> 'AHRB AHT 003'
        ('AHRB_N1', '001')  -> 'AHRB N1 001'
    """
    return f"{fond.replace('_', ' ')} {volume}"


# ---------------------------------------------------------------------------
# Progress tracking
# ---------------------------------------------------------------------------

def load_progress(progress_path):
    """Load set of completed slugs from the progress log."""
    completed = set()
    if progress_path and os.path.exists(progress_path):
        with open(progress_path) as f:
            for line in f:
                slug = line.strip()
                if slug:
                    completed.add(slug)
    return completed


def log_progress(progress_path, slug):
    """Append a completed slug to the progress log."""
    if progress_path:
        with open(progress_path, 'a') as f:
            f.write(slug + '\n')


def log_errors(errors_path, slug, errors):
    """Append error entries for a failed volume to the errors log.

    Each error is written as one line:
        <ISO timestamp> <slug>: <error detail>
    """
    if not errors_path or not errors:
        return
    ts = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    with open(errors_path, 'a') as f:
        for err in errors:
            f.write(f"{ts} {err}\n")


# ---------------------------------------------------------------------------
# Manifest generation
# ---------------------------------------------------------------------------

def build_volume_manifest(slug, label, images_info, base_url):
    """Build a minimal IIIF Presentation v3 manifest for a volume.

    Args:
        slug: Volume slug (e.g. 'ahrb-aht-003').
        label: Human-readable label (e.g. 'AHRB AHT 003').
        images_info: List of dicts with 'name', 'width', 'height',
                     sorted in page order.
        base_url: Base URL for IIIF resources.

    Returns:
        dict representing the manifest JSON.
    """
    manifest_id = f"{base_url}/{slug}/manifest.json"
    attr = _ATTRIBUTION.get('co-ahrb', _ATTRIBUTION['co-ahrb'])

    manifest = {
        "@context": "http://iiif.io/api/presentation/3/context.json",
        "id": manifest_id,
        "type": "Manifest",
        "label": {"es": [label]},
        "behavior": ["paged"],
        "rights": "http://creativecommons.org/licenses/by-nc/4.0/",
        "requiredStatement": {
            "label": {"en": ["Attribution"], "es": ["Atribución"]},
            "value": {
                "en": [attr['en']],
                "es": [attr['es']],
            },
        },
        "provider": [{
            "id": "https://neogranadina.org",
            "type": "Agent",
            "label": {"en": ["Neogranadina"], "es": ["Neogranadina"]},
            "homepage": [{
                "id": "https://zasqua.org",
                "type": "Text",
                "format": "text/html",
                "label": {"en": ["Zasqua"], "es": ["Zasqua"]},
            }],
        }],
        "items": [],
    }

    for i, img in enumerate(images_info, 1):
        image_name = img['name']
        width = img['width']
        height = img['height']

        thumb_w = 200
        thumb_h = round(height * thumb_w / width) if width > 0 else 200

        canvas_id = f"{base_url}/{slug}/canvas/{i}"
        canvas = {
            "id": canvas_id,
            "type": "Canvas",
            "label": {"none": [f"img {i}"]},
            "height": height,
            "width": width,
            "thumbnail": [{
                "id": (f"{base_url}/{slug}/{image_name}"
                       f"/full/{thumb_w},{thumb_h}/0/default.jpg"),
                "type": "Image",
                "format": "image/jpeg",
                "width": thumb_w,
                "height": thumb_h,
            }],
            "items": [{
                "id": f"{canvas_id}/page",
                "type": "AnnotationPage",
                "items": [{
                    "id": f"{canvas_id}/annotation",
                    "type": "Annotation",
                    "motivation": "painting",
                    "target": canvas_id,
                    "body": {
                        "id": (f"{base_url}/{slug}/{image_name}"
                               f"/full/max/0/default.jpg"),
                        "type": "Image",
                        "format": "image/jpeg",
                        "height": height,
                        "width": width,
                        "service": [{
                            "id": f"{base_url}/{slug}/{image_name}",
                            "type": "ImageService3",
                            "profile": "level0",
                        }],
                    },
                }],
            }],
        }
        manifest["items"].append(canvas)

    return manifest


# ---------------------------------------------------------------------------
# Image processing
# ---------------------------------------------------------------------------

def process_image(image_path, image_output_dir, base_url, slug, image_name):
    """Process a single image: preprocess, tile, thumbnails, full/max, patch.

    Returns:
        dict with 'name', 'width', 'height' from the generated info.json.

    Raises:
        Exception on any processing error.
    """
    processed_path, temp = preprocess_image(image_path)
    try:
        generate_tiles_vips(processed_path, image_output_dir)
        generate_thumbnails(processed_path, image_output_dir)
        generate_full_max(processed_path, image_output_dir)
        patch_info_json(image_output_dir, base_url, slug, image_name)
    finally:
        if temp and Path(temp.name).exists():
            Path(temp.name).unlink()

    # Read dimensions from the generated info.json
    info_path = image_output_dir / 'info.json'
    if info_path.exists():
        info = json.loads(info_path.read_text())
        return {
            'name': image_name,
            'width': info.get('width', 0),
            'height': info.get('height', 0),
        }
    return {'name': image_name, 'width': 0, 'height': 0}


# ---------------------------------------------------------------------------
# Volume processing
# ---------------------------------------------------------------------------

def process_volume(volume, config):
    """Process a single volume: pull, tile, manifest, upload, clean up.

    Args:
        volume: dict with 'fond', 'volume', 'image_dir', 'slug', 'label'.
        config: dict with runtime configuration.

    Returns:
        (slug, image_count, elapsed, errors) tuple.
    """
    slug = volume['slug']
    label = volume['label']
    image_dir = volume['image_dir']
    dropbox_root = config['dropbox_root']
    work_dir = Path(config['work_dir'])
    base_url = config['base_url']
    r2_remote = config['r2_remote']
    dry_run = config['dry_run']
    skip_upload = config['skip_upload']
    skip_pull = config['skip_pull']
    workers = config['workers']
    progress_path = config['progress_path']
    errors_path = config['errors_path']

    images_dir = work_dir / 'images' / slug
    tiles_dir = work_dir / 'tiles' / slug
    start = time.time()
    errors = []

    if dry_run:
        log(f"[DRY RUN] {slug}: {image_dir}")
        return slug, 0, 0, []

    try:
        # Step 1: Pull images from Dropbox
        if not skip_pull:
            images_dir.mkdir(parents=True, exist_ok=True)
            remote_path = f"{dropbox_root}/{image_dir}/"
            log(f"  {slug}: pulling from Dropbox...")
            cmd = [
                'rclone', 'copy',
                remote_path,
                str(images_dir),
                '--transfers', '16',
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                errors.append(f"{slug}: rclone pull failed: {result.stderr}")
                log_errors(errors_path, slug, errors)
                return slug, 0, time.time() - start, errors
            # Report what was pulled
            pulled_files = list(images_dir.iterdir()) if images_dir.exists() else []
            pull_elapsed = time.time() - start
            log(f"  {slug}: pulled {len(pulled_files)} files in {pull_elapsed:.0f}s")

        # Step 2: Discover and sort image files
        if not images_dir.exists():
            errors.append(f"{slug}: images directory not found: {images_dir}")
            log_errors(errors_path, slug, errors)
            return slug, 0, time.time() - start, errors

        image_files = sorted(
            f for f in images_dir.iterdir()
            if f.is_file() and f.suffix.lower() in (
                '.jpg', '.jpeg', '.png', '.tif', '.tiff'
            )
        )

        if not image_files:
            errors.append(f"{slug}: no image files found in {images_dir}")
            log_errors(errors_path, slug, errors)
            return slug, 0, time.time() - start, errors

        total_images = len(image_files)
        log(f"  {slug}: {total_images} images to tile")

        # Step 3: Tile all images
        tiles_dir.mkdir(parents=True, exist_ok=True)
        images_info = []
        image_count = 0

        for image_path in image_files:
            try:
                image_name = extract_image_name(image_path.name)
                image_output = tiles_dir / image_name

                info = process_image(
                    image_path, image_output, base_url, slug, image_name
                )
                images_info.append(info)
                image_count += 1

                # Log progress every 50 images or on the last one
                if image_count % 50 == 0 or image_count == total_images:
                    log(f"  {slug}: tiled {image_count}/{total_images}")

            except Exception as e:
                errors.append(f"{slug}/{image_path.name}: {e}")
                break  # stop volume immediately on image error

        # Clean up vips-properties.xml
        vips_xml = tiles_dir / 'vips-properties.xml'
        if vips_xml.exists():
            vips_xml.unlink()

        # Step 4: Generate manifest
        if images_info:
            manifest = build_volume_manifest(
                slug, label, images_info, base_url
            )
            manifest_path = tiles_dir / 'manifest.json'
            manifest_path.write_text(
                json.dumps(manifest, indent=2, ensure_ascii=False)
            )
            log(f"  {slug}: manifest written ({len(images_info)} canvases)")

        # Step 5: Upload to R2 (only when no errors)
        if not errors and not skip_upload and r2_remote and tiles_dir.exists():
            # Count tiles for reporting
            tile_count = sum(1 for _ in tiles_dir.rglob('*') if _.is_file())
            log(f"  {slug}: uploading {tile_count} files to R2...")
            upload_start = time.time()
            upload_to_r2(tiles_dir, r2_remote, slug)
            upload_elapsed = time.time() - upload_start
            log(f"  {slug}: upload done in {upload_elapsed:.0f}s")

        # Step 6: Clean up local files (only when successful — no errors)
        if not errors and not skip_upload:
            if images_dir.exists():
                shutil.rmtree(images_dir)
            if tiles_dir.exists():
                shutil.rmtree(tiles_dir)
            log(f"  {slug}: local files cleaned up")

        # Step 7: Log completion or errors
        if errors:
            log_errors(errors_path, slug, errors)
        else:
            log_progress(progress_path, slug)

        elapsed = time.time() - start
        return slug, image_count, elapsed, errors

    except Exception as e:
        elapsed = time.time() - start
        errors.append(f"{slug}: {e}")
        log_errors(errors_path, slug, errors)
        return slug, image_count if 'image_count' in dir() else 0, elapsed, errors


# ---------------------------------------------------------------------------
# CSV loading
# ---------------------------------------------------------------------------

def load_manifest_csv(csv_path):
    """Load the volume manifest CSV.

    Expected columns: fond, volume, image_dir

    Returns:
        List of dicts with 'fond', 'volume', 'image_dir', 'slug', 'label'.
    """
    volumes = []
    with open(csv_path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            fond = row['fond'].strip()
            volume = row['volume'].strip()
            image_dir = row['image_dir'].strip()
            slug = derive_volume_slug(fond, volume)
            label = derive_volume_label(fond, volume)
            volumes.append({
                'fond': fond,
                'volume': volume,
                'image_dir': image_dir,
                'slug': slug,
                'label': label,
            })
    return volumes


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Ingest Dropbox volumes into the Zasqua IIIF pipeline"
    )
    parser.add_argument(
        '--manifest', required=True,
        help='Path to volume manifest CSV (fond,volume,image_dir)',
    )
    parser.add_argument(
        '--dropbox-root', required=True,
        help='rclone path to Dropbox root (e.g. '
             '"dropbox:/Archivos Comunes/Imagenes/Copia seguridad AHRB")',
    )
    parser.add_argument(
        '--work-dir', default='/mnt/work',
        help='Local working directory for images and tiles '
             '(default: /mnt/work)',
    )
    parser.add_argument(
        '--base-url', default='https://iiif.zasqua.org',
        help='Base URL for IIIF (default: https://iiif.zasqua.org)',
    )
    parser.add_argument(
        '--r2-remote', default='r2:zasqua-iiif-tiles',
        help='rclone remote for R2 (default: r2:zasqua-iiif-tiles)',
    )
    parser.add_argument(
        '--workers', type=int, default=8,
        help='Number of images to tile in parallel (default: 8)',
    )
    parser.add_argument(
        '--progress', default='progress.log',
        help='Path to progress log file (default: progress.log)',
    )
    parser.add_argument(
        '--limit', type=int, default=0,
        help='Limit number of volumes to process',
    )
    parser.add_argument(
        '--dry-run', action='store_true',
        help='Print what would be done without processing',
    )
    parser.add_argument(
        '--skip-upload', action='store_true',
        help='Skip R2 upload (for local testing)',
    )
    parser.add_argument(
        '--skip-pull', action='store_true',
        help='Skip Dropbox pull (images already local)',
    )
    parser.add_argument(
        '--errors-log', default='errors.log',
        help='Path to errors log file (default: errors.log)',
    )
    parser.add_argument(
        '--force', action='store_true',
        help='Process volumes even if already in progress log',
    )

    args = parser.parse_args()

    # Load volume manifest
    log(f"Loading manifest: {args.manifest}")
    volumes = load_manifest_csv(args.manifest)
    log(f"  {len(volumes)} volumes found")

    # Load progress for resume
    if not args.force:
        completed = load_progress(args.progress)
        if completed:
            before = len(volumes)
            volumes = [v for v in volumes if v['slug'] not in completed]
            log(f"  {before - len(volumes)} already completed, "
                f"{len(volumes)} remaining")

    # Apply limit
    if args.limit:
        volumes = volumes[:args.limit]
        log(f"  Limited to {len(volumes)} volumes")

    if not volumes:
        log("Nothing to process.")
        return

    log(f"Processing {len(volumes)} volumes")
    log(f"Dropbox root: {args.dropbox_root}")
    log(f"Work dir: {args.work_dir}")
    log(f"Base URL: {args.base_url}")
    log(f"R2 remote: {args.r2_remote}")
    log(f"Workers: {args.workers}")

    # Build config
    config = {
        'dropbox_root': args.dropbox_root.rstrip('/'),
        'work_dir': args.work_dir,
        'base_url': args.base_url.rstrip('/'),
        'r2_remote': args.r2_remote,
        'dry_run': args.dry_run,
        'skip_upload': args.skip_upload,
        'skip_pull': args.skip_pull,
        'workers': args.workers,
        'progress_path': args.progress,
        'errors_path': args.errors_log,
    }

    # Ensure work directories exist
    Path(args.work_dir).mkdir(parents=True, exist_ok=True)

    # Process volumes sequentially (parallel tiling within each volume)
    start_time = time.time()
    total_images = 0
    total_errors = []
    processed_count = 0

    for volume in volumes:
        log(f"--- Volume {processed_count + 1}/{len(volumes)}: "
            f"{volume['slug']} ---")
        slug, count, elapsed, errs = process_volume(volume, config)
        total_images += count
        total_errors.extend(errs)
        processed_count += 1

        status = f"  {slug}: {count} images in {elapsed:.1f}s"
        if errs:
            status += f" — ERRORS: {len(errs)}"
        else:
            status += " — OK"
        status += f"  [{processed_count}/{len(volumes)}]"
        log(status)

        # Halt the entire run on rclone pull failure
        if any("rclone pull failed" in err for err in errs):
            log(f"FATAL: rclone pull failed for {slug} — halting run.")
            sys.exit(1)

    total_elapsed = time.time() - start_time
    log(f"Done -- {total_images} images tiled across "
        f"{processed_count} volumes in {total_elapsed:.1f}s")
    if total_images > 0:
        log(f"Average: {total_elapsed / total_images:.2f}s per image")

    if total_errors:
        log(f"{len(total_errors)} errors:")
        for err in total_errors:
            log(f"  {err}")
        sys.exit(1)


if __name__ == '__main__':
    main()
