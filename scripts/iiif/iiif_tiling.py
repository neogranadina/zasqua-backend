"""
Shared IIIF tiling module for Zasqua.

Provides image preprocessing, tile generation (via libvips), thumbnail
creation, full-resolution image generation, info.json patching, and R2
upload. Used by both the CA-based tile generation script and the Dropbox
volume ingest workflow.
"""

import json
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from PIL import Image, ImageOps


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

THUMBNAIL_WIDTHS = [96, 200]

# Patterns for extracting short image names from filenames.
# Order matters -- first match wins.
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


# ---------------------------------------------------------------------------
# Image name extraction
# ---------------------------------------------------------------------------

def extract_image_name(filename):
    """Extract a short image identifier from a filename.

    Returns:
        Short identifier like 'img_0004', 'IMG_001', or 'page_001'.

    Raises:
        ValueError: If no pattern matches the filename.
    """
    stem = Path(filename).stem
    for pattern in IMAGE_NAME_PATTERNS:
        match = pattern.match(stem)
        if match:
            return match.group(1)
    raise ValueError(f"Cannot extract image name from '{filename}'")


# ---------------------------------------------------------------------------
# Image preprocessing
# ---------------------------------------------------------------------------

def preprocess_image(image_path):
    """Preprocess an image for IIIF tiling.

    Applies EXIF orientation correction, transparency removal, and format
    conversion to JPEG.  Returns (processed_path, temp_file_or_None).
    The caller must clean up the temp file.
    """
    img = Image.open(image_path)

    # EXIF orientation correction
    img_original = img
    img = ImageOps.exif_transpose(img)
    if img is None:
        img = img_original

    has_exif_orientation = False
    exif = img_original.getexif()
    if exif and 274 in exif and exif[274] != 1:
        has_exif_orientation = True

    # Handle transparency and palette modes
    needs_conversion = False
    converted = img

    if img.mode in ['RGBA', 'LA']:
        rgb = Image.new('RGB', img.size, (255, 255, 255))
        rgb.paste(img, mask=img.split()[-1])
        converted = rgb
        needs_conversion = True
    elif img.mode == 'P':
        converted = img.convert('RGB')
        needs_conversion = True
    elif img.mode not in ['RGB', 'L']:
        converted = img.convert('RGB')
        needs_conversion = True

    # Save to temp JPEG if conversion needed or source isn't JPEG
    ext = Path(image_path).suffix.lower()
    if has_exif_orientation or needs_conversion or ext not in ['.jpg', '.jpeg']:
        temp = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False)
        converted.save(temp.name, 'JPEG', quality=95)
        temp.close()
        return Path(temp.name), temp
    else:
        return Path(image_path), None


# ---------------------------------------------------------------------------
# Tile generation (libvips)
# ---------------------------------------------------------------------------

def generate_tiles_vips(input_path, output_prefix, tile_size=512):
    """Run vips dzsave to generate IIIF3 tiles.

    Args:
        input_path: Path to the preprocessed JPEG image.
        output_prefix: Output path prefix -- vips creates a directory here.
        tile_size: Tile size in pixels (default 512).

    Raises:
        subprocess.CalledProcessError: If vips fails.
    """
    cmd = [
        'vips', 'dzsave',
        str(input_path),
        str(output_prefix),
        '--layout', 'iiif3',
        '--tile-size', str(tile_size),
    ]
    subprocess.run(cmd, check=True, capture_output=True)


# ---------------------------------------------------------------------------
# Thumbnails and full-resolution images
# ---------------------------------------------------------------------------

def generate_thumbnails(image_path, image_output_dir):
    """Generate thumbnails at predefined widths.

    Creates IIIF-compatible thumbnails at each width in THUMBNAIL_WIDTHS.
    Generates at BOTH width-only (full/{w},/) and exact (full/{w},{h}/)
    paths so thumbnails work on static hosting without URL rewriting.
    TIFY's thumbnails panel requests the width-only format; the manifest's
    canvas thumbnail URLs use the exact format.
    """
    img = Image.open(image_path)
    w, h = img.size

    for thumb_w in THUMBNAIL_WIDTHS:
        if thumb_w >= w:
            continue
        thumb_h = round(h * thumb_w / w)
        thumb = img.resize((thumb_w, thumb_h), Image.LANCZOS)

        # Width-only path: full/{w},/0/default.jpg (TIFY thumbnails panel)
        thumb_dir = image_output_dir / 'full' / f'{thumb_w},' / '0'
        thumb_dir.mkdir(parents=True, exist_ok=True)
        thumb.save(thumb_dir / 'default.jpg', 'JPEG', quality=90)

        # Exact path: full/{w},{h}/0/default.jpg (manifest canvas thumbnail)
        exact_dir = image_output_dir / 'full' / f'{thumb_w},{thumb_h}' / '0'
        exact_dir.mkdir(parents=True, exist_ok=True)
        thumb.save(exact_dir / 'default.jpg', 'JPEG', quality=90)

    img.close()


def generate_full_max(image_path, image_output_dir):
    """Generate full/max/0/default.jpg and full/{w},{h}/0/default.jpg.

    Creates the full-resolution whole-image files that IIIF Level 0
    requires. libvips doesn't generate these -- it only creates
    region tiles and (sometimes) a single small full/ entry.
    """
    img = Image.open(image_path)
    if img.mode not in ('RGB', 'L'):
        img = img.convert('RGB')

    # full/max/0/default.jpg
    max_dir = image_output_dir / 'full' / 'max' / '0'
    max_dir.mkdir(parents=True, exist_ok=True)
    dest = max_dir / 'default.jpg'
    img.save(dest, 'JPEG', quality=95)

    # full/{w},{h}/0/default.jpg
    w, h = img.size
    wh_dir = image_output_dir / 'full' / f'{w},{h}' / '0'
    if not wh_dir.exists():
        wh_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(dest, wh_dir / 'default.jpg')

    img.close()


# ---------------------------------------------------------------------------
# info.json patching
# ---------------------------------------------------------------------------

def patch_info_json(image_output_dir, base_url, doc_slug, image_name):
    """Patch info.json with correct id URL and sizes array.

    Updates the id to the production URL and builds the sizes array from:
    1. Thumbnail directories under full/ (both w,h and w, patterns)
    2. Intermediate sizes for each scale factor in tiles
    3. The full image resolution (always included)
    """
    info_path = image_output_dir / 'info.json'
    if not info_path.exists():
        return

    info = json.loads(info_path.read_text())

    # Set correct id URL
    info['id'] = f"{base_url}/{doc_slug}/{image_name}"

    img_w = info.get('width', 0)
    img_h = info.get('height', 0)

    # Build sizes from full/ directory (thumbnails and any libvips entries)
    full_dir = image_output_dir / 'full'
    sizes = []

    if full_dir.exists():
        for entry in sorted(full_dir.iterdir()):
            if not entry.is_dir() or entry.name == 'max':
                continue
            # "w,h" -- both dimensions explicit
            match = re.match(r'^(\d+),(\d+)$', entry.name)
            if match:
                sizes.append({
                    'width': int(match.group(1)),
                    'height': int(match.group(2)),
                })
                continue
            # "w," -- width-only, compute height from aspect ratio
            match = re.match(r'^(\d+),$', entry.name)
            if match and img_w and img_h:
                sw = int(match.group(1))
                sh = int(round(img_h * sw / img_w))
                sizes.append({'width': sw, 'height': sh})

    # Add intermediate sizes for each scale factor
    scale_factors = info.get('tiles', [{}])[0].get('scaleFactors', [])
    if img_w and img_h:
        for sf in scale_factors:
            sw = int(round(img_w / sf))
            sh = int(round(img_h / sf))
            sizes.append({'width': sw, 'height': sh})

    # Deduplicate and sort
    seen = set()
    unique = []
    for s in sizes:
        key = (s['width'], s['height'])
        if key not in seen:
            seen.add(key)
            unique.append(s)
    info['sizes'] = sorted(unique, key=lambda s: s['width'])

    info_path.write_text(json.dumps(info, indent=2, ensure_ascii=False))


# ---------------------------------------------------------------------------
# R2 upload
# ---------------------------------------------------------------------------

def upload_to_r2(local_dir, r2_remote, doc_slug):
    """Upload a document's tiles to R2 via rclone.

    Uses a 1-year cache-control header for tiles.
    """
    remote_path = f"{r2_remote}/{doc_slug}/"
    cmd = [
        'rclone', 'copy',
        str(local_dir),
        remote_path,
        '--header-upload',
        'Cache-Control: public, max-age=31536000, immutable',
        '--transfers', '32',
        '--checkers', '16',
    ]
    subprocess.run(cmd, check=True, capture_output=True)
