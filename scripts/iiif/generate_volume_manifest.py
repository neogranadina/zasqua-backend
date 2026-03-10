#!/usr/bin/env python3
"""
Generate a volume manifest CSV for ingest_dropbox_volumes.py.

Lists volume directories from an rclone remote (or local filesystem)
and outputs a CSV with columns: fond,volume,image_dir.

The output should be reviewed and adjusted before use.

Usage (from Dropbox via rclone):
    python generate_volume_manifest.py \\
      --root "dropbox:/Archivos Comunes/Imagenes/Copia seguridad AHRB" \\
      --fonds AHRB_AHT AHRB_N1 AHRB_N2 AHRB_NVL \\
      --output volumes.csv

Usage (from local filesystem):
    python generate_volume_manifest.py \\
      --root /mnt/dropbox/AHRB \\
      --fonds AHRB_AHT AHRB_N1 AHRB_N2 AHRB_NVL \\
      --output volumes.csv \\
      --local
"""

import argparse
import csv
import re
import subprocess
import sys
from pathlib import Path


def list_volumes_rclone(root, fond):
    """List volume directories from an rclone remote.

    Expects directories named like AHRB_AHT_003, AHRB_N1_001, etc.

    Returns:
        List of (fond, volume_number, image_dir) tuples, sorted.
    """
    remote_path = f"{root}/{fond}/"
    cmd = ['rclone', 'lsd', remote_path]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Warning: rclone lsd failed for {remote_path}: "
              f"{result.stderr.strip()}", file=sys.stderr)
        return []

    volumes = []
    # Pattern: AHRB_AHT_003, AHRB_N1_001, etc.
    pattern = re.compile(rf'^{re.escape(fond)}_(\d+)$')

    for line in result.stdout.strip().split('\n'):
        if not line.strip():
            continue
        # rclone lsd output: "     -1 2024-01-01 00:00:00        -1 dirname"
        parts = line.strip().split()
        if len(parts) < 5:
            continue
        dirname = parts[-1]
        match = pattern.match(dirname)
        if match:
            vol_num = match.group(1)
            image_dir = f"{fond}/{dirname}/proc/recortadas"
            volumes.append((fond, vol_num, image_dir))

    return sorted(volumes, key=lambda x: x[1])


def list_volumes_local(root, fond):
    """List volume directories from local filesystem.

    Returns:
        List of (fond, volume_number, image_dir) tuples, sorted.
    """
    fond_dir = Path(root) / fond
    if not fond_dir.is_dir():
        print(f"Warning: directory not found: {fond_dir}", file=sys.stderr)
        return []

    volumes = []
    pattern = re.compile(rf'^{re.escape(fond)}_(\d+)$')

    for entry in sorted(fond_dir.iterdir()):
        if not entry.is_dir():
            continue
        match = pattern.match(entry.name)
        if match:
            vol_num = match.group(1)
            image_dir = f"{fond}/{entry.name}/proc/recortadas"
            volumes.append((fond, vol_num, image_dir))

    return sorted(volumes, key=lambda x: x[1])


def main():
    parser = argparse.ArgumentParser(
        description="Generate volume manifest CSV for Dropbox ingest"
    )
    parser.add_argument(
        '--root', required=True,
        help='Root path (rclone remote or local directory)',
    )
    parser.add_argument(
        '--fonds', nargs='+', required=True,
        help='Fond codes to scan (e.g. AHRB_AHT AHRB_N1)',
    )
    parser.add_argument(
        '--output', default='-',
        help='Output CSV path (default: stdout)',
    )
    parser.add_argument(
        '--local', action='store_true',
        help='Scan local filesystem instead of rclone remote',
    )

    args = parser.parse_args()

    all_volumes = []
    for fond in args.fonds:
        if args.local:
            volumes = list_volumes_local(args.root, fond)
        else:
            volumes = list_volumes_rclone(args.root.rstrip('/'), fond)
        all_volumes.extend(volumes)
        print(f"  {fond}: {len(volumes)} volumes", file=sys.stderr)

    print(f"\nTotal: {len(all_volumes)} volumes", file=sys.stderr)

    # Write CSV
    if args.output == '-':
        writer = csv.writer(sys.stdout)
        writer.writerow(['fond', 'volume', 'image_dir'])
        for fond, vol_num, image_dir in all_volumes:
            writer.writerow([fond, vol_num, image_dir])
    else:
        with open(args.output, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['fond', 'volume', 'image_dir'])
            for fond, vol_num, image_dir in all_volumes:
                writer.writerow([fond, vol_num, image_dir])
        print(f"Written to {args.output}", file=sys.stderr)


if __name__ == '__main__':
    main()
