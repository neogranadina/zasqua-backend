# Zasqua Backend

Django application for the [Zasqua](https://zasqua.org) archival platform.

## Overview

Zasqua Backend manages archival descriptions, entities, and places for four Colombian and one Peruvian repository — roughly 104,000 descriptions and 52,000 entities. It provides:

- REST API for archival descriptions, entities, and places
- MPTT-based hierarchical data model (archival fonds, series, items)
- Management commands for data import and frontend export
- IIIF manifest generation for digitized materials

## Requirements

- Python 3.11+
- MySQL 8.0+

## Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your database credentials

# Run migrations
python manage.py migrate

# Start development server
python manage.py runserver
```

## Database Models

| Model | Description |
|-------|-------------|
| Repository | Archive institutions (AHR, AHRB, CIHJML, AHJCI, PE-BN) |
| Description | Archival descriptions with MPTT hierarchy |
| Entity | People, organizations, families |
| Place | Geographic locations |
| DescriptionEntity | Links descriptions to entities with roles |
| DescriptionPlace | Links descriptions to places with roles |

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `/api/repositories/` | List all repositories |
| `/api/repositories/{code}/` | Repository detail |
| `/api/descriptions/` | List descriptions with filtering |
| `/api/descriptions/{id}/` | Description detail |
| `/api/descriptions/{id}/children/` | Child descriptions |
| `/api/entities/` | List entities |
| `/api/places/` | List places |

## Management Commands

### Data Import

```bash
# Import from CollectiveAccess MySQL database
python manage.py import_ca --phase [descriptions|entities|places|links]

# Import AHR hierarchy from clean CSVs
python manage.py import_ahr_hierarchy

# Import AHT item-level records from CSV
python manage.py import_aht_items

# Update AHT legajo containers with metadata from CSV
python manage.py update_aht_legajos

# Import OCR text from CA representations
python manage.py import_ocr_text

# Restructure PE-BN CDIP items into section-level hierarchy
python manage.py restructure_pebn_sections
```

### Data Export

```bash
# Export JSON data for frontend static build
python manage.py export_frontend_data

# Export item metadata for title/entity processing
python manage.py export_acc_metadata
```

### IIIF

```bash
# Generate IIIF manifests for digitized descriptions
python manage.py generate_iiif_manifests --tiles-dir /path/to/tiles
```

All import commands support `--dry-run` to preview changes without modifying the database.

## Data Summary

| Repository | Location | Descriptions |
|------------|----------|-------------|
| AHR | Rionegro, Antioquia | ~53,000 |
| AHRB | Tunja, Boyaca | ~8,300 |
| CIHJML | Popayan, Cauca | ~25,000 |
| AHJCI | Istmina, Choco | ~300 |
| PE-BN | Lima, Peru | ~17,000 |

## Development

### Running Tests
```bash
python manage.py test
```

### Code Style
```bash
black .
flake8
```

## Related

- [Zasqua Frontend](https://github.com/neogranadina/zasqua-frontend) — Static site built with Eleventy and Pagefind

## License

GPL-3.0. See [LICENSE](LICENSE) for details.

---

Zasqua is developed by [Neogranadina](https://neogranadina.org) and the [Archives, Memory, and Preservation Lab](https://ampl.clair.ucsb.edu) of the University of California, Santa Barbara.
