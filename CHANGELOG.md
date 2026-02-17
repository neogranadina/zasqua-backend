# Changelog

All notable changes to the Zasqua backend will be documented in this file. Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.2.0] — 2026-02-17

IIIF manifest generation, metadata field expansion, and country support.

### Added

- `generate_iiif_manifests` management command — generates IIIF Presentation API v3 manifests from CA image manifest CSV joined with Django DB metadata
  - Per-repository bilingual attribution (requiredStatement), CC BY-NC 4.0 rights, Neogranadina provider
  - EAP 1477 (Endangered Archives Programme) acknowledgement for AHJCI materials
  - `--pdf-pages` option for pre-computed PDF page count resolution
  - Bulk-updates `iiif_manifest_url` on Description model
- `iiif_manifest_url` field added to `export_frontend_data` output
- `country` field on Repository model
- `finding_aids`, `section_title` fields on Description model
- 12 new fields in `export_frontend_data` output: imprint, edition_statement, series_statement, uniform_title, section_title, pages, reproduction_conditions, location_of_originals, finding_aids, related_materials, country, publication_title

### Fixed

- `otherfindingaid` attribute now maps to `finding_aids` instead of `related_materials`
- Multiple notes from CA concatenated with pipe separator instead of keeping only the first
- Country populated dynamically from Repository model instead of hardcoded in IIIF attribution

## [0.1.0] — 2026-02-14

First release. Django cataloging backend with CollectiveAccess migration, REST API, and static data export.

### Added

- Django 5.1 backend with 6-model schema: Repository, Description (MPTT), Entity, Place, DescriptionEntity, DescriptionPlace
- CollectiveAccess migration pipeline — 104,465 descriptions, 90,786 entities, 977 places imported with EAV attributes and structured date parsing
- Entity schema: honorific, primary_function, surname, given_name fields; Neogranadina IDs (ne-xxxxx) for all entities
- REST API (Django REST Framework) with paginated endpoints for descriptions, entities, places, and tree navigation
- N+1 query elimination in list endpoints using MPTT lft/rght values and queryset annotations
- `export_frontend_data` management command — exports descriptions, repositories, and tree children as static JSON for the frontend build
- `import_ocr_text` management command — imports and compresses OCR text from CollectiveAccess (14,272 descriptions)
- `short_name` field on Repository model for abbreviated display in search filters
- Meilisearch integration (retained as fallback, not required for the public site)

### Fixed

- Language field mapped from raw CollectiveAccess list-item IDs (192, 173, 195) to display names during export
- Children level detection for AHRB: legajo containers correctly identified at fonds level, CAB series return item-level children
