"""
Import data from CollectiveAccess MySQL database.

Usage:
    python manage.py import_ca --phase repositories
    python manage.py import_ca --phase collections
    python manage.py import_ca --phase objects
    python manage.py import_ca --phase entities
    python manage.py import_ca --phase entity_links
    python manage.py import_ca --phase places
    python manage.py import_ca --phase place_links
    python manage.py import_ca --phase denormalize
    python manage.py import_ca --phase all
"""

import mysql.connector
from django.core.management.base import BaseCommand
from django.db import transaction
from catalog.models import (
    Repository, Description, Entity, Place,
    DescriptionEntity, DescriptionPlace
)


# CA MySQL connection settings
CA_DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'abcneogranadina',
    'charset': 'utf8mb4',
}

# Repository mappings from CA collection IDs
# Codes normalized to: {country}-{repo} format, lowercase
REPOSITORY_MAP = {
    712: {'code': 'co-cihjml', 'name': 'Centro de Investigaciones Históricas José María Arboleda Llorente, Universidad del Cauca', 'city': 'Popayán', 'country_code': 'COL'},
    360: {'code': 'pe-bn', 'name': 'Biblioteca Nacional del Perú', 'city': 'Lima', 'country_code': 'PER'},
    14805: {'code': 'co-ahrb', 'name': 'Archivo Histórico Regional de Boyacá', 'city': 'Tunja', 'country_code': 'COL'},
    14940: {'code': 'co-ahr', 'name': 'Archivo Histórico de Rionegro', 'city': 'Rionegro', 'country_code': 'COL'},
    16479: {'code': 'co-ahjci', 'name': 'Archivo Histórico del Juzgado de Istmina', 'city': 'Istmina', 'country_code': 'COL'},
}

# Old code -> new code mapping (for migrations)
REPO_CODE_MIGRATION = {
    'CIHJML': 'co-cihjml',
    'PE-BN': 'pe-bn',
    'AHRB': 'co-ahrb',
    'CO.AHR': 'co-ahr',
    'AHJCI': 'co-ahjci',
}

# CA collection type to Zasqua level mapping
COLLECTION_TYPE_MAP = {
    'institucion': 'fonds',
    'fondo': 'fonds',
    'subfondo': 'subfonds',
    'serie': 'series',
    'subserie': 'subseries',
    'subsubserie': 'subseries',
    'coleccion': 'collection',
    'tomo': 'volume',
    'caja': 'file',
    'carpeta': 'file',
    'legajo': 'file',
    'proyecto': 'collection',  # Legacy containers
}

# CA relationship type to Zasqua role mapping
ENTITY_ROLE_MAP = {
    'creator': 'creator',
    'author': 'author',
    'publisher': 'publisher',
    'mencion': 'mentioned',
    'destinatario': 'recipient',
    'remitente': 'sender',
    'scribe': 'scribe',
    'testigo': 'witness',
    'photographer': 'photographer',
    'artist': 'artist',
}

PLACE_ROLE_MAP = {
    'place_of_creation': 'created',
    'subject': 'subject',
    'mentioned': 'mentioned',
    'sent_from': 'sent_from',
    'sent_to': 'sent_to',
    'published': 'published',
}


class Command(BaseCommand):
    help = 'Import data from CollectiveAccess MySQL database'

    def add_arguments(self, parser):
        parser.add_argument(
            '--phase',
            type=str,
            required=True,
            choices=['repositories', 'collections', 'objects', 'entities',
                     'entity_links', 'places', 'place_links', 'denormalize', 'all'],
            help='Which phase to run'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Print what would be done without making changes'
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=0,
            help='Limit number of records (for testing)'
        )

    def handle(self, *args, **options):
        self.dry_run = options['dry_run']
        self.limit = options['limit']
        self.verbosity = options['verbosity']

        phase = options['phase']

        if phase == 'all':
            phases = ['repositories', 'collections', 'objects', 'entities',
                      'entity_links', 'places', 'place_links', 'denormalize']
        else:
            phases = [phase]

        for p in phases:
            self.stdout.write(f"\n{'='*60}")
            self.stdout.write(f"Running phase: {p}")
            self.stdout.write(f"{'='*60}\n")

            method = getattr(self, f'import_{p}')
            method()

    def get_ca_connection(self):
        """Get MySQL connection to CA database."""
        return mysql.connector.connect(**CA_DB_CONFIG)

    def import_repositories(self):
        """Phase 1: Create repository records."""
        self.stdout.write("Creating repositories...")

        for ca_id, data in REPOSITORY_MAP.items():
            if self.dry_run:
                self.stdout.write(f"  Would create: {data['code']} - {data['name']}")
            else:
                repo, created = Repository.objects.update_or_create(
                    code=data['code'],
                    defaults={
                        'name': data['name'],
                        'city': data['city'],
                        'country_code': data['country_code'],
                    }
                )
                status = 'created' if created else 'updated'
                self.stdout.write(f"  {status}: {repo.code} - {repo.name}")

        self.stdout.write(self.style.SUCCESS(f"Repositories: {len(REPOSITORY_MAP)} done"))

    def import_collections(self):
        """Phase 2: Import CA collections as Description hierarchy."""
        self.stdout.write("Importing collections from CA...")

        conn = self.get_ca_connection()
        cursor = conn.cursor(dictionary=True)

        # Get all collections with their labels and types
        query = """
            SELECT
                c.collection_id,
                c.parent_id,
                c.idno,
                c.type_id,
                cl.name as title,
                lit.idno as type_code
            FROM ca_collections c
            JOIN ca_collection_labels cl ON c.collection_id = cl.collection_id
                AND cl.is_preferred = 1
            LEFT JOIN ca_list_items lit ON c.type_id = lit.item_id
            WHERE c.deleted = 0
            ORDER BY c.parent_id, c.collection_id
        """
        if self.limit:
            query += f" LIMIT {self.limit}"

        cursor.execute(query)
        collections = cursor.fetchall()

        self.stdout.write(f"  Found {len(collections)} collections")

        # Build parent mapping and track which repo each collection belongs to
        ca_to_zasqua = {}  # ca_collection_id -> Description.id
        repo_membership = {}  # ca_collection_id -> repo_code

        # First pass: identify repository membership
        for coll in collections:
            if coll['collection_id'] in REPOSITORY_MAP:
                repo_membership[coll['collection_id']] = REPOSITORY_MAP[coll['collection_id']]['code']

        # Propagate repository membership down the tree
        changed = True
        while changed:
            changed = False
            for coll in collections:
                if coll['collection_id'] not in repo_membership and coll['parent_id'] in repo_membership:
                    repo_membership[coll['collection_id']] = repo_membership[coll['parent_id']]
                    changed = True

        # Second pass: create descriptions
        created_count = 0
        skipped_count = 0

        for coll in collections:
            ca_id = coll['collection_id']

            # Skip repository-level collections (they're not descriptions)
            if ca_id in REPOSITORY_MAP:
                continue

            # Skip if we can't determine repository
            if ca_id not in repo_membership:
                skipped_count += 1
                continue

            repo_code = repo_membership[ca_id]
            type_code = (coll['type_code'] or '').lower()
            level = COLLECTION_TYPE_MAP.get(type_code, 'collection')

            # Get parent Description if exists
            parent = None
            if coll['parent_id'] and coll['parent_id'] not in REPOSITORY_MAP:
                parent_id = ca_to_zasqua.get(coll['parent_id'])
                if parent_id:
                    parent = Description.objects.filter(id=parent_id).first()

            if self.dry_run:
                self.stdout.write(f"  Would create: [{repo_code}] {coll['idno']} - {coll['title'][:50]}")
            else:
                try:
                    repo = Repository.objects.get(code=repo_code)
                    idno = str(coll['idno']) if coll['idno'] else str(ca_id)
                    desc, created = Description.objects.update_or_create(
                        ca_collection_id=ca_id,
                        defaults={
                            'repository': repo,
                            'parent': parent,
                            'description_level': level,
                            'reference_code': f"{repo_code}-{idno}",
                            'local_identifier': idno,
                            'title': coll['title'] or f"Collection {ca_id}",
                            'is_published': True,
                        }
                    )
                    ca_to_zasqua[ca_id] = desc.id
                    if created:
                        created_count += 1
                except Exception as e:
                    import traceback
                    self.stdout.write(self.style.ERROR(f"  Error on {ca_id}: {e}"))
                    if self.verbosity > 1:
                        traceback.print_exc()

        cursor.close()
        conn.close()

        self.stdout.write(self.style.SUCCESS(
            f"Collections: {created_count} created, {skipped_count} skipped"
        ))

    def import_objects(self):
        """Phase 3: Import CA objects as item-level Descriptions."""
        self.stdout.write("Importing objects from CA...")

        conn = self.get_ca_connection()
        cursor = conn.cursor(dictionary=True)

        # Get objects with their collection membership
        query = """
            SELECT
                o.object_id,
                o.idno,
                ol.name as title,
                oxc.collection_id
            FROM ca_objects o
            JOIN ca_object_labels ol ON o.object_id = ol.object_id
                AND ol.is_preferred = 1
            LEFT JOIN ca_objects_x_collections oxc ON o.object_id = oxc.object_id
            WHERE o.deleted = 0
            ORDER BY o.object_id
        """
        if self.limit:
            query += f" LIMIT {self.limit}"

        cursor.execute(query)
        objects = cursor.fetchall()

        self.stdout.write(f"  Found {len(objects)} objects")

        # Build collection -> Description mapping
        collection_to_desc = {}
        for desc in Description.objects.filter(ca_collection_id__isnull=False):
            collection_to_desc[desc.ca_collection_id] = desc

        created_count = 0
        skipped_count = 0
        batch = []
        batch_size = 1000

        for obj in objects:
            ca_id = obj['object_id']
            collection_id = obj['collection_id']

            # Find parent Description
            parent = collection_to_desc.get(collection_id) if collection_id else None
            if not parent:
                skipped_count += 1
                continue

            idno = str(obj['idno']) if obj['idno'] else str(ca_id)
            if self.dry_run:
                self.stdout.write(f"  Would create: {idno} - {obj['title'][:50]}")
                created_count += 1
            else:
                batch.append(Description(
                    repository=parent.repository,
                    parent=parent,
                    description_level='item',
                    reference_code=f"{parent.repository.code}-{idno}",
                    local_identifier=idno,
                    title=obj['title'] or f"Object {ca_id}",
                    ca_object_id=ca_id,
                    is_published=True,
                ))

                if len(batch) >= batch_size:
                    Description.objects.bulk_create(batch, ignore_conflicts=True)
                    created_count += len(batch)
                    self.stdout.write(f"  Created {created_count} objects...")
                    batch = []

        # Final batch
        if batch and not self.dry_run:
            Description.objects.bulk_create(batch, ignore_conflicts=True)
            created_count += len(batch)

        cursor.close()
        conn.close()

        self.stdout.write(self.style.SUCCESS(
            f"Objects: {created_count} created, {skipped_count} skipped"
        ))

    def import_entities(self):
        """Phase 4: Import CA entities as Entity records."""
        self.stdout.write("Importing entities from CA...")

        conn = self.get_ca_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT
                e.entity_id,
                e.idno,
                e.type_id,
                el.displayname,
                el.surname,
                el.forename,
                lit.idno as type_code
            FROM ca_entities e
            JOIN ca_entity_labels el ON e.entity_id = el.entity_id
                AND el.is_preferred = 1
            LEFT JOIN ca_list_items lit ON e.type_id = lit.item_id
            WHERE e.deleted = 0
            ORDER BY e.entity_id
        """
        if self.limit:
            query += f" LIMIT {self.limit}"

        cursor.execute(query)
        entities = cursor.fetchall()

        self.stdout.write(f"  Found {len(entities)} entities")

        created_count = 0
        batch = []
        batch_size = 100  # Smaller batch for SQLite

        for ent in entities:
            ca_id = ent['entity_id']
            type_code = (ent['type_code'] or 'ind').lower()
            entity_type = 'corporate' if type_code == 'org' else 'person'

            display_name = ent['displayname'] or f"Entity {ca_id}"
            # Create sort name: surname, forename
            if ent['surname'] and ent['forename']:
                sort_name = f"{ent['surname']}, {ent['forename']}"
            elif ent['surname']:
                sort_name = ent['surname']
            else:
                sort_name = display_name

            if self.dry_run:
                self.stdout.write(f"  Would create: {display_name}")
                created_count += 1
            else:
                batch.append(Entity(
                    display_name=display_name,
                    sort_name=sort_name,
                    entity_type=entity_type,
                    ca_entity_id=ca_id,
                ))

                if len(batch) >= batch_size:
                    Entity.objects.bulk_create(batch, ignore_conflicts=True)
                    created_count += len(batch)
                    self.stdout.write(f"  Created {created_count} entities...")
                    batch = []

        # Final batch
        if batch and not self.dry_run:
            Entity.objects.bulk_create(batch, ignore_conflicts=True)
            created_count += len(batch)

        cursor.close()
        conn.close()

        self.stdout.write(self.style.SUCCESS(f"Entities: {created_count} created"))

    def import_entity_links(self):
        """Phase 5: Import CA object-entity relationships."""
        self.stdout.write("Importing entity links from CA...")

        conn = self.get_ca_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT
                oxe.relation_id,
                oxe.object_id,
                oxe.entity_id,
                oxe.type_id,
                rt.type_code
            FROM ca_objects_x_entities oxe
            JOIN ca_relationship_types rt ON oxe.type_id = rt.type_id
            ORDER BY oxe.object_id
        """
        if self.limit:
            query += f" LIMIT {self.limit}"

        cursor.execute(query)
        links = cursor.fetchall()

        self.stdout.write(f"  Found {len(links)} entity links")

        # Build mappings
        obj_to_desc = {}
        for desc in Description.objects.filter(ca_object_id__isnull=False).values('id', 'ca_object_id'):
            obj_to_desc[desc['ca_object_id']] = desc['id']

        entity_map = {}
        for ent in Entity.objects.filter(ca_entity_id__isnull=False).values('id', 'ca_entity_id'):
            entity_map[ent['ca_entity_id']] = ent['id']

        created_count = 0
        skipped_count = 0
        batch = []
        batch_size = 1000

        for link in links:
            desc_id = obj_to_desc.get(link['object_id'])
            entity_id = entity_map.get(link['entity_id'])

            if not desc_id or not entity_id:
                skipped_count += 1
                continue

            type_code = (link['type_code'] or 'creator').lower()
            role = ENTITY_ROLE_MAP.get(type_code, 'creator')

            if self.dry_run:
                created_count += 1
            else:
                batch.append(DescriptionEntity(
                    description_id=desc_id,
                    entity_id=entity_id,
                    role=role,
                    ca_relationship_id=link['relation_id'],
                ))

                if len(batch) >= batch_size:
                    DescriptionEntity.objects.bulk_create(batch, ignore_conflicts=True)
                    created_count += len(batch)
                    self.stdout.write(f"  Created {created_count} links...")
                    batch = []

        # Final batch
        if batch and not self.dry_run:
            DescriptionEntity.objects.bulk_create(batch, ignore_conflicts=True)
            created_count += len(batch)

        cursor.close()
        conn.close()

        self.stdout.write(self.style.SUCCESS(
            f"Entity links: {created_count} created, {skipped_count} skipped"
        ))

    def import_places(self):
        """Phase 6: Import CA places (deduplicated)."""
        self.stdout.write("Importing places from CA...")

        conn = self.get_ca_connection()
        cursor = conn.cursor(dictionary=True)

        # Only get places that have object links and aren't garbage
        query = """
            SELECT DISTINCT
                p.place_id,
                pl.name
            FROM ca_places p
            JOIN ca_place_labels pl ON p.place_id = pl.place_id
                AND pl.is_preferred = 1
            JOIN ca_objects_x_places oxp ON p.place_id = oxp.place_id
            WHERE p.deleted = 0
                AND pl.name NOT LIKE '%|%'
                AND pl.name != ''
            ORDER BY pl.name
        """
        if self.limit:
            query += f" LIMIT {self.limit}"

        cursor.execute(query)
        places = cursor.fetchall()

        self.stdout.write(f"  Found {len(places)} usable places")

        # Deduplicate by name
        seen_names = {}
        created_count = 0

        for place in places:
            name = place['name'].strip()
            ca_id = place['place_id']

            if name in seen_names:
                # Add this CA ID to existing place's ca_place_ids
                if not self.dry_run:
                    existing = Place.objects.filter(label=name).first()
                    if existing and ca_id not in existing.ca_place_ids:
                        existing.ca_place_ids.append(ca_id)
                        existing.save()
                continue

            seen_names[name] = ca_id

            if self.dry_run:
                self.stdout.write(f"  Would create: {name}")
                created_count += 1
            else:
                Place.objects.update_or_create(
                    label=name,
                    defaults={
                        'display_name': name,
                        'ca_place_ids': [ca_id],
                        'needs_geocoding': True,
                    }
                )
                created_count += 1

        cursor.close()
        conn.close()

        self.stdout.write(self.style.SUCCESS(f"Places: {created_count} created (deduplicated)"))

    def import_place_links(self):
        """Phase 7: Import CA object-place relationships."""
        self.stdout.write("Importing place links from CA...")

        conn = self.get_ca_connection()
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT
                oxp.relation_id,
                oxp.object_id,
                oxp.place_id,
                pl.name as place_name
            FROM ca_objects_x_places oxp
            JOIN ca_place_labels pl ON oxp.place_id = pl.place_id
                AND pl.is_preferred = 1
            WHERE pl.name NOT LIKE '%|%'
                AND pl.name != ''
            ORDER BY oxp.object_id
        """
        if self.limit:
            query += f" LIMIT {self.limit}"

        cursor.execute(query)
        links = cursor.fetchall()

        self.stdout.write(f"  Found {len(links)} place links")

        # Build mappings
        obj_to_desc = {}
        for desc in Description.objects.filter(ca_object_id__isnull=False).values('id', 'ca_object_id'):
            obj_to_desc[desc['ca_object_id']] = desc['id']

        place_by_name = {}
        for place in Place.objects.values('id', 'label'):
            place_by_name[place['label']] = place['id']

        created_count = 0
        skipped_count = 0
        batch = []
        batch_size = 1000

        for link in links:
            desc_id = obj_to_desc.get(link['object_id'])
            place_name = link['place_name'].strip()
            place_id = place_by_name.get(place_name)

            if not desc_id or not place_id:
                skipped_count += 1
                continue

            if self.dry_run:
                created_count += 1
            else:
                batch.append(DescriptionPlace(
                    description_id=desc_id,
                    place_id=place_id,
                    role='mentioned',  # Default role, CA doesn't have typed place relationships
                    ca_relationship_id=link['relation_id'],
                ))

                if len(batch) >= batch_size:
                    DescriptionPlace.objects.bulk_create(batch, ignore_conflicts=True)
                    created_count += len(batch)
                    self.stdout.write(f"  Created {created_count} links...")
                    batch = []

        # Final batch
        if batch and not self.dry_run:
            DescriptionPlace.objects.bulk_create(batch, ignore_conflicts=True)
            created_count += len(batch)

        cursor.close()
        conn.close()

        self.stdout.write(self.style.SUCCESS(
            f"Place links: {created_count} created, {skipped_count} skipped"
        ))

    def import_denormalize(self):
        """Phase 8: Compute denormalized fields."""
        import time
        import sys

        self.stdout.write("Computing denormalized fields...")

        if self.dry_run:
            self.stdout.write("  Would update creator_display and place_display for all descriptions")
            return

        total_count = Description.objects.count()
        self.stdout.write(f"  Total descriptions to process: {total_count}")

        # Update creator_display
        self.stdout.write("  Updating creator_display...")
        updated = 0
        processed = 0
        start_time = time.time()
        for desc in Description.objects.prefetch_related('entity_links__entity').iterator(chunk_size=1000):
            processed += 1
            creators = desc.entity_links.filter(
                role__in=['creator', 'author']
            ).select_related('entity')[:3]

            if creators:
                creator_names = [c.entity.display_name for c in creators]
                desc.creator_display = '; '.join(creator_names)
                if len(creators) == 3:
                    desc.creator_display += ' et al.'
                desc.save(update_fields=['creator_display'])
                updated += 1

            if processed % 10000 == 0:
                elapsed = time.time() - start_time
                rate = processed / elapsed if elapsed > 0 else 0
                self.stdout.write(f"    Progress: {processed}/{total_count} ({rate:.0f}/sec)")
                sys.stdout.flush()

        self.stdout.write(f"    Updated {updated} descriptions with creator_display")

        # Update place_display
        self.stdout.write("  Updating place_display...")
        updated = 0
        processed = 0
        start_time = time.time()
        for desc in Description.objects.prefetch_related('place_links__place').iterator(chunk_size=1000):
            processed += 1
            places = desc.place_links.select_related('place')[:3]

            if places:
                place_names = [p.place.display_name for p in places]
                desc.place_display = '; '.join(place_names)
                desc.save(update_fields=['place_display'])
                updated += 1

            if processed % 10000 == 0:
                elapsed = time.time() - start_time
                rate = processed / elapsed if elapsed > 0 else 0
                self.stdout.write(f"    Progress: {processed}/{total_count} ({rate:.0f}/sec)")
                sys.stdout.flush()

        self.stdout.write(f"    Updated {updated} descriptions with place_display")

        # Update path_cache (optimized: in-memory parent chain + bulk_update)
        self.stdout.write("  Updating path_cache...")
        start_time = time.time()

        # Step 1: Load all (id, parent_id) into memory - single query
        self.stdout.write("    Loading parent mappings...")
        parent_map = dict(Description.objects.values_list('id', 'parent_id'))
        self.stdout.write(f"    Loaded {len(parent_map)} mappings in {time.time() - start_time:.1f}s")

        # Step 2: Build path for each ID by walking parent chain
        def build_path(desc_id):
            path_ids = [desc_id]
            current = desc_id
            while parent_map.get(current):
                current = parent_map[current]
                path_ids.insert(0, current)
            return '/' + '/'.join(str(x) for x in path_ids) + '/'

        # Step 3: Update in batches using bulk_update
        self.stdout.write("    Computing and saving paths...")
        batch = []
        updated = 0
        truncated = 0
        max_path_len = 0
        max_depth = 0
        batch_size = 1000

        for desc in Description.objects.only('id', 'path_cache').iterator():
            full_path = build_path(desc.id)
            path_len = len(full_path)
            depth = full_path.count('/') - 1

            if path_len > max_path_len:
                max_path_len = path_len
                max_depth = depth

            if path_len > 500:
                self.stdout.write(self.style.WARNING(
                    f"    TRUNCATE: ID {desc.id}, {path_len} chars, depth {depth}"
                ))
                full_path = full_path[:500]
                truncated += 1

            desc.path_cache = full_path
            batch.append(desc)

            if len(batch) >= batch_size:
                Description.objects.bulk_update(batch, ['path_cache'])
                updated += len(batch)
                elapsed = time.time() - start_time
                rate = updated / elapsed if elapsed > 0 else 0
                self.stdout.write(f"    Progress: {updated}/{total_count} ({rate:.0f}/sec)")
                sys.stdout.flush()
                batch = []

        # Final batch
        if batch:
            Description.objects.bulk_update(batch, ['path_cache'])
            updated += len(batch)

        elapsed = time.time() - start_time
        self.stdout.write(f"    Updated {updated} descriptions with path_cache in {elapsed:.1f}s")
        self.stdout.write(f"    Max path: {max_path_len} chars (depth {max_depth})")
        if truncated:
            self.stdout.write(self.style.WARNING(f"    {truncated} paths were truncated"))

        self.stdout.write(self.style.SUCCESS("Denormalization complete"))
