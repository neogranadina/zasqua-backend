"""
Clean PE-BN section titles for hierarchy restructuring.

This command applies automatic cleaning rules and generates a mapping
from original titles to cleaned titles.

Issues addressed:
- Trailing periods inconsistency
- Duplicated/merged section titles
- Typos (e.g., "AÑO 182")
- Inconsistent capitalization (ALL CAPS -> Title Case for book sections)
- Roman numeral formatting

Usage:
    python manage.py clean_pebn_sections --preview
    python manage.py clean_pebn_sections --export
"""

import csv
import re
from collections import defaultdict
from django.core.management.base import BaseCommand
import mysql.connector


CA_DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'abcneogranadina',
    'charset': 'utf8mb4',
}

CA_TABLE_OBJECTS = 57


# Manual fixes for specific typos/errors
MANUAL_FIXES = {
    # Typos
    'AÑO 182': 'Año 1823',  # Missing digit, from context this is 1823

    # Duplicated titles (doubled content)
    'V SANCHEZ CARRION, EL PROTECTORADO DE SAN MARTIN Y EL CONGRESO CONSTITUYENTE V SANCHEZ CARRION, EL PROTECTORADO DE SAN MARTIN Y EL CONGRESO CONSTITUYENTE':
        'V. Sánchez Carrión, el Protectorado de San Martín y el Congreso Constituyente',
    'VII. COMUNICACIONES DE LOS PATRIOTAS PERUANOS CON SAN MARTIN VII. COMUNICACIONES DE LOS PATRIOTAS PERUANOS CON SAN MARTIN':
        'VII. Comunicaciones de los patriotas peruanos con San Martín',
    'VIII SANCHEZ CARRION Y El CONGRESO DE PANAMA IX CARTAS DE SANCHEZ CARRION A BOLIVAR':
        'VIII. Sánchez Carrión y el Congreso de Panamá',  # Merged sections - take first

    # Merged section titles (two sections joined)
    'IV - V.ISCARDO Y LA REBELION DE TUPAC AMARU V - INGLATERRA Y LOS PLANES REVOLUCIONARIOS DE VISCARDO':
        'IV. Viscardo y la rebelión de Túpac Amaru',
    'V - INGLATERRA Y LOS PLANES REVOLUCIONARIOS DE VISCARDO VI - TESTIMONI O DE VISCARDO SOBRE LA ASAMBLEA DE OBISPOS DE TOSCANA':
        'V. Inglaterra y los planes revolucionarios de Viscardo',
}

# Words that should stay lowercase in title case (except when first word)
LOWERCASE_WORDS = {'de', 'del', 'la', 'las', 'los', 'el', 'en', 'y', 'a', 'por', 'con', 'sobre', 'entre', 'para', 'que', 'se'}

# Roman numerals to preserve
ROMAN_NUMERALS = {'I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 'XI', 'XII', 'XIII', 'XIV', 'XV', 'XVI', 'XVII', 'XVIII', 'XIX', 'XX', 'XXI', 'XXII', 'XXIII', 'XXIV', 'XXV'}

# Additional manual fixes for complex cases
ADDITIONAL_FIXES = {
    # Duplicated sections
    'III - LAS HERENCIAS DE LOS HERMANOS VISCARDO III - LAS HERENCIAS DE LOS HERMANOS VISCARDO':
        'III. Las herencias de los hermanos Viscardo',

    # Sections with Roman numerals - format consistently
    'I - LA FAMILIA DE JUAN PABLO VISCARDO Y GUZMAN':
        'I. La familia de Juan Pablo Viscardo y Guzmán',
    'II - VISCARDO Y LA COMPAÑIA DE JESUS':
        'II. Viscardo y la Compañía de Jesús',
    'III - LAS HERENCIAS DE LOS HERMANOS VISCARDO':
        'III. Las herencias de los hermanos Viscardo',
    'IV - V.ISCARDO Y LA REBELION DE TUPAC AMARU':
        'IV. Viscardo y la rebelión de Túpac Amaru',
    'V - INGLATERRA Y LOS PLANES REVOLUCIONARIOS DE VISCARDO':
        'V. Inglaterra y los planes revolucionarios de Viscardo',
    'VI - TESTIMONIO DE VISCARDO SOBRE LA ASAMBLEA DE OBISPOS DE TOSCANA':
        'VI. Testimonio de Viscardo sobre la asamblea de obispos de Toscana',
    'VII - MIRANDA, VISCARDO Y LA DIFUSION DE LA "CARTA"':
        'VII. Miranda, Viscardo y la difusión de la "Carta"',
    'VIII - PAPELES ATRIBUIDOS A VISCARDO QUE SE ENCUENTRAN EN EL ARCHIVO DE MIRANDA':
        'VIII. Papeles atribuidos a Viscardo que se encuentran en el archivo de Miranda',
    'IX - PRIMERAS EDICIONES DE LA "CARTA A LOS ESPAÑOLES AMERICANOS"':
        'IX. Primeras ediciones de la "Carta a los españoles americanos"',
    'X - RESEÑAS BIBLIOGRAFICAS DE LA "CARTA"':
        'X. Reseñas bibliográficas de la "Carta"',
    'XI - VERSIONES MANUSCRITAS CONTEMPORANEAS DE LA "CARTA"':
        'XI. Versiones manuscritas contemporáneas de la "Carta"',
    'XII - INFLUENCIA DE LA "CARTA" EN LOS DOCUMENTOS DE LA INDEPENDENCIA HISPANOAMERICANA':
        'XII. Influencia de la "Carta" en los documentos de la Independencia hispanoamericana',
    'XIII - ADDENDA':
        'XIII. Addenda',

    # Complex case remaining
    'VI- DOCUMENTOS SOBRE EL TRIBUNAL DEL PROTOMEDICATO':
        'VI. Documentos sobre el Tribunal del Protomedicato',

    # Numbered sections (Unanue)
    '1.-MEDICINA':
        '1. Medicina',
    '2.-CIENCIAS NATURALES':
        '2. Ciencias naturales',
    '3.-HISTORIA Y GEOGRAFIA':
        '3. Historia y geografía',
    '4.-DISCURSOS ACADEMICOS':
        '4. Discursos académicos',
    '5.-LITERATURA':
        '5. Literatura',
    '6.-POLITICA Y ADMINISTRACION PUBLICA':
        '6. Política y administración pública',

    # Year headers
    'AÑO 1823': 'Año 1823',
    'AÑO 1824': 'Año 1824',
    'AÑO 1825': 'Año 1825',
    'AÑO 1826': 'Año 1826',

    # Capítulo with mixed caps
    'Capítulo I - FOJAS DE SERVICIO - Patriotas': 'Capítulo I. Fojas de servicio - Patriotas',
    'Capítulo I - FOJAS DE SERVICIO - Realistas': 'Capítulo I. Fojas de servicio - Realistas',
    'Capítulo II - LISTAS DE COMISARIOS - AÑOS 1823-1824-1825 - Año 1824 - II. Fuerzas Militares Auxiliares':
        'Capítulo II. Listas de comisarios - Años 1823-1824-1825 - Año 1824 - II. Fuerzas militares auxiliares',
}


class Command(BaseCommand):
    help = 'Clean PE-BN section titles'

    def add_arguments(self, parser):
        parser.add_argument(
            '--preview',
            action='store_true',
            help='Preview changes without exporting'
        )
        parser.add_argument(
            '--export',
            action='store_true',
            help='Export cleaning mappings to CSV'
        )

    def handle(self, *args, **options):
        preview = options['preview']
        export = options['export']

        # Fetch current section titles from CA
        conn = mysql.connector.connect(**CA_DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        query = """
            SELECT DISTINCT av.value_longtext1 as title
            FROM ca_attributes a
            JOIN ca_attribute_values av ON a.attribute_id = av.attribute_id
            JOIN ca_metadata_elements me ON a.element_id = me.element_id
            WHERE a.table_num = %s
                AND me.element_code = 'narra_secc_titulo'
                AND av.value_longtext1 IS NOT NULL
                AND av.value_longtext1 != ''
        """
        cursor.execute(query, [CA_TABLE_OBJECTS])

        original_titles = [row['title'].strip() for row in cursor.fetchall()]
        cursor.close()
        conn.close()

        self.stdout.write(f"Found {len(original_titles)} unique section titles")

        # Apply cleaning
        mappings = []
        for orig in original_titles:
            cleaned = self.clean_title(orig)
            change_type = self.classify_change(orig, cleaned)
            mappings.append({
                'original': orig,
                'cleaned': cleaned,
                'change_type': change_type,
                'needs_review': change_type in ['manual_fix', 'complex']
            })

        # Sort by change type for easier review
        mappings.sort(key=lambda x: (x['change_type'], x['original']))

        # Statistics
        unchanged = sum(1 for m in mappings if m['change_type'] == 'unchanged')
        normalized = sum(1 for m in mappings if m['change_type'] == 'normalized')
        manual = sum(1 for m in mappings if m['change_type'] == 'manual_fix')
        complex_changes = sum(1 for m in mappings if m['change_type'] == 'complex')

        self.stdout.write(f"\nChanges:")
        self.stdout.write(f"  Unchanged: {unchanged}")
        self.stdout.write(f"  Normalized (case/periods): {normalized}")
        self.stdout.write(f"  Manual fixes applied: {manual}")
        self.stdout.write(f"  Complex (needs review): {complex_changes}")

        if preview:
            self.stdout.write("\n=== Preview of changes ===\n")

            for change_type in ['manual_fix', 'complex', 'normalized']:
                type_mappings = [m for m in mappings if m['change_type'] == change_type]
                if type_mappings:
                    self.stdout.write(f"\n--- {change_type.upper()} ({len(type_mappings)}) ---\n")
                    for m in type_mappings[:20]:  # Show first 20 of each type
                        self.stdout.write(f"  OLD: {m['original'][:80]}")
                        self.stdout.write(f"  NEW: {m['cleaned'][:80]}")
                        self.stdout.write("")
                    if len(type_mappings) > 20:
                        self.stdout.write(f"  ... and {len(type_mappings) - 20} more")

        if export:
            self.export_mappings(mappings)

    def clean_title(self, title):
        """Apply cleaning rules to a section title."""
        # Check for manual fixes first (merged dictionaries)
        all_fixes = {**MANUAL_FIXES, **ADDITIONAL_FIXES}
        if title in all_fixes:
            return all_fixes[title]

        cleaned = title.strip()

        # Remove trailing periods (but keep ellipsis)
        if cleaned.endswith('.') and not cleaned.endswith('...'):
            cleaned = cleaned[:-1].strip()

        # Normalize whitespace
        cleaned = re.sub(r'\s+', ' ', cleaned)

        # Check if mostly CAPS (convert to sentence case)
        # Count uppercase letters vs total letters
        letters = [c for c in cleaned if c.isalpha()]
        if letters:
            upper_ratio = sum(1 for c in letters if c.isupper()) / len(letters)
            # If more than 60% uppercase and at least 5 letters, convert
            # Skip only if it's JUST a Roman numeral (e.g., "I", "II", "Tomo I")
            is_just_roman = re.match(r'^(Tomo\s+)?[IVXL]+\.?$', cleaned.strip())
            if upper_ratio > 0.6 and len(letters) >= 5 and not is_just_roman:
                cleaned = self.title_case_spanish(cleaned)

        # Fix Roman numeral formatting (add period after if followed by space and word)
        cleaned = re.sub(r'^(I{1,3}|IV|VI{0,3}|IX|XI{0,3}|XIV|XV|XVI{0,3}|XIX|XX|XXI{0,3}|XXIV|XXV)\s+([A-ZÁÉÍÓÚÑÜ])',
                         lambda m: f"{m.group(1)}. {m.group(2)}", cleaned)

        # Fix spaced Roman numerals with dash
        cleaned = re.sub(r'^(I{1,3}|IV|VI{0,3}|IX|XI{0,3}|XIV|XXI{0,3}|XXIV|XXV)\s*[-—]\s*',
                         lambda m: f"{m.group(1)}. ", cleaned)

        return cleaned

    def title_case_spanish(self, text):
        """Convert ALL CAPS to sentence case (first word + proper nouns capitalized)."""
        # Split but keep delimiters like periods, colons
        parts = re.split(r'([.:\-])\s*', text)
        result_parts = []

        for part_idx, part in enumerate(parts):
            # Skip delimiters
            if part in '.:-' or not part.strip():
                result_parts.append(part)
                continue

            words = part.split()
            result_words = []

            for i, word in enumerate(words):
                word_upper = word.upper()
                # Strip punctuation for checking
                word_base = re.sub(r'[.,;:()"\']', '', word)
                word_base_lower = word_base.lower()

                # Keep Roman numerals
                if word_upper in ROMAN_NUMERALS or re.match(r'^[IVXL]+\.?$', word_upper):
                    result_words.append(word_upper.rstrip('.'))
                # First word of section - capitalize
                elif i == 0:
                    result_words.append(self.capitalize_preserving_punct(word))
                # Lowercase articles/prepositions
                elif word_base_lower in LOWERCASE_WORDS:
                    result_words.append(word.lower())
                # Proper nouns (check common ones) - strip punctuation for check
                elif self.is_proper_noun(word_base):
                    result_words.append(self.capitalize_preserving_punct(word))
                # Everything else - lowercase
                else:
                    result_words.append(word.lower())

            result_parts.append(' '.join(result_words))

        # Join parts back together
        result = ''
        for i, part in enumerate(result_parts):
            if i > 0 and result_parts[i-1] in '.:-':
                # Add space before content after delimiter
                if part.strip():
                    result += ' ' + part
                else:
                    result += part
            else:
                result += part

        # Normalize dash spacing (ensure " - " pattern)
        result = re.sub(r'\s*-\s*', ' - ', result)

        return result

    def capitalize_preserving_punct(self, word):
        """Capitalize a word while preserving trailing punctuation."""
        # Find trailing punctuation
        match = re.match(r'^(.+?)([.,;:()"\']*)$', word)
        if match:
            base = match.group(1)
            punct = match.group(2)
            return base.capitalize() + punct
        return word.capitalize()

    def is_proper_noun(self, word):
        """Check if word is a proper noun that should be capitalized."""
        # Common proper nouns in this corpus
        proper_nouns = {
            # Countries
            'peru', 'perú', 'españa', 'chile', 'argentina', 'colombia',
            'mexico', 'méxico', 'brasil', 'bolivia', 'ecuador', 'venezuela',
            'inglaterra', 'toscana',

            # Peruvian cities/places
            'lima', 'cuzco', 'cusco', 'arequipa', 'trujillo', 'huanuco',
            'huánuco', 'pasco', 'jauja', 'huancayo', 'huamanga', 'ayacucho',
            'ica', 'tacna', 'puno', 'junin', 'junín', 'chancay', 'huaraz',
            'huariaca', 'huamachuco', 'caraz', 'cerro', 'tarma', 'piura',
            'huancavelica', 'tongos', 'pampas', 'colcabamba',
            'lambayeque', 'cajamarca', 'chachapoyas', 'moyobamba', 'loreto',
            'panataguas', 'huamalíes', 'humalíes', 'huamalies', 'humalies',

            # Other places
            'panamá', 'panama', 'andes', 'cadiz', 'cádiz', 'madrid',

            # Historic figures (surnames)
            'san', 'martin', 'martín', 'bolivar', 'bolívar', 'cochrane',
            'arenales', 'pezuela', 'abascal', 'viscardo', 'miranda',
            'vidaurre', 'unanue', 'pumaccahua', 'tupac', 'túpac', 'amaru',
            'sanchez', 'sánchez', 'carrion', 'carrión', 'serna', 'laserna',
            'osorno', 'aviles', 'avilés', 'concordia', 'taboada', 'torrente',

            # First names (common ones)
            'marcos', 'fernando', 'carlos', 'juan', 'pedro', 'jose', 'josé',
            'francisco', 'maria', 'maría', 'isabel', 'diego', 'manuel',
            'antonio', 'gabriel', 'hipólito', 'hipolito', 'simón', 'simon',
            'ambrosio', 'frey', 'marques', 'marqués',

            # Institutions
            'cdip',
        }
        return word.lower() in proper_nouns

    def classify_change(self, original, cleaned):
        """Classify the type of change made."""
        if original == cleaned:
            return 'unchanged'

        all_fixes = {**MANUAL_FIXES, **ADDITIONAL_FIXES}
        if original in all_fixes:
            return 'manual_fix'

        # Simple normalization (case, periods, whitespace)
        simple_orig = re.sub(r'[\s.]+', '', original.lower())
        simple_clean = re.sub(r'[\s.]+', '', cleaned.lower())

        if simple_orig == simple_clean:
            return 'normalized'

        return 'complex'

    def export_mappings(self, mappings):
        """Export cleaning mappings to CSV."""
        output_path = '/Users/juancobo/Databases/zasqua/catalogues/pebn/section_title_mappings.csv'

        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['original_title', 'cleaned_title', 'change_type', 'needs_review'])

            for m in mappings:
                writer.writerow([
                    m['original'],
                    m['cleaned'],
                    m['change_type'],
                    'YES' if m['needs_review'] else ''
                ])

        self.stdout.write(f"\nExported to: {output_path}")
