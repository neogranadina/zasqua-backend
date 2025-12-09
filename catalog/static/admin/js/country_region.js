/**
 * Country/Region field handler for Repository admin.
 * Shows department dropdown for Colombia, text field for other countries.
 */
(function() {
    'use strict';

    const COLOMBIA_DEPARTMENTS = [
        ['', '---------'],
        ['amazonas', 'Amazonas'],
        ['antioquia', 'Antioquia'],
        ['arauca', 'Arauca'],
        ['atlantico', 'Atlántico'],
        ['bogota', 'Bogotá, D.C.'],
        ['bolivar', 'Bolívar'],
        ['boyaca', 'Boyacá'],
        ['caldas', 'Caldas'],
        ['caqueta', 'Caquetá'],
        ['casanare', 'Casanare'],
        ['cauca', 'Cauca'],
        ['cesar', 'Cesar'],
        ['choco', 'Chocó'],
        ['cordoba', 'Córdoba'],
        ['cundinamarca', 'Cundinamarca'],
        ['guainia', 'Guainía'],
        ['guaviare', 'Guaviare'],
        ['huila', 'Huila'],
        ['guajira', 'La Guajira'],
        ['magdalena', 'Magdalena'],
        ['meta', 'Meta'],
        ['narino', 'Nariño'],
        ['norte_santander', 'Norte de Santander'],
        ['putumayo', 'Putumayo'],
        ['quindio', 'Quindío'],
        ['risaralda', 'Risaralda'],
        ['san_andres', 'San Andrés y Providencia'],
        ['santander', 'Santander'],
        ['sucre', 'Sucre'],
        ['tolima', 'Tolima'],
        ['valle', 'Valle del Cauca'],
        ['vaupes', 'Vaupés'],
        ['vichada', 'Vichada'],
    ];

    function init() {
        const countryField = document.getElementById('id_country_code');
        const regionField = document.getElementById('id_region');

        if (!countryField || !regionField) return;

        const regionRow = regionField.closest('.form-row');
        const currentValue = regionField.value;

        // Create select element for departments
        const selectEl = document.createElement('select');
        selectEl.id = 'id_region_select';
        selectEl.name = 'region';
        selectEl.className = regionField.className;

        COLOMBIA_DEPARTMENTS.forEach(([value, label]) => {
            const option = document.createElement('option');
            option.value = value;
            option.textContent = label;
            if (value === currentValue) option.selected = true;
            selectEl.appendChild(option);
        });

        // Create text input for other countries
        const textEl = document.createElement('input');
        textEl.type = 'text';
        textEl.id = 'id_region_text';
        textEl.name = 'region';
        textEl.className = regionField.className;
        textEl.maxLength = 255;
        textEl.value = currentValue;

        // Hide original field
        regionField.style.display = 'none';
        regionField.name = 'region_original';

        // Insert new fields
        regionField.parentNode.insertBefore(selectEl, regionField.nextSibling);
        regionField.parentNode.insertBefore(textEl, regionField.nextSibling);

        function updateRegionField() {
            const isColombia = countryField.value === 'COL';
            selectEl.style.display = isColombia ? '' : 'none';
            textEl.style.display = isColombia ? 'none' : '';

            // Update which field submits
            selectEl.name = isColombia ? 'region' : '';
            textEl.name = isColombia ? '' : 'region';
        }

        countryField.addEventListener('change', updateRegionField);
        updateRegionField();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
