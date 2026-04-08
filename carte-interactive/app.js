/* ============================================================================
   Cahier d'enquête — ICPE en Gironde
   Map logic: CSV loading, filter compilation, color switching, layer control.
   No framework. Vanilla JS. Designed for speed at 2,888 markers.
============================================================================ */

(function () {
  'use strict';

  // ---------- constants ----------
  const CSV_URL = 'carte-interactive/data/liste-icpe-gironde_enrichi.csv';
  const RNN_URL = 'carte-interactive/data/reserves-naturelles-nationales.geojson';
  const RNR_URL = 'carte-interactive/data/reserves-naturelles-regionales.geojson';
  const GIRONDE_CONTOUR_URL = 'carte-interactive/data/gironde-contour.geojson';
  const GIRONDE_COMMUNES_URL = 'carte-interactive/data/gironde-communes.geojson';
  const SUGGESTION_LIMIT_PER_GROUP = 5;

  const CSS = (() => {
    const s = getComputedStyle(document.documentElement);
    const get = (k) => s.getPropertyValue(k).trim();
    return {
      ink: get('--ink'),
      paper: get('--paper'),
      rust: get('--rust'),
      ochre: get('--ochre'),
      lead: get('--lead'),
      fog: get('--fog'),
      rustDeep: get('--rust-deep'),
      rustMid: get('--rust-mid'),
      moss: get('--moss'),
      mossDeep: get('--moss-deep'),
      olive: get('--olive'),
      oliveDeep: get('--olive-deep'),
      copper: get('--copper'),
      azur: get('--azur'),
      rule: get('--rule'),
    };
  })();

  // ---------- color palette per dimension ----------
  const PALETTE = {
    regime: {
      AUTORISATION: CSS.rust,
      ENREGISTREMENT: CSS.ochre,
      NON_ICPE: CSS.lead,
      AUTRE: CSS.fog,
    },
    seveso: {
      SEUIL_HAUT: CSS.rustDeep,
      SEUIL_BAS: CSS.rustMid,
      NON_SEVESO: CSS.lead,
      '': CSS.fog, // non classé
    },
    priority: {
      true: CSS.copper,
      false: CSS.fog,
    },
    ied: {
      true: CSS.azur,
      false: CSS.fog,
    },
    secteur: {
      industrie: CSS.ink,
      carriere: CSS.ochre,
      autre: CSS.fog,
    },
  };

  const LEGEND_LABELS = {
    regime: [
      ['Autorisation', CSS.rust],
      ['Enregistrement', CSS.ochre],
      ['Non-ICPE', CSS.lead],
      ['Autre', CSS.fog],
    ],
    seveso: [
      ['Seuil haut', CSS.rustDeep],
      ['Seuil bas', CSS.rustMid],
      ['Non Seveso', CSS.lead],
      ['Non classé', CSS.fog],
    ],
    priority: [
      ['Priorité nationale', CSS.copper],
      ['Autre', CSS.fog],
    ],
    ied: [
      ['IED', CSS.azur],
      ['Autre', CSS.fog],
    ],
    secteur: [
      ['Industrie', CSS.ink],
      ['Carrière', CSS.ochre],
      ['Autre', CSS.fog],
    ],
  };

  const DIM_HUMAN = {
    regime: 'Régime',
    seveso: 'Seveso',
    priority: 'Priorité nationale',
    ied: 'IED',
    secteur: 'Secteur',
  };

  // NAF Rev 2 division labels for popup activité affichage
  // (subset — just the divisions that actually appear in the dataset)
  const NAF_DIVISIONS = {
    '1': 'Agriculture, chasse et services annexes',
    '2': 'Sylviculture et exploitation forestière',
    '3': 'Pêche et aquaculture',
    '5': 'Extraction de houille et de lignite',
    '6': 'Extraction d\'hydrocarbures',
    '7': 'Extraction de minerais métalliques',
    '8': 'Autres industries extractives',
    '9': 'Services de soutien aux industries extractives',
    '10': 'Industries alimentaires',
    '11': 'Fabrication de boissons',
    '13': 'Fabrication de textiles',
    '14': 'Industrie de l\'habillement',
    '15': 'Industrie du cuir',
    '16': 'Travail du bois',
    '17': 'Industrie du papier et du carton',
    '18': 'Imprimerie et reproduction',
    '19': 'Cokéfaction et raffinage',
    '20': 'Industrie chimique',
    '21': 'Industrie pharmaceutique',
    '22': 'Fabrication de produits en caoutchouc et en plastique',
    '23': 'Fabrication d\'autres produits minéraux non métalliques',
    '24': 'Métallurgie',
    '25': 'Fabrication de produits métalliques',
    '26': 'Fabrication de produits informatiques, électroniques',
    '27': 'Fabrication d\'équipements électriques',
    '28': 'Fabrication de machines et équipements',
    '29': 'Industrie automobile',
    '30': 'Fabrication d\'autres matériels de transport',
    '31': 'Fabrication de meubles',
    '32': 'Autres industries manufacturières',
    '33': 'Réparation et installation de machines',
    '35': 'Production et distribution d\'électricité, gaz',
    '36': 'Captage, traitement et distribution d\'eau',
    '37': 'Collecte et traitement des eaux usées',
    '38': 'Collecte, traitement et élimination des déchets',
    '39': 'Dépollution et autres services',
    '41': 'Construction de bâtiments',
    '42': 'Génie civil',
    '43': 'Travaux de construction spécialisés',
    '45': 'Commerce et réparation d\'automobiles',
    '46': 'Commerce de gros',
    '47': 'Commerce de détail',
    '49': 'Transports terrestres',
    '52': 'Entreposage et services auxiliaires des transports',
    '56': 'Restauration',
    '68': 'Activités immobilières',
    '77': 'Activités de location et location-bail',
    '81': 'Services relatifs aux bâtiments et aménagement paysager',
    '84': 'Administration publique et défense',
    '85': 'Enseignement',
    '86': 'Activités pour la santé humaine',
    '91': 'Bibliothèques, archives, musées',
    '93': 'Activités sportives, récréatives',
    '96': 'Autres services personnels',
  };

  const REGIME_LABEL = {
    AUTORISATION: 'Autorisation',
    ENREGISTREMENT: 'Enregistrement',
    NON_ICPE: 'Non-ICPE',
    AUTRE: 'Autre',
  };
  const REGIME_BADGE = {
    AUTORISATION: 'badge--rust',
    ENREGISTREMENT: 'badge--ochre',
    NON_ICPE: 'badge--lead',
    AUTRE: 'badge--fog',
  };
  const SEVESO_LABEL = {
    SEUIL_HAUT: 'Seveso seuil haut',
    SEUIL_BAS: 'Seveso seuil bas',
    NON_SEVESO: 'Non Seveso',
  };
  const SEVESO_BADGE = {
    SEUIL_HAUT: 'badge--rust-deep',
    SEUIL_BAS: 'badge--rust-mid',
    NON_SEVESO: 'badge--lead',
  };

  // ---------- state ----------
  const state = {
    rows: [],
    visibleRows: [],
    colorDim: 'regime',
    filters: {
      freeSearch: '',
      regime: new Set(['AUTORISATION', 'ENREGISTREMENT', 'NON_ICPE', 'AUTRE']),
      seveso: new Set(['SEUIL_HAUT', 'SEUIL_BAS', 'NON_SEVESO', '']),
      priority: 'all',
      ied: 'all',
      secteur: new Set(), // empty = no secteur filter; populated = OR of active secteurs
      // Pill-based filters (OR within each Set, AND across sets)
      commune: new Set(),   // Set<INSEE code>  e.g. '33063'
      epci: new Set(),      // Set<EPCI siren>  e.g. '243300316'
      structure: new Set(), // Set<normalised structure name>
      // Month window filter (disabled by default)
      monthEnabled: false,
      month: null,
    },
    mdateMax: null,
    // month keys derived from the dataset — set after CSV load
    monthSteps: [],
  };

  // Reference data for suggestions & EPCI checkbox list.
  // Populated at init from gironde-commune-epci.json + the CSV itself.
  const reference = {
    communes: [],   // [{code, nom, norm, epci_siren, epci_nom}]
    epcis: [],      // [{code, nom, norm, commune_count}]
    structures: [], // [{name, norm, count}]
    communeByInsee: new Map(),
  };

  function monthKey(isoDate) {
    // isoDate like "2025-02-10T..." → "2025-02"
    if (!isoDate || isoDate.length < 7) return '';
    return isoDate.substring(0, 7);
  }
  const MONTHS_FR = [
    'janv.', 'févr.', 'mars', 'avr.', 'mai', 'juin',
    'juil.', 'août', 'sept.', 'oct.', 'nov.', 'déc.',
  ];
  function formatMonthFR(ymkey) {
    // ymkey like "2025-02" → "févr. 2025"
    if (!ymkey) return '—';
    const [y, m] = ymkey.split('-');
    const idx = parseInt(m, 10) - 1;
    if (idx < 0 || idx > 11) return ymkey;
    return `${MONTHS_FR[idx]} ${y}`;
  }

  // ---------- utilities ----------
  const nfFR = new Intl.NumberFormat('fr-FR');
  function formatCount(n) { return nfFR.format(n); }

  function formatDateFR(iso) {
    if (!iso) return '—';
    const d = new Date(iso);
    if (isNaN(d)) return '—';
    return d.toLocaleDateString('fr-FR', { day: '2-digit', month: '2-digit', year: 'numeric' });
  }

  function escapeHTML(s) {
    if (s == null) return '';
    return String(s).replace(/[&<>"']/g, (c) => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    })[c]);
  }

  // Normalise a string for search: lowercase + strip diacritics + collapse
  // whitespace. Lets the user type "reserve" and match "RÉSERVE".
  function normaliseForSearch(s) {
    if (!s) return '';
    return String(s)
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '') // remove combining diacritical marks
      .toLowerCase()
      .replace(/\s+/g, ' ')
      .trim();
  }

  // Only allow http(s) URLs in href attributes — blocks javascript:, data:, etc.
  function safeHref(url) {
    if (!url) return '';
    try {
      const u = new URL(url);
      if (u.protocol !== 'https:' && u.protocol !== 'http:') return '';
      return escapeHTML(url);
    } catch (_) {
      return '';
    }
  }

  async function fetchJSON(url) {
    const res = await fetch(url);
    if (!res.ok) throw new Error(`fetch ${url}: ${res.status}`);
    return res.json();
  }

  // ---------- CSV loading (main thread; worker mode has silent-failure issues) ----------
  async function parseCSV() {
    const res = await fetch(CSV_URL);
    if (!res.ok) throw new Error(`CSV fetch ${res.status}`);
    const text = await res.text();
    const result = Papa.parse(text, {
      header: true,
      skipEmptyLines: true,
    });
    if (result.errors && result.errors.length > 0) {
      console.warn('PapaParse errors:', result.errors.slice(0, 5));
    }
    return result.data;
  }

  function transformRows(rawRows) {
    // Transform each CSV row into a compact object with pre-computed colors.
    // The CSV is the aliased enrichment output produced by
    // scripts/enrichir_libelles.py — see data/metadonnees_colonnes.csv for
    // the column dictionary.
    const rows = [];
    let mdateMax = null;
    for (const r of rawRows) {
      const geoPoint = r.coordonnees_lat_lon;
      if (!geoPoint) continue;
      const parts = geoPoint.split(',');
      if (parts.length !== 2) continue;
      const lat = parseFloat(parts[0]);
      const lon = parseFloat(parts[1]);
      if (!isFinite(lat) || !isFinite(lon)) continue;

      const regime = r.regime_icpe || 'AUTRE';
      const seveso = (r.categorie_seveso || '').trim();
      const priority = r.priorite_nationale === 'TRUE';
      const ied = r.directive_ied === 'TRUE';
      const industrie = r.activite_industrielle === 'TRUE';
      const carriere = r.activite_carriere === 'TRUE';
      const libelle = (r.nom_complet || r.nom_original || '(sans nom)').trim();
      const structure = (r.structure || '').trim();
      const structure_norm = normaliseForSearch(structure);
      const etablissement = (r.etablissement || '').trim();
      const insee = (r.code_insee_commune || '').trim();
      // Commune / EPCI are now carried directly in the enriched CSV
      // (scripts/enrichir_libelles.py joins on code INSEE before export).
      const commune_nom = (r.nom_commune || '').trim();
      const epci_siren = (r.epci_siren || '').trim();
      const epci_nom = (r.epci_nom || '').trim();

      // pre-compute per-dimension color
      const color = {
        regime: PALETTE.regime[regime] || CSS.fog,
        seveso: PALETTE.seveso[seveso] || CSS.fog,
        priority: priority ? PALETTE.priority.true : PALETTE.priority.false,
        ied: ied ? PALETTE.ied.true : PALETTE.ied.false,
        secteur: industrie ? PALETTE.secteur.industrie
                 : carriere ? PALETTE.secteur.carriere
                 : PALETTE.secteur.autre,
      };

      // date_enregistrement is the only date column in the aliased bulk export.
      // (mdate/cdate were strict duplicates and the pipeline dropped the extra.)
      const dateEnreg = r.date_enregistrement || '';
      // Track latest date via Date() comparison — robust against non-padded ISO.
      if (dateEnreg) {
        const d = new Date(dateEnreg);
        if (!isNaN(d) && (!mdateMax || d > new Date(mdateMax))) mdateMax = dateEnreg;
      }
      const cdate_month = monthKey(dateEnreg);

      rows.push({
        lat, lon,
        libelle,
        structure,
        structure_norm,
        etablissement,
        insee,
        commune_nom,
        epci_siren,
        epci_nom,
        // Search index: all the human-facing strings a journalist might type.
        // Accent-stripped and lowercased so "reserve" matches "RÉSERVE" etc.
        // Includes commune + EPCI names so free-text search finds them.
        search_index: normaliseForSearch(
          [libelle, structure, etablissement, r.siret, insee, commune_nom, epci_nom]
            .filter(Boolean).join(' ')
        ),
        regime,
        seveso,
        priority,
        ied,
        industrie,
        carriere,
        cdate_month,
        fiche: r.url_fiche_georisques || '',
        siret: r.siret || '',
        insee: r.code_insee_commune || '',
        date_enregistrement: dateEnreg,
        activite: (r.code_naf_division || '').toString(),
        isSeveso: seveso === 'SEUIL_HAUT' || seveso === 'SEUIL_BAS',
        color,
      });
    }
    state.mdateMax = mdateMax;
    return rows;
  }

  // ---------- reference data (communes / EPCIs / structures) ----------
  function buildReferenceData() {
    // Communes: deduplicated by INSEE, with a row count so the user sees
    // which ones actually have sites (useful for "why is my filter empty?")
    const communeMap = new Map(); // insee → {code, nom, norm, epci_siren, epci_nom, count}
    const epciMap = new Map();    // siren → {code, nom, norm, commune_count, site_count, communes: Set}
    const structureMap = new Map(); // norm → {name, norm, count}

    for (const row of state.rows) {
      if (row.insee && row.commune_nom) {
        const c = communeMap.get(row.insee);
        if (c) {
          c.count++;
        } else {
          communeMap.set(row.insee, {
            code: row.insee,
            nom: row.commune_nom,
            norm: normaliseForSearch(row.commune_nom),
            epci_siren: row.epci_siren,
            epci_nom: row.epci_nom,
            count: 1,
          });
        }
      }
      if (row.epci_siren && row.epci_nom) {
        const e = epciMap.get(row.epci_siren);
        if (e) {
          e.site_count++;
          e.communes.add(row.insee);
        } else {
          epciMap.set(row.epci_siren, {
            code: row.epci_siren,
            nom: row.epci_nom,
            norm: normaliseForSearch(row.epci_nom),
            site_count: 1,
            communes: new Set([row.insee]),
          });
        }
      }
      if (row.structure && row.structure_norm) {
        const s = structureMap.get(row.structure_norm);
        if (s) {
          s.count++;
        } else {
          structureMap.set(row.structure_norm, {
            name: row.structure,
            norm: row.structure_norm,
            count: 1,
          });
        }
      }
    }

    reference.communes = [...communeMap.values()].sort((a, b) => a.nom.localeCompare(b.nom, 'fr'));
    reference.epcis = [...epciMap.values()].sort((a, b) => a.nom.localeCompare(b.nom, 'fr'));
    // Structures: only those with at least 2 sites are useful as a filter
    // option (otherwise they're single sites and the site search handles them)
    reference.structures = [...structureMap.values()]
      .filter((s) => s.count >= 2)
      .sort((a, b) => b.count - a.count);
    reference.communeByInsee = communeMap;
  }

  // ---------- suggestion engine ----------
  function getSuggestions(query) {
    const tokens = normaliseForSearch(query).split(' ').filter(Boolean);
    if (tokens.length === 0) return [];
    const matches = (norm) => {
      for (const t of tokens) if (!norm.includes(t)) return false;
      return true;
    };
    const groups = [];
    // Communes
    const commHits = [];
    for (const c of reference.communes) {
      if (matches(c.norm) && !state.filters.commune.has(c.code)) {
        commHits.push(c);
        if (commHits.length >= SUGGESTION_LIMIT_PER_GROUP) break;
      }
    }
    if (commHits.length) groups.push({ type: 'commune', label: 'Communes', items: commHits });
    // EPCI
    const epciHits = [];
    for (const e of reference.epcis) {
      if (matches(e.norm) && !state.filters.epci.has(e.code)) {
        epciHits.push(e);
        if (epciHits.length >= SUGGESTION_LIMIT_PER_GROUP) break;
      }
    }
    if (epciHits.length) groups.push({ type: 'epci', label: 'EPCI', items: epciHits });
    // Structures (entreprises)
    const structHits = [];
    for (const s of reference.structures) {
      if (matches(s.norm) && !state.filters.structure.has(s.norm)) {
        structHits.push(s);
        if (structHits.length >= SUGGESTION_LIMIT_PER_GROUP) break;
      }
    }
    if (structHits.length) groups.push({ type: 'structure', label: 'Structures', items: structHits });
    // Sites (individual ICPE)
    const siteHits = [];
    for (const row of state.rows) {
      if (matches(row.search_index)) {
        siteHits.push(row);
        if (siteHits.length >= SUGGESTION_LIMIT_PER_GROUP) break;
      }
    }
    if (siteHits.length) groups.push({ type: 'site', label: 'Sites', items: siteHits });
    return groups;
  }

  // ---------- filter predicate ----------
  function buildPredicate() {
    const f = state.filters;
    // Free-text (substring) search — token-wise AND, accent-insensitive.
    const searchTokens = normaliseForSearch(f.freeSearch).split(' ').filter(Boolean);
    const hasSearch = searchTokens.length > 0;
    const hasSecteur = f.secteur.size > 0;
    const hasCommune = f.commune.size > 0;
    const hasEpci = f.epci.size > 0;
    const hasStructure = f.structure.size > 0;
    const monthActive = f.monthEnabled && f.month;

    return function (row) {
      if (!f.regime.has(row.regime)) return false;
      if (!f.seveso.has(row.seveso)) return false;
      if (f.priority === 'yes' && !row.priority) return false;
      if (f.priority === 'no' && row.priority) return false;
      if (f.ied === 'yes' && !row.ied) return false;
      if (f.ied === 'no' && row.ied) return false;
      if (hasSecteur) {
        let any = false;
        if (f.secteur.has('industrie') && row.industrie) any = true;
        if (f.secteur.has('carriere') && row.carriere) any = true;
        if (f.secteur.has('autre') && !row.industrie && !row.carriere) any = true;
        if (!any) return false;
      }
      // Pill filters: OR within each Set, AND across Sets.
      if (hasCommune && !f.commune.has(row.insee)) return false;
      if (hasEpci && !f.epci.has(row.epci_siren)) return false;
      if (hasStructure && !f.structure.has(row.structure_norm)) return false;
      if (hasSearch) {
        const idx = row.search_index;
        for (let i = 0; i < searchTokens.length; i++) {
          if (!idx.includes(searchTokens[i])) return false;
        }
      }
      // Month window — only show rows whose cdate falls in the selected month.
      // Rows without any recorded date pass through (they exist but have no
      // temporal anchor; hiding them would be silent data loss).
      if (monthActive && row.cdate_month && row.cdate_month !== f.month) return false;
      return true;
    };
  }

  // ---------- marker creation ----------
  const markerByRow = new WeakMap();

  function makeMarker(row) {
    const isSeveso = row.isSeveso;
    const marker = L.circleMarker([row.lat, row.lon], {
      radius: isSeveso ? 7 : 5,
      weight: isSeveso ? 2 : 1,
      color: isSeveso ? CSS.ink : CSS.paper,
      fillColor: row.color[state.colorDim],
      fillOpacity: 0.88,
      renderer: canvasRenderer,
    });
    marker._row = row;
    marker.bindTooltip(escapeHTML(row.libelle), {
      direction: 'top',
      offset: [0, -6],
      sticky: true,
      className: 'site-tooltip',
    });
    marker.on('click', () => {
      marker.bindPopup(buildPopupHTML(row), {
        className: 'site-popup',
        maxWidth: 340,
        minWidth: 260,
        autoPanPadding: [40, 40],
      }).openPopup();
    });
    markerByRow.set(row, marker);
    return marker;
  }

  function buildPopupHTML(row) {
    const parts = [];
    parts.push(`<h3 class="popup-name">${escapeHTML(row.libelle)}</h3>`);
    parts.push('<div class="popup-badges">');
    if (REGIME_LABEL[row.regime]) {
      parts.push(`<span class="badge ${REGIME_BADGE[row.regime]}">${REGIME_LABEL[row.regime]}</span>`);
    }
    if (SEVESO_LABEL[row.seveso]) {
      parts.push(`<span class="badge ${SEVESO_BADGE[row.seveso]}">${SEVESO_LABEL[row.seveso]}</span>`);
    }
    if (row.priority) parts.push(`<span class="badge badge--copper">Priorité nationale</span>`);
    if (row.ied) parts.push(`<span class="badge badge--azur">IED</span>`);
    parts.push('</div>');

    parts.push('<dl class="popup-grid">');
    if (row.structure && row.etablissement && row.structure !== row.libelle) {
      parts.push(`<dt>Structure</dt><dd>${escapeHTML(row.structure)}</dd>`);
      parts.push(`<dt>Établissement</dt><dd>${escapeHTML(row.etablissement)}</dd>`);
    }
    if (row.activite) {
      const label = NAF_DIVISIONS[row.activite] || null;
      if (label) {
        parts.push(`<dt>Activité</dt><dd class="popup-activity">${escapeHTML(label)} <em>(NAF ${escapeHTML(row.activite)})</em></dd>`);
      } else {
        parts.push(`<dt>Activité</dt><dd>NAF ${escapeHTML(row.activite)}</dd>`);
      }
    }
    if (row.date_enregistrement) parts.push(`<dt>Date d'enregistrement</dt><dd>${formatDateFR(row.date_enregistrement)}</dd>`);
    if (row.siret) parts.push(`<dt>SIRET</dt><dd>${escapeHTML(row.siret)}</dd>`);
    if (row.insee) parts.push(`<dt>INSEE</dt><dd>${escapeHTML(row.insee)}</dd>`);
    parts.push(`<dt>Lat, Lon</dt><dd>${row.lat.toFixed(5)}, ${row.lon.toFixed(5)}</dd>`);
    parts.push('</dl>');

    const href = safeHref(row.fiche);
    if (href) {
      parts.push(`<a class="popup-fiche" href="${href}" target="_blank" rel="noopener">Fiche Géorisques <span class="sr-only">(ouvre dans un nouvel onglet)</span>→</a>`);
    }
    return parts.join('');
  }

  // ---------- map setup ----------
  const canvasRenderer = L.canvas({ padding: 0.5 });
  const map = L.map('map', {
    preferCanvas: true,
    renderer: canvasRenderer,
    zoomControl: true,
    attributionControl: true,
    minZoom: 7,
    maxZoom: 18,
  });

  // initial view — will be overridden once we have Gironde bounds
  map.setView([44.85, -0.55], 9);

  // base layers
  const baseLayers = {
    'Voyager': L.tileLayer('https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png', {
      subdomains: 'abcd',
      maxZoom: 19,
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/attributions">CARTO</a>',
    }),
    'OSM France': L.tileLayer('https://{s}.tile.openstreetmap.fr/osmfr/{z}/{x}/{y}.png', {
      subdomains: 'abc',
      maxZoom: 19,
      attribution: '&copy; Contributeurs OpenStreetMap · OSM France',
    }),
    'Satellite': L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
      maxZoom: 19,
      attribution: 'Tiles &copy; Esri',
    }),
  };
  baseLayers['Voyager'].addTo(map);

  // overlay groups (created empty, populated after data loads)
  const girondeLayer = L.geoJSON(null, {
    style: { color: CSS.rule, weight: 2, fill: false },
    interactive: false,
  });
  const communesLayer = L.geoJSON(null, {
    style: { color: CSS.lead, weight: 0.5, opacity: 0.6, fill: false },
    interactive: false,
  });
  const rnnLayer = L.geoJSON(null, {
    style: { color: CSS.mossDeep, weight: 1.5, fillColor: CSS.moss, fillOpacity: 0.18 },
    onEachFeature: (feat, layer) => {
      const p = feat.properties || {};
      layer.bindPopup(buildReservePopup(p, 'Réserve Naturelle Nationale'), {
        className: 'site-popup reserve-popup',
      });
    },
  });
  const rnrLayer = L.geoJSON(null, {
    style: { color: CSS.oliveDeep, weight: 1.5, fillColor: CSS.olive, fillOpacity: 0.15 },
    onEachFeature: (feat, layer) => {
      const p = feat.properties || {};
      layer.bindPopup(buildReservePopup(p, 'Réserve Naturelle Régionale'), {
        className: 'site-popup reserve-popup',
      });
    },
  });

  function buildReservePopup(p, typeLabel) {
    const parts = [];
    parts.push(`<h3 class="popup-name">${escapeHTML(p.nom || 'Sans nom')}</h3>`);
    parts.push(`<div class="popup-badges"><span class="badge">${typeLabel}</span></div>`);
    parts.push('<dl class="popup-grid">');
    if (p.date_crea) parts.push(`<dt>Création</dt><dd>${formatDateFR(p.date_crea)}</dd>`);
    if (p.surf_ha) parts.push(`<dt>Surface</dt><dd>${Number(p.surf_ha).toLocaleString('fr-FR', {maximumFractionDigits: 1})} ha</dd>`);
    if (p.operateur) parts.push(`<dt>Opérateur</dt><dd>${escapeHTML(p.operateur)}</dd>`);
    if (p.gest_site) parts.push(`<dt>Gestionnaire</dt><dd>${escapeHTML(p.gest_site)}</dd>`);
    parts.push('</dl>');
    const href = safeHref(p.url_fiche);
    if (href) {
      parts.push(`<a class="popup-fiche" href="${href}" target="_blank" rel="noopener">Fiche INPN <span class="sr-only">(ouvre dans un nouvel onglet)</span>→</a>`);
    }
    return parts.join('');
  }

  // cluster group for ICPE markers
  const clusterGroup = L.markerClusterGroup({
    chunkedLoading: true,
    chunkedInterval: 100,
    removeOutsideVisibleBounds: true,
    maxClusterRadius: 48,
    showCoverageOnHover: false,
    spiderfyOnMaxZoom: true,
    iconCreateFunction: (cluster) => {
      const children = cluster.getAllChildMarkers();
      const n = children.length;
      // count categories at current color dim to pick accent ring
      const counts = new Map();
      for (const m of children) {
        const c = m._row.color[state.colorDim];
        counts.set(c, (counts.get(c) || 0) + 1);
      }
      let majority = CSS.rust, best = 0;
      for (const [c, k] of counts) {
        if (k > best) { best = k; majority = c; }
      }
      const size = n < 10 ? 32 : n < 100 ? 38 : n < 500 ? 44 : 52;
      return L.divIcon({
        html: `<div class="marker-cluster-ink" style="width:${size}px;height:${size}px;--cluster-accent:${majority};">${formatCount(n)}</div>`,
        className: '',
        iconSize: [size, size],
      });
    },
  });

  // ordering: reserves under markers, contours on top of tiles but below markers
  girondeLayer.addTo(map);
  rnnLayer.addTo(map);
  // communesLayer and rnrLayer added via control
  clusterGroup.addTo(map);

  // layer control
  const overlays = {
    'Contour Gironde': girondeLayer,
    'Communes': communesLayer,
    'Réserves Nat. Nationales': rnnLayer,
    'Réserves Nat. Régionales': rnrLayer,
    'ICPE': clusterGroup,
  };
  L.control.layers(baseLayers, overlays, { collapsed: true, position: 'topright' }).addTo(map);

  // ---------- cached DOM references (queried once at module load) ----------
  const siteCountEl    = document.getElementById('site-count');
  const siteMdateEl    = document.getElementById('site-mdate');
  const counterShown   = document.getElementById('counter-shown');
  const counterTotal   = document.getElementById('counter-total');
  const slider         = document.getElementById('time-slider');
  const sliderValue    = document.getElementById('time-slider-value');
  const sliderCountEl  = document.getElementById('time-slider-count');
  const legendEl       = document.getElementById('legend');
  const legendDimEl    = document.getElementById('legend-dim');
  const legendItemsEl  = document.getElementById('legend-items');

  // Pre-computed counts per month key, populated at CSV load
  const monthCounts = new Map();

  function showError(msg) {
    const existing = document.querySelector('.error-banner');
    if (existing) existing.remove();
    const div = document.createElement('div');
    div.className = 'error-banner';
    div.setAttribute('role', 'alert'); // implies aria-live=assertive
    div.textContent = msg;
    document.body.appendChild(div);
  }

  async function init() {
    // Start all data loads in parallel (all local static files).
    // Every fetch goes through fetchJSON so errors surface consistently
    // in Promise.allSettled's .reason instead of being swallowed.
    const [csvResult, girondeResult, rnnResult, rnrResult] = await Promise.allSettled([
      parseCSV(),
      fetchJSON(GIRONDE_CONTOUR_URL),
      fetchJSON(RNN_URL),
      fetchJSON(RNR_URL),
    ]);

    // Gironde contour
    if (girondeResult.status === 'fulfilled' && girondeResult.value) {
      girondeLayer.addData(girondeResult.value);
      try {
        map.fitBounds(girondeLayer.getBounds(), { padding: [20, 20] });
      } catch (_) { /* ignore */ }
    } else {
      console.warn('Gironde contour load failed', girondeResult.reason);
    }

    // RNN / RNR — log failures instead of silently hiding the layer
    if (rnnResult.status === 'fulfilled' && rnnResult.value && rnnResult.value.features) {
      rnnLayer.addData(rnnResult.value);
    } else if (rnnResult.status === 'rejected') {
      console.warn('RNN load failed', rnnResult.reason);
    }
    if (rnrResult.status === 'fulfilled' && rnrResult.value && rnrResult.value.features) {
      rnrLayer.addData(rnrResult.value);
    } else if (rnrResult.status === 'rejected') {
      console.warn('RNR load failed', rnrResult.reason);
    }

    // CSV
    if (csvResult.status !== 'fulfilled') {
      showError('Impossible de charger la liste des ICPE.');
      console.error(csvResult.reason);
      return;
    }
    state.rows = transformRows(csvResult.value);
    if (state.rows.length === 0) {
      showError('Aucun site géolocalisé dans les données — format de CSV inattendu.');
      return;
    }

    // Build reference lists used by the search suggestions and the EPCI
    // filter checkbox list. Everything is derived from the rows — the
    // enriched CSV now carries commune/EPCI directly.
    buildReferenceData();

    // header metadata
    siteCountEl.textContent = `${formatCount(state.rows.length)} sites`;
    siteMdateEl.textContent = formatDateFR(state.mdateMax);
    siteMdateEl.setAttribute('datetime', state.mdateMax || '');
    counterTotal.textContent = formatCount(state.rows.length);

    // derive month keys from the data (unique YYYY-MM values, sorted)
    const mSet = new Set();
    for (const row of state.rows) {
      if (row.cdate_month) mSet.add(row.cdate_month);
    }
    state.monthSteps = Array.from(mSet).sort();
    // default month selection = most recent (useful when user enables the toggle)
    state.filters.month = state.monthSteps.length
      ? state.monthSteps[state.monthSteps.length - 1]
      : null;

    // Pre-compute per-month counts once (static after CSV load)
    monthCounts.clear();
    for (const row of state.rows) {
      if (row.cdate_month) {
        monthCounts.set(row.cdate_month, (monthCounts.get(row.cdate_month) || 0) + 1);
      }
    }

    // configure the slider (starts disabled; checkbox enables it)
    if (state.monthSteps.length >= 1) {
      slider.min = '0';
      slider.max = String(Math.max(0, state.monthSteps.length - 1));
      slider.step = '1';
      slider.value = slider.max;
      slider.disabled = state.monthSteps.length < 2;
      slider.setAttribute('aria-valuetext', formatMonthFR(state.filters.month));
      sliderValue.textContent = formatMonthFR(state.filters.month);
    } else {
      slider.disabled = true;
    }

    // Build markers once; applyFilters() below does the initial cluster add.
    for (const row of state.rows) makeMarker(row);

    // legend
    renderLegend();
    applyFilters(); // also performs the initial clusterGroup population

    // wire up controls
    wireUp();
  }

  // ---------- filtering ----------
  function applyFilters() {
    const predicate = buildPredicate();
    const visible = state.rows.filter(predicate);
    state.visibleRows = visible;
    counterShown.textContent = formatCount(visible.length);

    // Bottom-bar month count:
    //   - filter active → count of currently visible rows (same as counter)
    //   - filter inactive → preview count from the pre-computed monthCounts
    //     map (static, O(1) lookup), intersected with other active filters
    //     for coherence between the preview and the eventual enabled view
    if (state.filters.monthEnabled) {
      sliderCountEl.textContent = formatCount(visible.length);
    } else if (state.filters.month) {
      // Preview = how many of state.visibleRows would remain if the month
      // filter were enabled right now. Scan visible (usually small after
      // other filters) rather than the full dataset.
      const m = state.filters.month;
      let n = 0;
      for (const r of visible) if (r.cdate_month === m) n++;
      sliderCountEl.textContent = formatCount(n);
    } else {
      sliderCountEl.textContent = '—';
    }

    // Rebuild cluster layer with the filtered subset
    clusterGroup.clearLayers();
    const markers = new Array(visible.length);
    for (let i = 0; i < visible.length; i++) markers[i] = markerByRow.get(visible[i]);
    clusterGroup.addLayers(markers);
  }

  function switchColorDim(dim) {
    state.colorDim = dim;
    // Mutate each marker's fillColor option in place — no setStyle redraw
    // per marker. Then ask the canvas renderer to repaint once for the
    // whole layer, batched into a single frame.
    for (const row of state.rows) {
      const m = markerByRow.get(row);
      if (m) m.options.fillColor = row.color[dim];
    }
    requestAnimationFrame(() => {
      if (canvasRenderer._redraw) canvasRenderer._redraw();
      clusterGroup.refreshClusters();
    });
    renderLegend();
  }

  // ---------- legend ----------
  function renderLegend() {
    const dim = state.colorDim;
    legendDimEl.textContent = DIM_HUMAN[dim];
    const frag = document.createDocumentFragment();
    for (const [label, color] of LEGEND_LABELS[dim]) {
      const li = document.createElement('li');
      const swatch = document.createElement('span');
      swatch.className = 'legend-swatch';
      swatch.style.background = color;
      li.appendChild(swatch);
      li.appendChild(document.createTextNode(label));
      frag.appendChild(li);
    }
    legendItemsEl.replaceChildren(frag);
    legendEl.classList.toggle('hide-seveso-row', dim === 'seveso');
  }

  // ---------- EPCI checkbox list (populated from reference data) ----------
  function populateEpciList() {
    const container = document.getElementById('epci-list');
    if (!container) return;
    container.innerHTML = '';
    for (const epci of reference.epcis) {
      const label = document.createElement('label');
      label.className = 'check';
      const cb = document.createElement('input');
      cb.type = 'checkbox';
      cb.value = epci.code;
      cb.dataset.filter = 'epci';
      cb.addEventListener('change', () => {
        if (cb.checked) state.filters.epci.add(epci.code);
        else state.filters.epci.delete(epci.code);
        applyFilters();
      });
      const text = document.createElement('span');
      text.className = 'check__text';
      text.appendChild(document.createTextNode(epci.nom));
      const count = document.createElement('span');
      count.className = 'check__count';
      count.textContent = ` (${formatCount(epci.site_count)})`;
      text.appendChild(count);
      label.appendChild(cb);
      label.appendChild(text);
      container.appendChild(label);
    }
  }

  // ---------- search combobox: pills + suggestion dropdown ----------
  let searchDebounce;
  let sliderDebounce;

  function renderPills() {
    const container = document.getElementById('search-pills');
    if (!container) return;
    container.innerHTML = '';
    const add = (type, key, label) => {
      const pill = document.createElement('span');
      pill.className = `pill pill--${type}`;
      pill.setAttribute('role', 'listitem');
      pill.appendChild(document.createTextNode(label));
      const x = document.createElement('button');
      x.type = 'button';
      x.className = 'pill__remove';
      x.setAttribute('aria-label', `Retirer ${label}`);
      x.textContent = '×';
      x.addEventListener('click', () => {
        state.filters[type].delete(key);
        renderPills();
        applyFilters();
      });
      pill.appendChild(x);
      container.appendChild(pill);
    };
    for (const code of state.filters.commune) {
      const c = reference.communeByInsee.get(code);
      add('commune', code, c ? c.nom : code);
    }
    for (const code of state.filters.epci) {
      const e = reference.epcis.find((x) => x.code === code);
      add('epci', code, e ? e.nom : code);
    }
    for (const norm of state.filters.structure) {
      const s = reference.structures.find((x) => x.norm === norm);
      add('structure', norm, s ? s.name : norm);
    }
    // Also reflect EPCI pills in the checkbox list
    document.querySelectorAll('input[type="checkbox"][data-filter="epci"]').forEach((cb) => {
      cb.checked = state.filters.epci.has(cb.value);
    });
  }

  function renderSuggestions(groups) {
    const panel = document.getElementById('suggestions');
    if (!panel) return;
    if (groups.length === 0) {
      panel.classList.add('is-hidden');
      panel.innerHTML = '';
      return;
    }
    panel.classList.remove('is-hidden');
    panel.innerHTML = '';
    for (const group of groups) {
      const h = document.createElement('div');
      h.className = 'suggestions__group-label';
      h.textContent = group.label;
      panel.appendChild(h);
      const ul = document.createElement('ul');
      ul.className = 'suggestions__list';
      for (const item of group.items) {
        const li = document.createElement('li');
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = `suggestion suggestion--${group.type}`;
        const title = group.type === 'site'
          ? item.libelle
          : group.type === 'structure'
            ? item.name
            : item.nom;
        const count = group.type === 'commune'
          ? item.count
          : group.type === 'epci'
            ? item.site_count
            : group.type === 'structure'
              ? item.count
              : null;
        btn.appendChild(document.createTextNode(title));
        if (count != null) {
          const c = document.createElement('span');
          c.className = 'suggestion__count';
          c.textContent = ` (${formatCount(count)})`;
          btn.appendChild(c);
        }
        btn.addEventListener('click', () => selectSuggestion(group.type, item));
        li.appendChild(btn);
        ul.appendChild(li);
      }
      panel.appendChild(ul);
    }
  }

  function selectSuggestion(type, item) {
    const searchInput = document.getElementById('search-input');
    if (type === 'commune') {
      state.filters.commune.add(item.code);
    } else if (type === 'epci') {
      state.filters.epci.add(item.code);
    } else if (type === 'structure') {
      state.filters.structure.add(item.norm);
    } else if (type === 'site') {
      // Zoom + open popup, don't create a pill.
      map.flyTo([item.lat, item.lon], 15, { duration: 0.7 });
      const m = markerByRow.get(item);
      if (m) setTimeout(() => m.fire('click'), 700);
    }
    searchInput.value = '';
    state.filters.freeSearch = '';
    renderPills();
    renderSuggestions([]);
    applyFilters();
    searchInput.focus();
  }

  function wireSearchCombobox() {
    const searchInput = document.getElementById('search-input');
    const panel = document.getElementById('suggestions');

    searchInput.addEventListener('input', () => {
      const q = searchInput.value;
      renderSuggestions(getSuggestions(q));
      // Apply free-text filter with debounce (pills already applied instantly)
      clearTimeout(searchDebounce);
      searchDebounce = setTimeout(() => {
        state.filters.freeSearch = q;
        applyFilters();
      }, 150);
    });
    searchInput.addEventListener('focus', () => {
      if (searchInput.value) {
        renderSuggestions(getSuggestions(searchInput.value));
      }
    });
    // Close suggestions on outside click
    document.addEventListener('click', (e) => {
      if (!panel.contains(e.target) && e.target !== searchInput) {
        panel.classList.add('is-hidden');
      }
    });
    // Keyboard: Escape closes, Enter picks first suggestion
    searchInput.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') {
        panel.classList.add('is-hidden');
      } else if (e.key === 'Enter') {
        const firstBtn = panel.querySelector('.suggestion');
        if (firstBtn) {
          e.preventDefault();
          firstBtn.click();
        }
      }
    });
  }

  // Wire up a single role=radiogroup: sync aria-checked, roving tabindex,
  // arrow-key navigation, and activation on click or Enter/Space.
  function wireRadioGroup(selector, onChange) {
    const buttons = Array.from(document.querySelectorAll(selector));
    if (buttons.length === 0) return;
    const activate = (btn) => {
      for (const b of buttons) {
        const isActive = b === btn;
        b.classList.toggle('is-active', isActive);
        b.setAttribute('aria-checked', isActive ? 'true' : 'false');
        b.tabIndex = isActive ? 0 : -1;
      }
      onChange(btn);
    };
    // initialise aria-checked / tabindex from whatever has is-active already
    const initial = buttons.find((b) => b.classList.contains('is-active')) || buttons[0];
    for (const b of buttons) {
      b.setAttribute('role', 'radio');
      b.setAttribute('aria-checked', b === initial ? 'true' : 'false');
      b.tabIndex = b === initial ? 0 : -1;
      b.addEventListener('click', () => activate(b));
      b.addEventListener('keydown', (e) => {
        if (e.key === 'ArrowRight' || e.key === 'ArrowDown') {
          e.preventDefault();
          const i = buttons.indexOf(b);
          const next = buttons[(i + 1) % buttons.length];
          next.focus();
          activate(next);
        } else if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') {
          e.preventDefault();
          const i = buttons.indexOf(b);
          const prev = buttons[(i - 1 + buttons.length) % buttons.length];
          prev.focus();
          activate(prev);
        } else if (e.key === ' ' || e.key === 'Enter') {
          e.preventDefault();
          activate(b);
        }
      });
    }
  }

  function wireUp() {
    // color-by segmented
    wireRadioGroup('[data-color-dim]', (btn) => switchColorDim(btn.dataset.colorDim));

    // régime/seveso/secteur checkboxes
    const bindCheckGroup = (filterKey) => {
      document.querySelectorAll(`input[type="checkbox"][data-filter="${filterKey}"]`).forEach((cb) => {
        cb.addEventListener('change', () => {
          if (cb.checked) state.filters[filterKey].add(cb.value);
          else state.filters[filterKey].delete(cb.value);
          applyFilters();
        });
      });
    };
    bindCheckGroup('regime');
    bindCheckGroup('seveso');
    bindCheckGroup('secteur');

    // priority / ied radios
    for (const key of ['priority', 'ied']) {
      wireRadioGroup(`[data-filter="${key}"]`, (btn) => {
        state.filters[key] = btn.dataset.value;
        applyFilters();
      });
    }

    // populate the EPCI checkbox list dynamically from reference data
    populateEpciList();

    // search with suggestions + pills
    wireSearchCombobox();

    // month filter — checkbox toggles it on/off, slider picks the month
    const monthCheckbox = document.getElementById('month-enabled');
    const timebar = document.getElementById('timebar');
    const setSliderEnabled = (on) => {
      slider.disabled = !on || state.monthSteps.length < 2;
      timebar.classList.toggle('is-disabled', !on);
    };
    // Force the checkbox off at init (browser bfcache can restore a stale
    // checked state when reloading).
    monthCheckbox.checked = false;
    state.filters.monthEnabled = false;
    setSliderEnabled(false);

    // Snap the slider back to the earliest month. Used when enabling the
    // month filter or starting playback — keeping the previous slider
    // position would make the default view confusing ("why did everything
    // jump to April?"). Starting from the beginning is a predictable
    // chronology.
    const snapSliderToStart = () => {
      if (state.monthSteps.length === 0) return;
      slider.value = '0';
      const m = state.monthSteps[0];
      state.filters.month = m;
      sliderValue.textContent = formatMonthFR(m);
      slider.setAttribute('aria-valuetext', formatMonthFR(m));
    };

    monthCheckbox.addEventListener('change', () => {
      state.filters.monthEnabled = monthCheckbox.checked;
      setSliderEnabled(monthCheckbox.checked);
      if (monthCheckbox.checked) snapSliderToStart();
      applyFilters();
    });
    slider.addEventListener('input', () => {
      const idx = parseInt(slider.value, 10);
      const m = state.monthSteps[idx];
      state.filters.month = m;
      sliderValue.textContent = formatMonthFR(m);
      slider.setAttribute('aria-valuetext', formatMonthFR(m));
      if (state.filters.monthEnabled) {
        clearTimeout(sliderDebounce);
        sliderDebounce = setTimeout(applyFilters, 60);
      }
    });

    // Play / pause for the month slider — auto-advances one month per tick.
    // Enabling Play also activates the month filter (the animation is
    // meaningless while every site is visible).
    const playBtn = document.getElementById('time-play');
    const playIcon = playBtn.querySelector('.time-play-icon');
    const loopCheckbox = document.getElementById('time-loop');
    const PLAY_INTERVAL_MS = 900;
    let playTimer = null;

    const setPlayButtonEnabled = (on) => {
      playBtn.disabled = !on;
    };
    // Button is enabled whenever we have at least 2 months to walk between.
    setPlayButtonEnabled(state.monthSteps.length >= 2);

    const hud = document.getElementById('playback-hud');
    const hudText = document.getElementById('playback-hud-text');
    const showHud = (month) => {
      if (!hud) return;
      hudText.textContent = formatMonthFR(month);
      hud.classList.add('is-visible');
    };
    const hideHud = () => {
      if (hud) hud.classList.remove('is-visible');
    };

    const stopPlayback = () => {
      if (playTimer) {
        clearInterval(playTimer);
        playTimer = null;
      }
      playIcon.textContent = '▶';
      playBtn.setAttribute('aria-label', 'Lecture');
      playBtn.setAttribute('title', 'Lecture');
      hideHud();
    };
    const startPlayback = () => {
      if (state.monthSteps.length < 2) return;
      // Auto-enable the month filter if it's off, AND snap to the earliest
      // month so playback always walks the full timeline from the start.
      if (!state.filters.monthEnabled) {
        state.filters.monthEnabled = true;
        monthCheckbox.checked = true;
        setSliderEnabled(true);
      }
      snapSliderToStart();
      applyFilters();
      playIcon.textContent = '⏸';
      playBtn.setAttribute('aria-label', 'Pause');
      playBtn.setAttribute('title', 'Pause');
      showHud(state.filters.month);
      playTimer = setInterval(() => {
        const maxIdx = state.monthSteps.length - 1;
        let idx = parseInt(slider.value, 10);
        if (idx >= maxIdx) {
          if (loopCheckbox.checked) {
            idx = 0;
          } else {
            stopPlayback();
            return;
          }
        } else {
          idx += 1;
        }
        slider.value = String(idx);
        // Reuse the same path as manual input
        const m = state.monthSteps[idx];
        state.filters.month = m;
        sliderValue.textContent = formatMonthFR(m);
        slider.setAttribute('aria-valuetext', formatMonthFR(m));
        showHud(m);
        applyFilters();
      }, PLAY_INTERVAL_MS);
    };
    playBtn.addEventListener('click', () => {
      if (playTimer) stopPlayback(); else startPlayback();
    });
    // Unchecking the month filter should stop playback.
    monthCheckbox.addEventListener('change', () => {
      if (!monthCheckbox.checked) stopPlayback();
    });

    // reset
    document.getElementById('reset-button').addEventListener('click', () => {
      state.filters.freeSearch = '';
      state.filters.regime = new Set(['AUTORISATION', 'ENREGISTREMENT', 'NON_ICPE', 'AUTRE']);
      state.filters.seveso = new Set(['SEUIL_HAUT', 'SEUIL_BAS', 'NON_SEVESO', '']);
      state.filters.priority = 'all';
      state.filters.ied = 'all';
      state.filters.secteur = new Set();
      state.filters.commune = new Set();
      state.filters.epci = new Set();
      state.filters.structure = new Set();
      state.filters.monthEnabled = false;
      if (state.monthSteps.length) {
        state.filters.month = state.monthSteps[state.monthSteps.length - 1];
        slider.value = slider.max;
        sliderValue.textContent = formatMonthFR(state.filters.month);
      }
      monthCheckbox.checked = false;
      setSliderEnabled(false);
      stopPlayback();
      if (loopCheckbox) loopCheckbox.checked = false;
      slider.setAttribute('aria-valuetext', formatMonthFR(state.filters.month));
      // reflect in DOM
      searchInput.value = '';
      document.querySelectorAll('input[type="checkbox"][data-filter="regime"]').forEach((cb) => cb.checked = true);
      document.querySelectorAll('input[type="checkbox"][data-filter="seveso"]').forEach((cb) => cb.checked = true);
      document.querySelectorAll('input[type="checkbox"][data-filter="secteur"]').forEach((cb) => cb.checked = false);
      for (const key of ['priority', 'ied']) {
        document.querySelectorAll(`[data-filter="${key}"]`).forEach((b) => {
          const isAll = b.dataset.value === 'all';
          b.classList.toggle('is-active', isAll);
          b.setAttribute('aria-checked', isAll ? 'true' : 'false');
          b.tabIndex = isAll ? 0 : -1;
        });
      }
      applyFilters();
    });

    // lazy communes: fetch from static file on first enable.
    // Uses an in-flight flag to avoid a race where rapid toggling while the
    // fetch is pending would double-add the features.
    let communesFetchStarted = false;
    map.on('overlayadd', async (e) => {
      if (e.layer === communesLayer && !communesFetchStarted) {
        communesFetchStarted = true;
        try {
          const data = await fetchJSON(GIRONDE_COMMUNES_URL);
          communesLayer.addData(data);
        } catch (err) {
          communesFetchStarted = false; // allow retry on next toggle
          console.error('communes load failed', err);
          showError('Impossible de charger les communes.');
        }
      }
    });

    // legend toggle
    const legendToggle = document.getElementById('legend-toggle');
    const legendClose = document.getElementById('legend-close');
    const setLegendOpen = (open) => {
      legendEl.classList.toggle('is-hidden', !open);
      legendToggle.setAttribute('aria-expanded', open ? 'true' : 'false');
      try { localStorage.setItem('legend-open', open ? '1' : '0'); } catch (_) {}
    };
    const stored = (() => { try { return localStorage.getItem('legend-open'); } catch (_) { return null; } })();
    setLegendOpen(stored === null ? true : stored === '1');
    legendToggle.addEventListener('click', () => {
      const isHidden = legendEl.classList.contains('is-hidden');
      setLegendOpen(isHidden); // toggle open
    });
    legendClose.addEventListener('click', () => setLegendOpen(false));
  }

  // ---------- go ----------
  init().catch((err) => {
    showError('Erreur au chargement de la carte.');
    console.error(err);
  });

})();
