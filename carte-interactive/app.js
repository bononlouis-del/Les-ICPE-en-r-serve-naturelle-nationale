/* ============================================================================
   Cahier d'enquête — ICPE en Gironde
   Map logic: CSV loading, filter compilation, color switching, layer control.
   No framework. Vanilla JS. Designed for speed at 2,888 markers.
============================================================================ */

(function () {
  'use strict';

  // ---------- constants ----------
  const CSV_URL = 'carte-interactive/liste-icpe-gironde_enrichi.csv';
  const RNN_URL = 'carte-interactive/data/reserves-naturelles-nationales.geojson';
  const RNR_URL = 'carte-interactive/data/reserves-naturelles-regionales.geojson';
  const GIRONDE_CONTOUR_URL = 'https://geo.api.gouv.fr/departements/33/contour?format=geojson';
  const GIRONDE_COMMUNES_URL = 'https://geo.api.gouv.fr/departements/33/communes?format=geojson&geometry=contour';
  const CACHE_TTL_MS = 30 * 24 * 3600 * 1000; // 30 days

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
      search: '',
      regime: new Set(['AUTORISATION', 'ENREGISTREMENT', 'NON_ICPE', 'AUTRE']),
      seveso: new Set(['SEUIL_HAUT', 'SEUIL_BAS', 'NON_SEVESO', '']),
      priority: 'all',
      ied: 'all',
      secteur: new Set(), // empty = no secteur filter; populated = OR of active secteurs
      // cutoff month (YYYY-MM) — rows with cdate <= cutoff are visible.
      // Null means "no time filter" (show everything).
      cutoff: null,
    },
    mdateMax: null,
    // monthly steps derived from the dataset — set after CSV load
    monthSteps: [],
  };

  const MONTHS_FR = [
    'janv.', 'févr.', 'mars', 'avr.', 'mai', 'juin',
    'juil.', 'août', 'sept.', 'oct.', 'nov.', 'déc.',
  ];
  function formatMonthYearFR(ym) {
    // ym is "YYYY-MM"
    if (!ym) return '—';
    const [y, m] = ym.split('-');
    return `${MONTHS_FR[parseInt(m, 10) - 1]} ${y}`;
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

  function cacheGet(key) {
    try {
      const raw = localStorage.getItem(key);
      if (!raw) return null;
      const { t, v } = JSON.parse(raw);
      if (Date.now() - t > CACHE_TTL_MS) return null;
      return v;
    } catch (_) { return null; }
  }
  function cacheSet(key, v) {
    try { localStorage.setItem(key, JSON.stringify({ t: Date.now(), v })); } catch (_) {}
  }

  async function fetchJSONCached(url, cacheKey) {
    const hit = cacheGet(cacheKey);
    if (hit) return hit;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`fetch ${url}: ${res.status}`);
    const json = await res.json();
    cacheSet(cacheKey, json);
    return json;
  }

  // ---------- CSV loading ----------
  function parseCSV() {
    return new Promise((resolve, reject) => {
      Papa.parse(CSV_URL, {
        worker: true,
        download: true,
        header: true,
        skipEmptyLines: true,
        complete: (result) => resolve(result.data),
        error: reject,
      });
    });
  }

  function transformRows(rawRows) {
    // Transform each CSV row into a compact object with pre-computed colors
    const rows = [];
    let mdateMax = null;
    for (const r of rawRows) {
      const geoPoint = r['Geo Point'];
      if (!geoPoint) continue;
      const parts = geoPoint.split(',');
      if (parts.length !== 2) continue;
      const lat = parseFloat(parts[0]);
      const lon = parseFloat(parts[1]);
      if (!isFinite(lat) || !isFinite(lon)) continue;

      const regime = r.regime || 'AUTRE';
      const seveso = (r.cat_seveso || '').trim();
      const priority = r.priorite_nationale === 'TRUE';
      const ied = r.ied === 'TRUE';
      const industrie = r.industrie === 'TRUE';
      const carriere = r.carriere === 'TRUE';
      const libelleComplet = (r.libelle_complet || r.libelle || '(sans nom)').trim();
      const structure = (r.structure || '').trim();
      const etablissement = (r.etablissement || '').trim();
      const libelle = libelleComplet; // unified display name

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

      const mdate = r.mdate || '';
      if (mdate && (!mdateMax || mdate > mdateMax)) mdateMax = mdate;

      // cdate → year-month string (YYYY-MM) for the time slider
      const cdate = r.cdate || '';
      const cdate_ym = cdate ? cdate.substring(0, 7) : '';

      rows.push({
        lat, lon,
        libelle,
        structure,
        etablissement,
        // search across name + etablissement + siret so partial queries work
        search_index: (libelle + ' ' + etablissement + ' ' + (r.siret || '')).toLowerCase(),
        regime,
        seveso,
        priority,
        ied,
        industrie,
        carriere,
        cdate_ym,
        fiche: r.fiche || '',
        siret: r.siret || '',
        insee: r.insee || '',
        cdate,
        mdate,
        activite: (r.activite_principale || '').toString(),
        isSeveso: seveso === 'SEUIL_HAUT' || seveso === 'SEUIL_BAS',
        color,
      });
    }
    state.mdateMax = mdateMax;
    return rows;
  }

  // ---------- filter predicate ----------
  function buildPredicate() {
    const f = state.filters;
    const search = f.search.trim().toLowerCase();
    const hasSearch = search.length > 0;
    const hasSecteur = f.secteur.size > 0;
    const cutoff = f.cutoff;

    return function (row) {
      if (!f.regime.has(row.regime)) return false;
      if (!f.seveso.has(row.seveso)) return false;
      if (f.priority === 'yes' && !row.priority) return false;
      if (f.priority === 'no' && row.priority) return false;
      if (f.ied === 'yes' && !row.ied) return false;
      if (f.ied === 'no' && row.ied) return false;
      if (hasSecteur) {
        // OR between active secteur flags
        let any = false;
        if (f.secteur.has('industrie') && row.industrie) any = true;
        if (f.secteur.has('carriere') && row.carriere) any = true;
        if (!any) return false;
      }
      if (hasSearch && row.search_index.indexOf(search) === -1) return false;
      // time cutoff — ISO year-month strings are lexicographically comparable
      if (cutoff && row.cdate_ym && row.cdate_ym > cutoff) return false;
      return true;
    };
  }

  // ---------- marker creation ----------
  let rowToMarker; // populated after map is created
  let markerByRow = new WeakMap();

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
    if (row.mdate) parts.push(`<dt>Mise à jour</dt><dd>${formatDateFR(row.mdate)}</dd>`);
    if (row.siret) parts.push(`<dt>SIRET</dt><dd>${escapeHTML(row.siret)}</dd>`);
    if (row.insee) parts.push(`<dt>INSEE</dt><dd>${escapeHTML(row.insee)}</dd>`);
    parts.push(`<dt>Lat, Lon</dt><dd>${row.lat.toFixed(5)}, ${row.lon.toFixed(5)}</dd>`);
    parts.push('</dl>');

    if (row.fiche) {
      parts.push(`<a class="popup-fiche" href="${escapeHTML(row.fiche)}" target="_blank" rel="noopener">Fiche Géorisques →</a>`);
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
    if (p.url_fiche) {
      parts.push(`<a class="popup-fiche" href="${escapeHTML(p.url_fiche)}" target="_blank" rel="noopener">Fiche INPN →</a>`);
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

  // ---------- data loading flow ----------
  const siteCountEl = document.getElementById('site-count');
  const siteMdateEl = document.getElementById('site-mdate');
  const counterShown = document.getElementById('counter-shown');
  const counterTotal = document.getElementById('counter-total');

  function showError(msg) {
    const existing = document.querySelector('.error-banner');
    if (existing) existing.remove();
    const div = document.createElement('div');
    div.className = 'error-banner';
    div.textContent = msg;
    document.body.appendChild(div);
  }

  async function init() {
    // Start all data loads in parallel
    const [csvResult, girondeResult, rnnResult, rnrResult] = await Promise.allSettled([
      parseCSV(),
      fetchJSONCached(GIRONDE_CONTOUR_URL, 'cache_gironde_contour'),
      fetch(RNN_URL).then(r => r.ok ? r.json() : null).catch(() => null),
      fetch(RNR_URL).then(r => r.ok ? r.json() : null).catch(() => null),
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

    // RNN / RNR
    if (rnnResult.status === 'fulfilled' && rnnResult.value && rnnResult.value.features) {
      rnnLayer.addData(rnnResult.value);
    }
    if (rnrResult.status === 'fulfilled' && rnrResult.value && rnrResult.value.features) {
      rnrLayer.addData(rnrResult.value);
    }

    // CSV
    if (csvResult.status !== 'fulfilled') {
      showError('Impossible de charger la liste des ICPE.');
      console.error(csvResult.reason);
      return;
    }
    state.rows = transformRows(csvResult.value);

    // header metadata
    siteCountEl.textContent = `${formatCount(state.rows.length)} sites`;
    siteMdateEl.textContent = formatDateFR(state.mdateMax);
    siteMdateEl.setAttribute('datetime', state.mdateMax || '');
    counterTotal.textContent = formatCount(state.rows.length);

    // derive monthly steps from the data (unique YYYY-MM values, sorted)
    const ymSet = new Set();
    for (const row of state.rows) {
      if (row.cdate_ym) ymSet.add(row.cdate_ym);
    }
    state.monthSteps = Array.from(ymSet).sort();
    // default cutoff = max month (show all)
    state.filters.cutoff = state.monthSteps.length ? state.monthSteps[state.monthSteps.length - 1] : null;

    // configure the slider
    const slider = document.getElementById('time-slider');
    const sliderValue = document.getElementById('time-slider-value');
    const sliderCount = document.getElementById('time-slider-count');
    if (state.monthSteps.length >= 2) {
      slider.min = '0';
      slider.max = String(state.monthSteps.length - 1);
      slider.step = '1';
      slider.value = slider.max; // rightmost = all
      sliderValue.textContent = formatMonthYearFR(state.filters.cutoff);
    } else {
      // only one month or none — disable the control
      slider.disabled = true;
    }

    // build markers
    const markers = state.rows.map(makeMarker);
    clusterGroup.addLayers(markers);

    // legend
    renderLegend();
    applyFilters(); // sets initial visible count

    // wire up controls
    wireUp();
  }

  // ---------- filtering ----------
  function applyFilters() {
    const predicate = buildPredicate();
    const visible = state.rows.filter(predicate);
    state.visibleRows = visible;
    counterShown.textContent = formatCount(visible.length);
    const sliderCount = document.getElementById('time-slider-count');
    if (sliderCount) sliderCount.textContent = formatCount(visible.length);

    // Rebuild cluster layer with the filtered subset
    clusterGroup.clearLayers();
    const markers = visible.map((row) => markerByRow.get(row) || makeMarker(row));
    clusterGroup.addLayers(markers);
  }

  function switchColorDim(dim) {
    state.colorDim = dim;
    // Update marker fill colors in place (no rebuild)
    for (const row of state.rows) {
      const m = markerByRow.get(row);
      if (m) m.setStyle({ fillColor: row.color[dim] });
    }
    // Redraw clusters to update accent ring
    clusterGroup.refreshClusters();
    renderLegend();
  }

  // ---------- legend ----------
  function renderLegend() {
    const dim = state.colorDim;
    document.getElementById('legend-dim').textContent = DIM_HUMAN[dim];
    const ul = document.getElementById('legend-items');
    ul.innerHTML = '';
    for (const [label, color] of LEGEND_LABELS[dim]) {
      const li = document.createElement('li');
      li.innerHTML = `<span class="legend-swatch" style="background:${color}"></span>${escapeHTML(label)}`;
      ul.appendChild(li);
    }
    // Hide the "Seveso contour" note when Seveso is the active dim
    const legend = document.getElementById('legend');
    legend.classList.toggle('hide-seveso-row', dim === 'seveso');
  }

  // ---------- event wiring ----------
  let searchDebounce;
  function wireUp() {
    // color-by segmented
    document.querySelectorAll('[data-color-dim]').forEach((btn) => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('[data-color-dim]').forEach((b) => b.classList.remove('is-active'));
        btn.classList.add('is-active');
        switchColorDim(btn.dataset.colorDim);
      });
    });

    // régime/seveso checkboxes
    document.querySelectorAll('input[type="checkbox"][data-filter="regime"]').forEach((cb) => {
      cb.addEventListener('change', () => {
        if (cb.checked) state.filters.regime.add(cb.value);
        else state.filters.regime.delete(cb.value);
        applyFilters();
      });
    });
    document.querySelectorAll('input[type="checkbox"][data-filter="seveso"]').forEach((cb) => {
      cb.addEventListener('change', () => {
        if (cb.checked) state.filters.seveso.add(cb.value);
        else state.filters.seveso.delete(cb.value);
        applyFilters();
      });
    });
    document.querySelectorAll('input[type="checkbox"][data-filter="secteur"]').forEach((cb) => {
      cb.addEventListener('change', () => {
        if (cb.checked) state.filters.secteur.add(cb.value);
        else state.filters.secteur.delete(cb.value);
        applyFilters();
      });
    });

    // priority / ied radios
    ['priority', 'ied'].forEach((key) => {
      document.querySelectorAll(`[data-filter="${key}"]`).forEach((btn) => {
        btn.addEventListener('click', () => {
          document.querySelectorAll(`[data-filter="${key}"]`).forEach((b) => b.classList.remove('is-active'));
          btn.classList.add('is-active');
          state.filters[key] = btn.dataset.value;
          applyFilters();
        });
      });
    });

    // search (debounced)
    const searchInput = document.getElementById('search-input');
    searchInput.addEventListener('input', () => {
      clearTimeout(searchDebounce);
      searchDebounce = setTimeout(() => {
        state.filters.search = searchInput.value;
        applyFilters();
      }, 150);
    });

    // time slider — instant update (predicate is cheap)
    const slider = document.getElementById('time-slider');
    const sliderValue = document.getElementById('time-slider-value');
    const sliderCount = document.getElementById('time-slider-count');
    slider.addEventListener('input', () => {
      const idx = parseInt(slider.value, 10);
      const cutoff = state.monthSteps[idx];
      state.filters.cutoff = cutoff;
      sliderValue.textContent = formatMonthYearFR(cutoff);
      applyFilters();
    });

    // reset
    document.getElementById('reset-button').addEventListener('click', () => {
      state.filters.search = '';
      state.filters.regime = new Set(['AUTORISATION', 'ENREGISTREMENT', 'NON_ICPE', 'AUTRE']);
      state.filters.seveso = new Set(['SEUIL_HAUT', 'SEUIL_BAS', 'NON_SEVESO', '']);
      state.filters.priority = 'all';
      state.filters.ied = 'all';
      state.filters.secteur = new Set();
      if (state.monthSteps.length) {
        state.filters.cutoff = state.monthSteps[state.monthSteps.length - 1];
        slider.value = slider.max;
        sliderValue.textContent = formatMonthYearFR(state.filters.cutoff);
      }
      // reflect in DOM
      searchInput.value = '';
      document.querySelectorAll('input[type="checkbox"][data-filter="regime"]').forEach((cb) => cb.checked = true);
      document.querySelectorAll('input[type="checkbox"][data-filter="seveso"]').forEach((cb) => cb.checked = true);
      document.querySelectorAll('input[type="checkbox"][data-filter="secteur"]').forEach((cb) => cb.checked = false);
      document.querySelectorAll('[data-filter="priority"]').forEach((b) => b.classList.toggle('is-active', b.dataset.value === 'all'));
      document.querySelectorAll('[data-filter="ied"]').forEach((b) => b.classList.toggle('is-active', b.dataset.value === 'all'));
      applyFilters();
    });

    // lazy communes: when user enables the Communes overlay, fetch + simplify on demand
    map.on('overlayadd', async (e) => {
      if (e.layer === communesLayer && communesLayer.getLayers().length === 0) {
        try {
          let cached = cacheGet('cache_gironde_communes_simplified');
          if (!cached) {
            const raw = await (await fetch(GIRONDE_COMMUNES_URL)).json();
            if (window.turf && window.turf.simplify) {
              cached = window.turf.simplify(raw, { tolerance: 0.001, highQuality: false });
            } else {
              cached = raw;
            }
            cacheSet('cache_gironde_communes_simplified', cached);
          }
          communesLayer.addData(cached);
        } catch (err) {
          console.error('communes load failed', err);
        }
      }
    });
  }

  // ---------- go ----------
  init().catch((err) => {
    showError('Erreur au chargement de la carte.');
    console.error(err);
  });

})();
