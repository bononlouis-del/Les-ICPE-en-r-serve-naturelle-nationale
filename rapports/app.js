/**
 * app.js — Verification page for /rapports/.
 *
 * Uses DuckDB WASM for SQL search on fiches.parquet.
 * Uses PDF.js (desktop only) for cropped snippet rendering.
 * Mobile falls back to a link that opens the PDF at the right page.
 */

import {
  parseFicheIdFromHash,
  buildPdfUrl,
  buildSqlLikePattern,
  canvasCoordinatesFromBbox,
  formatSearchResult,
  isMobileViewport,
} from './lib.js';

// --- Configuration -------------------------------------------------------

const PARQUET_URL = '../carte/data/fiches.parquet';
const DUCKDB_CDN = 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm';
const PDFJS_CDN = 'https://cdn.jsdelivr.net/npm/pdfjs-dist@4.8.69/build/pdf.min.mjs';
const PDFJS_WORKER_CDN = 'https://cdn.jsdelivr.net/npm/pdfjs-dist@4.8.69/build/pdf.worker.min.mjs';
const SEARCH_DEBOUNCE_MS = 300;
const MAX_RESULTS = 100;
const CANVAS_WIDTH = 500;
const CANVAS_HEIGHT = 380;
const BBOX_PADDING = 0.15;

// --- State ---------------------------------------------------------------

let db = null;
let con = null;
let pdfjsLib = null;
let pdfDocCache = {};  // url → PDFDocumentProxy
let debounceTimer = null;
let currentFicheId = null;

// --- DOM refs ------------------------------------------------------------

const searchInput = document.getElementById('search-input');
const searchHint = document.getElementById('search-hint');
const resultsEl = document.getElementById('results');
const resultsEmpty = document.getElementById('results-empty');
const detailEl = document.getElementById('detail');
const detailEmpty = document.getElementById('detail-empty');

// --- Init ----------------------------------------------------------------

async function init() {
  try {
    searchHint.textContent = 'Chargement DuckDB…';
    const duckdb = await import(DUCKDB_CDN);
    searchHint.textContent = 'Initialisation…';
    const bundles = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(bundles);
    // Cross-origin Workers are blocked by browsers. Fetch the worker
    // script as a blob and construct the Worker from that blob URL.
    const workerScript = await fetch(bundle.mainWorker);
    const workerBlob = new Blob([await workerScript.text()], { type: 'application/javascript' });
    const workerUrl = URL.createObjectURL(workerBlob);
    const worker = new Worker(workerUrl);
    const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
    db = new duckdb.AsyncDuckDB(logger, worker);
    // Single-threaded only: skip pthreadWorker because GitHub Pages
    // does not serve COOP/COEP headers required for SharedArrayBuffer.
    await db.instantiate(bundle.mainModule);
    con = await db.connect();

    // Register parquet
    await db.registerFileURL('fiches.parquet', PARQUET_URL, 4 /* HTTP */, false);
    // Warm up — count rows
    const countResult = await con.query("SELECT COUNT(*) AS n FROM 'fiches.parquet'");
    const count = countResult.toArray()[0].n;
    searchHint.textContent = count.toLocaleString('fr-FR') + ' fiches indexées';
    searchInput.disabled = false;
    searchInput.focus();

    // Check hash for deep link
    const hashId = parseFicheIdFromHash(location.hash);
    if (hashId) {
      await loadFiche(hashId);
    }
  } catch (err) {
    searchHint.textContent = 'Erreur de chargement — rechargez la page';
    console.error('DuckDB init failed:', err);
  }
}

// --- Search --------------------------------------------------------------

searchInput.addEventListener('input', () => {
  clearTimeout(debounceTimer);
  debounceTimer = setTimeout(runSearch, SEARCH_DEBOUNCE_MS);
});

searchInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') {
    clearTimeout(debounceTimer);
    runSearch();
  }
});

async function runSearch() {
  const term = searchInput.value.trim();
  if (!term || !con) {
    resultsEl.innerHTML = '';
    resultsEl.appendChild(resultsEmpty);
    return;
  }

  const pattern = buildSqlLikePattern(term);
  const fullText = document.getElementById('fulltext-toggle')?.checked ?? false;
  try {
    // Default: search on lightweight columns only (~0.5 MB via HTTP range).
    // Full-text: adds body column (~13 MB on first use, cached after).
    const bodyClause = fullText
      ? "OR LOWER(body) LIKE LOWER(?) ESCAPE '\\\\'"
      : '';
    const params = fullText
      ? [pattern, pattern, pattern, pattern, pattern, pattern, MAX_RESULTS]
      : [pattern, pattern, pattern, pattern, MAX_RESULTS];
    const result = await con.query(`
      SELECT fiche_id, titre, nom_complet, nom_commune, date_inspection,
             type_suite, extraction_method, fiche_num
      FROM 'fiches.parquet'
      WHERE LOWER(COALESCE(titre, '')) LIKE LOWER(?) ESCAPE '\\'
         OR LOWER(nom_complet) LIKE LOWER(?) ESCAPE '\\'
         OR LOWER(COALESCE(nom_commune, '')) LIKE LOWER(?) ESCAPE '\\'
         OR LOWER(COALESCE(theme, '')) LIKE LOWER(?) ESCAPE '\\'
         ${bodyClause}
         ${fullText ? "OR LOWER(COALESCE(constats_body, '')) LIKE LOWER(?) ESCAPE '\\\\'" : ''}
      LIMIT ?
    `, params);

    const rows = result.toArray();
    renderResults(rows);
    searchHint.textContent = rows.length >= MAX_RESULTS
      ? MAX_RESULTS + '+ résultats'
      : rows.length + ' résultat' + (rows.length > 1 ? 's' : '');
  } catch (err) {
    console.error('Search error:', err);
    searchHint.textContent = 'Erreur de recherche';
  }
}

function renderResults(rows) {
  resultsEl.innerHTML = '';
  if (rows.length === 0) {
    const p = document.createElement('p');
    p.className = 'results__empty';
    p.textContent = 'Aucun résultat pour cette recherche.';
    resultsEl.appendChild(p);
    return;
  }
  for (const row of rows) {
    const { title, subtitle, badge } = formatSearchResult(row);
    const item = document.createElement('div');
    item.className = 'result-item' + (row.fiche_id === currentFicheId ? ' active' : '');
    item.dataset.ficheId = row.fiche_id;

    const h = document.createElement('p');
    h.className = 'result-item__title';
    h.textContent = title;
    item.appendChild(h);

    if (subtitle) {
      const sub = document.createElement('p');
      sub.className = 'result-item__subtitle';
      sub.textContent = subtitle;
      item.appendChild(sub);
    }
    if (badge) {
      const b = document.createElement('span');
      b.className = 'result-item__badge';
      if (/mise en demeure/i.test(badge)) b.className += ' result-item__badge--demeure';
      else if (badge !== 'Sans suite') b.className += ' result-item__badge--suite';
      b.textContent = badge;
      item.appendChild(b);
    }

    item.addEventListener('click', () => {
      location.hash = '#' + row.fiche_id;
    });
    resultsEl.appendChild(item);
  }
}

// --- Detail panel --------------------------------------------------------

window.addEventListener('hashchange', () => {
  const id = parseFicheIdFromHash(location.hash);
  if (id) loadFiche(id);
});

async function loadFiche(ficheId) {
  if (!con) return;
  currentFicheId = ficheId;

  // Highlight in results
  document.querySelectorAll('.result-item').forEach((el) => {
    el.classList.toggle('active', el.dataset.ficheId === ficheId);
  });

  try {
    const result = await con.query(`
      SELECT * FROM 'fiches.parquet' WHERE fiche_id = ?
    `, [ficheId]);
    const rows = result.toArray();
    if (rows.length === 0) {
      detailEl.innerHTML = '<p class="detail__empty">Fiche introuvable.</p>';
      return;
    }
    renderDetail(rows[0]);
  } catch (err) {
    console.error('Load fiche error:', err);
    detailEl.innerHTML = '<p class="detail__empty">Erreur de chargement.</p>';
  }
}

function renderDetail(row) {
  detailEl.innerHTML = '';

  // Header
  const header = document.createElement('div');
  header.className = 'detail__header';

  const title = document.createElement('h2');
  title.className = 'detail__title';
  title.textContent = row.fiche_num
    ? `Fiche N° ${row.fiche_num} — ${row.titre || '(sans titre)'}`
    : `${row.nom_complet} — rapport complet`;
  header.appendChild(title);

  const meta = document.createElement('div');
  meta.className = 'detail__meta';
  const metaParts = [
    row.nom_commune, row.date_inspection, row.id_icpe ? 'ICPE ' + row.id_icpe : '',
    row.siret ? 'SIRET ' + row.siret : '',
  ].filter(Boolean);
  metaParts.forEach((text) => {
    const s = document.createElement('span');
    s.textContent = text;
    meta.appendChild(s);
  });
  header.appendChild(meta);
  detailEl.appendChild(header);

  // Structured fields (only for fiches, not prose)
  if (row.fiche_num) {
    const fields = document.createElement('div');
    fields.className = 'detail__fields';
    const fieldDefs = [
      ['Thème', row.theme],
      ['Type de suites', row.type_suite],
      ['Déjà contrôlé', row.deja_controle],
      ['Référence', row.reference_reglementaire],
    ];
    for (const [label, value] of fieldDefs) {
      if (!value) continue;
      const f = document.createElement('div');
      f.className = 'field';
      const fl = document.createElement('div');
      fl.className = 'field__label';
      fl.textContent = label;
      f.appendChild(fl);
      const fv = document.createElement('div');
      fv.className = 'field__value';
      fv.textContent = value;
      f.appendChild(fv);
      fields.appendChild(f);
    }
    // Constats (full width)
    if (row.constats_body) {
      const f = document.createElement('div');
      f.className = 'field field--full';
      const fl = document.createElement('div');
      fl.className = 'field__label';
      fl.textContent = 'Constats';
      f.appendChild(fl);
      const fv = document.createElement('div');
      fv.className = 'field__value';
      fv.textContent = row.constats_body.length > 2000
        ? row.constats_body.slice(0, 2000) + '…'
        : row.constats_body;
      fv.style.whiteSpace = 'pre-wrap';
      f.appendChild(fv);
      fields.appendChild(f);
    }
    detailEl.appendChild(fields);
  }

  // PDF snippet or link
  let regions = row.regions;
  if (typeof regions === 'string') {
    try { regions = JSON.parse(regions); } catch { regions = null; }
  }
  const firstRegion = Array.isArray(regions) && regions.length > 0 ? regions[0] : null;
  const page = firstRegion ? firstRegion.page : 1;
  const pdfUrl = row.url_pages || '';

  if (isMobileViewport() || !pdfUrl) {
    // Mobile: link only
    if (pdfUrl) {
      const link = document.createElement('a');
      link.className = 'pdf-link';
      link.href = buildPdfUrl(pdfUrl, page);
      link.target = '_blank';
      link.rel = 'noopener';
      link.textContent = `📄 Page ${page} — ouvrir le rapport`;
      detailEl.appendChild(link);
    }
  } else {
    // Desktop: canvas snippet
    const snippet = document.createElement('div');
    snippet.className = 'snippet';

    const canvas = document.createElement('canvas');
    canvas.className = 'snippet__canvas';
    canvas.width = CANVAS_WIDTH;
    canvas.height = CANVAS_HEIGHT;
    canvas.title = 'Cliquer pour ouvrir le PDF complet';
    canvas.addEventListener('click', () => {
      window.open(buildPdfUrl(pdfUrl, page), '_blank', 'noopener');
    });
    snippet.appendChild(canvas);

    const caption = document.createElement('div');
    caption.className = 'snippet__caption';
    caption.innerHTML = `Page ${page} du rapport · <a href="${buildPdfUrl(pdfUrl, page)}" target="_blank" rel="noopener">ouvrir le PDF complet →</a>`;
    snippet.appendChild(caption);
    detailEl.appendChild(snippet);

    // Render async
    renderSnippet(canvas, pdfUrl, firstRegion);
  }

  // Context block
  const context = document.createElement('div');
  context.className = 'context';
  const contextTitle = document.createElement('div');
  contextTitle.className = 'context__title';
  contextTitle.textContent = 'Contexte installation';
  context.appendChild(contextTitle);
  const grid = document.createElement('div');
  grid.className = 'context__grid';
  const contextItems = [
    ['Régime', row.regime_icpe],
    ['Seveso', row.categorie_seveso],
    ['Commune', row.nom_commune],
    ['EPCI', row.epci_nom],
    ['Extraction', row.extraction_method],
    ['Source', row.source_pdf],
  ];
  for (const [label, value] of contextItems) {
    if (!value) continue;
    const s = document.createElement('span');
    s.innerHTML = `<strong>${label}</strong> : ${value}`;
    grid.appendChild(s);
  }
  context.appendChild(grid);

  // Link to markdown
  if (row.url_markdown) {
    const mdLink = document.createElement('div');
    mdLink.style.marginTop = '12px';
    mdLink.innerHTML = `<a href="${row.url_markdown}" target="_blank" rel="noopener" style="color:var(--moss);font-size:13px;font-family:var(--font-body)">voir le markdown complet →</a>`;
    context.appendChild(mdLink);
  }
  detailEl.appendChild(context);
}

// --- PDF.js snippet rendering (desktop only) -----------------------------

async function loadPdfJs() {
  if (pdfjsLib) return pdfjsLib;
  const mod = await import(PDFJS_CDN);
  mod.GlobalWorkerOptions.workerSrc = PDFJS_WORKER_CDN;
  pdfjsLib = mod;
  return mod;
}

async function renderSnippet(canvas, pdfUrl, region) {
  const ctx = canvas.getContext('2d');
  // Background while loading
  ctx.fillStyle = '#f5f3ed';
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  ctx.fillStyle = '#999';
  ctx.font = '13px sans-serif';
  ctx.textAlign = 'center';
  ctx.fillText('Chargement du PDF…', canvas.width / 2, canvas.height / 2);

  try {
    const lib = await loadPdfJs();
    // Cache PDF document per URL
    if (!pdfDocCache[pdfUrl]) {
      pdfDocCache[pdfUrl] = await lib.getDocument(pdfUrl).promise;
    }
    const doc = pdfDocCache[pdfUrl];
    const pageNum = region ? region.page : 1;
    const page = await doc.getPage(pageNum);
    const viewport = page.getViewport({ scale: 1.5 });

    // Render full page to offscreen canvas
    const offscreen = document.createElement('canvas');
    offscreen.width = viewport.width;
    offscreen.height = viewport.height;
    const offCtx = offscreen.getContext('2d');
    await page.render({ canvasContext: offCtx, viewport }).promise;

    // Crop to bbox if available
    if (region && region.bbox && region.bbox.length === 4) {
      const pagePts = { width: page.view[2], height: page.view[3] };
      const coords = canvasCoordinatesFromBbox(
        region.bbox, pagePts,
        { width: canvas.width, height: canvas.height },
        BBOX_PADDING,
      );
      if (coords) {
        const scale = viewport.scale;
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        ctx.fillStyle = '#fdfbf4';
        ctx.fillRect(0, 0, canvas.width, canvas.height);
        ctx.drawImage(
          offscreen,
          coords.sx * scale, coords.sy * scale, coords.sw * scale, coords.sh * scale,
          coords.dx, coords.dy, coords.dw, coords.dh,
        );
        return;
      }
    }

    // Fallback: render full page scaled to fit
    const fitScale = Math.min(canvas.width / viewport.width, canvas.height / viewport.height);
    const dw = viewport.width * fitScale;
    const dh = viewport.height * fitScale;
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.fillStyle = '#fdfbf4';
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    ctx.drawImage(offscreen, (canvas.width - dw) / 2, (canvas.height - dh) / 2, dw, dh);
  } catch (err) {
    console.warn('PDF render failed:', err);
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.fillStyle = '#f5f3ed';
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    ctx.fillStyle = '#999';
    ctx.font = '13px sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText('PDF indisponible', canvas.width / 2, canvas.height / 2);
  }
}

// --- Boot ----------------------------------------------------------------

init();
