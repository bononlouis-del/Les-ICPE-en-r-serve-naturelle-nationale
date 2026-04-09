/**
 * lib.js — Pure functions for the /rapports/ verification page.
 *
 * Testable in rapports/test.html via console.assert.
 * No DOM access, no side effects, no imports.
 */

// --- Fiche ID parsing --------------------------------------------------

/**
 * Parse a fiche_id from a URL hash fragment.
 * E.g. "#ACME_123_2024-01-01_12345_f03" → "ACME_123_2024-01-01_12345_f03"
 * Returns null if hash is empty or doesn't look like a fiche_id.
 */
export function parseFicheIdFromHash(hash) {
  if (!hash || hash === '#') return null;
  const id = hash.startsWith('#') ? hash.slice(1) : hash;
  // Must end with _fNN or _prose
  if (/_f\d{2,}$/.test(id) || /_prose$/.test(id)) return id;
  return null;
}

// --- PDF URL building --------------------------------------------------

/**
 * Build a PDF URL with page anchor.
 * @param {string} baseUrl - Full URL to the PDF (url_pages from parquet)
 * @param {number} page - 1-based page number
 * @returns {string} URL with #page=N appended
 */
export function buildPdfUrl(baseUrl, page) {
  if (!baseUrl) return '';
  const p = Math.max(1, Math.floor(page || 1));
  return baseUrl + '#page=' + p;
}

// --- SQL building ------------------------------------------------------

/**
 * Build a SQL LIKE clause for DuckDB search.
 * Escapes % and _ in the user's query, wraps in %...%.
 * @param {string} term - Raw search input
 * @returns {string} The LIKE pattern
 */
export function buildSqlLikePattern(term) {
  if (!term) return '%%';
  const escaped = term.replace(/%/g, '\\%').replace(/_/g, '\\_');
  return '%' + escaped + '%';
}

// --- Canvas coordinates ------------------------------------------------

/**
 * Convert a PDF bbox (points) to canvas pixel coordinates.
 * @param {number[]} bbox - [x0, y0, x1, y1] in PDF points
 * @param {{width: number, height: number}} pagePts - Page size in points
 * @param {{width: number, height: number}} canvasPx - Canvas size in pixels
 * @param {number} padding - Padding ratio (e.g. 0.15 for 15%)
 * @returns {{sx: number, sy: number, sw: number, sh: number, dx: number, dy: number, dw: number, dh: number}}
 */
export function canvasCoordinatesFromBbox(bbox, pagePts, canvasPx, padding) {
  if (!bbox || bbox.length < 4) return null;
  const [x0, y0, x1, y1] = bbox;
  const bw = x1 - x0;
  const bh = y1 - y0;
  const padX = bw * (padding || 0.15);
  const padY = bh * (padding || 0.15);

  // Source region in page points (clamped to page bounds)
  const sx = Math.max(0, x0 - padX);
  const sy = Math.max(0, y0 - padY);
  const sw = Math.min(pagePts.width - sx, bw + 2 * padX);
  const sh = Math.min(pagePts.height - sy, bh + 2 * padY);

  // Scale to fit canvas, preserving aspect ratio
  const scale = Math.min(canvasPx.width / sw, canvasPx.height / sh);
  const dw = sw * scale;
  const dh = sh * scale;
  const dx = (canvasPx.width - dw) / 2;
  const dy = (canvasPx.height - dh) / 2;

  return { sx, sy, sw, sh, dx, dy, dw, dh };
}

// --- Search result formatting ------------------------------------------

/**
 * Format a row from the parquet search results for display.
 * @param {object} row - A row from DuckDB query result
 * @returns {{title: string, subtitle: string, badge: string}}
 */
export function formatSearchResult(row) {
  const titre = row.titre || '(rapport complet)';
  const commune = row.nom_commune || '';
  const date = row.date_inspection || '';
  const suite = row.type_suite || '';
  const subtitle = [commune, date].filter(Boolean).join(' · ');
  return { title: titre, subtitle, badge: suite };
}

/**
 * Check if current viewport is mobile (<720px).
 * @returns {boolean}
 */
export function isMobileViewport() {
  return window.matchMedia('(max-width: 719px)').matches;
}
