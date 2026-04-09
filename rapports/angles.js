/**
 * angles.js — Recipe book: run SQL angles on fiches.parquet via DuckDB WASM.
 *
 * Loads angles from angles/index.json, fetches each .md for its SQL block,
 * and provides a "Download CSV" button per angle that executes the query
 * in-browser and triggers a file download.
 */

const PARQUET_URL = '../carte/data/fiches.parquet';
const DUCKDB_CDN = 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm';
const INDEX_URL = 'angles/index.json';

let db = null;
let con = null;

// --- Init ----------------------------------------------------------------

async function init() {
  const loadingEl = document.getElementById('loading');
  const container = document.getElementById('angles-container');

  try {
    // Load DuckDB WASM
    const duckdb = await import(DUCKDB_CDN);
    const bundles = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(bundles);
    const worker = new Worker(bundle.mainWorker);
    const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
    db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    con = await db.connect();
    await db.registerFileURL('fiches.parquet', PARQUET_URL, 4 /* HTTP */, false);
    loadingEl.hidden = true;

    // Load index
    const indexResp = await fetch(INDEX_URL);
    if (!indexResp.ok) throw new Error('Failed to fetch angles index');
    const angles = await indexResp.json();

    // Load and render each angle
    for (const angle of angles) {
      const mdResp = await fetch('angles/' + angle.file);
      if (!mdResp.ok) continue;
      const mdText = await mdResp.text();
      const sql = extractSqlFromMarkdown(mdText);
      const explanation = extractExplanation(mdText);
      if (!sql) continue;
      renderAngle(container, angle, sql, explanation);
    }
  } catch (err) {
    loadingEl.innerHTML = '<span style="color:var(--rust)">Erreur de chargement. Rechargez la page.</span>';
    console.error('Angles init error:', err);
  }
}

// --- Markdown parsing ----------------------------------------------------

function extractSqlFromMarkdown(md) {
  const match = md.match(/```sql\n([\s\S]+?)```/);
  return match ? match[1].trim() : null;
}

function extractExplanation(md) {
  // Everything after the closing ``` of the SQL block
  const idx = md.indexOf('```sql');
  if (idx < 0) return '';
  const afterSql = md.indexOf('```', idx + 6);
  if (afterSql < 0) return '';
  const rest = md.slice(afterSql + 3).trim();
  // Strip the ## heading and return the paragraph
  return rest.replace(/^##[^\n]+\n+/, '').trim();
}

// --- Rendering -----------------------------------------------------------

function renderAngle(container, angle, sql, explanation) {
  const section = document.createElement('section');
  section.style.cssText = 'margin-bottom:40px;padding-bottom:32px;border-bottom:1px solid var(--rule-soft);';

  // Title + question
  const h2 = document.createElement('h2');
  h2.style.cssText = 'font-family:var(--font-display);font-size:20px;font-weight:500;color:var(--ink);margin:0 0 4px;';
  h2.textContent = angle.title;
  section.appendChild(h2);

  const question = document.createElement('p');
  question.style.cssText = 'font-family:var(--font-body);font-size:14px;color:var(--ink-soft);margin:0 0 8px;';
  question.textContent = angle.question;
  section.appendChild(question);

  // Caveat
  if (angle.caveat) {
    const caveat = document.createElement('p');
    caveat.style.cssText = 'font-family:var(--font-data);font-size:11px;color:var(--lead);background:var(--paper-2);padding:8px 12px;border-radius:4px;margin:0 0 12px;';
    caveat.textContent = '⚠ ' + angle.caveat;
    section.appendChild(caveat);
  }

  // SQL block (collapsible)
  const details = document.createElement('details');
  details.style.cssText = 'margin-bottom:12px;';
  const summary = document.createElement('summary');
  summary.style.cssText = 'font-family:var(--font-data);font-size:12px;color:var(--ink-soft);cursor:pointer;';
  summary.textContent = 'Voir la requête SQL';
  details.appendChild(summary);
  const pre = document.createElement('pre');
  pre.style.cssText = 'font-family:var(--font-data);font-size:12px;background:var(--paper-2);padding:12px;border-radius:4px;overflow-x:auto;margin:8px 0 0;';
  pre.textContent = sql;
  details.appendChild(pre);
  section.appendChild(details);

  // Explanation
  if (explanation) {
    const p = document.createElement('p');
    p.style.cssText = 'font-family:var(--font-body);font-size:13px;color:var(--ink);line-height:1.5;margin:0 0 12px;';
    p.textContent = explanation;
    section.appendChild(p);
  }

  // Button bar
  const bar = document.createElement('div');
  bar.style.cssText = 'display:flex;gap:12px;align-items:center;';

  const btn = document.createElement('button');
  btn.style.cssText = 'font-family:var(--font-body);font-size:13px;padding:8px 16px;border:1px solid var(--moss);background:transparent;color:var(--moss);border-radius:4px;cursor:pointer;';
  btn.textContent = 'Télécharger CSV';
  btn.addEventListener('click', () => runAngle(sql, angle.file, btn, previewEl));
  bar.appendChild(btn);

  const previewBtn = document.createElement('button');
  previewBtn.style.cssText = 'font-family:var(--font-body);font-size:13px;padding:8px 16px;border:1px solid var(--rule);background:transparent;color:var(--ink-soft);border-radius:4px;cursor:pointer;';
  previewBtn.textContent = 'Aperçu (10 lignes)';
  previewBtn.addEventListener('click', () => runPreview(sql, previewEl));
  bar.appendChild(previewBtn);

  section.appendChild(bar);

  // Preview area
  const previewEl = document.createElement('div');
  previewEl.style.cssText = 'margin-top:12px;overflow-x:auto;';
  section.appendChild(previewEl);

  container.appendChild(section);
}

// --- Query execution -----------------------------------------------------

async function runAngle(sql, filename, btn, previewEl) {
  if (!con) return;
  btn.disabled = true;
  btn.textContent = 'Exécution…';
  try {
    const result = await con.query(sql);
    const rows = result.toArray();
    if (rows.length === 0) {
      btn.textContent = 'Aucun résultat';
      setTimeout(() => { btn.textContent = 'Télécharger CSV'; btn.disabled = false; }, 2000);
      return;
    }
    const csvContent = toCsv(rows);
    downloadCsv(csvContent, filename.replace('.md', '.csv'));
    btn.textContent = rows.length + ' lignes exportées';
    setTimeout(() => { btn.textContent = 'Télécharger CSV'; btn.disabled = false; }, 2000);
  } catch (err) {
    console.error('Angle query error:', err);
    btn.textContent = 'Erreur SQL';
    btn.disabled = false;
  }
}

async function runPreview(sql, previewEl) {
  if (!con) return;
  try {
    // Add LIMIT 10 if not already present
    let previewSql = sql;
    if (!/LIMIT\s+\d+\s*$/i.test(previewSql.trim())) {
      previewSql = previewSql.replace(/;\s*$/, '') + ' LIMIT 10';
    }
    const result = await con.query(previewSql);
    const rows = result.toArray();
    if (rows.length === 0) {
      previewEl.innerHTML = '<p style="font-size:13px;color:var(--ink-soft)">Aucun résultat.</p>';
      return;
    }
    previewEl.innerHTML = renderTable(rows);
  } catch (err) {
    previewEl.innerHTML = '<p style="color:var(--rust);font-size:13px;">Erreur SQL : ' + err.message + '</p>';
  }
}

// --- CSV generation ------------------------------------------------------

function toCsv(rows) {
  if (rows.length === 0) return '';
  const keys = Object.keys(rows[0]);
  const lines = [keys.join(',')];
  for (const row of rows) {
    const values = keys.map((k) => {
      const v = row[k];
      if (v == null) return '';
      const s = String(v);
      if (s.includes(',') || s.includes('"') || s.includes('\n')) {
        return '"' + s.replace(/"/g, '""') + '"';
      }
      return s;
    });
    lines.push(values.join(','));
  }
  return lines.join('\n');
}

function downloadCsv(content, filename) {
  const blob = new Blob([content], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

// --- Table rendering -----------------------------------------------------

function renderTable(rows) {
  if (rows.length === 0) return '';
  const keys = Object.keys(rows[0]);
  let html = '<table style="border-collapse:collapse;font-family:var(--font-data);font-size:12px;width:100%;">';
  html += '<thead><tr>';
  for (const k of keys) {
    html += '<th style="text-align:left;padding:6px 10px;border-bottom:2px solid var(--rule);color:var(--ink-soft);white-space:nowrap;">' + escapeHtml(k) + '</th>';
  }
  html += '</tr></thead><tbody>';
  for (const row of rows) {
    html += '<tr>';
    for (const k of keys) {
      const v = row[k];
      const display = v == null ? '' : String(v);
      const truncated = display.length > 80 ? display.slice(0, 77) + '…' : display;
      html += '<td style="padding:4px 10px;border-bottom:1px solid var(--rule-soft);white-space:nowrap;">' + escapeHtml(truncated) + '</td>';
    }
    html += '</tr>';
  }
  html += '</tbody></table>';
  return html;
}

function escapeHtml(s) {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

// --- Boot ----------------------------------------------------------------

init();
