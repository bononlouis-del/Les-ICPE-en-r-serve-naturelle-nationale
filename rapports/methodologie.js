/**
 * methodologie.js — Fetch and render two methodology docs via marked.js CDN.
 *
 * 1. rapports/methodologie.md    — pipeline d'extraction des rapports
 * 2. docs/methodo-carte.md       — pipeline carte + audit des coordonnées
 */

const MARKED_CDN = 'https://cdn.jsdelivr.net/npm/marked@14.1.3/+esm';

const DOCS = [
  { url: 'methodologie.md', label: 'Rapports d\'inspection' },
  { url: '../docs/methodo-carte.md', label: 'Carte interactive et audit des coordonnées' },
];

async function init() {
  const container = document.getElementById('content');
  try {
    const markedModule = await import(MARKED_CDN);
    const responses = await Promise.all(DOCS.map((d) => fetch(d.url, { cache: 'no-store' })));
    let html = '';
    for (let i = 0; i < DOCS.length; i++) {
      if (!responses[i].ok) {
        html += `<p style="color:var(--rust)">Impossible de charger ${DOCS[i].url}.</p>`;
        continue;
      }
      const text = await responses[i].text();
      if (i > 0) {
        html += '<hr style="margin:48px 0;border:none;border-top:2px solid var(--rule);">';
      }
      html += markedModule.marked.parse(text);
    }
    container.innerHTML = html;
  } catch (err) {
    container.innerHTML = '<p style="color:var(--rust)">Erreur de chargement. Rechargez la page.</p>';
    console.error('Methodology load error:', err);
  }
}

init();
