/**
 * methodologie.js — Fetch methodologie.md and render via marked.js CDN.
 */

const MARKED_CDN = 'https://cdn.jsdelivr.net/npm/marked@14.1.3/+esm';

async function init() {
  const container = document.getElementById('content');
  try {
    const [mdResp, markedModule] = await Promise.all([
      fetch('methodologie.md', { cache: 'no-store' }),
      import(MARKED_CDN),
    ]);
    if (!mdResp.ok) throw new Error('Failed to fetch methodologie.md');
    const text = await mdResp.text();
    container.innerHTML = markedModule.marked.parse(text);
  } catch (err) {
    container.innerHTML = '<p style="color:var(--rust)">Erreur de chargement. Rechargez la page.</p>';
    console.error('Methodology load error:', err);
  }
}

init();
