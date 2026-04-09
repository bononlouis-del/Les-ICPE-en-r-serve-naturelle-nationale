# Méthodologie — Rapports d'inspection

Cette section décrit le pipeline qui transforme les rapports
d'inspection PDF en fiches de constat structurées exploitables dans
l'outil `/rapports/`. Elle fait suite à la méthodologie de la carte
et de l'audit des coordonnées documentée ci-dessous : les sources de
données (section 1), l'enrichissement (section 2) et le téléchargement
des rapports (section 1.5) y sont déjà couverts.

## 7. Téléchargement des rapports PDF

Le script `scripts/telecharger_rapports_inspection.py` télécharge les
rapports d'inspection publiables depuis l'endpoint Géorisques, les
renomme de façon déterministe et les stocke dans `rapports-inspection/`.

- **Nommage** : `{slug}_{id_icpe}_{date}_{siret}.pdf`, avec fallbacks
  `nosiret` / `nodate`
- **Concurrence** : 3 workers, 0,5 s entre batches
- **Erreurs durables** : les identifiants en 404 définitif sont
  mémorisés dans `_404.txt` et skippés aux runs suivants
- **Idempotence** : un fichier déjà présent sur disque n'est pas
  retéléchargé

**Volume** : 1 784 rapports listés, 1 782 téléchargés (1 en 404
définitif — COREP), 2 échoués à l'extraction (PDFs vides de 1 Ko
et 3 Ko).

## 8. Extraction en markdown

Le script `scripts/extract_rapports_markdown.py` (v0.2.0) convertit
chaque PDF en fichier markdown avec un front matter YAML strict, validé
contre un JSON Schema (`scripts/schemas/markdown_frontmatter.json`).

### 8.1 Classification et routage

Chaque PDF est classifié d'après son texte natif (lu via PyMuPDF), puis
routé vers le chemin d'extraction approprié :

| Chemin | Part du corpus | Méthode |
|---|---:|---|
| `dreal_parser` | 91,3 % (1 627) | Gabarit DREAL reconnu par 4 marqueurs. Parsé en sections (Contexte, Constats, sous-sections 2-1 à 2-4, fiches N° X). |
| `pymupdf4llm_generic` | 8,6 % (153) | Gabarit non reconnu. Conversion via `pymupdf4llm` — préserve titres et tables, pas de structure sémantique. |
| `failed` | 0,1 % (2) | PDFs source effectivement vides. Le markdown contient le front matter et la raison d'échec. |

### 8.2 Traitement des scans

Les ~60 PDFs sans couche texte ont été OCRisés en place via
`ocrmypdf --force-ocr --language fra+eng` avant la classification.
L'OCR est **permanent** : les PDFs dans `rapports-inspection/`
contiennent désormais la couche texte et n'ont pas besoin d'être
ré-OCRisés.

### 8.3 Sidecar structuré (`_fiches.jsonl`)

Pour chaque PDF, l'extracteur écrit une ligne dans le sidecar
`rapports-inspection-markdown/_fiches.jsonl` contenant :

- La liste des fiches de constat structurées (numéro, titre, body
  complet, sous-section d'origine)
- Les **régions visuelles** de chaque fiche dans le PDF : numéro de
  page (1-based) et bounding box en points PDF `[x0, y0, x1, y1]`,
  calculés via `page.search_for()` avec découpe du titre au titre
  suivant
- Les PDFs non-DREAL reçoivent une entrée sidecar avec `fiches: []`

**Couverture bbox** : 98,9 % des fiches (10 483 / 10 599) ont des
régions visuelles exploitables. Les 1,1 % restantes (titre non trouvé
par `search_for`, typiquement OCR dégradé) ont `regions: []` — le
client affiche le PDF à la page 1 en fallback.

### 8.4 Idempotence

Le manifeste append-only `_manifest.jsonl` trace chaque extraction
(SHA-256 du PDF source + version de l'extracteur). Au re-run, un PDF
déjà extrait au bon SHA et à la bonne version est skippé. Un bump de
version invalide automatiquement toutes les entrées précédentes.

## 9. Construction du pivot (`fiches.parquet`)

Le script `scripts/construire_fiches.py` lit le sidecar et produit le
pivot unique consommé par l'outil `/rapports/`.

### 9.1 Parsing des champs labélisés

Pour chaque fiche DREAL, le body est parsé par regex pour extraire les
7 champs du gabarit :

| Champ | Label recherché | Couverture |
|---|---|---:|
| `reference_reglementaire` | `Référence réglementaire :` | 99,7 % |
| `theme` | `Thème(s) :` | 99,8 % |
| `deja_controle` | `Point de contrôle déjà contrôlé :` | 99,6 % |
| `prescription` | `Prescription contrôlée :` | 99,5 % |
| `constats_body` | `Constats :` | 99,7 % |
| `type_suite` | `Type de suites proposées :` | 99,8 % |
| `proposition_suite` | `Proposition de suites :` | 99,4 % |

Les regex utilisent `\s+` entre les mots des labels pour absorber les
espaces doubles du gabarit DREAL. Les artefacts de fin de champ
(numéros de page, barres de tableaux) sont retirés automatiquement.

### 9.2 Jointures

Le pivot joint deux CSV pour enrichir chaque fiche :

- `carte/data/rapports-inspection.csv` via `nom_fichier_local` →
  récupère `url_markdown`, `url_pages`, `identifiant_fichier`
- `carte/data/liste-icpe-gironde_enrichi.csv` via `id_icpe` normalisé →
  récupère `nom_commune`, `code_insee_commune`, `regime_icpe`,
  `categorie_seveso`, `epci_nom`, `epci_siren`

### 9.3 Inclusion des rapports non structurés

Les 153 rapports `pymupdf4llm_generic` et les 2 `failed` sont inclus
comme **prose rows** : 1 ligne par rapport, `fiche_num = null`,
`body = texte complet du markdown` (sans front matter). Ils sont
trouvables par recherche textuelle dans l'outil mais n'ont pas de
champs structurés (thème, type de suites, etc.).

### 9.4 Validation et provenance

- Chaque ligne est validée contre `scripts/schemas/fiche.json`
  (`additionalProperties: false`, halt au premier échec)
- Unicité des `fiche_id` vérifiée (index séquentiel 0-padded par
  PDF : `{source_pdf_stem}_f01`, `_f02`, … ou `_prose`)
- Manifeste de provenance `fiches-manifest.jsonl` (SHA-256 des
  3 fichiers d'entrée + des 3 fichiers de sortie)

### 9.5 Résultat final

| Métrique | Valeur |
|---|---:|
| Fiches structurées | 10 599 |
| Prose rows | 393 |
| **Total lignes** | **10 992** |
| Rapports avec fiches | 1 389 |
| Rapports sans fiches | 393 |
| Taille parquet | 25 Mo |

## 10. Limites des données extraites

1. **Exhaustivité** : seuls les rapports *publiables* sont sur
   Géorisques. Des inspections ont lieu sans rapport public (suivi
   informel, contrôles inopinés non documentés, sanctions pénales
   confidentielles).

2. **Gabarit régional** : le parser DREAL Nouvelle-Aquitaine est
   spécifique à cette région. Les rapports d'autres DREAL ont un
   format différent. Comparaison inter-régions impossible avec ce
   pipeline.

3. **Gravité formelle vs. réelle** : « Sans suite » ne signifie pas
   « pas de problème » — l'inspecteur peut avoir observé des
   manquements mineurs. « Mise en demeure » ne signifie pas
   « danger immédiat ». Le champ `type_suite` est un indicateur
   administratif, pas un score de risque.

4. **Qualité de l'OCR** : les ~60 rapports scannés ont été OCRisés
   avec Tesseract (modèle français). La qualité dépend du scan
   original et peut produire des erreurs dans les champs parsés.

5. **Robustesse du parsing** : les rapports avec espaces doubles,
   sauts de ligne inattendus, ou fautes de frappe dans les labels ont
   un taux de parsing légèrement inférieur (~0,2 % de champs manqués).

## 11. Versions

| Artefact | Version | Description |
|---|---|---|
| `extract_rapports_markdown.py` | 0.2.0 | Sidecar `_fiches.jsonl` + bbox par fiche |
| `construire_fiches.py` | 0.1.0 | Pivot initial avec prose rows |
| Corpus Géorisques | Avril 2026 | SHA dans `PROVENANCE.txt` |

---

*Retour : [Vérifier](./) · [Analyser par angle](angles.html) · [Accueil](../)*
