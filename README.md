# ICPE en Gironde — cahier d'enquête

Carte d'exploration interactive des **Installations Classées pour la
Protection de l'Environnement** (ICPE) en Gironde, avec superposition des
réserves naturelles nationales et régionales.

**Carte en ligne :** <https://bononlouis-del.github.io/Les-ICPE-en-r-serve-naturelle-nationale/>

## Ce que la carte permet

- Visualiser 2 888 installations classées en Gironde, colorées selon le
  régime, le niveau Seveso, la priorité nationale, l'IED ou le secteur.
- Filtrer par combinaison de critères (recherche, régime, Seveso, priorité,
  IED, secteur) avec recalcul instantané.
- Parcourir un instantané temporel mensuel via un curseur : voir quels
  dossiers ICPE étaient actifs à une date donnée.
- Basculer l'affichage du contour du département, des communes, des
  Réserves Naturelles Nationales et Régionales.
- Ouvrir directement la fiche Géorisques de chaque site.

## Sources de données

| Donnée | Source |
|---|---|
| Liste ICPE Gironde (manuelle, géométries) | [data.gouv.fr — export Géorisques](https://www.data.gouv.fr/) |
| Bulk ICPE Gironde (canonique) | [API Géorisques V1](https://www.georisques.gouv.fr/doc-api) |
| Contour Gironde | [geo.api.gouv.fr](https://geo.api.gouv.fr/decoupage-administratif) |
| Communes Gironde | [geo.api.gouv.fr](https://geo.api.gouv.fr/decoupage-administratif) |
| Réserves Naturelles Nationales | [IGN Géoplateforme (WFS patrinat_rnn)](https://data.geopf.fr/wfs/ows) |
| Réserves Naturelles Régionales | [IGN Géoplateforme (WFS patrinat_rnr)](https://data.geopf.fr/wfs/ows) |

Deux sources ICPE coexistent :

- `carte-interactive/liste-icpe-gironde.csv` (2 888 lignes) — export
  historique depuis data.gouv.fr, contient les géométries pré-formatées
  (`Geo Point`, `Geo Shape`) que la carte consomme directement.
- `données-georisques/` — export bulk officiel de l'API Géorisques V1
  pour le département 33, canonique. ZIP archivé horodaté dans
  `raw/` (sha256 dans `PROVENANCE.txt`), éclaté en cinq CSV normalisés
  reliés par `codeAiot` : installations, inspections, rapports
  d'inspection, documents hors inspection (arrêtés, rapports publics,
  mises en demeure), rubriques ICPE.

Les deux sources sont croisées par `scripts/enrichir_libelles.py` qui
calcule trois colonnes (`structure`, `etablissement`, `nom_complet`)
pour désambiguïser les libellés en doublon (ex. les 22 entrées
`BORDEAUX METROPOLE`) et produit
`carte-interactive/data/liste-icpe-gironde_enrichi.csv` — c'est le
fichier que la carte charge.

Par-dessus, `scripts/telecharger_rapports_inspection.py` télécharge les
rapports d'inspection publiables depuis Géorisques (1 784 PDFs), les
renomme de façon déterministe à partir du libellé désambiguïsé, les
stocke dans `rapports-inspection/` et produit
`carte-interactive/data/rapports-inspection.csv` avec une URL GitHub
Pages pour chaque rapport. Le fichier enrichi reçoit une colonne
supplémentaire `nb_rapports_inspection` comptant les rapports
disponibles par installation.

Ensuite, `scripts/extract_rapports_markdown.py` convertit ces PDFs en
fichiers markdown déterministes dans `rapports-inspection-markdown/`,
avec un front matter YAML strict validé par JSON Schema. Chaque
markdown est classifié vers l'un des chemins suivants :

- **`dreal_parser`** (≈ 88 %) — PDF reconnu comme gabarit DREAL
  Nouvelle-Aquitaine, parsé en sections sémantiques (Contexte,
  Constats, Fiches de constat N° X comme H4 indexables).
- **`pymupdf4llm_generic`** (≈ 9 %) — PDF texte au gabarit non DREAL
  (courriers, propositions de suites), converti via `pymupdf4llm`.
- **`ocr_then_dreal_parser`** / **`ocr_then_pymupdf4llm`** (≈ 3 %) —
  scan sans couche texte, OCRisé via `ocrmypdf --force-ocr --language
  fra+eng` puis routé vers le parser correspondant. L'OCR est fait
  en place (atomique via tmp + os.replace).

Un `_manifest.jsonl` append-only trace chaque extraction avec
`source_sha256` et `markdown_sha256`, ce qui garantit l'idempotence :
un PDF déjà extrait au bon sha et à la bonne version du script est
skippé. La colonne `url_markdown` du CSV rapports pointe vers la
version markdown GitHub Pages.

Le dictionnaire des colonnes (schéma multi-fichiers : `fichier`,
`nom_original`, `alias`, `definition`) est dans
`carte-interactive/data/metadonnees_colonnes.csv`. Il décrit les
colonnes de `liste-icpe-gironde_enrichi.csv` **et** celles de
`rapports-inspection.csv`, chaque script du pipeline possédant ses
propres lignes via le helper partagé `scripts/_metadonnees_util.py`.

Les données des réserves naturelles sont pré-traitées (filtre
bounding-box Gironde) par `carte-interactive/scripts/prep_reserves.py`.

## Structure du dépôt

```
├── index.html                     # point d'entrée (racine, servi par Pages)
├── README.md
├── scripts/                       # pipeline Géorisques + extraction markdown
│   ├── fetch_georisques.py                 # téléchargement + extraction bulk officiel
│   ├── enrichir_libelles.py                # enrichissement des libellés ICPE
│   ├── telecharger_rapports_inspection.py  # téléchargement des PDFs d'inspection
│   ├── extract_rapports_markdown.py        # extraction markdown des PDFs (pymupdf + ocrmypdf)
│   ├── _metadonnees_util.py                # helper partagé pour le dictionnaire multi-fichiers
│   ├── schemas/
│   │   └── markdown_frontmatter.json       # JSON Schema draft-07 du front matter YAML
│   └── tests/                              # tests stdlib + uv (unittest discover)
├── données-georisques/            # source canonique API Géorisques V1
│   ├── raw/                       # archives ZIP datées (traçabilité sha256)
│   ├── InstallationClassee.csv    # installations (brut)
│   ├── InstallationClassee_enrichi.csv
│   ├── inspection.csv             # historique des inspections
│   ├── metadataFichierInspection.csv
│   ├── metadataFichierHorsInspection.csv
│   ├── rubriqueIC.csv             # rubriques ICPE classées
│   ├── PROVENANCE.txt             # URL + sha256 du ZIP source
│   ├── diff_report.txt            # diff bulk ↔ CSV manuel (automatique)
│   └── diff_analysis.md           # investigation humaine des écarts
├── rapports-inspection/           # PDFs d'inspection téléchargés depuis Géorisques
│   ├── *.pdf                      # nommés {slug}_{id_icpe}_{date}_{siret}.pdf
│   ├── _404.txt                   # mémoire des identifiants définitivement 404
│   └── _erreurs.log               # rapport du dernier run (durables + transitoires)
├── rapports-inspection-markdown/  # versions markdown des PDFs (1 .md par PDF)
│   ├── *.md                       # front matter YAML + corps sémantique
│   ├── _manifest.jsonl            # provenance append-only (sha256, version, timestamp)
│   └── _erreurs.log               # rapport des extractions failed du dernier run
└── carte-interactive/
    ├── app.js                     # logique de la carte
    ├── style.css                  # design « cahier d'enquête »
    ├── liste-icpe-gironde.csv     # source manuelle (export data.gouv.fr)
    ├── data/
    │   ├── liste-icpe-gironde_enrichi.csv  # consommé par la carte (colonnes aliasées + nb_rapports_inspection)
    │   ├── rapports-inspection.csv         # 1 ligne par rapport, URL Pages + statut téléchargement
    │   ├── metadonnees_colonnes.csv        # dictionnaire multi-fichiers (fichier, nom_original, alias, definition)
    │   ├── reserves-naturelles-nationales.geojson
    │   └── reserves-naturelles-regionales.geojson
    ├── fonts/                     # Fraunces + IBM Plex (WOFF2)
    └── scripts/                   # prep_reserves.py, fetch_fonts.sh
```

## Rafraîchir les données

Les scripts 1 à 3 ne dépendent que de la stdlib Python 3.13+. Le script
4 a des dépendances tierces déclarées inline via PEP 723 (`pymupdf`,
`pymupdf4llm`, `jsonschema`) résolues automatiquement par `uv run`, plus
`ocrmypdf` invoqué via `uvx` pour les scans. Sur macOS :
`brew install tesseract tesseract-lang` une fois pour disposer du pack
français.

```bash
# 1. Télécharge et extrait le bulk officiel, archive le ZIP, écrit le diff
python3 scripts/fetch_georisques.py

# 2. Recalcule structure / etablissement / nom_complet + commune / EPCI
python3 scripts/enrichir_libelles.py

# 3. Télécharge les rapports d'inspection PDF, renomme, indexe avec URL Pages
python3 scripts/telecharger_rapports_inspection.py

# 4. Convertit les PDFs d'inspection en markdown avec front matter YAML
uv run scripts/extract_rapports_markdown.py

# Flags utiles du script 3 :
#   --limit 5   : test progressif sur 5 PDFs
#   --dry-run   : calcule le plan sans rien écrire ni télécharger

# Flags utiles du script 4 :
#   --limit 10  : test progressif sur 10 PDFs
#   --dry-run   : liste ce qui serait fait
#   --force     : ignore le manifeste et ré-extrait tout
#   --no-ocr    : marque les scans FAILED au lieu d'appeler ocrmypdf
#   --validate  : relit tous les .md et valide leur front matter
#   --only-ocr  : pré-OCRise les scans sans écrire de markdown
```

Chaque exécution de `fetch_georisques.py` archive un nouveau ZIP horodaté
dans `données-georisques/raw/` et met à jour `PROVENANCE.txt` et
`diff_report.txt`.

`telecharger_rapports_inspection.py` est **idempotent** : il ne
retéléchargera pas un PDF déjà présent dans `rapports-inspection/`,
donc une interruption (Ctrl-C) est reprise proprement au rerun. Les
téléchargements sont parallélisés par batches de 3 avec 0.5 s de
pause entre batches (politesse envers le serveur Géorisques). Les
échecs durables (HTTP 404) sont mémorisés dans
`rapports-inspection/_404.txt` pour ne pas être retentés, les échecs
transitoires (5xx, réseau, timeout) le seront au prochain run.

`extract_rapports_markdown.py` est **idempotent** grâce au manifeste
append-only `rapports-inspection-markdown/_manifest.jsonl` : un PDF
déjà extrait au bon `source_sha256` et à la bonne `extraction_version`
est skippé. L'OCR est fait en place sur les scans (`rapports-inspection/`
est modifié), toujours atomiquement via tmp + `os.replace`. Le run
complet sur 1 782 PDFs écrit 1 780 markdowns exploitables (≈ 88 %
`dreal_parser`, ≈ 9 % `pymupdf4llm_generic`, ≈ 3 % avec OCR préalable)
et 2 markdowns `failed` pour des PDFs source effectivement vides (1 KB
et 3 KB). Les 2 cibles `failed` contiennent tout de même un front matter
complet et une raison lisible, et ont un `url_markdown` valide pour
garder la cohérence 1 PDF = 1 .md.

Les tests :

```bash
# Tests unitaires (stdlib uniquement, pas de deps)
python3 -m unittest discover scripts/tests

# Suite complète (intégration + schema, requiert uv)
uv run --with jsonschema --with pymupdf --with pymupdf4llm \
    -m unittest discover scripts/tests
```

- **Réserves naturelles** : `uv run carte-interactive/scripts/prep_reserves.py`
- **Polices** : `bash carte-interactive/scripts/fetch_fonts.sh`

## Pile technique

- Leaflet 1.9 (canvas renderer) + Leaflet.markercluster
- PapaParse (worker mode) pour le CSV
- `@turf/simplify` pour alléger le contour des communes à la volée
- Polices auto-hébergées : Fraunces (display), IBM Plex Sans (UI), IBM Plex
  Mono (données)
- Pas de framework, pas de build, pas de bundler. Page statique pure.

## Licence

Code : MIT. Données : voir les sources respectives (Etalab / Licence Ouverte
pour les couches IGN et geo.api.gouv.fr ; conditions Géorisques pour le
fichier ICPE).
