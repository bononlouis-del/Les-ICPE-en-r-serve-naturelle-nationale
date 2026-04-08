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
├── scripts/                       # pipeline Géorisques
│   ├── fetch_georisques.py                 # téléchargement + extraction bulk officiel
│   ├── enrichir_libelles.py                # enrichissement des libellés ICPE
│   ├── telecharger_rapports_inspection.py  # téléchargement des PDFs d'inspection
│   └── _metadonnees_util.py                # helper partagé pour le dictionnaire multi-fichiers
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

Le pipeline Géorisques est rejouable (stdlib Python uniquement, aucune
dépendance à installer) :

```bash
# 1. Télécharge et extrait le bulk officiel, archive le ZIP, écrit le diff
python3 scripts/fetch_georisques.py

# 2. Recalcule structure / etablissement / nom_complet + commune / EPCI
python3 scripts/enrichir_libelles.py

# 3. Télécharge les rapports d'inspection PDF, renomme, indexe avec URL Pages
python3 scripts/telecharger_rapports_inspection.py

# Flags utiles du script 3 :
#   --limit 5   : test progressif sur 5 PDFs
#   --dry-run   : calcule le plan sans rien écrire ni télécharger
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
