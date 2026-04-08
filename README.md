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
| Liste ICPE Gironde | [Géorisques](https://www.georisques.gouv.fr/) |
| Contour Gironde | [geo.api.gouv.fr](https://geo.api.gouv.fr/decoupage-administratif) |
| Communes Gironde | [geo.api.gouv.fr](https://geo.api.gouv.fr/decoupage-administratif) |
| Réserves Naturelles Nationales | [IGN Géoplateforme (WFS patrinat_rnn)](https://data.geopf.fr/wfs/ows) |
| Réserves Naturelles Régionales | [IGN Géoplateforme (WFS patrinat_rnr)](https://data.geopf.fr/wfs/ows) |

Le fichier `carte-interactive/liste-icpe-gironde.csv` est la source
originale (2 888 lignes). Les données des réserves naturelles sont
pré-traitées (filtre bounding-box Gironde) par
`carte-interactive/scripts/prep_reserves.py`.

## Structure du dépôt

```
├── index.html                     # point d'entrée (racine, servi par Pages)
├── README.md
└── carte-interactive/
    ├── app.js                     # logique de la carte
    ├── style.css                  # design « cahier d'enquête »
    ├── liste-icpe-gironde.csv     # données source
    ├── data/                      # GeoJSON pré-traités
    ├── fonts/                     # Fraunces + IBM Plex (WOFF2)
    └── scripts/                   # prep_reserves.py, fetch_fonts.sh
```

Tout — sauf `index.html` et `README.md` — vit dans `carte-interactive/`.

## Rafraîchir les données

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
