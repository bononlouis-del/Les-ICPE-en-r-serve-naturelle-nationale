# Méthodologie d'extraction des rapports d'inspection

Ce document décrit comment les 10 599 fiches de constat exploitables dans cet outil ont été produites à partir des rapports d'inspection publiés sur Géorisques. Il sert de référence pour évaluer la fiabilité des données et comprendre leurs limites.

## Origine du corpus

**Source** : [Géorisques](https://www.georisques.gouv.fr/) — API V1, export bulk département 33 (Gironde).

**Date d'export** : avril 2026. Le ZIP archivé avec son SHA-256 est dans `données-georisques/raw/` et `PROVENANCE.txt`.

**Périmètre** : tous les rapports d'inspection publiables référencés dans `metadataFichierInspection.csv` pour les installations classées de Gironde. Cela exclut les rapports non publiables, les arrêtés préfectoraux et les mises en demeure (qui sont dans `metadataFichierHorsInspection.csv`).

**Volume** : 1 784 rapports listés, 1 782 téléchargés (1 en 404 définitif — COREP), 1 780 extraits, 2 échoués (PDFs vides de 1 KB et 3 KB).

## Les 5 étapes du pipeline

### 1. Téléchargement bulk Géorisques (`fetch_georisques.py`)

Télécharge le ZIP officiel de l'API Géorisques V1, l'archive horodaté, l'éclate en 5 CSV normalisés. Compare avec le CSV manuel data.gouv.fr et documente les écarts dans `diff_report.txt`.

**Déterministe** : le ZIP est identique à contenu identique. SHA-256 archivé.

### 2. Enrichissement des libellés (`enrichir_libelles.py`)

Désambiguïse les libellés ICPE en doublon via 3 colonnes calculées (`structure`, `etablissement`, `nom_complet`), enrichit avec commune et EPCI via geo.api.gouv.fr. Depuis avril 2026, le script est bulk-canonical (Scope Y') : il drive depuis les 2 890 lignes du bulk.

**Déterministe** : les colonnes ajoutées sont des transformations textuelles reproductibles + un cache local des appels API.

### 3. Téléchargement des PDFs (`telecharger_rapports_inspection.py`)

Télécharge les 1 784 rapports PDF depuis l'endpoint Géorisques, les renomme de façon déterministe (`{slug}_{id_icpe}_{date}_{siret}.pdf`), gère les erreurs durables (404) et transitoires (5xx, timeout). 3 workers concurrents, 0.5 s entre batches.

**Déterministe** : un PDF déjà présent sur disque n'est pas retéléchargé.

### 4. Extraction en markdown (`extract_rapports_markdown.py`)

Convertit chaque PDF en un fichier markdown avec un front matter YAML strict, validé contre un JSON Schema. Chaque PDF est classifié :

| Chemin | % du corpus | Méthode |
|--------|-------------|---------|
| `dreal_parser` | 91,3 % (1 627 rapports) | Texte lu via PyMuPDF, gabarit DREAL reconnu par 4 marqueurs, parsé en sections (Contexte, Constats, sous-sections 2-1 à 2-4, fiches N° X) |
| `pymupdf4llm_generic` | 8,6 % (153 rapports) | Texte lu via pymupdf4llm (préserve les titres et tables mais pas de structure DREAL) |
| `failed` | 0,1 % (2 rapports) | PDFs source effectivement vides (1 KB et 3 KB) — le markdown contient le front matter + la raison d'échec |

Les scans sans couche texte (~60 PDFs) ont été OCRisés en place via `ocrmypdf --force-ocr --language fra+eng` avant la classification. L'OCR est permanent : les PDFs source dans `rapports-inspection/` contiennent désormais la couche texte.

Le sidecar `_fiches.jsonl` contient pour chaque PDF la liste des fiches structurées (numéro, titre, body complet, sous-section) et leurs régions visuelles (page 1-based + bounding box en points PDF) pour l'affichage des snippets.

**Déterministe** : le `_manifest.jsonl` trace chaque extraction (SHA-256 source + version extracteur). Un PDF déjà extrait au bon SHA est skippé.

### 5. Construction du pivot (`construire_fiches.py`)

Lit le sidecar `_fiches.jsonl`, parse les 7 champs labélisés DREAL (Référence réglementaire, Thème(s), Point de contrôle déjà contrôlé, Prescription contrôlée, Constats, Type de suites proposées, Proposition de suites), joint les métadonnées depuis `rapports-inspection.csv` et `liste-icpe-gironde_enrichi.csv`, et écrit `fiches.parquet`.

Les 153 rapports `pymupdf4llm_generic` et les 2 `failed` sont inclus comme « prose rows » (1 ligne par rapport, `fiche_num = null`, `body = texte complet du markdown`). Ils sont trouvables par recherche textuelle mais n'ont pas de champs structurés.

**Résultat final** : 10 599 fiches structurées + 393 prose rows = 10 992 lignes.

**Coverage des champs** :
- `type_suite` parsé sur 99,8 % des fiches
- `theme` parsé sur 99,8 %
- `constats_body` parsé sur 99,7 %
- Bounding box présente sur 98,9 % des fiches (1,1 % non trouvées par search_for, fallback page 1)

**Déterministe** : validation par ligne contre un JSON Schema strict, halt au premier échec. Manifest de provenance append-only.

## Ce que ces données NE permettent PAS de dire

1. **Exhaustivité des inspections** : seuls les rapports *publiables* sont sur Géorisques. Des inspections ont lieu sans rapport public (suivi informel, contrôles inopinés non documentés, sanctions pénales confidentielles).

2. **Comparaison inter-régions** : le gabarit DREAL Nouvelle-Aquitaine est spécifique. Les rapports d'autres régions ont un format différent. Le parser DREAL ne fonctionne pas sur d'autres gabarits.

3. **Gravité réelle** : le champ « Type de suites proposées » est un indicateur formel. « Sans suite » ne signifie pas « pas de problème » — l'inspecteur peut avoir observé des manquements mineurs en dessous du seuil de sanction. À l'inverse, « Mise en demeure » ne signifie pas « danger immédiat ».

4. **Qualité de l'OCR** : les ~60 rapports scannés ont été OCRisés avec Tesseract (modèle français). La qualité dépend du scan original. Les fiches extraites de ces rapports peuvent contenir des erreurs de reconnaissance.

5. **Complétude des champs labélisés** : le parsing des champs (Thème, Type de suites, etc.) dépend de la conformité du rapport au gabarit DREAL. Les rapports avec des espaces doubles, des sauts de ligne inattendus, ou des fautes de frappe dans les labels ont un taux de parsing légèrement inférieur.

## Reproduction

```bash
# Pré-requis : Python 3.11+, uv, tesseract + pack français
# brew install tesseract tesseract-lang  # macOS

# 1. Télécharger le bulk officiel
python3 scripts/fetch_georisques.py

# 2. Enrichir les libellés
python3 scripts/enrichir_libelles.py

# 3. Télécharger les PDFs
python3 scripts/telecharger_rapports_inspection.py

# 4. Extraire en markdown + sidecar
uv run scripts/extract_rapports_markdown.py

# 5. Construire le pivot
uv run scripts/construire_fiches.py

# 6. (Optionnel) Reconstruire l'index des angles
python3 scripts/build_angles_index.py
```

Chaque étape est idempotente. Un re-run ne retélécharge / ré-extrait que ce qui a changé.

## Versioning

| Artefact | Version | Commit |
|----------|---------|--------|
| extract_rapports_markdown.py | 0.2.0 | Sidecar _fiches.jsonl + bbox |
| construire_fiches.py | 0.1.0 | Pivot initial |
| Corpus Géorisques | Avril 2026 | SHA dans PROVENANCE.txt |

---

*Retour : [Vérifier](./) · [Analyser par angle](angles.html) · [Accueil](../)*
