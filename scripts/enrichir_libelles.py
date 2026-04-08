#!/usr/bin/env python3
"""
enrichir_libelles.py — Désambiguïsation des libellés ICPE.

Ajoute trois colonnes (``structure``, ``etablissement``, ``libelle_complet``)
à deux fichiers :

- ``données-georisques/InstallationClassee.csv``
  → ``données-georisques/InstallationClassee_enrichi.csv``
  (conserve les noms de colonnes Géorisques d'origine)

- ``carte-interactive/liste-icpe-gironde.csv``
  → ``carte-interactive/data/liste-icpe-gironde_enrichi.csv``
  (colonnes renommées avec des alias lisibles)
  → ``carte-interactive/data/metadonnees_colonnes.csv``
  (dictionnaire nom_original / alias / définition)

Les fichiers originaux ne sont jamais modifiés.

Algorithme en deux passes (appliqué sur le bulk, qui seul contient
les adresses ; puis joint au manuel par ``codeAiot`` ↔ ``ident``) :

**Passe 1 — classification initiale**

1. Si ``raisonSociale`` commence par "MAIRIE -" / "Mairie -"
   → intact (structure = libellé, etablissement vide).
2. Sinon si ``raisonSociale`` contient un séparateur " - " ou " – "
   → split ``structure`` / ``etablissement_base``.
3. Sinon si le libellé est en doublon dans le bulk
   → structure = libellé, etablissement_base = "".
4. Sinon → structure = libellé, etablissement_base = "", pas ambigu.

**Passe 2 — désambiguïsation progressive**

Pour chaque groupe de lignes partageant le même ``libelle_complet`` après
la passe 1, enrichir l'``etablissement`` en concaténant ``commune`` puis
``adresse1``, jusqu'à ce que toutes les lignes du groupe soient distinctes.
Tant qu'il reste des collisions résiduelles **et** qu'aucun ajout n'est
possible (commune et adresse déjà incluses), suffixer " (#1, #2, …)" dans
l'ordre ``codeAiot``. Le suffixe est donc réellement un dernier recours.

Usage :
    python3 scripts/enrichir_libelles.py
"""

from __future__ import annotations

import csv
import re
import sys
from collections import defaultdict
from pathlib import Path

# --- Configuration ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BULK_IN = PROJECT_ROOT / "données-georisques" / "InstallationClassee.csv"
BULK_OUT = PROJECT_ROOT / "données-georisques" / "InstallationClassee_enrichi.csv"
MANUAL_IN = PROJECT_ROOT / "carte-interactive" / "liste-icpe-gironde.csv"
MANUAL_OUT_DIR = PROJECT_ROOT / "carte-interactive" / "data"
MANUAL_OUT = MANUAL_OUT_DIR / "liste-icpe-gironde_enrichi.csv"
METADATA_OUT = MANUAL_OUT_DIR / "metadonnees_colonnes.csv"

SEPARATOR_PATTERN = re.compile(r"^(.+?)\s+[-–]\s+(.+)$")
MAIRIE_PATTERN = re.compile(r"^mairie\s+[-–]\s+", re.IGNORECASE)
DISPLAY_SEP = " — "  # em-dash pour l'affichage, distinct du séparateur source

# Spécification des colonnes du CSV manuel enrichi.
# Tuples : (source_key, alias, nom_original_metadata, definition).
# - source_key : clé dans le dict de ligne (nom interne).
# - alias : nom de colonne dans le fichier aliasé écrit dans data/.
# - nom_original_metadata : ce qui apparaît dans metadonnees_colonnes.csv.
#   Pour les colonnes calculées par ce script, on écrit "(calculé)".
# - definition : description lisible par un humain.
# L'ordre de cette liste fixe l'ordre des colonnes dans le fichier aliasé.
# Les colonnes absentes de cette spec sont supprimées à l'écriture
# (en pratique, on ne supprime que la colonne anonyme vide du CSV source).
MANUAL_COLUMN_SPEC: list[tuple[str, str, str, str]] = [
    (
        "ident",
        "id_icpe",
        "ident",
        "Identifiant unique de l'installation classée (codeAIOT Géorisques, "
        "sans les zéros de tête). Clé de jointure stable entre exports.",
    ),
    (
        "libelle",
        "nom_original",
        "libelle",
        "Raison sociale de l'installation telle que saisie dans Géorisques. "
        "Peut être ambigu : plusieurs installations peuvent partager le même "
        "libellé.",
    ),
    (
        "structure",
        "structure",
        "(calculé)",
        "Nom de la structure ou organisation mère, calculé à partir du "
        "libellé original. Si le libellé contient un séparateur ' - ' (hors "
        "cas MAIRIE), partie avant le séparateur ; sinon libellé entier.",
    ),
    (
        "etablissement",
        "etablissement",
        "(calculé)",
        "Sous-nom identifiant l'établissement spécifique quand le libellé est "
        "ambigu ou composite. Vide quand le libellé est déjà unique. Calculé "
        "à partir du libellé, de la commune et de l'adresse de l'export bulk "
        "Géorisques.",
    ),
    (
        "libelle_complet",
        "nom_complet",
        "(calculé)",
        "Nom complet désambiguïsé pour affichage et analyse. Garanti unique "
        "dans le jeu de données. Concaténation de structure et établissement "
        "avec un suffixe (#1, #2, …) en tout dernier recours.",
    ),
    (
        "insee",
        "code_insee_commune",
        "insee",
        "Code INSEE de la commune d'implantation (5 chiffres, ex : 33063 = "
        "Bordeaux).",
    ),
    (
        "Geo Point",
        "coordonnees_lat_lon",
        "Geo Point",
        "Latitude et longitude de l'installation (WGS84), au format 'lat, lon'.",
    ),
    (
        "Geo Shape",
        "geometrie_geojson",
        "Geo Shape",
        "Géométrie de l'installation au format GeoJSON (type Point), "
        "utilisable directement pour la cartographie.",
    ),
    (
        "gid",
        "id_ligne_export",
        "gid",
        "Identifiant séquentiel de la ligne dans l'export data.gouv.fr "
        "(1 à 2888). Non stable entre deux exports — utiliser id_icpe pour "
        "les jointures.",
    ),
    (
        "siret",
        "siret",
        "siret",
        "Numéro SIRET de l'exploitant (14 chiffres). Vide si non renseigné "
        "(283 lignes sans SIRET). Un même SIRET peut couvrir plusieurs "
        "installations ICPE distinctes.",
    ),
    (
        "regime",
        "regime_icpe",
        "regime",
        "Régime ICPE en vigueur : AUTORISATION, ENREGISTREMENT, AUTRE, "
        "NON_ICPE. Détermine le niveau de contrôle administratif.",
    ),
    (
        "cat_seveso",
        "categorie_seveso",
        "cat_seveso",
        "Catégorie Seveso : NON_SEVESO, SEUIL_BAS, SEUIL_HAUT. Indique le "
        "niveau de risque technologique majeur.",
    ),
    (
        "priorite_nationale",
        "priorite_nationale",
        "priorite_nationale",
        "TRUE si l'installation est identifiée comme priorité nationale "
        "d'inspection, FALSE sinon.",
    ),
    (
        "fiche",
        "url_fiche_georisques",
        "fiche",
        "URL de la fiche publique Géorisques détaillant l'installation "
        "(inspections, arrêtés, rubriques).",
    ),
    (
        "bovins",
        "elevage_bovins",
        "bovins",
        "TRUE si l'installation comprend un élevage bovin déclaré, FALSE sinon.",
    ),
    (
        "porcs",
        "elevage_porcs",
        "porcs",
        "TRUE si l'installation comprend un élevage porcin déclaré, FALSE sinon.",
    ),
    (
        "volailles",
        "elevage_volailles",
        "volailles",
        "TRUE si l'installation comprend un élevage de volailles déclaré, "
        "FALSE sinon.",
    ),
    (
        "carriere",
        "activite_carriere",
        "carriere",
        "TRUE si l'installation est une carrière (extraction de matériaux), "
        "FALSE sinon.",
    ),
    (
        "eolienne",
        "activite_eolienne",
        "eolienne",
        "TRUE si l'installation est un parc éolien ou une éolienne classée, "
        "FALSE sinon.",
    ),
    (
        "industrie",
        "activite_industrielle",
        "industrie",
        "TRUE si l'installation a une activité industrielle déclarée, "
        "FALSE sinon.",
    ),
    (
        "ied",
        "directive_ied",
        "ied",
        "TRUE si l'installation relève de la directive IED (Industrial "
        "Emissions Directive, 2010/75/UE) imposant les MTD, FALSE sinon.",
    ),
    (
        "activite_principale",
        "code_naf_division",
        "activite_principale",
        "Code NAF division (niveau 2 chiffres) de l'activité principale. "
        "Ex : 11 = fabrication de boissons, 46 = commerce de gros, 38 = "
        "collecte/traitement des déchets. Vide pour 978 lignes.",
    ),
    (
        "cdate",
        "date_enregistrement",
        "cdate",
        "Date d'enregistrement dans l'export data.gouv.fr (ISO 8601). La "
        "colonne mdate de la source était un doublon strict et a été droppée.",
    ),
    (
        "année",
        "annee_enregistrement",
        "année",
        "Année extraite de la date d'enregistrement (2025 ou 2026).",
    ),
]

# Colonnes du CSV manuel volontairement droppées de la sortie aliasée :
# - '' (colonne anonyme toujours vide, artefact)
# - geom_o  (toujours 0.0, signification non documentée, aucun usage)
# - geom_err (toujours vide, signification non documentée, aucun usage)
# - mdate   (doublon strict de cdate dans ce jeu de données)
DROPPED_COLUMNS = {"", "geom_o", "geom_err", "mdate"}


# --- Logique d'enrichissement ---------------------------------------------


def _normalize_ident(value: str) -> str:
    """Aligne codeAiot (bulk) et ident (manuel) en strippant les zéros gauches."""
    return value.strip().lstrip("0") or "0"


def enrich_bulk_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Calcule structure/etablissement/libelle_complet pour chaque ligne.

    Implémente l'algorithme en 2 passes + suffixe final décrit en tête de
    module : classification initiale puis désambiguïsation progressive en
    injectant commune puis adresse1 dans les seuls groupes en collision.
    """
    # Passe 1 — classification initiale. On écrit dans des clés temporaires
    # (_structure, _etab, _libcomp) pour pouvoir mutiler les valeurs dans
    # les passes suivantes sans toucher les champs finaux.
    for row in rows:
        raison = row["raisonSociale"]
        if MAIRIE_PATTERN.match(raison):
            row["_structure"] = raison
            row["_etab"] = ""
        elif match := SEPARATOR_PATTERN.match(raison):
            row["_structure"] = match.group(1).strip()
            row["_etab"] = match.group(2).strip()
        else:
            row["_structure"] = raison
            row["_etab"] = ""
        row["_libcomp"] = _rebuild_libcomp(row["_structure"], row["_etab"])

    # Passe 2 — désambiguïsation progressive. Pour chaque critère successif
    # (commune, puis adresse1), on ne modifie QUE les lignes dont le
    # libelle_complet collide encore. Les lignes uniques sont laissées
    # intactes (pas de bruit inutile).
    for field in ("commune", "adresse1"):
        for group in _collision_groups(rows):
            for row in group:
                addition = row.get(field, "").strip()
                if not addition or addition in row["_etab"]:
                    continue
                row["_etab"] = (
                    f"{row['_etab']}{DISPLAY_SEP}{addition}"
                    if row["_etab"]
                    else addition
                )
                row["_libcomp"] = _rebuild_libcomp(row["_structure"], row["_etab"])

    # Passe 3 — suffixe (#n) en tout dernier recours, pour les groupes
    # où ni commune ni adresse1 n'ont pu lever l'ambiguïté.
    collision_count = 0
    for group in _collision_groups(rows):
        collision_count += len(group)
        group.sort(key=lambda r: r["codeAiot"])
        for index, row in enumerate(group, start=1):
            if row["_etab"]:
                row["_etab"] = f"{row['_etab']} (#{index})"
            else:
                row["_etab"] = f"(#{index})"
            row["_libcomp"] = _rebuild_libcomp(row["_structure"], row["_etab"])

    if collision_count:
        print(
            f"[dedup] {collision_count} lignes suffixées (#n) en dernier "
            f"recours après épuisement de commune + adresse1"
        )

    # Promotion des clés temporaires vers les colonnes finales.
    enriched: list[dict[str, str]] = []
    for row in rows:
        out = {k: v for k, v in row.items() if not k.startswith("_")}
        out["structure"] = row["_structure"]
        out["etablissement"] = row["_etab"]
        out["libelle_complet"] = row["_libcomp"]
        enriched.append(out)
    return enriched


def _rebuild_libcomp(structure: str, etablissement: str) -> str:
    """Concatène structure et établissement avec le séparateur d'affichage."""
    return (
        f"{structure}{DISPLAY_SEP}{etablissement}" if etablissement else structure
    )


def _collision_groups(
    rows: list[dict[str, str]],
) -> list[list[dict[str, str]]]:
    """Retourne les groupes de lignes partageant le même _libcomp (>1 ligne)."""
    buckets: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        buckets[row["_libcomp"]].append(row)
    return [group for group in buckets.values() if len(group) > 1]


# --- I/O -------------------------------------------------------------------


def read_bulk() -> tuple[list[str], list[dict[str, str]]]:
    with BULK_IN.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)
    return fieldnames, rows


def write_bulk(
    fieldnames: list[str], enriched: list[dict[str, str]]
) -> None:
    out_fields = fieldnames + ["structure", "etablissement", "libelle_complet"]
    with BULK_OUT.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=out_fields, delimiter=";",
            quoting=csv.QUOTE_MINIMAL,
        )
        writer.writeheader()
        writer.writerows(enriched)
    print(f"[bulk] écrit {BULK_OUT.relative_to(PROJECT_ROOT)} ({len(enriched)} lignes)")


def read_manual() -> tuple[list[str], list[dict[str, str]]]:
    with MANUAL_IN.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        rows = list(reader)
    return fieldnames, rows


def join_manual(
    manual_fields: list[str],
    manual_rows: list[dict[str, str]],
    enriched_bulk: list[dict[str, str]],
) -> list[dict[str, str]]:
    """Joint les 3 colonnes enrichies au manuel via ident/codeAiot normalisés."""
    index = {
        _normalize_ident(row["codeAiot"]): {
            "structure": row["structure"],
            "etablissement": row["etablissement"],
            "libelle_complet": row["libelle_complet"],
        }
        for row in enriched_bulk
    }

    matched = 0
    orphan = 0
    for row in manual_rows:
        key = _normalize_ident(row.get("ident", ""))
        match = index.get(key)
        if match:
            row.update(match)
            matched += 1
        else:
            # Orphelin : fallback libellé pur.
            libelle = row.get("libelle", "")
            row["structure"] = libelle
            row["etablissement"] = ""
            row["libelle_complet"] = libelle
            orphan += 1

    print(f"[manual] appariés avec le bulk : {matched}")
    if orphan:
        print(f"[manual] orphelins (fallback libellé seul) : {orphan}")
    return manual_rows


def write_manual(enriched_manual: list[dict[str, str]]) -> None:
    """Écrit le CSV manuel aliasé dans data/ selon MANUAL_COLUMN_SPEC.

    Seules les colonnes listées dans la spec sont écrites (ce qui drop
    automatiquement la colonne anonyme vide du CSV d'origine). Les noms
    des colonnes dans le fichier de sortie sont les *alias* définis dans
    la spec.
    """
    MANUAL_OUT_DIR.mkdir(parents=True, exist_ok=True)

    alias_fields = [alias for _src, alias, _orig, _definition in MANUAL_COLUMN_SPEC]

    # Détection des colonnes du CSV d'entrée absentes de la spec ET
    # non explicitement droppées. Sert à repérer un changement de schéma
    # de la source (nouvelle colonne ajoutée par data.gouv.fr qu'on n'a
    # pas encore documentée).
    if enriched_manual:
        input_keys = set(enriched_manual[0].keys())
        spec_keys = {src for src, *_ in MANUAL_COLUMN_SPEC}
        unexpected = input_keys - spec_keys - DROPPED_COLUMNS
        if unexpected:
            print(
                "[manual] attention : colonnes non documentées dans la spec "
                f"(non écrites) : {sorted(unexpected)}"
            )

    with MANUAL_OUT.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=alias_fields, quoting=csv.QUOTE_MINIMAL,
        )
        writer.writeheader()
        for row in enriched_manual:
            writer.writerow(
                {
                    alias: row.get(src, "")
                    for src, alias, _orig, _definition in MANUAL_COLUMN_SPEC
                }
            )
    print(
        f"[manual] écrit {MANUAL_OUT.relative_to(PROJECT_ROOT)} "
        f"({len(enriched_manual)} lignes, {len(alias_fields)} colonnes aliasées)"
    )


def write_metadata() -> None:
    """Écrit le dictionnaire nom_original / alias / definition."""
    MANUAL_OUT_DIR.mkdir(parents=True, exist_ok=True)
    with METADATA_OUT.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["nom_original", "alias", "definition"])
        for _src, alias, nom_orig, definition in MANUAL_COLUMN_SPEC:
            writer.writerow([nom_orig, alias, definition])
    print(
        f"[meta] écrit {METADATA_OUT.relative_to(PROJECT_ROOT)} "
        f"({len(MANUAL_COLUMN_SPEC)} lignes)"
    )


# --- Rapport ---------------------------------------------------------------


def report_stats(enriched: list[dict[str, str]]) -> None:
    total = len(enriched)
    with_etab = sum(1 for r in enriched if r["etablissement"])
    unique_libelle_complet = len({r["libelle_complet"] for r in enriched})
    print(
        f"[stats] {total} lignes  |  "
        f"{with_etab} avec etablissement rempli  |  "
        f"{unique_libelle_complet} libelle_complet distincts "
        f"({total - unique_libelle_complet} collisions résiduelles)"
    )


# --- Main ------------------------------------------------------------------


def main() -> int:
    bulk_fields, bulk_rows = read_bulk()
    print(f"[bulk] lu {BULK_IN.relative_to(PROJECT_ROOT)} ({len(bulk_rows)} lignes)")

    enriched_bulk = enrich_bulk_rows(bulk_rows)
    report_stats(enriched_bulk)
    write_bulk(bulk_fields, enriched_bulk)

    if MANUAL_IN.exists():
        manual_fields, manual_rows = read_manual()
        print(
            f"[manual] lu {MANUAL_IN.relative_to(PROJECT_ROOT)} "
            f"({len(manual_rows)} lignes)"
        )
        enriched_manual = join_manual(manual_fields, manual_rows, enriched_bulk)
        write_manual(enriched_manual)
        write_metadata()
    else:
        print(f"[manual] introuvable, saut : {MANUAL_IN}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
