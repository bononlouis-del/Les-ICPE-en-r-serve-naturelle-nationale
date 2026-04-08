#!/usr/bin/env python3
"""
enrichir_libelles.py — Désambiguïsation des libellés ICPE + enrichissement
géographique (commune / EPCI).

Ajoute trois colonnes calculées (``structure``, ``etablissement``,
``libelle_complet``) via l'algorithme de désambiguïsation, et trois
colonnes référentielles (``nom_commune``, ``epci_siren``, ``epci_nom``)
depuis geo.api.gouv.fr en les joignant sur le code INSEE de la commune.

Fichiers produits :

- ``données-georisques/InstallationClassee_enrichi.csv``
  (conserve les noms de colonnes Géorisques d'origine)

- ``carte-interactive/data/liste-icpe-gironde_enrichi.csv``
  (colonnes renommées avec des alias lisibles ; les colonnes externes
  écrites par d'autres scripts — ex. ``nb_rapports_inspection`` ajoutée
  par ``telecharger_rapports_inspection.py`` — sont préservées verbatim
  lors du re-run de ce script, voir ``write_manual``)
- ``carte-interactive/data/metadonnees_colonnes.csv``
  (dictionnaire multi-fichiers partagé — schéma 4 colonnes
  ``fichier / nom_original / alias / definition``. Ce script possède les
  lignes dont ``fichier == MANUAL_OUTPUT_FILENAME`` ; les lignes
  appartenant à d'autres fichiers sont préservées via le helper
  ``_metadonnees_util.merge_metadata``)

Les fichiers originaux ne sont jamais modifiés.

Réseau : lors du premier run (ou quand le cache
``carte-interactive/data/gironde-commune-epci.json`` est absent), le script
interroge ``geo.api.gouv.fr`` pour récupérer la correspondance code INSEE
→ nom de commune / EPCI. Le résultat est mis en cache sur disque et
réutilisé pour les runs suivants. Pour forcer un refresh, supprimer le
fichier de cache puis relancer.

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
import json
import re
import sys
import urllib.request
from collections import defaultdict
from pathlib import Path

# --- Configuration ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BULK_IN = PROJECT_ROOT / "données-georisques" / "InstallationClassee.csv"
BULK_OUT = PROJECT_ROOT / "données-georisques" / "InstallationClassee_enrichi.csv"
MANUAL_IN = PROJECT_ROOT / "carte-interactive" / "liste-icpe-gironde.csv"
MANUAL_OUT_DIR = PROJECT_ROOT / "carte-interactive" / "data"
MANUAL_OUTPUT_FILENAME = "liste-icpe-gironde_enrichi.csv"
MANUAL_OUT = MANUAL_OUT_DIR / MANUAL_OUTPUT_FILENAME
METADATA_OUT = MANUAL_OUT_DIR / "metadonnees_colonnes.csv"

# Le helper _metadonnees_util est au même niveau que ce script.
# L'import se fait au niveau fonction pour isoler la dépendance de path.
sys.path.insert(0, str(Path(__file__).resolve().parent))

# Cache de la correspondance code INSEE → {nom, epci_siren, epci_nom}.
# Présent = le script n'appelle pas l'API. Supprimer pour forcer un refresh.
COMMUNE_EPCI_CACHE = MANUAL_OUT_DIR / "gironde-commune-epci.json"

# Endpoints geo.api.gouv.fr (Etalab, public, sans auth).
# Utilisés uniquement quand le cache local est absent.
COMMUNES_API = "https://geo.api.gouv.fr/departements/33/communes?fields=nom,code,codeEpci"
EPCIS_API = "https://geo.api.gouv.fr/epcis?fields=nom,code"

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
        "nom_commune",
        "nom_commune",
        "(calculé)",
        "Nom de la commune d'implantation, résolu depuis le code INSEE via "
        "geo.api.gouv.fr (source : IGN Admin Express). Vide si le code INSEE "
        "est manquant ou ne correspond à aucune commune référencée en "
        "Gironde.",
    ),
    (
        "epci_siren",
        "epci_siren",
        "(calculé)",
        "Numéro SIREN de l'EPCI (Établissement Public de Coopération "
        "Intercommunale) auquel la commune appartient. Résolu depuis le code "
        "INSEE via geo.api.gouv.fr. Vide si la commune n'est rattachée à "
        "aucun EPCI référencé.",
    ),
    (
        "epci_nom",
        "epci_nom",
        "(calculé)",
        "Nom de l'EPCI (Établissement Public de Coopération Intercommunale) "
        "auquel la commune appartient (ex : 'Bordeaux Métropole', 'CA du "
        "Libournais'). Résolu via geo.api.gouv.fr depuis le code INSEE.",
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


# --- Enrichissement commune / EPCI ----------------------------------------


def _fetch_json(url: str) -> object:
    """GET + JSON parse, user-agent explicite pour geo.api.gouv.fr."""
    req = urllib.request.Request(
        url, headers={"User-Agent": "enrichir_libelles.py"}
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.load(resp)


def load_commune_epci_lookup() -> dict[str, dict[str, str | None]]:
    """Retourne un dict INSEE → {nom, epci_siren, epci_nom}.

    Préfère le cache disque si présent (pour les runs offline et pour la
    reproductibilité). Sinon appelle geo.api.gouv.fr (Etalab), met en
    cache, retourne. Écriture JSON compacte (une seule ligne) — 48 KB pour
    les 534 communes de Gironde.
    """
    if COMMUNE_EPCI_CACHE.exists():
        with COMMUNE_EPCI_CACHE.open(encoding="utf-8") as handle:
            cached = json.load(handle)
        print(
            f"[commune] cache {COMMUNE_EPCI_CACHE.relative_to(PROJECT_ROOT)} "
            f"({len(cached)} communes)"
        )
        return cached

    print(f"[commune] cache absent, appel {COMMUNES_API}")
    communes_raw = _fetch_json(COMMUNES_API)
    assert isinstance(communes_raw, list), "format inattendu (communes)"
    print(f"[commune] {len(communes_raw)} communes récupérées")

    print(f"[commune] appel {EPCIS_API}")
    epcis_raw = _fetch_json(EPCIS_API)
    assert isinstance(epcis_raw, list), "format inattendu (epcis)"
    epci_by_code: dict[str, str] = {
        e["code"]: e["nom"] for e in epcis_raw if isinstance(e, dict)
    }
    print(
        f"[commune] {len(epcis_raw)} EPCIs indexés "
        f"(dont {len({c.get('codeEpci') for c in communes_raw if c.get('codeEpci')})} "
        f"distincts en Gironde)"
    )

    lookup: dict[str, dict[str, str | None]] = {}
    for entry in communes_raw:
        if not isinstance(entry, dict):
            continue
        code = entry.get("code")
        if not code:
            continue
        siren = entry.get("codeEpci")
        lookup[code] = {
            "nom": entry.get("nom"),
            "epci_siren": siren,
            "epci_nom": epci_by_code.get(siren) if siren else None,
        }

    COMMUNE_EPCI_CACHE.parent.mkdir(parents=True, exist_ok=True)
    with COMMUNE_EPCI_CACHE.open("w", encoding="utf-8") as handle:
        json.dump(lookup, handle, ensure_ascii=False, separators=(",", ":"))
    print(
        f"[commune] cache écrit {COMMUNE_EPCI_CACHE.relative_to(PROJECT_ROOT)} "
        f"({len(lookup)} communes)"
    )
    return lookup


def enrich_with_commune_epci(
    rows: list[dict[str, str]],
    insee_key: str,
    lookup: dict[str, dict[str, str | None]],
) -> None:
    """Injecte nom_commune / epci_siren / epci_nom sur chaque ligne in-place.

    ``insee_key`` : nom de la colonne contenant le code INSEE dans chaque
    dict de ligne (diffère entre le bulk et le manuel).
    """
    matched = 0
    missing = 0
    for row in rows:
        code = (row.get(insee_key) or "").strip()
        info = lookup.get(code) if code else None
        if info:
            row["nom_commune"] = info.get("nom") or ""
            row["epci_siren"] = info.get("epci_siren") or ""
            row["epci_nom"] = info.get("epci_nom") or ""
            matched += 1
        else:
            row["nom_commune"] = ""
            row["epci_siren"] = ""
            row["epci_nom"] = ""
            missing += 1
    print(
        f"[commune] appariés : {matched}  |  sans correspondance : {missing}"
    )


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
    out_fields = fieldnames + [
        "structure",
        "etablissement",
        "libelle_complet",
        "nom_commune",
        "epci_siren",
        "epci_nom",
    ]
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
    """Joint les colonnes enrichies (désambiguïsation libellés uniquement)
    au manuel via ident/codeAiot normalisés.

    L'enrichissement commune/EPCI est calculé séparément sur les lignes
    du manuel (elles portent leur propre colonne ``insee``), ce qui
    permet d'enrichir correctement même les lignes orphelines.
    """
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

    Les colonnes listées dans la spec sont écrites en premier, dans
    l'ordre de la spec. Les colonnes absentes de la spec **et non
    explicitement droppées** sont droppées (c'est ainsi que la colonne
    anonyme vide du CSV d'origine est éliminée). Les noms de colonnes
    du fichier de sortie sont les *alias* définis dans la spec.

    **Préservation des colonnes externes** : si le fichier de sortie
    existe déjà et contient des colonnes qui ne sont ni dans
    MANUAL_COLUMN_SPEC ni dans DROPPED_COLUMNS, ces colonnes sont
    considérées comme gérées par un autre script (ex.
    ``nb_rapports_inspection`` écrite par
    ``telecharger_rapports_inspection.py``) et **préservées verbatim**
    à la fin du fichier réécrit. Les valeurs sont indexées par
    ``id_icpe`` dans le fichier existant et réinjectées dans les lignes
    correspondantes. Les nouvelles installations (absentes du fichier
    existant) reçoivent des valeurs vides pour ces colonnes : au prochain
    run du script qui gère ces colonnes, elles seront recalculées.
    """
    MANUAL_OUT_DIR.mkdir(parents=True, exist_ok=True)

    alias_fields = [alias for _src, alias, _orig, _definition in MANUAL_COLUMN_SPEC]
    own_alias_set = set(alias_fields)

    # Préservation des colonnes externes : on lit le fichier existant (si
    # présent) pour détecter des colonnes gérées par d'autres scripts, et
    # on indexe leurs valeurs par id_icpe. Ces colonnes seront ré-écrites
    # à la fin du fichier, en préservant leurs valeurs ligne par ligne.
    preserved_cols: list[str] = []
    preserved_values: dict[str, dict[str, str]] = {}
    if MANUAL_OUT.exists():
        with MANUAL_OUT.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            existing_headers = list(reader.fieldnames or [])
            preserved_cols = [
                col
                for col in existing_headers
                if col not in own_alias_set and col not in DROPPED_COLUMNS
            ]
            if preserved_cols:
                for existing_row in reader:
                    key = existing_row.get("id_icpe", "").strip()
                    if key:
                        preserved_values[key] = {
                            col: existing_row.get(col, "") for col in preserved_cols
                        }
                print(
                    f"[manual] colonnes externes préservées : {preserved_cols} "
                    f"({len(preserved_values)} lignes indexées par id_icpe)"
                )

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

    out_fields = alias_fields + preserved_cols
    with MANUAL_OUT.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=out_fields, quoting=csv.QUOTE_MINIMAL,
        )
        writer.writeheader()
        for row in enriched_manual:
            output_row = {
                alias: row.get(src, "")
                for src, alias, _orig, _definition in MANUAL_COLUMN_SPEC
            }
            if preserved_cols:
                external = preserved_values.get(output_row.get("id_icpe", ""), {})
                for col in preserved_cols:
                    output_row[col] = external.get(col, "")
            writer.writerow(output_row)

    print(
        f"[manual] écrit {MANUAL_OUT.relative_to(PROJECT_ROOT)} "
        f"({len(enriched_manual)} lignes, {len(out_fields)} colonnes "
        f"[{len(alias_fields)} spec + {len(preserved_cols)} préservées])"
    )


def write_metadata() -> None:
    """Merge les entrées de MANUAL_COLUMN_SPEC dans le dictionnaire partagé.

    Ce script possède les lignes de ``metadonnees_colonnes.csv`` dont
    ``fichier == MANUAL_OUTPUT_FILENAME``. Les lignes appartenant à
    d'autres fichiers de données (ex. ``rapports-inspection.csv``,
    géré par ``telecharger_rapports_inspection.py``) sont **préservées
    verbatim** par le helper ``_metadonnees_util.merge_metadata``. Voir
    ce module pour le protocole complet d'ownership multi-fichiers.

    Migration automatique : si le fichier existant est au legacy schéma
    3-colonnes (nom_original / alias / definition) utilisé avant le
    refactor multi-fichiers, il sera détecté comme "schema inconnu" et
    réécrit intégralement au nouveau schéma 4-colonnes (fichier /
    nom_original / alias / definition).
    """
    from _metadonnees_util import merge_metadata

    MANUAL_OUT_DIR.mkdir(parents=True, exist_ok=True)
    own_rows = [
        {
            "fichier": MANUAL_OUTPUT_FILENAME,
            "nom_original": nom_orig,
            "alias": alias,
            "definition": definition,
        }
        for _src, alias, nom_orig, definition in MANUAL_COLUMN_SPEC
    ]
    merge_metadata(METADATA_OUT, MANUAL_OUTPUT_FILENAME, own_rows)


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

    # 1. Désambiguïsation libellés (passe bulk, le manuel hérite ensuite).
    enriched_bulk = enrich_bulk_rows(bulk_rows)
    report_stats(enriched_bulk)

    # 2. Enrichissement commune / EPCI depuis geo.api.gouv.fr (ou cache).
    #    La colonne INSEE côté bulk s'appelle 'codeInsee'.
    commune_lookup = load_commune_epci_lookup()
    enrich_with_commune_epci(enriched_bulk, "codeInsee", commune_lookup)

    write_bulk(bulk_fields, enriched_bulk)

    if MANUAL_IN.exists():
        manual_fields, manual_rows = read_manual()
        print(
            f"[manual] lu {MANUAL_IN.relative_to(PROJECT_ROOT)} "
            f"({len(manual_rows)} lignes)"
        )
        enriched_manual = join_manual(manual_fields, manual_rows, enriched_bulk)
        # Côté manuel, la colonne INSEE s'appelle 'insee' (avant alias).
        enrich_with_commune_epci(enriched_manual, "insee", commune_lookup)
        write_manual(enriched_manual)
        write_metadata()
    else:
        print(f"[manual] introuvable, saut : {MANUAL_IN}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
