#!/usr/bin/env python3
"""
telecharger_rapports_inspection.py — Téléchargement des rapports d'inspection ICPE.

Télécharge les rapports d'inspection publiables de Géorisques pour les
installations classées de Gironde, les renomme de façon déterministe à
partir du libellé désambiguïsé, et produit un CSV indexant chaque rapport
avec son URL GitHub Pages post-push.

Sources (read-only) :
  - données-georisques/metadataFichierInspection.csv
    Liste des fichiers d'inspection publiables (identifiant, nom, type,
    codeAiot). 1 ligne par rapport.
  - données-georisques/inspection.csv
    Historique des inspections, joignable via identifiantFichier pour
    récupérer la dateInspection (absente de metadataFichierInspection).
  - carte-interactive/data/liste-icpe-gironde_enrichi.csv
    Fournit nom_complet (libellé désambiguïsé) et siret pour chaque
    installation, via id_icpe ↔ codeAiot.

Produits (écrits) :
  - rapports-inspection/*.pdf
    Les PDFs eux-mêmes, nommés
    {slug}_{id_icpe}_{date}_{siret}.pdf
    avec fallbacks nosiret / nodate et slug ASCII-safe.
  - rapports-inspection/_404.txt
    Mémoire persistante des identifiants définitivement introuvables
    (HTTP 404). Au prochain run, ces identifiants sont skippés pour
    éviter de retenter inutilement.
  - rapports-inspection/_erreurs.log
    Rapport lisible du dernier run : transitoires + durables, avec
    raison et identifiants. Écrasé à chaque exécution.
  - carte-interactive/data/rapports-inspection.csv
    1 ligne par rapport source (incl. les doublons d'identifiant qui
    partagent le même fichier PDF local). Colonnes aliasées lisibles.
  - carte-interactive/data/liste-icpe-gironde_enrichi.csv (modifié)
    Ajoute/remplace la colonne nb_rapports_inspection comptant les
    rapports téléchargés avec succès par installation.
  - carte-interactive/data/metadonnees_colonnes.csv (mis à jour)
    Ajoute/remplace les lignes décrivant les colonnes de
    rapports-inspection.csv et nb_rapports_inspection dans l'enrichi,
    via le helper _metadonnees_util partagé avec enrichir_libelles.py.

Téléchargement : 3 workers concurrents, 0.5s de pause entre batches,
timeout 30s par requête, retry exponentiel pour 5xx/timeout/réseau,
backoff long pour 429, skip durable pour 404. Idempotent : un fichier
déjà présent sur disque n'est pas retéléchargé.

Dedup : 1 identifiant = 1 fichier PDF local. Quand un même identifiant
est référencé par plusieurs installations (1 seul cas connu sur 1784),
les lignes du CSV partagent le même nom_fichier_local et la même URL.
Le filename utilise les infos de la 1re installation triée par codeAiot.

Usage :
  python3 scripts/telecharger_rapports_inspection.py             # tout
  python3 scripts/telecharger_rapports_inspection.py --limit 5   # test
  python3 scripts/telecharger_rapports_inspection.py --dry-run   # plan only

Stdlib uniquement.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import datetime as dt
import re
import sys
import time
import unicodedata
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path

# Le helper _metadonnees_util est au même niveau que ce script.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from _metadonnees_util import merge_metadata  # noqa: E402

# --- Configuration ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Sources
METADATA_FICHIER_INSPECTION = (
    PROJECT_ROOT / "données-georisques" / "metadataFichierInspection.csv"
)
INSPECTION_CSV = PROJECT_ROOT / "données-georisques" / "inspection.csv"
MANUAL_ENRICHI = (
    PROJECT_ROOT / "carte-interactive" / "data" / "liste-icpe-gironde_enrichi.csv"
)

# Sorties
PDF_DIR = PROJECT_ROOT / "rapports-inspection"
ERREURS_LOG = PDF_DIR / "_erreurs.log"
LOG_404 = PDF_DIR / "_404.txt"
RAPPORTS_CSV = (
    PROJECT_ROOT / "carte-interactive" / "data" / "rapports-inspection.csv"
)
METADATA_CSV = (
    PROJECT_ROOT / "carte-interactive" / "data" / "metadonnees_colonnes.csv"
)

# URLs
SOURCE_URL_TEMPLATE = (
    "https://www.georisques.gouv.fr/webappReport/ws/installations/inspection/{id}"
)
PAGES_URL_TEMPLATE = (
    "https://bononlouis-del.github.io/"
    "Les-ICPE-en-r-serve-naturelle-nationale/"
    "rapports-inspection/{filename}"
)

# Téléchargement
USER_AGENT = "projet-icpe-ijba/1.0 (journalism education)"
MAX_WORKERS = 3
BATCH_PAUSE = 0.5  # seconds entre deux batches de MAX_WORKERS
REQUEST_TIMEOUT = 30  # seconds
MIN_PDF_SIZE = 1024  # octets, en dessous on considère que c'est une page d'erreur
RETRY_BACKOFF_5XX = [1, 2, 4]  # secondes
RETRY_BACKOFF_429 = [10, 30, 60]  # secondes, plus conservateur

# Sanitisation des noms de fichiers
SLUG_MAX_LEN = 120
DEDUP_SUFFIX = re.compile(r"\s*\(#(\d+)\)")

# Noms des fichiers cible (pour ownership du dictionnaire multi-fichiers)
MANUAL_OUTPUT_FILENAME = "liste-icpe-gironde_enrichi.csv"
RAPPORTS_OUTPUT_FILENAME = "rapports-inspection.csv"

# Spécification des colonnes de rapports-inspection.csv.
# Schéma : (source_key, alias, nom_original_metadata, definition).
# Utilisé pour (a) générer le CSV avec des noms lisibles, (b) alimenter
# le dictionnaire metadonnees_colonnes.csv via merge_metadata.
REPORTS_COLUMN_SPEC: list[tuple[str, str, str, str]] = [
    (
        "id_icpe",
        "id_icpe",
        "(calculé)",
        "Identifiant de l'installation classée (codeAiot sans zéros de "
        "tête). Clé de jointure avec liste-icpe-gironde_enrichi.csv.",
    ),
    (
        "nom_complet",
        "nom_complet",
        "(calculé)",
        "Libellé désambiguïsé de l'installation (copié depuis "
        "liste-icpe-gironde_enrichi.csv au moment du téléchargement).",
    ),
    (
        "siret",
        "siret",
        "(calculé)",
        "SIRET de l'exploitant (copié depuis liste-icpe-gironde_enrichi.csv). "
        "Vide si non renseigné dans la source.",
    ),
    (
        "date_inspection",
        "date_inspection",
        "dateInspection",
        "Date de l'inspection (format YYYY-MM-DD), jointe depuis "
        "inspection.csv via identifiantFichier. Vide si absente dans "
        "la source.",
    ),
    (
        "identifiant_fichier",
        "identifiant_fichier",
        "identifiant",
        "Identifiant opaque du fichier côté Géorisques. Clé d'unicité "
        "du fichier PDF.",
    ),
    (
        "type_fichier",
        "type_fichier",
        "type",
        "Type de document selon Géorisques. Toujours 'Rapport "
        "d'inspection publiable' dans ce CSV.",
    ),
    (
        "nom_fichier_source",
        "nom_fichier_source",
        "nom",
        "Nom du fichier tel que fourni par Géorisques (non utilisé pour "
        "le stockage local).",
    ),
    (
        "nom_fichier_local",
        "nom_fichier_local",
        "(calculé)",
        "Nom de fichier local après sanitisation : "
        "{slug_nom_complet}_{id_icpe}_{date}_{siret}.pdf avec fallbacks "
        "nosiret / nodate. Deux lignes peuvent partager le même nom "
        "quand un identifiant est référencé par plusieurs installations "
        "(dedup par identifiant).",
    ),
    (
        "url_source_georisques",
        "url_source_georisques",
        "(calculé)",
        "URL canonique du PDF côté Géorisques (webappReport). Utilisée "
        "par le script pour le téléchargement.",
    ),
    (
        "url_pages",
        "url_pages",
        "(calculé)",
        "URL GitHub Pages du PDF local post-push. S'ouvre directement "
        "dans le navigateur quand on clique.",
    ),
    (
        "statut_telechargement",
        "statut_telechargement",
        "(calculé)",
        "Statut du dernier téléchargement : 'ok' (téléchargé cette "
        "fois-ci), 'skip' (déjà présent), 'fail_404' (absent côté "
        "source, durable), 'fail_transitoire' (timeout/5xx/réseau, "
        "à retenter).",
    ),
    (
        "taille_octets",
        "taille_octets",
        "(calculé)",
        "Taille du fichier PDF local en octets (stat() après écriture). "
        "Vide si pas téléchargé.",
    ),
]

# Entrée à ajouter au dictionnaire pour la colonne que ce script écrit
# dans liste-icpe-gironde_enrichi.csv.
NB_RAPPORTS_METADATA = {
    "fichier": MANUAL_OUTPUT_FILENAME,
    "nom_original": "(calculé)",
    "alias": "nb_rapports_inspection",
    "definition": (
        "Nombre de rapports d'inspection publiables téléchargés avec succès "
        "pour cette installation (écrit par "
        "telecharger_rapports_inspection.py). Seuls les statuts 'ok' et "
        "'skip' comptent. Valeur 0 si aucun rapport téléchargé ou "
        "installation sans inspection publiable."
    ),
}


# --- Helpers purs ----------------------------------------------------------


def sanitize_slug(nom_complet: str) -> str:
    """Transforme un nom complet en slug ASCII-safe, cap à SLUG_MAX_LEN."""
    # Remplace les suffixes de dédup "(#n)" par "-n"
    slug = DEDUP_SUFFIX.sub(r"-\1", nom_complet)
    # Normalise Unicode et strip les accents
    slug = unicodedata.normalize("NFKD", slug)
    slug = "".join(c for c in slug if not unicodedata.combining(c))
    # Remplace l'em-dash et ses variantes par un simple dash
    slug = slug.replace("—", "-").replace("–", "-")
    # Garde uniquement les caractères sûrs (alphanumérique, dash, underscore)
    slug = re.sub(r"[^A-Za-z0-9\-_]+", "-", slug)
    # Compresse les dashes consécutifs
    slug = re.sub(r"-+", "-", slug)
    # Strip les dashes en début / fin
    slug = slug.strip("-_")
    # Cap longueur et re-strip au cas où le cap coupe au milieu d'un dash
    if len(slug) > SLUG_MAX_LEN:
        slug = slug[:SLUG_MAX_LEN].rstrip("-_")
    return slug or "sans-nom"


def build_filename(slug: str, id_icpe: str, date: str, siret: str) -> str:
    """Construit le nom de fichier local selon le template convenu."""
    return f"{slug}_{id_icpe}_{date or 'nodate'}_{siret or 'nosiret'}.pdf"


def normalize_aiot(code: str) -> str:
    """Strip les zéros de tête du codeAiot pour matcher id_icpe."""
    return code.strip().lstrip("0") or "0"


def build_source_url(identifiant: str) -> str:
    """URL Géorisques canonique pour télécharger le PDF."""
    return SOURCE_URL_TEMPLATE.format(id=identifiant)


def build_pages_url(filename: str) -> str:
    """URL GitHub Pages post-push pour le PDF local."""
    return PAGES_URL_TEMPLATE.format(filename=filename)


# --- Chargement et jointure ------------------------------------------------


def load_rapports_metadata() -> list[dict[str, str]]:
    """Charge metadataFichierInspection.csv et déjà normalise id_icpe."""
    rows: list[dict[str, str]] = []
    with METADATA_FICHIER_INSPECTION.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        for row in reader:
            rows.append(
                {
                    "id_icpe": normalize_aiot(row["codeAiot"]),
                    "identifiant_fichier": row["identifiant"],
                    "type_fichier": row["type"],
                    "nom_fichier_source": row["nom"],
                }
            )
    print(f"[load] {len(rows)} rapports chargés depuis {METADATA_FICHIER_INSPECTION.name}")
    return rows


def load_inspection_dates() -> dict[str, str]:
    """Index identifiantFichier → dateInspection."""
    index: dict[str, str] = {}
    with INSPECTION_CSV.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        for row in reader:
            ident = row.get("identifiantFichier", "").strip()
            date = row.get("dateInspection", "").strip()
            if ident and date:
                index[ident] = date
    print(f"[load] {len(index)} dates d'inspection indexées depuis {INSPECTION_CSV.name}")
    return index


def load_enrichi_lookup() -> dict[str, dict[str, str]]:
    """Index id_icpe → {nom_complet, siret} depuis l'enrichi manuel."""
    index: dict[str, dict[str, str]] = {}
    with MANUAL_ENRICHI.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            key = row.get("id_icpe", "").strip()
            if key:
                index[key] = {
                    "nom_complet": row.get("nom_complet", ""),
                    "siret": row.get("siret", ""),
                }
    print(f"[load] {len(index)} installations indexées depuis {MANUAL_ENRICHI.name}")
    return index


def join_all(
    rapports: list[dict[str, str]],
    dates: dict[str, str],
    enrichi: dict[str, dict[str, str]],
) -> list[dict[str, str]]:
    """Joint dates et infos installation sur chaque rapport."""
    orphans_enrichi = 0
    for row in rapports:
        row["date_inspection"] = dates.get(row["identifiant_fichier"], "")
        info = enrichi.get(row["id_icpe"], {})
        if info:
            row["nom_complet"] = info["nom_complet"]
            row["siret"] = info["siret"]
        else:
            # Rapport d'une installation absente de l'enrichi manuel —
            # extrêmement rare (0 cas observés en pratique), fallback propre.
            orphans_enrichi += 1
            row["nom_complet"] = f"installation-{row['id_icpe']}"
            row["siret"] = ""
    with_date = sum(1 for r in rapports if r["date_inspection"])
    print(
        f"[join] dates trouvées : {with_date}/{len(rapports)}  |  "
        f"orphelins enrichi : {orphans_enrichi}"
    )
    return rapports


# --- Dedup et nommage ------------------------------------------------------


def assign_local_filenames(rapports: list[dict[str, str]]) -> None:
    """Calcule nom_fichier_local de façon déterministe et sans collision.

    Deux cas à gérer sans perdre de données :

    1. **Dedup (même identifiant, plusieurs installations)** : 1 seul PDF
       côté Géorisques référencé par N installations. Toutes les N lignes
       du CSV partagent le même nom_fichier_local (1 fichier sur disque,
       N lignes pointant vers lui).

    2. **Collision (identifiants différents, même {id_icpe, date, siret, slug})** :
       2 rapports distincts pour la même installation, même jour (ex.
       "Partie publiable" et "Rapport public" d'une même inspection).
       Sans désambiguïsation, leurs filenames seraient identiques et le
       second écraserait le premier sur disque. On ajoute un suffixe
       déterministe composé des 6 derniers caractères de l'identifiant
       Géorisques aux fichiers impliqués dans une collision.

    Le nom de fichier est calculé depuis la 1re installation par tri
    (id_icpe, identifiant) pour rester stable entre runs.
    """
    # Étape 1 — grouper par identifiant (dedup cas 1)
    by_identifier: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rapports:
        by_identifier[row["identifiant_fichier"]].append(row)

    # Étape 2 — filename naïf par identifiant (primary = plus petit id_icpe)
    candidate: dict[str, str] = {}  # identifiant → filename naïf
    for identifier, group in by_identifier.items():
        group.sort(key=lambda r: r["id_icpe"])
        primary = group[0]
        slug = sanitize_slug(primary["nom_complet"])
        candidate[identifier] = build_filename(
            slug=slug,
            id_icpe=primary["id_icpe"],
            date=primary["date_inspection"],
            siret=primary["siret"],
        )

    # Étape 3 — détection des collisions (cas 2). Deux identifiants
    # différents ne doivent jamais partager un filename naïf.
    filename_to_identifiers: dict[str, list[str]] = defaultdict(list)
    for identifier, filename in candidate.items():
        filename_to_identifiers[filename].append(identifier)

    collision_groups = [
        (fn, ids)
        for fn, ids in filename_to_identifiers.items()
        if len(ids) > 1
    ]
    desambig_count = 0
    for filename, identifiers in collision_groups:
        desambig_count += len(identifiers)
        for identifier in sorted(identifiers):
            # Suffixe déterministe = 6 derniers caractères de l'identifiant
            # Géorisques. Les identifiants font 32+ chars alphanumériques,
            # 6 suffisent à garantir l'unicité dans une collision.
            suffix = identifier[-6:]
            base, ext = filename.rsplit(".", 1)
            candidate[identifier] = f"{base}_{suffix}.{ext}"

    # Étape 4 — application sur toutes les lignes (partage du filename
    # entre lignes qui partagent l'identifiant, donc le cas 1 est géré
    # par ce partage, pas par la dédup filename).
    shared_identifier_count = 0
    for identifier, group in by_identifier.items():
        filename = candidate[identifier]
        for row in group:
            row["nom_fichier_local"] = filename
            row["url_source_georisques"] = build_source_url(identifier)
            row["url_pages"] = build_pages_url(filename)
        if len(group) > 1:
            shared_identifier_count += 1

    unique_files = len(set(candidate.values()))
    print(
        f"[rename] {len(rapports)} lignes → {unique_files} fichiers uniques"
    )
    if shared_identifier_count:
        print(
            f"[rename] {shared_identifier_count} identifiants partagés "
            f"entre plusieurs installations (dedup — N lignes, 1 fichier)"
        )
    if desambig_count:
        print(
            f"[rename] {desambig_count} fichiers désambiguïsés par suffixe "
            f"d'identifiant ({len(collision_groups)} collisions résolues)"
        )


# --- Téléchargement --------------------------------------------------------


def load_404_memory() -> set[str]:
    """Charge les identifiants définitivement 404 des runs précédents."""
    if not LOG_404.exists():
        return set()
    with LOG_404.open(encoding="utf-8") as handle:
        return {line.strip() for line in handle if line.strip() and not line.startswith("#")}


def save_404_memory(identifiers: set[str]) -> None:
    """Persiste la liste des 404 durables, triée pour un diff stable."""
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Identifiants de rapports d'inspection connus comme HTTP 404",
        "# (fichier absent côté Géorisques). Skippés au prochain run pour",
        "# éviter les requêtes inutiles. Éditable à la main si tu veux forcer",
        "# une nouvelle tentative.",
        "",
    ]
    lines.extend(sorted(identifiers))
    LOG_404.write_text("\n".join(lines) + "\n", encoding="utf-8")


def fetch_one(url: str, dest: Path) -> tuple[str, int, str]:
    """Télécharge un fichier. Retourne (statut, taille_octets, raison).

    Statuts possibles :
      - "ok"              : téléchargé avec succès, fichier écrit
      - "fail_404"        : 404 durable côté Géorisques
      - "fail_5xx"        : 500-599 (transitoire)
      - "fail_429"        : rate limit (transitoire, backoff long)
      - "fail_net"        : erreur réseau ou timeout (transitoire)
      - "fail_tiny"       : réponse < MIN_PDF_SIZE (probablement page d'erreur)
    """
    try:
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            data = resp.read()
        if len(data) < MIN_PDF_SIZE:
            return ("fail_tiny", len(data), f"corps < {MIN_PDF_SIZE} octets")
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        return ("ok", len(data), "")
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return ("fail_404", 0, "HTTP 404")
        if exc.code == 429:
            return ("fail_429", 0, "HTTP 429")
        if 500 <= exc.code < 600:
            return ("fail_5xx", 0, f"HTTP {exc.code}")
        return (f"fail_{exc.code}", 0, f"HTTP {exc.code}")
    except urllib.error.URLError as exc:
        return ("fail_net", 0, f"réseau: {exc.reason}")
    except TimeoutError:
        return ("fail_net", 0, "timeout")
    except OSError as exc:
        # Catche ConnectionResetError, BrokenPipeError, et autres erreurs
        # socket de bas niveau qui échappent à URLError parce qu'elles
        # surviennent pendant resp.read() (après le début du transfert).
        return ("fail_net", 0, f"OS: {type(exc).__name__}: {exc}")


def fetch_with_retry(url: str, dest: Path) -> tuple[str, int, str]:
    """Wrapper avec retry exponentiel pour les échecs transitoires."""
    last = ("fail_net", 0, "aucune tentative")
    for attempt in range(3):
        statut, taille, raison = fetch_one(url, dest)
        if statut == "ok":
            return statut, taille, raison
        if statut == "fail_404" or statut == "fail_tiny":
            # Durable, pas de retry
            return statut, taille, raison
        last = (statut, taille, raison)
        if attempt < 2:
            if statut == "fail_429":
                time.sleep(RETRY_BACKOFF_429[attempt])
            else:
                time.sleep(RETRY_BACKOFF_5XX[attempt])
            continue
    return last


# --- Planification et exécution --------------------------------------------


class DownloadResult:
    """Conteneur minimaliste pour le résultat d'un téléchargement."""

    __slots__ = ("statut", "taille", "raison")

    def __init__(self, statut: str, taille: int, raison: str):
        self.statut = statut
        self.taille = taille
        self.raison = raison


def plan_downloads(
    rapports: list[dict[str, str]],
    known_404: set[str],
    limit: int | None,
) -> tuple[list[tuple[str, str, Path]], dict[str, DownloadResult]]:
    """Calcule le plan de téléchargement dedupliqué par identifiant.

    Retourne :
      - plan : liste de (identifiant, url, chemin_destination) à télécharger
      - results : dict identifiant → DownloadResult déjà rempli pour
        les statuts connus avant exécution (skip si existant, fail_404
        si mémoire, not_planned si au-delà de limit).
    """
    seen: dict[str, None] = {}  # ordre stable via dict Python 3.7+
    for row in sorted(rapports, key=lambda r: (r["id_icpe"], r["identifiant_fichier"])):
        seen.setdefault(row["identifiant_fichier"], None)

    plan: list[tuple[str, str, Path]] = []
    results: dict[str, DownloadResult] = {}
    for identifier in seen:
        # Retrouve une ligne pour cet identifiant pour récupérer le filename
        row = next(r for r in rapports if r["identifiant_fichier"] == identifier)
        dest = PDF_DIR / row["nom_fichier_local"]
        url = row["url_source_georisques"]

        if identifier in known_404:
            results[identifier] = DownloadResult(
                "fail_404", 0, "connu dans _404.txt"
            )
            continue
        if dest.exists():
            results[identifier] = DownloadResult(
                "skip", dest.stat().st_size, "déjà présent"
            )
            continue
        plan.append((identifier, url, dest))

    # Limit : on ne télécharge que les N premiers du plan (déterministe
    # grâce au tri stable plus haut).
    not_planned: list[tuple[str, str, Path]] = []
    if limit is not None and len(plan) > limit:
        not_planned = plan[limit:]
        plan = plan[:limit]
    for identifier, _url, _dest in not_planned:
        results[identifier] = DownloadResult(
            "not_planned", 0, f"au-delà de --limit {limit}"
        )

    return plan, results


def execute_downloads(
    plan: list[tuple[str, str, Path]]
) -> dict[str, DownloadResult]:
    """Télécharge le plan en parallèle avec politesse entre batches."""
    results: dict[str, DownloadResult] = {}
    total = len(plan)
    if total == 0:
        return results

    counter = 0
    for batch_start in range(0, total, MAX_WORKERS):
        batch = plan[batch_start : batch_start + MAX_WORKERS]
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=MAX_WORKERS
        ) as executor:
            future_map = {
                executor.submit(fetch_with_retry, url, dest): (identifier, dest)
                for identifier, url, dest in batch
            }
            for future in concurrent.futures.as_completed(future_map):
                identifier, dest = future_map[future]
                statut, taille, raison = future.result()
                counter += 1
                results[identifier] = DownloadResult(statut, taille, raison)
                label = "ok   " if statut == "ok" else statut.ljust(5)
                print(
                    f"[download] {counter:>4}/{total:<4}  {label}  "
                    f"{dest.name}  ({taille} octets)"
                )
                if raison and statut != "ok":
                    print(f"           └─ {raison}")

        if batch_start + MAX_WORKERS < total:
            time.sleep(BATCH_PAUSE)

    return results


# --- Écritures -------------------------------------------------------------


def write_rapports_csv(rapports: list[dict[str, str]]) -> None:
    """Écrit carte-interactive/data/rapports-inspection.csv."""
    RAPPORTS_CSV.parent.mkdir(parents=True, exist_ok=True)
    alias_fields = [alias for _src, alias, _orig, _def in REPORTS_COLUMN_SPEC]
    with RAPPORTS_CSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=alias_fields, quoting=csv.QUOTE_MINIMAL
        )
        writer.writeheader()
        for row in sorted(
            rapports, key=lambda r: (r["id_icpe"], r["identifiant_fichier"])
        ):
            writer.writerow(
                {alias: row.get(src, "") for src, alias, _, _ in REPORTS_COLUMN_SPEC}
            )
    print(
        f"[write] {RAPPORTS_CSV.relative_to(PROJECT_ROOT)} "
        f"({len(rapports)} lignes, {len(alias_fields)} colonnes)"
    )


def update_manual_enrichi_counts(counts: dict[str, int]) -> None:
    """Ajoute/remplace nb_rapports_inspection dans liste-icpe-gironde_enrichi.csv."""
    with MANUAL_ENRICHI.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fields = list(reader.fieldnames or [])
        rows = list(reader)

    if "nb_rapports_inspection" not in fields:
        fields.append("nb_rapports_inspection")

    for row in rows:
        key = row.get("id_icpe", "").strip()
        row["nb_rapports_inspection"] = str(counts.get(key, 0))

    with MANUAL_ENRICHI.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=fields, quoting=csv.QUOTE_MINIMAL
        )
        writer.writeheader()
        writer.writerows(rows)
    print(
        f"[write] {MANUAL_ENRICHI.relative_to(PROJECT_ROOT)} "
        f"(+1 colonne nb_rapports_inspection, {len(rows)} lignes)"
    )


def write_metadata_rapports() -> None:
    """Merge les entrées de ce script dans le dictionnaire partagé."""
    own_rows = [
        {
            "fichier": RAPPORTS_OUTPUT_FILENAME,
            "nom_original": nom_orig,
            "alias": alias,
            "definition": definition,
        }
        for _src, alias, nom_orig, definition in REPORTS_COLUMN_SPEC
    ]
    merge_metadata(METADATA_CSV, RAPPORTS_OUTPUT_FILENAME, own_rows)


def write_metadata_nb_rapports() -> None:
    """Merge l'entrée nb_rapports_inspection dans le dictionnaire partagé."""
    merge_metadata(METADATA_CSV, MANUAL_OUTPUT_FILENAME, [NB_RAPPORTS_METADATA])


def write_erreurs_log(
    rapports: list[dict[str, str]],
    results: dict[str, DownloadResult],
    started_at: dt.datetime,
) -> tuple[int, int]:
    """Écrit le log humain des erreurs. Retourne (durables, transitoires)."""
    durables: list[tuple[str, dict[str, str], DownloadResult]] = []
    transitoires: list[tuple[str, dict[str, str], DownloadResult]] = []

    # Dédup par identifiant pour ne pas lister le même PDF plusieurs fois
    seen_ids: set[str] = set()
    for row in sorted(
        rapports, key=lambda r: (r["id_icpe"], r["identifiant_fichier"])
    ):
        identifier = row["identifiant_fichier"]
        if identifier in seen_ids:
            continue
        seen_ids.add(identifier)
        result = results.get(identifier)
        if not result:
            continue
        if result.statut == "fail_404" or result.statut == "fail_tiny":
            durables.append((identifier, row, result))
        elif result.statut.startswith("fail_"):
            transitoires.append((identifier, row, result))

    PDF_DIR.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append(
        f"# Erreurs de téléchargement — run du {started_at.isoformat(timespec='seconds')}"
    )
    lines.append("")
    lines.append(f"Durables (404 / page d'erreur)      : {len(durables)}")
    lines.append(f"Transitoires (timeout, 5xx, réseau) : {len(transitoires)}")
    lines.append("")
    lines.append("## Échecs durables")
    lines.append("")
    if durables:
        for identifier, row, result in durables:
            lines.append(
                f"- {row['nom_fichier_local']}  ({result.statut} : {result.raison})"
            )
            lines.append(
                f"    id_icpe={row['id_icpe']}  identifiant={identifier}"
            )
            lines.append(f"    url={row['url_source_georisques']}")
    else:
        lines.append("_(aucun)_")
    lines.append("")
    lines.append("## Échecs transitoires (à retenter au prochain run)")
    lines.append("")
    if transitoires:
        for identifier, row, result in transitoires:
            lines.append(
                f"- {row['nom_fichier_local']}  ({result.statut} : {result.raison})"
            )
            lines.append(
                f"    id_icpe={row['id_icpe']}  identifiant={identifier}"
            )
    else:
        lines.append("_(aucun)_")
    lines.append("")

    ERREURS_LOG.write_text("\n".join(lines), encoding="utf-8")
    print(
        f"[write] {ERREURS_LOG.relative_to(PROJECT_ROOT)} "
        f"({len(durables)} durables, {len(transitoires)} transitoires)"
    )
    return len(durables), len(transitoires)


# --- Application des statuts sur les rapports ------------------------------


def apply_results_to_rapports(
    rapports: list[dict[str, str]],
    results: dict[str, DownloadResult],
) -> None:
    """Inscrit statut_telechargement et taille_octets sur chaque ligne."""
    for row in rapports:
        result = results.get(row["identifiant_fichier"])
        if result is None:
            row["statut_telechargement"] = "not_planned"
            row["taille_octets"] = ""
            continue
        if result.statut == "fail_404" or result.statut == "fail_tiny":
            row["statut_telechargement"] = "fail_404"
        elif result.statut.startswith("fail_"):
            row["statut_telechargement"] = "fail_transitoire"
        else:
            row["statut_telechargement"] = result.statut
        row["taille_octets"] = str(result.taille) if result.taille else ""


def count_successes_per_installation(
    rapports: list[dict[str, str]],
) -> dict[str, int]:
    """Compte les rapports téléchargés avec succès par id_icpe."""
    counts: dict[str, int] = defaultdict(int)
    for row in rapports:
        if row["statut_telechargement"] in ("ok", "skip"):
            counts[row["id_icpe"]] += 1
    return dict(counts)


# --- Main ------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Télécharge les rapports d'inspection ICPE depuis Géorisques, "
            "les nomme de façon déterministe, et produit rapports-inspection.csv."
        )
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limite le nombre de téléchargements (test progressif).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Planifie mais ne télécharge rien. Utile pour valider le plan.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started_at = dt.datetime.now()

    PDF_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Chargement et jointures
    rapports = load_rapports_metadata()
    dates = load_inspection_dates()
    enrichi = load_enrichi_lookup()
    join_all(rapports, dates, enrichi)

    # 2. Nommage et déduplication par identifiant
    assign_local_filenames(rapports)

    # 3. Planification
    known_404 = load_404_memory()
    if known_404:
        print(f"[plan] {len(known_404)} identifiants dans la mémoire _404.txt")
    plan, precomputed_results = plan_downloads(rapports, known_404, args.limit)
    print(
        f"[plan] {len(plan)} à télécharger, "
        f"{sum(1 for r in precomputed_results.values() if r.statut == 'skip')} déjà présents, "
        f"{sum(1 for r in precomputed_results.values() if r.statut == 'fail_404')} skip 404 connus, "
        f"{sum(1 for r in precomputed_results.values() if r.statut == 'not_planned')} non planifiés"
    )

    if args.dry_run:
        print("[dry-run] plan calculé, aucune écriture, exit.")
        for identifier, url, dest in plan[:10]:
            print(f"  DRY  {dest.name}")
        if len(plan) > 10:
            print(f"  … et {len(plan) - 10} autres")
        return 0

    # 4. Exécution des téléchargements
    download_results = execute_downloads(plan)

    # 5. Consolidation des résultats (precomputed + download)
    results: dict[str, DownloadResult] = {**precomputed_results, **download_results}

    # 6. Mise à jour de la mémoire _404.txt
    new_404 = {
        identifier
        for identifier, result in download_results.items()
        if result.statut == "fail_404"
    }
    all_404 = known_404 | new_404
    if new_404:
        print(f"[memory] {len(new_404)} nouveaux 404 à mémoriser")
    save_404_memory(all_404)

    # 7. Application des statuts sur les lignes de rapports
    apply_results_to_rapports(rapports, results)

    # 8. Écriture des sorties
    write_rapports_csv(rapports)
    counts = count_successes_per_installation(rapports)
    update_manual_enrichi_counts(counts)
    write_metadata_rapports()
    write_metadata_nb_rapports()
    durables, transitoires = write_erreurs_log(rapports, results, started_at)

    # 9. Résumé
    elapsed = dt.datetime.now() - started_at
    statut_counts = Counter(r["statut_telechargement"] for r in rapports)
    total_size = sum(
        int(r["taille_octets"]) for r in rapports if r["taille_octets"]
    )
    print()
    print("=" * 60)
    print("Téléchargement rapports d'inspection — terminé")
    print(f"  total traités      : {len(rapports)}")
    for statut, n in statut_counts.most_common():
        print(f"    {statut:<20} : {n}")
    print(f"  durables (404+tiny): {durables}")
    print(f"  transitoires       : {transitoires}")
    print(f"  taille totale DL   : {total_size / 1024 / 1024:.1f} Mo")
    print(f"  temps              : {elapsed}")
    print(f"  installations avec ≥1 rapport ok : {len(counts)}")
    print(f"  log erreurs        : {ERREURS_LOG.relative_to(PROJECT_ROOT)}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
