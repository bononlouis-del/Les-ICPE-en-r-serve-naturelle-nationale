#!/usr/bin/env python3
"""
fetch_georisques.py — Export bulk officiel Géorisques pour la Gironde.

Télécharge l'archive ZIP publiée par l'API Géorisques V1 pour le département 33,
archive la version datée dans ``données-georisques/raw/``, extrait les 5 CSV
normalisés (encodage ISO-8859-1, séparateur ``;``), les convertit en UTF-8
dans ``données-georisques/``, et compare la liste des installations avec le
CSV manuel ``ENQUETE DATA - ICPE EN GIRONDE.csv`` (colonne ``ident``) pour
tracer les installations qui diffèrent entre les deux sources.

Source : https://www.georisques.gouv.fr/doc-api
Endpoint : GET /api/v1/csv/installations_classees?departement=33

Usage :
    python3 scripts/fetch_georisques.py

Aucune dépendance externe (stdlib uniquement).
"""

from __future__ import annotations

import csv
import datetime as dt
import hashlib
import io
import sys
import urllib.request
import zipfile
from pathlib import Path

# --- Configuration ---------------------------------------------------------

API_URL = (
    "https://www.georisques.gouv.fr/api/v1/csv/installations_classees"
    "?departement=33"
)
SOURCE_ENCODING = "iso-8859-1"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "données-georisques"
RAW_DIR = DATA_DIR / "raw"
MANUAL_CSV = PROJECT_ROOT / "carte-interactive" / "liste-icpe-gironde.csv"
DIFF_REPORT = DATA_DIR / "diff_report.txt"
PROVENANCE_FILE = DATA_DIR / "PROVENANCE.txt"

EXPECTED_FILES = {
    "InstallationClassee.csv",
    "inspection.csv",
    "metadataFichierInspection.csv",
    "metadataFichierHorsInspection.csv",
    "rubriqueIC.csv",
}


def download_zip() -> tuple[bytes, Path]:
    """Télécharge l'archive et l'archive dans raw/ avec un nom horodaté."""
    print(f"[fetch] GET {API_URL}")
    req = urllib.request.Request(
        API_URL,
        headers={"User-Agent": "projet-icpe-ijba/1.0 (journalism education)"},
    )
    with urllib.request.urlopen(req, timeout=60) as response:
        payload = response.read()

    timestamp = dt.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    raw_path = RAW_DIR / f"{timestamp}_gironde_bulk.zip"
    raw_path.write_bytes(payload)
    sha256 = hashlib.sha256(payload).hexdigest()
    print(f"[fetch] {len(payload):,} octets → {raw_path.relative_to(PROJECT_ROOT)}")
    print(f"[fetch] sha256={sha256}")

    _write_provenance(raw_path, len(payload), sha256)
    return payload, raw_path


def _write_provenance(raw_path: Path, size: int, sha256: str) -> None:
    """Trace la provenance de l'export pour audit."""
    PROVENANCE_FILE.write_text(
        "\n".join(
            [
                "# Provenance de l'export bulk Géorisques",
                "",
                f"date_téléchargement : {dt.datetime.now().isoformat(timespec='seconds')}",
                f"url                 : {API_URL}",
                f"archive             : {raw_path.relative_to(PROJECT_ROOT)}",
                f"taille_octets       : {size}",
                f"sha256              : {sha256}",
                f"encodage_source     : {SOURCE_ENCODING}",
                "séparateur          : ;",
                "",
                "Les CSV extraits dans ce dossier sont convertis en UTF-8.",
                "Les originaux ISO-8859-1 restent disponibles dans raw/ (ZIP).",
                "",
            ]
        ),
        encoding="utf-8",
    )


def extract_and_convert(payload: bytes) -> dict[str, Path]:
    """Extrait le ZIP en mémoire et écrit chaque CSV en UTF-8."""
    written: dict[str, Path] = {}
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        names = set(archive.namelist())
        missing = EXPECTED_FILES - names
        extras = names - EXPECTED_FILES
        if missing:
            raise RuntimeError(
                f"fichiers attendus absents du ZIP : {sorted(missing)}"
            )
        if extras:
            print(f"[extract] fichiers supplémentaires ignorés : {sorted(extras)}")

        for name in sorted(EXPECTED_FILES):
            raw_bytes = archive.read(name)
            text = raw_bytes.decode(SOURCE_ENCODING)
            out_path = DATA_DIR / name
            out_path.write_text(text, encoding="utf-8")
            line_count = text.count("\n")
            written[name] = out_path
            print(
                f"[extract] {name:<40} {line_count:>6} lignes → "
                f"{out_path.relative_to(PROJECT_ROOT)}"
            )
    return written


def _load_bulk_codes(installation_csv: Path) -> dict[str, dict[str, str]]:
    """Charge les codeAiot du bulk indexés en entier (sans zéros à gauche)."""
    codes: dict[str, dict[str, str]] = {}
    with installation_csv.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        for row in reader:
            code_aiot = row["codeAiot"].strip()
            key = code_aiot.lstrip("0") or "0"
            codes[key] = {
                "codeAiot": code_aiot,
                "raisonSociale": row.get("raisonSociale", ""),
                "commune": row.get("commune", ""),
                "regimeVigueur": row.get("regimeVigueur", ""),
                "etatActivite": row.get("etatActivite", ""),
            }
    return codes


def _load_manual_codes(manual_csv: Path) -> dict[str, dict[str, str]]:
    """Charge les identifiants du CSV manuel (colonne ``ident``)."""
    codes: dict[str, dict[str, str]] = {}
    with manual_csv.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            ident = row["ident"].strip()
            key = ident.lstrip("0") or "0"
            codes[key] = {
                "ident": ident,
                "libelle": row.get("libelle", ""),
                "insee": row.get("insee", ""),
                "regime": row.get("regime", ""),
            }
    return codes


def compare_sources(installation_csv: Path, manual_csv: Path) -> None:
    """Compare bulk vs CSV manuel, écrit un rapport de diff dans DIFF_REPORT."""
    if not manual_csv.exists():
        print(f"[diff] CSV manuel introuvable : {manual_csv}")
        return

    bulk = _load_bulk_codes(installation_csv)
    manual = _load_manual_codes(manual_csv)

    bulk_keys = set(bulk)
    manual_keys = set(manual)

    only_in_bulk = sorted(bulk_keys - manual_keys, key=int)
    only_in_manual = sorted(manual_keys - bulk_keys, key=int)
    common = bulk_keys & manual_keys

    lines: list[str] = []
    lines.append("# Comparaison bulk Géorisques vs CSV manuel")
    lines.append("")
    lines.append(f"date            : {dt.datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"bulk            : {installation_csv.relative_to(PROJECT_ROOT)}")
    lines.append(f"manuel          : {manual_csv.relative_to(PROJECT_ROOT)}")
    lines.append("")
    lines.append(f"bulk            : {len(bulk):>5} installations")
    lines.append(f"manuel          : {len(manual):>5} installations")
    lines.append(f"en commun       : {len(common):>5}")
    lines.append(f"uniquement bulk : {len(only_in_bulk):>5}")
    lines.append(f"uniquement manuel : {len(only_in_manual):>3}")
    lines.append("")

    lines.append("## Présent dans le bulk, absent du CSV manuel")
    lines.append("")
    if only_in_bulk:
        for key in only_in_bulk:
            row = bulk[key]
            lines.append(
                f"- {row['codeAiot']} | {row['raisonSociale']} | "
                f"{row['commune']} | {row['regimeVigueur']} | "
                f"{row['etatActivite']}"
            )
    else:
        lines.append("_(aucune)_")
    lines.append("")

    lines.append("## Présent dans le CSV manuel, absent du bulk")
    lines.append("")
    if only_in_manual:
        for key in only_in_manual:
            row = manual[key]
            lines.append(
                f"- {row['ident']} | {row['libelle']} | "
                f"INSEE {row['insee']} | {row['regime']}"
            )
    else:
        lines.append("_(aucune)_")
    lines.append("")

    DIFF_REPORT.write_text("\n".join(lines), encoding="utf-8")
    print(f"[diff] rapport écrit : {DIFF_REPORT.relative_to(PROJECT_ROOT)}")
    print(
        f"[diff] bulk={len(bulk)}  manuel={len(manual)}  "
        f"seulement_bulk={len(only_in_bulk)}  seulement_manuel={len(only_in_manual)}"
    )


def main() -> int:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    payload, _raw_path = download_zip()
    written = extract_and_convert(payload)
    compare_sources(written["InstallationClassee.csv"], MANUAL_CSV)
    return 0


if __name__ == "__main__":
    sys.exit(main())
