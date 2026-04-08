"""
_metadonnees_util.py — Dictionnaire multi-fichiers partagé entre scripts.

Utilitaire partagé par les scripts du pipeline Géorisques qui écrivent
dans ``carte-interactive/data/metadonnees_colonnes.csv`` : chaque script
possède les entrées correspondant aux colonnes d'**un** fichier de données
(son ``owner_fichier``) et cohabite dans un fichier unique avec les
entrées possédées par d'autres scripts.

Protocole d'ownership :

1. La clé d'unicité est la paire ``(fichier, alias)``.
2. Un script ne peut écrire/modifier que les lignes dont le ``fichier``
   correspond à **son** ``owner_fichier``.
3. À l'écriture, un script :
   - charge l'existant,
   - supprime les lignes ``(owner_fichier, alias_in_own_rows)``,
   - ajoute ses lignes propres,
   - réécrit le fichier entier.
4. Les lignes appartenant à d'autres fichiers sont **préservées verbatim**.

Schéma du CSV : ``fichier, nom_original, alias, definition``

Migration depuis l'ancien schéma 3-colonnes : si ``load_metadata`` détecte
un header différent de ``METADATA_SCHEMA``, elle retourne une liste vide
— le prochain appel à ``merge_metadata`` reconstruit le fichier intégralement
au nouveau schéma. Donc la migration est automatique dès le premier run
du pipeline après l'introduction de ce helper.

Pas de dépendance externe : stdlib uniquement.
"""

from __future__ import annotations

import csv
from pathlib import Path

METADATA_SCHEMA = ["fichier", "nom_original", "alias", "definition"]


def load_metadata(path: Path) -> list[dict[str, str]]:
    """Lit le dictionnaire existant, ou retourne [] si absent / schéma legacy.

    Si le fichier existe mais n'utilise pas ``METADATA_SCHEMA`` (par exemple
    l'ancien schéma 3-colonnes avant ce helper), on retourne une liste vide
    : le prochain ``merge_metadata`` réécrira complètement au bon schéma.
    C'est la stratégie de migration automatique.
    """
    if not path.exists():
        return []
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if list(reader.fieldnames or []) != METADATA_SCHEMA:
            return []
        return list(reader)


def merge_metadata(
    path: Path,
    owner_fichier: str,
    owner_rows: list[dict[str, str]],
) -> None:
    """Merge les lignes de l'owner dans le dictionnaire partagé.

    Le script appelant possède les lignes dont ``fichier == owner_fichier``
    et dont ``alias`` est dans ``owner_rows``. Ces lignes sont remplacées ;
    toutes les autres (y compris celles appartenant à d'autres fichiers)
    sont préservées.

    Args:
        path: Chemin du fichier de métadonnées (créé si absent).
        owner_fichier: Nom du fichier de données que l'owner gère
            (ex. "liste-icpe-gironde_enrichi.csv"). Doit correspondre à
            la valeur du champ ``fichier`` dans chaque ``owner_rows``.
        owner_rows: Liste de dicts avec les clés de ``METADATA_SCHEMA``.
            L'ordre de cette liste fixe l'ordre des lignes écrites pour
            cet owner.
    """
    # Garde-fou : chaque ligne owner doit être cohérente.
    for row in owner_rows:
        for key in METADATA_SCHEMA:
            if key not in row:
                raise ValueError(
                    f"owner_rows : clé {key!r} manquante dans {row!r}"
                )
        if row["fichier"] != owner_fichier:
            raise ValueError(
                f"owner_rows : fichier={row['fichier']!r} "
                f"ne correspond pas à owner_fichier={owner_fichier!r}"
            )

    existing = load_metadata(path)
    own_aliases = {row["alias"] for row in owner_rows}

    preserved = [
        row
        for row in existing
        if not (row["fichier"] == owner_fichier and row["alias"] in own_aliases)
    ]

    merged = preserved + owner_rows

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=METADATA_SCHEMA, quoting=csv.QUOTE_MINIMAL
        )
        writer.writeheader()
        writer.writerows(merged)

    print(
        f"[meta] écrit {path.name} "
        f"({len(merged)} lignes total, {len(owner_rows)} pour {owner_fichier})"
    )
