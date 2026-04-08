"""Helper pour charger ``extract_rapports_markdown.py`` comme module.

Le script extracteur n'est pas un package installable : il vit dans
``scripts/`` avec un en-tête PEP 723 pour ``uv run``. Les tests ont
besoin d'importer ses fonctions ; on utilise ``importlib.util`` pour
le charger à partir de son chemin disque, puis on l'enregistre dans
``sys.modules`` pour que ``dataclass`` résolve correctement
``cls.__module__`` au moment de la décoration.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

_MODULE_NAME = "extract_rapports_markdown"
_SCRIPT_PATH = Path(__file__).resolve().parent.parent / "extract_rapports_markdown.py"


def load_extractor() -> ModuleType:
    """Charge (ou retourne depuis le cache) le module extracteur."""
    if _MODULE_NAME in sys.modules:
        return sys.modules[_MODULE_NAME]
    spec = importlib.util.spec_from_file_location(_MODULE_NAME, _SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(
            f"impossible de charger le module {_MODULE_NAME} "
            f"depuis {_SCRIPT_PATH}"
        )
    module = importlib.util.module_from_spec(spec)
    # Enregistre AVANT exec_module : @dataclass(slots=True) inspecte
    # ``sys.modules[cls.__module__]`` pendant le traitement et plante
    # avec un AttributeError si le module n'y est pas encore.
    sys.modules[_MODULE_NAME] = module
    spec.loader.exec_module(module)
    return module
