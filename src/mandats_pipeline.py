import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

_SRC_DIR = Path(__file__).parent.resolve()
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from core.runtime_config import configure_environment

ROOT_DIR = _SRC_DIR.parent.resolve()
configure_environment(ROOT_DIR)
OUTPUT_DIR = ROOT_DIR / "output"

from extract_people_from_casiers import extract_people_from_project
from pappers.pappers_enrichment import enrich_people, write_debug_json

logger = logging.getLogger(__name__)


def build_mandats_for_project(project_id: str) -> Tuple[Dict[str, Dict[str, List[dict]]], Dict[str, dict]]:
    people_by_folder = extract_people_from_project(project_id)
    societes_by_folder: Dict[str, Dict[str, List[dict]]] = {}
    debug_by_folder: Dict[str, dict] = {}

    if not people_by_folder:
        logger.info("Aucune personne extraite depuis les casiers")
        return societes_by_folder, debug_by_folder

    for folder_name, people in people_by_folder.items():
        logger.info(f"Recherche Pappers dossier '{folder_name}' ({len(people)} personne(s))")
        companies_by_person, debug_payload = enrich_people(people)
        societes_by_folder[folder_name] = companies_by_person
        debug_by_folder[folder_name] = debug_payload

        for person_name, companies in companies_by_person.items():
            logger.info(f"  {person_name}: {len(companies)} société(s)")

    return societes_by_folder, debug_by_folder


def run(project_id: str, excel_output: Optional[str] = None) -> Path:
    project_dir = OUTPUT_DIR / project_id
    project_dir.mkdir(parents=True, exist_ok=True)

    societes_by_folder, debug_by_folder = build_mandats_for_project(project_id)

    total_folders = len(societes_by_folder)
    total_persons = sum(len(people_map) for people_map in societes_by_folder.values())
    total_societes = sum(len(companies) for people_map in societes_by_folder.values() for companies in people_map.values())

    results_path = project_dir / "mandats_results.json"
    payload = {
        "project_id": project_id,
        "societes_par_personne": societes_by_folder,
        "summary": {
            "folders": total_folders,
            "persons": total_persons,
            "societes": total_societes,
        },
    }
    results_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"Résultats JSON écrits: {results_path}")

    debug_path = project_dir / "mandats_debug_recherche.json"
    write_debug_json(
        debug_path,
        {
            "project_id": project_id,
            "debug_recherche": debug_by_folder,
        },
    )
    logger.info(f"Debug JSON écrit: {debug_path}")

    logger.info("Excel désactivé pour l'instant")

    return results_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline sociétés via casiers -> Pappers /recherche")
    parser.add_argument("--project", "-p", required=True, help="project_id")
    parser.add_argument("--excel-output", default=None, help="Chemin de sortie Excel optionnel (ignoré pour l'instant)")
    args = parser.parse_args()

    run(args.project, excel_output=args.excel_output)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s — %(levelname)s — %(message)s",
    )
    main()
