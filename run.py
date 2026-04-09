#!/usr/bin/env python3
"""
Point d'entrée unique — RAIZERS Automation.

Usage :
    python run.py pipeline  --project "/RAIZERS - En audit/SIGNATURE"
    python run.py extract   --project raizers-en-audit-signature
    python run.py mandats   --project raizers-en-audit-signature
    python run.py fill      --results output/.../extraction_results.json
"""
import argparse
import logging
import sys
from pathlib import Path

# Ajouter src/ au path pour que les imports fonctionnent
ROOT_DIR = Path(__file__).parent.resolve()
sys.path.insert(0, str(ROOT_DIR / "src"))


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s — %(levelname)s — %(message)s",
    )

    parser = argparse.ArgumentParser(description="RAIZERS Automation Pipeline")
    sub = parser.add_subparsers(dest="command")

    # --- pipeline : Dropbox → chunks ---
    p1 = sub.add_parser("pipeline", help="Sync Dropbox + extraction texte + chunking")
    p1.add_argument("--project", "-p", required=True,
                     help='Chemin Dropbox (ex: "/RAIZERS - En audit/SIGNATURE")')

    # --- extract : LLM extraction ---
    p2 = sub.add_parser("extract", help="Extraction structurée via LLM")
    p2.add_argument("--project", "-p", required=True, help="project_id")

    # --- mandats : Pappers enrichissement ---
    p4 = sub.add_parser("mandats", help="Pipeline mandats Pappers (casiers → /recherche → /entreprise)")
    p4.add_argument("--project", "-p", required=True, help="project_id")

    # --- fill : remplissage Excel seul ---
    p3 = sub.add_parser("fill", help="Remplir un Excel depuis extraction_results.json")
    p3.add_argument("--results", default=None, help="Chemin vers extraction_results.json")
    p3.add_argument("--questions", default=str(ROOT_DIR / "config" / "questions.json"))
    p3.add_argument("--output-dir", default=None)

    args = parser.parse_args()

    if args.command == "pipeline":
        from pipeline import run
        run(args.project)

    elif args.command == "extract":
        from extract_structured import run
        run(args.project)

    elif args.command == "mandats":
        from mandats_pipeline import run
        run(args.project)

    elif args.command == "fill":
        from excel_filler import main as fill_main
        # Override sys.argv pour le sous-parser d'excel_filler
        argv = ["excel_filler.py",
                "--results", args.results or "",
                "--questions", args.questions]
        if args.output_dir:
            argv += ["--output-dir", args.output_dir]
        sys.argv = argv
        fill_main()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
