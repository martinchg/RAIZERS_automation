#!/usr/bin/env python3
"""
Point d'entrée unique — RAIZERS Automation.

Usage :
    python run.py pipeline  --project "/RAIZERS - En audit/SIGNATURE" --audit-folder "3. Opération - Rue de la Loge"
    python run.py extract   --project raizers-en-audit-signature
    python run.py mandats   --project raizers-en-audit-signature
    python run.py fill      --project raizers-en-audit-signature
    python run.py fill      --results output/.../extraction_results.json
"""
import argparse
import importlib
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
    p1.add_argument(
        "--audit-folder",
        default=None,
        help="Nom du dossier d'audit à inclure en plus de '1. Opérateur'",
    )

    # --- extract : LLM extraction ---
    p2 = sub.add_parser("extract", help="Extraction structurée via LLM")
    p2.add_argument("--project", "-p", required=True, help="project_id")

    # --- mandats : Pappers enrichissement ---
    p4 = sub.add_parser("mandats", help="Pipeline mandats Pappers (casiers → /recherche → /entreprise)")
    p4.add_argument("--project", "-p", required=True, help="project_id")

    # --- fill : remplissage Excel seul ---
    p3 = sub.add_parser("fill", help="Remplir un Excel depuis extraction_results.json")
    p3.add_argument("--project", "-p", default=None, help="project_id")
    p3.add_argument("--results", default=None, help="Chemin vers extraction_results.json")
    p3.add_argument(
        "--questions",
        default=None,
        help="Chemin vers un fichier de questions unique (sinon charge les fichiers split du dossier config)",
    )
    p3.add_argument("--output-dir", default=None)

    args = parser.parse_args()

    if args.command == "pipeline":
        import pipeline as pipeline_module
        pipeline_module = importlib.reload(pipeline_module)
        pipeline_module.run(args.project, selected_audit_folder=args.audit_folder)

    elif args.command == "extract":
        from extraction.extract_structured import run
        run(args.project)

    elif args.command == "mandats":
        from mandats_pipeline import run
        run(args.project)

    elif args.command == "fill":
        from sheets.excel_filler import main as fill_main
        results_path = args.results
        output_dir = args.output_dir

        if args.project:
            project_output_dir = ROOT_DIR / "output" / args.project
            results_path = results_path or str(project_output_dir / "extraction_results.json")
            output_dir = output_dir or str(project_output_dir)

        if not results_path:
            parser.error("fill requiert --project ou --results")

        # Override sys.argv pour le sous-parser d'excel_filler
        argv = ["excel_filler.py", "--results", results_path]
        if args.questions:
            argv += ["--questions", args.questions]
        if output_dir:
            argv += ["--output-dir", output_dir]
        sys.argv = argv
        fill_main()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
