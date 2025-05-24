#!/usr/bin/env python3
import sys
import argparse
from pathlib import Path
from backend.config import SUPABASE_URL, SUPABASE_SERVICE_KEY
from backend.init_env import init_project

# Ensure the project root is in sys.path for all imports
project_root = Path(__file__).resolve().parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

def preprocess_data(args=None):
    # Import and call the preprocess pipeline
    from backend.preprocess_data import main as preprocess_main
    if args is not None:
        sys.argv = ['preprocess_data.py'] + args
    preprocess_main()

def load_to_db():
    from backend.etl.load_to_db import main as load_main
    load_main()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run CORDIS data workflow")
    subparsers = parser.add_subparsers(dest="command", required=True)

    prep_parser = subparsers.add_parser("preprocess", help="Run preprocessing pipeline")
    prep_parser.add_argument("--no-clean", action="store_true", help="Skip the cleaning stage")
    prep_parser.add_argument("--transform", action="store_true", help="Run the transformation stage")
    prep_parser.add_argument("--no-enrich", action="store_true", help="Skip enrichment stage")

    subparsers.add_parser("load", help="Load processed data to Supabase")

    args, unknown = parser.parse_known_args()

    if args.command == "preprocess":
        # Pass through relevant args to the preprocessing script
        cli_args = []
        if args.no_clean:
            cli_args.append("--no-clean")
        if args.transform:
            cli_args.append("--transform")
        if args.no_enrich:
            cli_args.append("--no-enrich")
        preprocess_data(args=cli_args + unknown)
    elif args.command == "load":
        # Check if Supabase credentials are set
        load_to_db()
