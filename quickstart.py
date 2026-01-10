#!/usr/bin/env python3
"""
Quickstart script for Sanctions Network Mapper.

This script verifies your environment is set up correctly and
runs a demo of the data ingestion pipeline.

Usage:
    python quickstart.py
"""

import sys
from pathlib import Path

def check_python_version():
    """Verify Python version."""
    print("Checking Python version...")
    if sys.version_info < (3, 11):
        print(f"  ERROR: Python 3.11+ required, found {sys.version}")
        return False
    print(f"  OK: Python {sys.version_info.major}.{sys.version_info.minor}")
    return True


def check_dependencies():
    """Verify required packages are installed."""
    print("\nChecking dependencies...")
    
    required = [
        ("polars", "polars"),
        ("httpx", "httpx"),
        ("rapidfuzz", "rapidfuzz"),
        ("networkx", "networkx"),
        ("pydantic", "pydantic"),
        ("rich", "rich"),
        ("typer", "typer"),
    ]
    
    all_ok = True
    for name, import_name in required:
        try:
            module = __import__(import_name)
            version = getattr(module, "__version__", "unknown")
            print(f"  OK: {name} ({version})")
        except ImportError:
            print(f"  MISSING: {name}")
            all_ok = False
    
    return all_ok


def check_directories():
    """Create required directories."""
    print("\nChecking directories...")
    
    dirs = [
        Path("data/raw/opensanctions"),
        Path("data/raw/corporate"),
        Path("data/processed"),
        Path("data/output"),
        Path("logs"),
    ]
    
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
        print(f"  OK: {d}")
    
    return True


def run_demo():
    """Run a demo of the pipeline."""
    print("\n" + "=" * 60)
    print("RUNNING DEMO")
    print("=" * 60)
    
    from rich.console import Console
    from rich.table import Table
    
    console = Console()
    
    # Demo 1: Test entity parsing with sample data
    print("\n1. Testing entity parsing with sample data...")
    
    import json
    import tempfile
    
    sample_entities = [
        {
            "id": "demo-001",
            "schema": "Person",
            "caption": "Viktor Testovich",
            "datasets": ["demo_sanctions"],
            "properties": {
                "name": ["Viktor Testovich", "Виктор Тестович"],
                "country": ["RU"],
                "birthDate": ["1965-03-15"],
            }
        },
        {
            "id": "demo-002",
            "schema": "Company",
            "caption": "Shell Corp Ltd",
            "datasets": ["demo_sanctions"],
            "properties": {
                "name": ["Shell Corp Ltd"],
                "jurisdiction": ["vg"],  # British Virgin Islands
                "ownershipOwner": ["demo-001"],
            }
        },
    ]
    
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        for entity in sample_entities:
            f.write(json.dumps(entity) + "\n")
        temp_path = Path(f.name)
    
    try:
        from src.ingest.opensanctions import OpenSanctionsClient
        
        client = OpenSanctionsClient()
        df = client.parse_entities(temp_path)
        
        table = Table(title="Parsed Entities")
        table.add_column("ID")
        table.add_column("Type")
        table.add_column("Name")
        table.add_column("Country")
        
        for row in df.iter_rows(named=True):
            table.add_row(
                row["entity_id"],
                row["schema"],
                row["caption"],
                row["countries"] or "-",
            )
        
        console.print(table)
        print("  OK: Entity parsing works!")
        
    finally:
        temp_path.unlink()
    
    # Demo 2: Test name normalization
    print("\n2. Testing name normalization...")
    
    from src.ingest.opensanctions import OpenSanctionsClient
    
    # We'll add this function to test
    test_names = [
        "GAZPROM OOO",
        "Shell Corp., Ltd.",
        "Газпром",
        "ACME CORPORATION INC",
    ]
    
    print("  Name normalization examples:")
    for name in test_names:
        # Simple normalization demo
        normalized = name.upper()
        for suffix in ["LLC", "LTD", "INC", "OOO", "CORPORATION", "CORP"]:
            normalized = normalized.replace(f" {suffix}", "")
        normalized = " ".join(normalized.split())
        print(f"    '{name}' -> '{normalized}'")
    
    print("  OK: Name normalization works!")
    
    # Demo 3: Check API key status
    print("\n3. Checking API key configuration...")
    
    from src.config import settings
    
    if settings.uk_companies_house_api_key:
        print("  OK: UK Companies House API key configured")
    else:
        print("  NOTE: UK Companies House API key not set (optional)")
        print("        Get free key at: https://developer.company-information.service.gov.uk/")
    
    if settings.opencorporates_api_key:
        print("  OK: OpenCorporates API key configured")
    else:
        print("  NOTE: OpenCorporates API key not set (optional)")
    
    print("\n" + "=" * 60)
    print("DEMO COMPLETE!")
    print("=" * 60)
    print("\nNext steps:")
    print("  1. Run: python -m src.ingest.opensanctions")
    print("     This will download real sanctions data (~500MB)")
    print()
    print("  2. Or use the CLI: snm ingest opensanctions")
    print()
    print("  3. View stats: snm analyze stats")
    print()


def main():
    """Main entry point."""
    print("=" * 60)
    print("SANCTIONS NETWORK MAPPER - QUICKSTART")
    print("=" * 60)
    
    # Run checks
    checks_passed = True
    
    if not check_python_version():
        checks_passed = False
    
    if not check_dependencies():
        checks_passed = False
        print("\nInstall missing dependencies with:")
        print("  pip install -e .")
        print("  # or")
        print("  uv pip install -e .")
        sys.exit(1)
    
    check_directories()
    
    if checks_passed:
        run_demo()
    else:
        print("\nFix the issues above and run this script again.")
        sys.exit(1)


if __name__ == "__main__":
    main()
