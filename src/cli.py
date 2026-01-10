"""
Command Line Interface for Sanctions Network Mapper

Provides commands for data ingestion, analysis, and reporting.

Usage:
    snm ingest opensanctions       # Download and parse sanctions data
    snm ingest corporate           # Download corporate data
    snm analyze resolve            # Run entity resolution
    snm analyze network            # Build and analyze network
    snm report generate            # Generate risk reports
"""

import logging
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from src.config import settings

# Create CLI app
app = typer.Typer(
    name="snm",
    help="Sanctions Network Mapper - Cross-reference sanctions with corporate data",
    add_completion=False,
)

# Sub-commands
ingest_app = typer.Typer(help="Data ingestion commands")
analyze_app = typer.Typer(help="Analysis commands")
report_app = typer.Typer(help="Reporting commands")

app.add_typer(ingest_app, name="ingest")
app.add_typer(analyze_app, name="analyze")
app.add_typer(report_app, name="report")

# Rich console for pretty output
console = Console()


def setup_logging(verbose: bool = False):
    """Configure logging with rich handler."""
    level = logging.DEBUG if verbose else logging.INFO
    
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )


@app.callback()
def main(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
):
    """
    Sanctions Network Mapper CLI
    
    Cross-reference global sanctions lists with corporate registries
    to identify potential shell company networks.
    """
    setup_logging(verbose)
    settings.ensure_directories()


# =============================================================================
# Ingest Commands
# =============================================================================

@ingest_app.command("opensanctions")
def ingest_opensanctions_cmd(
    dataset: str = typer.Option(
        "sanctions",
        "--dataset", "-d",
        help="Dataset to download: sanctions, default, peps, crime",
    ),
    force: bool = typer.Option(
        False,
        "--force", "-f",
        help="Force re-download even if cached",
    ),
    output_dir: Optional[Path] = typer.Option(
        None,
        "--output", "-o",
        help="Output directory for processed files",
    ),
):
    """
    Download and process OpenSanctions data.
    
    Downloads the latest sanctions data, parses entities and relationships,
    and saves to Parquet files for further processing.
    """
    from src.ingest.opensanctions import ingest_opensanctions
    
    console.print(f"\n[bold blue]Ingesting OpenSanctions ({dataset})[/bold blue]\n")
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Downloading and processing...", total=None)
        
        entities_df, relationships_df = ingest_opensanctions(
            dataset=dataset,
            output_dir=output_dir,
            force_download=force,
        )
        
        progress.update(task, completed=True)
    
    # Display summary
    table = Table(title="Ingestion Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    
    table.add_row("Total Entities", f"{len(entities_df):,}")
    table.add_row("Total Relationships", f"{len(relationships_df):,}")
    
    # Schema breakdown
    schema_counts = entities_df.group_by("schema").len().sort("len", descending=True)
    for row in schema_counts.head(5).iter_rows():
        table.add_row(f"  {row[0]}", f"{row[1]:,}")
    
    console.print(table)
    console.print("\n[green]✓ Ingestion complete![/green]\n")


@ingest_app.command("corporate")
def ingest_corporate_cmd(
    source: str = typer.Option(
        "uk",
        "--source", "-s",
        help="Data source: uk (Companies House), oc (OpenCorporates)",
    ),
    query: Optional[str] = typer.Option(
        None,
        "--query", "-q",
        help="Search query for company names",
    ),
):
    """
    Search and download corporate registry data.
    
    Currently supports:
    - UK Companies House (requires API key)
    - OpenCorporates (optional API key)
    """
    if source == "uk":
        from src.ingest.uk_companies_house import UKCompaniesHouseClient
        
        if not settings.uk_companies_house_api_key:
            console.print(
                "[red]Error: UK_COMPANIES_HOUSE_API_KEY not set[/red]\n"
                "Get a free key at: https://developer.company-information.service.gov.uk/"
            )
            raise typer.Exit(1)
        
        with UKCompaniesHouseClient() as client:
            if query:
                console.print(f"\n[bold]Searching UK companies: {query}[/bold]\n")
                results = client.search_companies(query, limit=10)
                
                table = Table(title=f"Search Results: {query}")
                table.add_column("Company Number")
                table.add_column("Name")
                table.add_column("Status")
                
                for r in results:
                    table.add_row(
                        r.get("company_number", ""),
                        r.get("title", ""),
                        r.get("company_status", ""),
                    )
                
                console.print(table)
            else:
                console.print("[yellow]Specify --query to search for companies[/yellow]")
    
    elif source == "oc":
        from src.ingest.opencorporates import OpenCorporatesClient
        
        with OpenCorporatesClient() as client:
            if query:
                console.print(f"\n[bold]Searching OpenCorporates: {query}[/bold]\n")
                
                table = Table(title=f"Search Results: {query}")
                table.add_column("Jurisdiction")
                table.add_column("Number")
                table.add_column("Name")
                table.add_column("Status")
                
                for company in client.search_companies(query, limit=10):
                    table.add_row(
                        company.jurisdiction_code,
                        company.company_number,
                        company.name[:50],
                        company.current_status or "",
                    )
                
                console.print(table)
            else:
                console.print("[yellow]Specify --query to search for companies[/yellow]")
    
    else:
        console.print(f"[red]Unknown source: {source}[/red]")
        raise typer.Exit(1)


# =============================================================================
# Analyze Commands
# =============================================================================

@analyze_app.command("stats")
def analyze_stats_cmd():
    """
    Show statistics about loaded data.
    """
    import polars as pl
    
    entities_path = settings.processed_data_dir / "sanctions_entities.parquet"
    relationships_path = settings.processed_data_dir / "sanctions_relationships.parquet"
    
    if not entities_path.exists():
        console.print("[red]No data found. Run 'snm ingest opensanctions' first.[/red]")
        raise typer.Exit(1)
    
    entities_df = pl.read_parquet(entities_path)
    
    console.print("\n[bold blue]Data Statistics[/bold blue]\n")
    
    # Entity stats
    table = Table(title="Entity Statistics")
    table.add_column("Schema", style="cyan")
    table.add_column("Count", style="green", justify="right")
    
    schema_counts = entities_df.group_by("schema").len().sort("len", descending=True)
    for row in schema_counts.iter_rows():
        table.add_row(row[0], f"{row[1]:,}")
    
    console.print(table)
    
    # Relationship stats
    if relationships_path.exists():
        relationships_df = pl.read_parquet(relationships_path)
        
        table = Table(title="Relationship Statistics")
        table.add_column("Type", style="cyan")
        table.add_column("Count", style="green", justify="right")
        
        rel_counts = relationships_df.group_by("relationship_type").len().sort("len", descending=True)
        for row in rel_counts.iter_rows():
            table.add_row(row[0], f"{row[1]:,}")
        
        console.print(table)
    
    # Dataset breakdown
    console.print("\n[bold]Top Source Datasets:[/bold]")
    
    # Flatten and count datasets
    all_datasets: dict[str, int] = {}
    for datasets in entities_df["datasets"].to_list():
        if datasets:
            for ds in datasets.split(","):
                all_datasets[ds] = all_datasets.get(ds, 0) + 1
    
    sorted_datasets = sorted(all_datasets.items(), key=lambda x: -x[1])[:10]
    for ds, count in sorted_datasets:
        console.print(f"  {ds}: {count:,}")


# =============================================================================
# Report Commands
# =============================================================================

@report_app.command("summary")
def report_summary_cmd(
    output: Optional[Path] = typer.Option(
        None,
        "--output", "-o",
        help="Output file path",
    ),
):
    """
    Generate a summary report of the loaded data.
    """
    import polars as pl
    from datetime import datetime
    
    entities_path = settings.processed_data_dir / "sanctions_entities.parquet"
    
    if not entities_path.exists():
        console.print("[red]No data found. Run 'snm ingest opensanctions' first.[/red]")
        raise typer.Exit(1)
    
    entities_df = pl.read_parquet(entities_path)
    
    output_path = output or (settings.output_dir / f"summary_{datetime.now():%Y%m%d}.md")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Generate report
    schema_counts = entities_df.group_by("schema").len().sort("len", descending=True)
    
    report = f"""# Sanctions Data Summary Report
Generated: {datetime.now():%Y-%m-%d %H:%M}

## Overview

- **Total Entities**: {len(entities_df):,}
- **Data Source**: OpenSanctions

## Entity Types

| Schema | Count |
|--------|-------|
"""
    
    for row in schema_counts.iter_rows():
        report += f"| {row[0]} | {row[1]:,} |\n"
    
    report += """
## Notes

This data includes sanctioned entities from OFAC, EU, UN, and other sources.
Run entity resolution against corporate registries for evasion detection.
"""
    
    output_path.write_text(report)
    console.print(f"\n[green]Report saved to: {output_path}[/green]\n")


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    app()
