#!/usr/bin/env python3
"""
Interactive Sanctions Data Explorer

A CLI tool for exploring and analyzing sanctions data with an intuitive interface.

Usage:
    python src/analyze/explorer.py
"""

import sys
from pathlib import Path

import polars as pl
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt, IntPrompt, Confirm
from rich import box

console = Console()

entities: pl.DataFrame | None = None
relationships: pl.DataFrame | None = None


def load_data() -> bool:
    global entities, relationships
    
    entities_path = Path("data/processed/sanctions_entities.parquet")
    relationships_path = Path("data/processed/sanctions_relationships.parquet")
    
    if not entities_path.exists():
        console.print("[red]Error: Data not found. Run 'snm ingest opensanctions' first.[/red]")
        return False
    
    with console.status("[bold green]Loading data..."):
        entities = pl.read_parquet(entities_path)
        if relationships_path.exists():
            relationships = pl.read_parquet(relationships_path)
        else:
            relationships = pl.DataFrame({"source_id": [], "target_id": [], "relationship_type": []})
    
    console.print(f"[green]Loaded {len(entities):,} entities and {len(relationships):,} relationships[/green]\n")
    return True


def show_menu():
    menu = """
[bold cyan]═══════════════════════════════════════════════════════════════[/bold cyan]
[bold white]              SANCTIONS DATA EXPLORER[/bold white]
[bold cyan]═══════════════════════════════════════════════════════════════[/bold cyan]

[bold yellow]EXPLORE[/bold yellow]
  [cyan]1[/cyan]  Overview & Statistics
  [cyan]2[/cyan]  Search by Name
  [cyan]3[/cyan]  Browse by Entity Type (Person, Company, etc.)
  [cyan]4[/cyan]  Browse by Country

[bold yellow]ANALYZE[/bold yellow]
  [cyan]5[/cyan]  High-Risk Jurisdictions (Shell Companies)
  [cyan]6[/cyan]  Ownership Analysis (Who Owns What)
  [cyan]7[/cyan]  Sanctions Lists Breakdown
  [cyan]8[/cyan]  Recently Added Entities
  [cyan]9[/cyan]  Entities with Identifiers (INN, LEI, etc.)

[bold yellow]EXPORT[/bold yellow]
  [cyan]10[/cyan] Export Current Results to CSV

[bold yellow]OTHER[/bold yellow]
  [cyan]11[/cyan] Custom Query (Advanced)
  [cyan]h[/cyan]  Show this menu
  [cyan]q[/cyan]  Quit
"""
    console.print(menu)


def show_overview():
    console.print("\n[bold]═══ OVERVIEW ═══[/bold]\n")
    console.print(f"[bold]Total Entities:[/bold] {len(entities):,}")
    console.print(f"[bold]Total Relationships:[/bold] {len(relationships):,}")
    
    table = Table(title="\nEntity Types", box=box.ROUNDED)
    table.add_column("Type", style="cyan")
    table.add_column("Count", justify="right", style="green")
    table.add_column("Percentage", justify="right", style="yellow")
    
    type_counts = entities.group_by("schema").len().sort("len", descending=True)
    total = len(entities)
    for row in type_counts.iter_rows():
        pct = (row[1] / total) * 100
        table.add_row(row[0], f"{row[1]:,}", f"{pct:.1f}%")
    console.print(table)
    
    if len(relationships) > 0:
        table = Table(title="\nRelationship Types", box=box.ROUNDED)
        table.add_column("Type", style="cyan")
        table.add_column("Count", justify="right", style="green")
        rel_counts = relationships.group_by("relationship_type").len().sort("len", descending=True)
        for row in rel_counts.iter_rows():
            table.add_row(row[0], f"{row[1]:,}")
        console.print(table)
    
    console.print("\n[bold]Top 10 Countries:[/bold]")
    country_counts = {}
    for countries in entities["countries"].to_list():
        for c in countries.split("|"):
            if c:
                country_counts[c] = country_counts.get(c, 0) + 1
    
    table = Table(box=box.SIMPLE)
    table.add_column("Country", style="cyan")
    table.add_column("Entities", justify="right", style="green")
    for country, count in sorted(country_counts.items(), key=lambda x: -x[1])[:10]:
        table.add_row(country, f"{count:,}")
    console.print(table)


def search_by_name():
    console.print("\n[bold]═══ SEARCH BY NAME ═══[/bold]\n")
    query = Prompt.ask("Enter search term").strip().lower()
    if not query:
        return None
    
    results = entities.filter(
        pl.col("names").str.to_lowercase().str.contains(query) |
        pl.col("aliases").str.to_lowercase().str.contains(query) |
        pl.col("caption").str.to_lowercase().str.contains(query)
    )
    
    console.print(f"\n[green]Found {len(results):,} matches for '{query}'[/green]\n")
    if len(results) == 0:
        return None
    display_results(results)
    return results


def browse_by_type():
    console.print("\n[bold]═══ BROWSE BY ENTITY TYPE ═══[/bold]\n")
    type_counts = entities.group_by("schema").len().sort("len", descending=True)
    
    console.print("[bold]Available types:[/bold]")
    types_list = []
    for i, row in enumerate(type_counts.iter_rows(), 1):
        types_list.append(row[0])
        console.print(f"  [cyan]{i}[/cyan]. {row[0]} ({row[1]:,})")
    
    choice = Prompt.ask("\nEnter number or type name").strip()
    try:
        idx = int(choice) - 1
        selected_type = types_list[idx] if 0 <= idx < len(types_list) else choice
    except ValueError:
        selected_type = choice
    
    results = entities.filter(pl.col("schema") == selected_type)
    if len(results) == 0:
        console.print(f"[yellow]No entities of type '{selected_type}'[/yellow]")
        return None
    
    console.print(f"\n[green]Found {len(results):,} {selected_type} entities[/green]\n")
    display_results(results)
    return results


def browse_by_country():
    console.print("\n[bold]═══ BROWSE BY COUNTRY ═══[/bold]\n")
    country_counts = {}
    for countries in entities["countries"].to_list():
        for c in countries.split("|"):
            if c:
                country_counts[c] = country_counts.get(c, 0) + 1
    
    sorted_countries = sorted(country_counts.items(), key=lambda x: -x[1])[:20]
    console.print("[bold]Top countries:[/bold]")
    for country, count in sorted_countries[:10]:
        console.print(f"  {country}: {count:,}")
    
    code = Prompt.ask("\nEnter country code (e.g., RU, IR, CN)").strip().upper()
    if not code:
        return None
    
    results = entities.filter(pl.col("countries").str.contains(code))
    if len(results) == 0:
        console.print(f"[yellow]No entities for country '{code}'[/yellow]")
        return None
    
    console.print(f"\n[green]Found {len(results):,} entities associated with {code}[/green]\n")
    display_results(results)
    return results


def analyze_high_risk_jurisdictions():
    console.print("\n[bold]═══ HIGH-RISK JURISDICTIONS ═══[/bold]\n")
    high_risk = {"vg": "British Virgin Islands", "ky": "Cayman Islands", "sc": "Seychelles",
                 "pa": "Panama", "bz": "Belize", "ws": "Samoa", "mh": "Marshall Islands",
                 "cy": "Cyprus", "mt": "Malta", "lu": "Luxembourg"}
    
    companies = entities.filter(pl.col("schema") == "Company")
    
    table = Table(box=box.ROUNDED)
    table.add_column("Jurisdiction", style="cyan")
    table.add_column("Code", style="yellow")
    table.add_column("Companies", justify="right", style="red")
    
    total_shell = 0
    jurisdiction_data = []
    for code, name in high_risk.items():
        count = len(companies.filter(pl.col("jurisdiction") == code))
        if count > 0:
            jurisdiction_data.append((name, code, count))
            total_shell += count
    
    for name, code, count in sorted(jurisdiction_data, key=lambda x: -x[2]):
        table.add_row(name, code, f"{count:,}")
    
    console.print(table)
    console.print(f"\n[bold red]Total in secrecy jurisdictions: {total_shell:,}[/bold red]")
    
    if total_shell > 0 and Confirm.ask("\nView company details?"):
        code = Prompt.ask("Enter jurisdiction code", default="vg")
        results = companies.filter(pl.col("jurisdiction") == code)
        display_results(results)
        return results
    return None


def analyze_ownership():
    console.print("\n[bold]═══ OWNERSHIP ANALYSIS ═══[/bold]\n")
    ownership = relationships.filter(pl.col("relationship_type").is_in(["owned_by", "owns"]))
    
    if len(ownership) == 0:
        console.print("[yellow]No ownership relationships found.[/yellow]")
        return None
    
    console.print(f"[bold]Total ownership relationships:[/bold] {len(ownership):,}\n")
    
    owned_by = relationships.filter(pl.col("relationship_type") == "owned_by")
    top_owners = owned_by.group_by("target_id").len().sort("len", descending=True).head(20)
    
    table = Table(title="Top Owners", box=box.ROUNDED)
    table.add_column("#", style="dim")
    table.add_column("Owner", style="cyan")
    table.add_column("Type", style="yellow")
    table.add_column("Owns", justify="right", style="green")
    
    for i, row in enumerate(top_owners.iter_rows(), 1):
        owner_id, count = row[0], row[1]
        owner = entities.filter(pl.col("entity_id") == owner_id)
        if len(owner) > 0:
            table.add_row(str(i), owner["caption"][0][:45], owner["schema"][0], f"{count:,}")
    
    console.print(table)
    
    if Confirm.ask("\nExplore a specific owner's holdings?"):
        idx = IntPrompt.ask("Enter owner number", default=1)
        if 1 <= idx <= len(top_owners):
            owner_id = top_owners.row(idx - 1)[0]
            owned_ids = owned_by.filter(pl.col("target_id") == owner_id)["source_id"].to_list()
            results = entities.filter(pl.col("entity_id").is_in(owned_ids))
            owner_name = entities.filter(pl.col("entity_id") == owner_id)["caption"][0]
            console.print(f"\n[bold]Entities owned by {owner_name}:[/bold]\n")
            display_results(results)
            return results
    return None


def analyze_sanctions_lists():
    console.print("\n[bold]═══ SANCTIONS LISTS BREAKDOWN ═══[/bold]\n")
    dataset_counts = {}
    for ds_str in entities["datasets"].to_list():
        for ds in ds_str.split(","):
            if ds:
                dataset_counts[ds] = dataset_counts.get(ds, 0) + 1
    
    table = Table(title="Sanctions Lists", box=box.ROUNDED)
    table.add_column("Dataset", style="cyan")
    table.add_column("Entities", justify="right", style="green")
    
    for ds, count in sorted(dataset_counts.items(), key=lambda x: -x[1])[:20]:
        table.add_row(ds, f"{count:,}")
    console.print(table)
    
    if Confirm.ask("\nFilter by a specific dataset?"):
        ds = Prompt.ask("Enter dataset name")
        results = entities.filter(pl.col("datasets").str.contains(ds))
        if len(results) > 0:
            console.print(f"\n[green]Found {len(results):,} entities[/green]\n")
            display_results(results)
            return results
    return None


def analyze_recent():
    console.print("\n[bold]═══ RECENTLY ADDED ENTITIES ═══[/bold]\n")
    with_dates = entities.filter(pl.col("first_seen") != "")
    
    year_counts = {}
    for date in with_dates["first_seen"].to_list():
        if date and len(date) >= 4:
            year = date[:4]
            year_counts[year] = year_counts.get(year, 0) + 1
    
    table = Table(box=box.SIMPLE)
    table.add_column("Year", style="cyan")
    table.add_column("Entities", justify="right", style="green")
    for year in sorted(year_counts.keys(), reverse=True)[:10]:
        table.add_row(year, f"{year_counts[year]:,}")
    console.print(table)
    
    if Confirm.ask("\nView entities from a specific year?"):
        year = Prompt.ask("Enter year", default="2024")
        results = entities.filter(pl.col("first_seen").str.starts_with(year))
        if len(results) > 0:
            display_results(results)
            return results
    return None


def analyze_identifiers():
    console.print("\n[bold]═══ ENTITIES WITH IDENTIFIERS ═══[/bold]\n")
    ids = {"inn_code": "Russian INN", "ogrn_code": "Russian OGRN", "lei_code": "LEI",
           "swift_bic": "SWIFT/BIC", "imo_number": "IMO", "registration_number": "Registration #"}
    
    table = Table(box=box.ROUNDED)
    table.add_column("Identifier", style="cyan")
    table.add_column("Count", justify="right", style="green")
    
    for col, name in ids.items():
        count = len(entities.filter(pl.col(col) != ""))
        table.add_row(name, f"{count:,}")
    console.print(table)
    
    console.print("\n[cyan]1[/cyan]. Russian INN  [cyan]2[/cyan]. LEI  [cyan]3[/cyan]. IMO  [cyan]4[/cyan]. Back")
    choice = Prompt.ask("Select", default="4")
    
    col_map = {"1": "inn_code", "2": "lei_code", "3": "imo_number"}
    if choice in col_map:
        col = col_map[choice]
        results = entities.filter(pl.col(col) != "")
        display_results(results, extra_cols=[col])
        return results
    return None


def export_results(results):
    console.print("\n[bold]═══ EXPORT TO CSV ═══[/bold]\n")
    if results is None or len(results) == 0:
        console.print("[yellow]No results to export.[/yellow]")
        return
    
    filename = Prompt.ask("Enter filename", default="export.csv")
    if not filename.endswith(".csv"):
        filename += ".csv"
    
    output_path = Path("data/output") / filename
    output_path.parent.mkdir(parents=True, exist_ok=True)
    results.write_csv(output_path)
    console.print(f"[green]Exported {len(results):,} rows to {output_path}[/green]")


def custom_query():
    console.print("\n[bold]═══ CUSTOM QUERY ═══[/bold]\n")
    console.print(f"[dim]Columns: {', '.join(entities.columns[:10])}...[/dim]")
    console.print("[dim]Example: entities.filter(pl.col('countries').str.contains('IR')).head(10)[/dim]\n")
    
    query = Prompt.ask("Enter Polars expression (or 'back')")
    if query.lower() == "back":
        return None
    
    try:
        result = eval(query)
        if isinstance(result, pl.DataFrame):
            console.print(f"\n[green]Result: {len(result):,} rows[/green]\n")
            console.print(result)
            return result
        else:
            console.print(f"\n{result}")
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
    return None


def display_results(df, extra_cols=None, limit=25):
    if len(df) == 0:
        return
    
    table = Table(box=box.ROUNDED)
    table.add_column("#", style="dim", width=4)
    table.add_column("Name", style="cyan", max_width=40)
    table.add_column("Type", style="yellow", width=12)
    table.add_column("Countries", style="green", width=12)
    table.add_column("Jurisdiction", style="magenta", width=10)
    
    for col in (extra_cols or []):
        table.add_column(col, style="blue", max_width=15)
    
    for i, row in enumerate(df.head(limit).iter_rows(named=True), 1):
        row_data = [str(i), (row.get("caption") or "")[:40], row.get("schema") or "",
                    (row.get("countries") or "")[:12], row.get("jurisdiction") or ""]
        for col in (extra_cols or []):
            row_data.append((str(row.get(col) or ""))[:15])
        table.add_row(*row_data)
    
    console.print(table)
    if len(df) > limit:
        console.print(f"[dim]Showing {limit} of {len(df):,}. Export to see all.[/dim]")


def main():
    console.print(Panel.fit("[bold]SANCTIONS DATA EXPLORER[/bold]", border_style="cyan"))
    
    if not load_data():
        sys.exit(1)
    
    show_menu()
    last_results = None
    
    while True:
        try:
            choice = Prompt.ask("\n[bold cyan]>[/bold cyan]").strip().lower()
            
            if choice in ["q", "quit", "exit"]:
                console.print("[dim]Goodbye![/dim]")
                break
            elif choice in ["h", "help", "menu"]:
                show_menu()
            elif choice == "1": show_overview()
            elif choice == "2": last_results = search_by_name()
            elif choice == "3": last_results = browse_by_type()
            elif choice == "4": last_results = browse_by_country()
            elif choice == "5": last_results = analyze_high_risk_jurisdictions()
            elif choice == "6": last_results = analyze_ownership()
            elif choice == "7": last_results = analyze_sanctions_lists()
            elif choice == "8": last_results = analyze_recent()
            elif choice == "9": last_results = analyze_identifiers()
            elif choice == "10": export_results(last_results)
            elif choice == "11": last_results = custom_query()
            else: console.print("[yellow]Unknown command. Type 'h' for help.[/yellow]")
        except KeyboardInterrupt:
            console.print("\n[dim]Type 'q' to quit.[/dim]")
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]")


if __name__ == "__main__":
    main()
