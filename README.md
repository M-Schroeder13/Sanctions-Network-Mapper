# Sanctions Network Mapper

A powerful intelligence analysis tool for exploring global sanctions data. Cross-reference OFAC, EU, UN, and 100+ other sanctions lists to identify sanctioned entities, analyze ownership networks, and detect potential shell company structures.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Installation](#installation)
   - [GitHub Codespaces (Recommended)](#github-codespaces-recommended)
   - [Local Installation](#local-installation)
3. [Loading Sanctions Data](#loading-sanctions-data)
4. [Using the Interactive Explorer](#using-the-interactive-explorer)
5. [Command Line Interface](#command-line-interface)
6. [Polars Query Guide](#polars-query-guide)
7. [Data Reference](#data-reference)
8. [Troubleshooting](#troubleshooting)

---

## Quick Start

```bash
# 1. Install dependencies (automatic in Codespaces)
uv venv && source .venv/bin/activate && uv pip install -e .

# 2. Download sanctions data (~500MB, takes 2-5 minutes)
snm ingest opensanctions

# 3. Parse the data (takes 1-2 minutes)
python src/ingest/parse_sanctions.py

# 4. Launch the interactive explorer
python src/analyze/explorer.py
```

---

## Installation

### GitHub Codespaces (Recommended)

GitHub Codespaces provides a complete, pre-configured development environment in your browser. No local setup required.

#### Step 1: Create a Codespace

1. Go to your repository on GitHub
2. Click the green **Code** button
3. Select the **Codespaces** tab
4. Click **Create codespace on main**

#### Step 2: Wait for Setup

The devcontainer will automatically:
- Install Python 3.12
- Install the `uv` package manager
- Create a virtual environment
- Install all dependencies
- Create data directories

This takes approximately 2-3 minutes. You'll see setup progress in the terminal.

#### Step 3: Verify Installation

Once setup completes, run:

```bash
python quickstart.py
```

You should see:
```
============================================================
SANCTIONS NETWORK MAPPER - QUICKSTART
============================================================
Checking Python version...
  OK: Python 3.12
Checking dependencies...
  OK: polars (1.x.x)
  OK: httpx (0.27.x)
  ...
```

#### Step 4: Activate Virtual Environment (if needed)

If you open a new terminal, activate the virtual environment:

```bash
source .venv/bin/activate
```

You'll see `(.venv)` at the start of your terminal prompt when activated.

---

### Local Installation

#### Prerequisites

- Python 3.11 or higher
- Git
- 2GB free disk space (for sanctions data)

#### Step 1: Clone the Repository

```bash
git clone https://github.com/YOUR_USERNAME/sanctions-network-mapper.git
cd sanctions-network-mapper
```

#### Step 2: Install uv (Recommended)

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
```

#### Step 3: Create Virtual Environment and Install

```bash
# Create virtual environment
uv venv

# Activate it
source .venv/bin/activate  # Linux/macOS
# OR
.venv\Scripts\activate     # Windows

# Install dependencies
uv pip install -e .
```

#### Alternative: Using pip

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

#### Step 4: Create Data Directories

```bash
mkdir -p data/raw/opensanctions data/processed data/output
```

#### Step 5: Verify Installation

```bash
python quickstart.py
```

---

## Loading Sanctions Data

Before you can explore sanctions data, you need to download and parse it.

### Step 1: Download Sanctions Data

```bash
snm ingest opensanctions
```

This downloads the latest sanctions data from OpenSanctions (~500MB). The download is cached, so subsequent runs use the cached file.

**Expected output:**
```
Ingesting OpenSanctions (sanctions)

Downloading and processing...
Download progress: 10.0%
Download progress: 20.0%
...
```

### Step 2: Parse the Data

```bash
python src/ingest/parse_sanctions.py
```

This parses the JSON data into optimized Parquet files.

**Expected output:**
```
Parsing: data/raw/opensanctions/sanctions_20260115.json
Output:  data/processed

  Processed 100,000 entities...
  Processed 200,000 entities...

Total lines processed: 250,000
Errors: 0

Creating DataFrames with explicit string schema...
  Entities: 250,000 rows
  Relationships: 45,000 rows

Saving to Parquet...
  Saved: data/processed/sanctions_entities.parquet
  Saved: data/processed/sanctions_relationships.parquet

Schema breakdown:
  LegalEntity: 120,000
  Person: 80,000
  Company: 30,000
  Organization: 15,000
  Vessel: 5,000
```

### Step 3: Verify Data Loaded

```bash
snm analyze stats
```

---

## Using the Interactive Explorer

The Interactive Explorer provides a menu-driven interface for exploring sanctions data without writing code.

### Launch the Explorer

```bash
python src/analyze/explorer.py
```

### Main Menu

```
═══════════════════════════════════════════════════════════════
              SANCTIONS DATA EXPLORER
═══════════════════════════════════════════════════════════════

EXPLORE
  1  Overview & Statistics
  2  Search by Name
  3  Browse by Entity Type (Person, Company, etc.)
  4  Browse by Country

ANALYZE
  5  High-Risk Jurisdictions (Shell Companies)
  6  Ownership Analysis (Who Owns What)
  7  Sanctions Lists Breakdown
  8  Recently Added Entities
  9  Entities with Identifiers (INN, LEI, etc.)

EXPORT
  10 Export Current Results to CSV

OTHER
  11 Custom Query (Advanced)
  h  Show this menu
  q  Quit
```

### Menu Options Explained

#### Option 1: Overview & Statistics
Shows total entity counts, breakdown by type (Person, Company, Vessel, etc.), relationship types, and top countries.

#### Option 2: Search by Name
Search for entities by name. Searches across names, aliases, and captions. Case-insensitive.

**Example:**
```
> 2
Enter search term: gazprom

Found 47 matches for 'gazprom'

┌────┬──────────────────────────────────┬────────────┬────────────┬────────────┐
│ #  │ Name                             │ Type       │ Countries  │ Jurisdiction│
├────┼──────────────────────────────────┼────────────┼────────────┼────────────┤
│ 1  │ Gazprom PJSC                     │ Company    │ RU         │ ru         │
│ 2  │ Gazprombank                      │ Company    │ RU         │ ru         │
│ 3  │ Gazprom Neft                     │ Company    │ RU         │ ru         │
└────┴──────────────────────────────────┴────────────┴────────────┴────────────┘
```

#### Option 3: Browse by Entity Type
Filter entities by type: Person, Company, LegalEntity, Organization, Vessel, Aircraft, etc.

#### Option 4: Browse by Country
Filter entities by country code (ISO 3166-1 alpha-2).

**Common country codes:**
- `RU` - Russia
- `IR` - Iran
- `KP` - North Korea
- `CN` - China
- `SY` - Syria
- `BY` - Belarus
- `VE` - Venezuela

#### Option 5: High-Risk Jurisdictions
Find companies registered in secrecy jurisdictions commonly used for shell companies:
- `vg` - British Virgin Islands
- `ky` - Cayman Islands
- `pa` - Panama
- `sc` - Seychelles
- `bz` - Belize
- `cy` - Cyprus
- `mt` - Malta

#### Option 6: Ownership Analysis
Analyze ownership relationships. See who owns the most entities and explore their holdings.

#### Option 7: Sanctions Lists Breakdown
See which sanctions lists (OFAC, EU, UN, etc.) contain the most entities.

**Common dataset codes:**
- `us_ofac_sdn` - US OFAC Specially Designated Nationals
- `eu_fsf` - EU Financial Sanctions
- `un_sc_sanctions` - UN Security Council
- `gb_hmt_sanctions` - UK HM Treasury
- `ua_nsdc_sanctions` - Ukraine NSDC

#### Option 8: Recently Added Entities
See when entities were added to sanctions lists, broken down by year.

#### Option 9: Entities with Identifiers
Find entities with specific identifiers useful for cross-referencing:
- **Russian INN** - Tax identification number
- **Russian OGRN** - Registration number
- **LEI** - Legal Entity Identifier (global standard)
- **IMO** - Ship identification number
- **SWIFT/BIC** - Bank identifier

#### Option 10: Export to CSV
Export the last search results to a CSV file for use in Excel or other tools.

#### Option 11: Custom Query
Write custom Polars queries for advanced analysis. See [Polars Query Guide](#polars-query-guide).

### Navigation Tips

- Type a number and press Enter to select a menu option
- Type `h` to show the menu again
- Type `q` to quit
- Press `Ctrl+C` to cancel an operation
- When viewing results, you may be prompted to view details or export

---

## Command Line Interface

The `snm` command provides quick access to common operations.

### Available Commands

```bash
# Show help
snm --help

# Data ingestion
snm ingest opensanctions              # Download and parse sanctions data
snm ingest opensanctions --force      # Force re-download even if cached
snm ingest corporate --source uk -q "Barclays"  # Search UK Companies House

# Analysis
snm analyze stats                     # Show data statistics

# Reports
snm report summary                    # Generate a summary report
snm report summary -o report.md       # Save report to specific file
```

### Verbose Mode

Add `-v` for detailed logging:

```bash
snm -v ingest opensanctions
```

---

## Polars Query Guide

The Custom Query option (11) in the Interactive Explorer lets you write Polars queries directly. This section teaches you the syntax.

### Basic Query Structure

```python
dataframe.filter(condition).select([columns]).sort(column).head(n)
```

### Available DataFrames

- `entities` - All sanctioned entities
- `relationships` - Ownership and other relationships between entities

### Entity Columns

| Column | Description | Example Values |
|--------|-------------|----------------|
| `entity_id` | Unique identifier | `ofac-12345` |
| `schema` | Entity type | `Person`, `Company`, `Vessel` |
| `caption` | Display name | `Gazprom PJSC` |
| `datasets` | Source sanctions lists (comma-separated) | `us_ofac_sdn,eu_fsf` |
| `names` | All known names (pipe-separated) | `Gazprom\|Газпром` |
| `aliases` | Aliases (pipe-separated) | `GP\|Gasprom` |
| `countries` | Associated countries (pipe-separated) | `RU\|CY` |
| `jurisdiction` | Registration jurisdiction | `ru`, `vg`, `ky` |
| `birth_date` | Birth date (persons) | `1970-01-15` |
| `incorporation_date` | Incorporation date (companies) | `2005-03-20` |
| `inn_code` | Russian tax ID | `7736050003` |
| `lei_code` | Legal Entity Identifier | `213800EFPB... ` |
| `imo_number` | Ship IMO number | `9123456` |
| `first_seen` | When added to sanctions | `2022-02-24` |

### Filter Conditions

#### Exact Match
```python
entities.filter(pl.col("schema") == "Person")
entities.filter(pl.col("jurisdiction") == "vg")
```

#### Multiple Values (IN)
```python
entities.filter(pl.col("schema").is_in(["Person", "Company"]))
entities.filter(pl.col("jurisdiction").is_in(["vg", "ky", "pa"]))
```

#### Not Equal
```python
entities.filter(pl.col("schema") != "Person")
```

#### String Contains (Case-Sensitive)
```python
entities.filter(pl.col("names").str.contains("Gazprom"))
```

#### String Contains (Case-Insensitive)
```python
entities.filter(pl.col("names").str.to_lowercase().str.contains("gazprom"))
```

#### Starts With
```python
entities.filter(pl.col("first_seen").str.starts_with("2024"))
```

#### Not Empty
```python
entities.filter(pl.col("inn_code") != "")
```

#### Is Empty
```python
entities.filter(pl.col("jurisdiction") == "")
```

### Combining Conditions

#### AND (use `&`)
```python
entities.filter(
    (pl.col("schema") == "Person") & 
    (pl.col("countries").str.contains("RU"))
)
```

#### OR (use `|`)
```python
entities.filter(
    (pl.col("countries").str.contains("RU")) | 
    (pl.col("countries").str.contains("BY"))
)
```

#### NOT (use `~`)
```python
entities.filter(~pl.col("schema").is_in(["Person", "Company"]))
```

### Selecting Columns

```python
# Single column
entities.select("caption")

# Multiple columns
entities.select(["caption", "schema", "countries"])
```

### Sorting

```python
# Ascending
entities.sort("caption")

# Descending
entities.sort("caption", descending=True)
```

### Limiting Results

```python
# First 10 rows
entities.head(10)

# Last 10 rows
entities.tail(10)
```

### Grouping & Counting

```python
# Count by entity type
entities.group_by("schema").len()

# Count and sort descending
entities.group_by("schema").len().sort("len", descending=True)

# Count by jurisdiction
entities.group_by("jurisdiction").len().sort("len", descending=True)
```

### Practical Examples

#### Russian Persons
```python
entities.filter(
    (pl.col("schema") == "Person") & 
    (pl.col("countries").str.contains("RU"))
).head(20)
```

#### Iranian Companies
```python
entities.filter(
    (pl.col("schema") == "Company") & 
    (pl.col("countries").str.contains("IR"))
).head(20)
```

#### Shell Companies in BVI
```python
entities.filter(
    (pl.col("schema") == "Company") & 
    (pl.col("jurisdiction") == "vg")
).head(20)
```

#### Banks (Search by Name)
```python
entities.filter(
    pl.col("names").str.to_lowercase().str.contains("bank")
).head(20)
```

#### Entities Added in 2024
```python
entities.filter(
    pl.col("first_seen").str.starts_with("2024")
).head(20)
```

#### Vessels (Ships)
```python
entities.filter(pl.col("schema") == "Vessel").head(20)
```

#### OFAC SDN List Only
```python
entities.filter(
    pl.col("datasets").str.contains("us_ofac_sdn")
).head(20)
```

#### North Korean Entities
```python
entities.filter(pl.col("countries").str.contains("KP")).head(20)
```

#### Entities with Russian Tax ID (INN)
```python
entities.filter(pl.col("inn_code") != "").head(20)
```

#### Entities with LEI
```python
entities.filter(pl.col("lei_code") != "").select(
    ["caption", "lei_code", "jurisdiction"]
).head(20)
```

#### Companies in Multiple Secrecy Jurisdictions
```python
entities.filter(
    pl.col("jurisdiction").is_in(["vg", "ky", "pa", "sc", "bz"])
).head(50)
```

#### Russian or Belarusian Entities in Secrecy Jurisdictions
```python
entities.filter(
    (pl.col("countries").str.contains("RU") | pl.col("countries").str.contains("BY")) &
    (pl.col("jurisdiction").is_in(["vg", "ky", "pa", "sc"]))
).head(20)
```

### Quick Reference

| Operation | Syntax |
|-----------|--------|
| Equals | `pl.col("x") == "value"` |
| Not equals | `pl.col("x") != "value"` |
| In list | `pl.col("x").is_in(["a", "b"])` |
| Contains | `pl.col("x").str.contains("text")` |
| Starts with | `pl.col("x").str.starts_with("text")` |
| Lowercase | `pl.col("x").str.to_lowercase()` |
| Not empty | `pl.col("x") != ""` |
| AND | `(cond1) & (cond2)` |
| OR | `(cond1) \| (cond2)` |
| NOT | `~condition` |
| Group count | `.group_by("x").len()` |
| Sort desc | `.sort("x", descending=True)` |
| Limit | `.head(n)` |

---

## Data Reference

### Entity Types (Schema)

| Schema | Description | Typical Count |
|--------|-------------|---------------|
| `LegalEntity` | Generic legal entities | ~50% |
| `Person` | Individual people | ~30% |
| `Company` | Corporations and businesses | ~10% |
| `Organization` | Non-commercial organizations | ~5% |
| `Vessel` | Ships and boats | ~3% |
| `Aircraft` | Planes and helicopters | ~1% |
| `CryptoWallet` | Cryptocurrency addresses | ~1% |

### Relationship Types

| Type | Description |
|------|-------------|
| `owned_by` | Entity is owned by another entity |
| `owns` | Entity owns another entity |
| `directed_by` | Company directed by person |
| `directs` | Person directs company |
| `family_of` | Family relationship |
| `related_to` | General relationship |
| `associate_of` | Associate relationship |
| `member_of` | Membership relationship |

### Country Codes (ISO 3166-1 Alpha-2)

| Code | Country |
|------|---------|
| `RU` | Russia |
| `IR` | Iran |
| `KP` | North Korea |
| `CN` | China |
| `SY` | Syria |
| `BY` | Belarus |
| `VE` | Venezuela |
| `CU` | Cuba |
| `MM` | Myanmar |
| `UA` | Ukraine |

### Jurisdiction Codes

| Code | Jurisdiction | Notes |
|------|--------------|-------|
| `vg` | British Virgin Islands | High-risk secrecy jurisdiction |
| `ky` | Cayman Islands | High-risk secrecy jurisdiction |
| `pa` | Panama | High-risk secrecy jurisdiction |
| `sc` | Seychelles | High-risk secrecy jurisdiction |
| `bz` | Belize | High-risk secrecy jurisdiction |
| `mh` | Marshall Islands | High-risk secrecy jurisdiction |
| `cy` | Cyprus | Often used for shell companies |
| `mt` | Malta | Often used for shell companies |
| `lu` | Luxembourg | Financial center |
| `gb` | United Kingdom | |
| `us_de` | Delaware, USA | Popular for US incorporation |
| `ru` | Russia | |

### Sanctions List Codes

| Code | Full Name |
|------|-----------|
| `us_ofac_sdn` | US OFAC Specially Designated Nationals |
| `us_ofac_cons` | US OFAC Consolidated Sanctions |
| `eu_fsf` | EU Financial Sanctions |
| `un_sc_sanctions` | UN Security Council Sanctions |
| `gb_hmt_sanctions` | UK HM Treasury Sanctions |
| `ua_nsdc_sanctions` | Ukraine NSDC Sanctions |
| `ch_seco_sanctions` | Swiss SECO Sanctions |
| `au_dfat_sanctions` | Australia DFAT Sanctions |
| `ca_dfatd_sema` | Canada DFATD Sanctions |

---

## Troubleshooting

### "No module named 'polars'" or similar

Your virtual environment isn't activated. Run:

```bash
source .venv/bin/activate
```

### "No data found. Run 'snm ingest opensanctions' first."

You need to download and parse the sanctions data:

```bash
snm ingest opensanctions
python src/ingest/parse_sanctions.py
```

### Download Fails or Times Out

The sanctions file is ~500MB. If your connection is slow:

1. Try again - the download resumes from where it left off
2. If behind a firewall, ensure access to `data.opensanctions.org`

### Parse Fails with Schema Error

Use the robust parser:

```bash
python src/ingest/parse_sanctions.py
```

This handles schema inconsistencies in the source data.

### Codespace Won't Start

1. Delete the existing Codespace from GitHub (Your Codespaces → Delete)
2. Create a new Codespace
3. Wait for the full setup to complete before running commands

### "Permission denied" Errors

On Linux/macOS, ensure the scripts are executable:

```bash
chmod +x .devcontainer/setup.sh
```

### Out of Memory

The full dataset requires ~2GB RAM. If you're limited:

1. Process smaller chunks
2. Use more selective filters
3. Close other applications

### Results Look Wrong

1. Verify data is loaded: `snm analyze stats`
2. Check your query syntax
3. Remember that string operations are case-sensitive by default

---

## Getting Help

- **Menu help**: Type `h` in the Interactive Explorer
- **Command help**: `snm --help` or `snm ingest --help`
- **Polars documentation**: https://pola.rs/

---

## License

MIT License - See LICENSE file for details.

## Data Source

Sanctions data provided by [OpenSanctions](https://opensanctions.org/) under CC BY 4.0 license.

## Disclaimer

This tool is for research and educational purposes. Users are responsible for ensuring compliance with applicable laws and regulations. This is not legal advice.
