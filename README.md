# Sanctions Evasion Network Mapper

A comprehensive intelligence analysis tool that cross-references global sanctions lists with corporate registry data to identify potential shell company networks used to evade financial restrictions.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Cost Analysis](#cost-analysis)
3. [Architecture](#architecture)
4. [Installation](#installation)
5. [Code Walkthrough](#code-walkthrough)
   - [Data Ingestion](#data-ingestion)
   - [Entity Resolution](#entity-resolution)
   - [Network Analysis](#network-analysis)
   - [Orchestration](#orchestration)
   - [Reporting](#reporting)
6. [Usage](#usage)
7. [Intelligence Tradecraft Notes](#intelligence-tradecraft-notes)

---

## Project Overview

### What This Tool Does

This project automates the investigative process that financial intelligence analysts perform manually:

1. **Ingests** sanctions data from OpenSanctions (aggregating OFAC, EU, UN, and 100+ other lists)
2. **Cross-references** sanctioned entities against corporate registries worldwide
3. **Resolves entities** using fuzzy matching to catch name variations and transliterations
4. **Builds networks** showing ownership chains and relationships
5. **Identifies risks** by calculating proximity to sanctioned entities
6. **Generates reports** highlighting potential evasion structures

### Why This Matters for IC/Defense Careers

This project demonstrates competencies directly relevant to:

- **Treasury/OFAC**: Sanctions enforcement and evasion detection
- **FinCEN**: Financial crimes investigation
- **FBI**: Counter-intelligence financial analysis
- **DIA/CIA**: Following the money in adversary networks
- **NGA**: Pattern-of-life analysis methodology (applied to corporate entities)

---

## Cost Analysis

### Free Tier (Recommended Starting Point)

| Resource | Cost | Notes |
|----------|------|-------|
| **OpenSanctions Data** | $0 | Bulk downloads freely available |
| **UK Companies House API** | $0 | Free registration, generous limits |
| **Python/Libraries** | $0 | All open source |
| **PostgreSQL** | $0 | Local or free cloud tier (Supabase, Neon) |
| **Neo4j Community** | $0 | Local installation |
| **GitHub** | $0 | Public repos free |

**Total Free Tier: $0/month**

### Enhanced Tier (Production Quality)

| Resource | Cost | Notes |
|----------|------|-------|
| **OpenCorporates API** | $0-200/mo | Free: 500 req/mo; Paid: bulk access |
| **Cloud Compute** | $5-20/mo | DigitalOcean droplet or AWS t3.small |
| **Managed PostgreSQL** | $15-25/mo | DigitalOcean, Railway, or Supabase Pro |
| **Neo4j AuraDB** | $0-65/mo | Free tier available; Pro for large graphs |
| **Dagster Cloud** | $0-100/mo | Free for small workloads |

**Total Enhanced Tier: $20-410/month** depending on scale

### One-Time Costs

| Resource | Cost | Notes |
|----------|------|-------|
| **Domain (optional)** | $12/year | For hosting reports/dashboards |
| **Gephi (visualization)** | $0 | Open source |

### Data Source Pricing Details

#### OpenSanctions
- **Bulk Data**: Free (CC BY 4.0 license for non-commercial)
- **Commercial License**: Contact for pricing
- **API Access**: Included with bulk download

#### OpenCorporates
- **Free Tier**: 500 API calls/month
- **Starter**: $100/month (10,000 calls)
- **Professional**: $200/month (50,000 calls)
- **Bulk Data**: Custom pricing (recommended for this project)

#### UK Companies House
- **Streaming API**: Free, unlimited
- **Bulk Data Products**: Free (Public Data)
- **Rate Limits**: 600 requests/5 minutes

#### Alternative Free Sources
- **SEC EDGAR**: Free (US public companies)
- **EU Business Registers**: Varies by country (many free)
- **OpenOwnership**: Free (beneficial ownership data)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES                                   │
├─────────────────┬─────────────────┬─────────────────┬───────────────────┤
│  OpenSanctions  │ OpenCorporates  │ UK Companies    │ Vessel Registries │
│  (Sanctions)    │ (Global Corps)  │ House (UK)      │ (Ships/Aircraft)  │
└────────┬────────┴────────┬────────┴────────┬────────┴─────────┬─────────┘
         │                 │                 │                  │
         ▼                 ▼                 ▼                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         INGESTION LAYER                                  │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                    src/ingest/                                   │    │
│  │  • opensanctions.py  - Download & parse FtM JSON format         │    │
│  │  • opencorporates.py - Query corporate registry APIs            │    │
│  │  • vessel_registries.py - Maritime/aviation data (future)       │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      RAW DATA STORAGE                                    │
│                   ./data/raw/ (Parquet files)                           │
│  • sanctions_entities.parquet                                           │
│  • sanctions_relationships.parquet                                      │
│  • corporate_entities.parquet                                           │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      TRANSFORMATION LAYER                                │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                src/transform/                                    │    │
│  │                                                                  │    │
│  │  entity_resolution.py:                                          │    │
│  │  • normalize_name() - Standardize for matching                  │    │
│  │  • cyrillic_to_latin() - Handle Russian names                   │    │
│  │  • match_by_identifier() - Exact ID matching                    │    │
│  │  • fuzzy_name_match() - Probabilistic matching                  │    │
│  │                                                                  │    │
│  │  network_builder.py:                                            │    │
│  │  • Build NetworkX graph from entities + relationships           │    │
│  │  • Add edges from entity resolution matches                     │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       ANALYSIS LAYER                                     │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │              src/analyze/ + src/transform/                       │    │
│  │                                                                  │    │
│  │  Network Metrics:                                               │    │
│  │  • identify_hub_entities() - High-connectivity nodes            │    │
│  │  • identify_bridge_entities() - Betweenness centrality          │    │
│  │  • detect_communities() - Cluster detection                     │    │
│  │  • calculate_sanctions_exposure() - Risk scoring                │    │
│  │                                                                  │    │
│  │  Path Analysis:                                                 │    │
│  │  • find_sanctioned_corporate_paths() - Evasion routes           │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        OUTPUT LAYER                                      │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────┐   │
│  │   PostgreSQL     │  │     Neo4j        │  │   Visualization      │   │
│  │   (Tabular)      │  │   (Graph DB)     │  │   (GEXF/Gephi)       │   │
│  └──────────────────┘  └──────────────────┘  └──────────────────────┘   │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │                    src/export/reports.py                         │    │
│  │  • Markdown risk reports                                        │    │
│  │  • Power BI data exports                                        │    │
│  └─────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/sanctions-network-mapper.git
cd sanctions-network-mapper

# Install uv (fast Python package manager)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install dependencies
uv sync

# Or with pip
pip install -e .
```

### Required System Dependencies

```bash
# Ubuntu/Debian
sudo apt-get install -y postgresql neo4j

# macOS
brew install postgresql neo4j
```

---

## Code Walkthrough

This section explains every function, why it exists, and how it works.

---

### Data Ingestion

#### `src/ingest/opensanctions.py`

This module downloads and parses sanctions data from OpenSanctions, which aggregates 100+ global sanctions lists into a unified format.

---

##### Class: `OpenSanctionsClient`

```python
class OpenSanctionsClient:
    """Client for downloading and parsing OpenSanctions data."""
    
    BASE_URL = "https://data.opensanctions.org/datasets/latest"
```

**Why**: We need a centralized client to handle all interactions with OpenSanctions. The `BASE_URL` points to their latest data exports, ensuring we always get current sanctions data.

**How**: Class-based design allows us to maintain state (cache directory, HTTP client) across multiple operations.

---

```python
    DATASETS = {
        "default": "default/entities.ftm.json",
        "sanctions": "sanctions/entities.ftm.json",
        "peps": "peps/entities.ftm.json",
    }
```

**Why**: OpenSanctions provides multiple datasets:
- `default`: Everything (sanctions + PEPs + wanted persons)
- `sanctions`: Just sanctioned entities (our primary focus)
- `peps`: Politically Exposed Persons (useful for enhanced due diligence)

**How**: Dictionary lookup makes it easy to switch between datasets without hardcoding URLs.

---

```python
    def __init__(self, cache_dir: Path = Path("./data/raw/opensanctions")):
        self.cache_dir = cache_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.client = httpx.Client(timeout=120.0)
```

**Why**: 
- `cache_dir`: We cache downloads to avoid re-fetching large files (the sanctions dataset is ~500MB)
- `mkdir(parents=True, exist_ok=True)`: Creates the full directory path if it doesn't exist, doesn't error if it does
- `timeout=120.0`: Long timeout because the download can take minutes on slow connections

**How**: `httpx.Client` is a modern, async-capable HTTP client. We use the sync version for simplicity but could easily switch to async for parallel downloads.

---

```python
    def download_dataset(self, dataset: str = "default") -> Path:
        """Download a dataset and return the local path."""
        if dataset not in self.DATASETS:
            raise ValueError(f"Unknown dataset: {dataset}")
        
        url = f"{self.BASE_URL}/{self.DATASETS[dataset]}"
        local_path = self.cache_dir / f"{dataset}_{datetime.now():%Y%m%d}.json"
```

**Why**: 
- Input validation prevents cryptic errors from bad URLs
- Date-stamped filenames (`sanctions_20250109.json`) let us keep historical versions for comparison

**How**: F-string formatting constructs the URL and filename. `%Y%m%d` format gives us `YYYYMMDD`.

---

```python
        with self.client.stream("GET", url) as response:
            response.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in response.iter_bytes(chunk_size=8192):
                    f.write(chunk)
        
        return local_path
```

**Why**: 
- **Streaming download**: The file is ~500MB. Loading it all into memory would crash on low-RAM systems.
- `raise_for_status()`: Immediately errors on HTTP failures (404, 500, etc.)
- `chunk_size=8192`: 8KB chunks balance memory usage vs. I/O overhead

**How**: The `stream()` context manager keeps the connection open while we iterate through chunks. Each chunk is written immediately to disk, keeping memory usage constant regardless of file size.

---

```python
    def parse_entities(self, filepath: Path) -> pl.DataFrame:
        """
        Parse FtM (Follow the Money) JSON format into a DataFrame.
        
        FtM schema uses entity types like:
        - Person
        - Company  
        - Organization
        - LegalEntity
        - Vessel
        - Aircraft
        """
```

**Why**: OpenSanctions uses the "Follow the Money" (FtM) data model, an open standard for anti-corruption investigations. Understanding this schema is essential for correctly parsing the data.

**How**: The docstring documents the schema types we'll encounter, serving as reference for future developers.

---

```python
        entities = []
        
        with open(filepath, "r") as f:
            for line in f:
                entity = json.loads(line)
```

**Why**: The file is newline-delimited JSON (NDJSON), not a single JSON array. Each line is a complete entity. This format allows:
- Streaming parse (one entity at a time)
- Easy parallel processing
- Append-only updates

**How**: We read line-by-line and parse each independently, building a list of records.

---

```python
                record = {
                    "entity_id": entity.get("id"),
                    "schema": entity.get("schema"),
                    "caption": entity.get("caption"),
                    "datasets": ",".join(entity.get("datasets", [])),
                    "first_seen": entity.get("first_seen"),
                    "last_seen": entity.get("last_seen"),
                }
```

**Why**: We extract core metadata that exists on every entity type:
- `entity_id`: Unique identifier (format: `ofac-1234` or similar)
- `schema`: Entity type (Person, Company, etc.) - critical for analysis
- `caption`: Human-readable name
- `datasets`: Which sanctions lists include this entity (comma-joined for storage)
- `first_seen`/`last_seen`: When they appeared on/disappeared from lists

**How**: `.get()` with no default returns `None` for missing keys, avoiding KeyError exceptions.

---

```python
                props = entity.get("properties", {})
                
                record["names"] = "|".join(props.get("name", []))
                record["aliases"] = "|".join(props.get("alias", []))
                record["countries"] = "|".join(props.get("country", []))
                record["addresses"] = "|".join(props.get("address", []))
```

**Why**: FtM stores multi-valued properties as arrays. A sanctioned person might have:
- Multiple names (legal name, aliases, transliterations)
- Multiple nationalities
- Multiple known addresses

We join with `|` (pipe) because it rarely appears in names, unlike commas.

**How**: The `props.get("name", [])` pattern returns an empty list if the key is missing, so `"|".join()` produces an empty string rather than erroring.

---

```python
                record["birth_date"] = props.get("birthDate", [None])[0]
                record["nationality"] = "|".join(props.get("nationality", []))
```

**Why**: Person-specific fields. Birth date is critical for disambiguation (many people share names). We take only the first value `[0]` because there should only be one birth date.

**How**: `[None]` as default means if `birthDate` is missing, we get `[None][0] = None` rather than an IndexError.

---

```python
                record["incorporation_date"] = props.get("incorporationDate", [None])[0]
                record["jurisdiction"] = props.get("jurisdiction", [None])[0]
                record["registration_number"] = props.get("registrationNumber", [None])[0]
```

**Why**: Company-specific fields. These are gold for exact matching:
- `jurisdiction`: Where the company is registered (country/state code)
- `registration_number`: Official company number (unique within jurisdiction)

**How**: Same pattern as above - extract single values from potential arrays.

---

```python
                record["inn_code"] = props.get("innCode", [None])[0]
                record["ogrn_code"] = props.get("ogrnCode", [None])[0]
                record["lei_code"] = props.get("leiCode", [None])[0]
```

**Why**: Critical identifiers for Russian entities:
- `INN`: Russian taxpayer identification number (10-12 digits)
- `OGRN`: Russian registration number (13-15 digits)
- `LEI`: Legal Entity Identifier (global standard, 20 characters)

These enable high-confidence exact matching. If two records share an INN, they're definitively the same entity.

**How**: These fields are especially important post-2022 given the extensive Russia sanctions.

---

```python
        return pl.DataFrame(entities)
```

**Why**: Polars DataFrames are:
- 10-100x faster than pandas for large datasets
- Memory-efficient (columnar storage)
- Type-safe with better null handling

**How**: The list of dictionaries is directly converted to a DataFrame, with Polars inferring column types.

---

```python
    def extract_relationships(self, filepath: Path) -> pl.DataFrame:
        """
        Extract relationships between entities.
        
        FtM relationships include:
        - ownership (Company -> Company/Person)
        - directorship (Person -> Company)
        - family (Person -> Person)
        - associate (Person -> Person)
        """
```

**Why**: Relationships are the heart of network analysis. Sanctions evaders hide behind:
- Shell company ownership chains
- Nominee directors
- Family members not yet sanctioned

**How**: FtM encodes relationships as properties on entities, not as separate edge records.

---

```python
        relationships = []
        
        with open(filepath, "r") as f:
            for line in f:
                entity = json.loads(line)
                props = entity.get("properties", {})
                
                source_id = entity.get("id")
                
                for owner in props.get("ownershipOwner", []):
                    relationships.append({
                        "source_id": source_id,
                        "target_id": owner,
                        "relationship_type": "owned_by",
                    })
```

**Why**: `ownershipOwner` indicates who owns this entity. We extract directed edges: `source_id` (the company) → `target_id` (the owner). This lets us traverse ownership chains in either direction.

**How**: Each relationship property can have multiple values (a company might have several owners), so we iterate and create an edge for each.

---

```python
                for director in props.get("directorshipDirector", []):
                    relationships.append({
                        "source_id": source_id,
                        "target_id": director,
                        "relationship_type": "directed_by",
                    })
                
                for relative in props.get("relative", []):
                    relationships.append({
                        "source_id": source_id,
                        "target_id": relative,
                        "relationship_type": "related_to",
                    })
        
        return pl.DataFrame(relationships)
```

**Why**: 
- Directors often serve as nominees for sanctioned individuals
- Family relationships reveal potential sanctions evasion through relatives

**How**: Same pattern - extract edges with typed relationships for later analysis.

---

#### `src/ingest/opencorporates.py`

This module interfaces with corporate registry APIs to get company information for cross-referencing.

---

##### Class: `Company` (Pydantic Model)

```python
class Company(BaseModel):
    """Standardized company record."""
    company_number: str
    name: str
    jurisdiction_code: str
    incorporation_date: str | None
    company_type: str | None
    current_status: str | None
    registered_address: str | None
    officers: list[dict] = []
```

**Why**: Pydantic models provide:
- Type validation (catches data errors early)
- Serialization/deserialization
- Documentation via type hints
- Default values for optional fields

**How**: This is our canonical company representation. All data sources (OpenCorporates, UK Companies House, etc.) get normalized to this format.

---

##### Class: `OpenCorporatesClient`

```python
    def search_companies(
        self, 
        query: str,
        jurisdiction_code: str | None = None,
        limit: int = 100
    ) -> Generator[Company, None, None]:
```

**Why**: Generator function because:
- Results can be large (thousands of companies)
- We may want to stop early
- Memory-efficient iteration

**How**: `Generator[Company, None, None]` type hint indicates this yields `Company` objects, takes no send values, and has no return value.

---

```python
        params = {
            "q": query,
            "per_page": min(limit, 100),
        }
        if jurisdiction_code:
            params["jurisdiction_code"] = jurisdiction_code
        if self.api_key:
            params["api_token"] = self.api_key
```

**Why**: 
- `per_page` is capped at 100 by the API
- Optional `jurisdiction_code` lets us narrow searches (e.g., only BVI companies)
- API key increases rate limits

**How**: Building params dict dynamically avoids sending null values.

---

```python
        sleep(1.0)
```

**Why**: Rate limiting. Free tier allows very limited requests. Even paid tiers have limits. Being a good API citizen prevents bans.

**How**: Simple sleep between requests. Production code would use exponential backoff.

---

##### Class: `UKCompaniesHouseClient`

```python
class UKCompaniesHouseClient:
    """
    Direct access to UK Companies House API.
    Free, requires registration for API key.
    https://developer.company-information.service.gov.uk/
    """
    
    BASE_URL = "https://api.company-information.service.gov.uk"
    
    def __init__(self, api_key: str):
        self.client = httpx.Client(
            timeout=30.0,
            auth=(api_key, "")
        )
```

**Why**: UK Companies House is the gold standard for free corporate data:
- Completely free API
- Full beneficial ownership data (PSC register)
- High-quality, official data

**How**: The API uses HTTP Basic Auth with the API key as username and empty password. `httpx` handles this natively with the `auth` tuple.

---

```python
    def get_persons_significant_control(self, company_number: str) -> list[dict]:
        """
        Get PSC (Persons with Significant Control) - beneficial owners.
        This is the GOLD for identifying hidden ownership.
        """
```

**Why**: This is the most valuable endpoint. PSC data reveals:
- Who actually controls the company (not just registered officers)
- Ownership percentages
- Nature of control (shares, voting rights, significant influence)

UK law requires companies to report PSC, making this data uniquely comprehensive.

**How**: Simple GET request returning structured ownership data.

---

### Entity Resolution

#### `src/transform/entity_resolution.py`

This is where the intelligence analysis happens - matching entities across different data sources despite variations in names, transliterations, and deliberate obfuscation.

---

##### Class: `EntityResolver`

```python
class EntityResolver:
    """
    Match entities across sanctions lists and corporate registries.
    
    Matching strategies:
    1. Exact match on identifiers (registration numbers, tax IDs)
    2. Fuzzy name matching with transliteration handling
    3. Address matching
    4. Network-based matching (shared officers, addresses)
    """
    
    def __init__(
        self,
        name_threshold: int = 85,
        address_threshold: int = 80,
    ):
        self.name_threshold = name_threshold
        self.address_threshold = address_threshold
```

**Why**: Configurable thresholds because:
- Higher threshold (90+) = fewer false positives, may miss variants
- Lower threshold (80) = catches more variants, more manual review needed
- Different thresholds for names vs. addresses (addresses have more standard formats)

**How**: Instance variables let us tune behavior without code changes.

---

##### Method: `normalize_name()`

```python
    def normalize_name(self, name: str) -> str:
        """
        Normalize company/person names for matching.
        
        Handles:
        - Case normalization
        - Punctuation removal
        - Common abbreviations (LLC, Ltd, GmbH, etc.)
        - Transliteration artifacts
        """
        if not name:
            return ""
        
        name = name.upper()
```

**Why**: Uppercase normalization because:
- "ACME Corp" and "Acme Corp" should match
- Simplifies all subsequent comparisons
- Uppercase is more consistent across encodings

**How**: Python's `.upper()` handles Unicode correctly.

---

```python
        name = re.sub(r'[^\w\s]', ' ', name)
```

**Why**: Remove punctuation because:
- "Acme, Inc." and "Acme Inc" should match
- Punctuation varies by jurisdiction and data entry
- `\w` keeps word characters (letters, numbers, underscore), `\s` keeps whitespace

**How**: Regex substitution replaces all non-word, non-space characters with spaces.

---

```python
        suffixes = [
            (r'\bLLC\b', ''),
            (r'\bLTD\b', ''),
            (r'\bLIMITED\b', ''),
            (r'\bINC\b', ''),
            (r'\bCORP\b', ''),
            (r'\bCORPORATION\b', ''),
            (r'\bGMBH\b', ''),
            (r'\bAG\b', ''),
            (r'\bSA\b', ''),
            (r'\bOOO\b', ''),  # Russian LLC equivalent
            (r'\bZAO\b', ''),  # Russian closed JSC
            (r'\bOAO\b', ''),  # Russian open JSC
            (r'\bPAO\b', ''),  # Russian public JSC
        ]
        
        for pattern, replacement in suffixes:
            name = re.sub(pattern, replacement, name)
```

**Why**: Corporate suffixes are:
- Inconsistently recorded ("Ltd" vs "Limited" vs "Ltd.")
- Not meaningful for matching (many companies share name + different suffix)
- Jurisdiction-specific (GmbH is German, OOO is Russian)

The Russian suffixes are especially important given current sanctions focus.

**How**: `\b` word boundaries ensure we don't remove "LIMITED" from "UNLIMITED CORP". Each suffix is removed entirely (replaced with empty string).

---

```python
        name = ' '.join(name.split())
        
        return name.strip()
```

**Why**: After all transformations, we may have multiple spaces. This:
- Splits on any whitespace (including multiple spaces)
- Rejoins with single spaces
- Strips leading/trailing whitespace

**How**: `split()` with no argument splits on all whitespace and removes empty strings.

---

##### Method: `cyrillic_to_latin()`

```python
    def cyrillic_to_latin(self, text: str) -> str:
        """
        Transliterate Cyrillic to Latin.
        Essential for Russian sanctions matching.
        """
        cyrillic_to_latin_map = {
            'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd',
            'е': 'e', 'ё': 'yo', 'ж': 'zh', 'з': 'z', 'и': 'i',
            'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n',
            'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't',
            'у': 'u', 'ф': 'f', 'х': 'kh', 'ц': 'ts', 'ч': 'ch',
            'ш': 'sh', 'щ': 'shch', 'ъ': '', 'ы': 'y', 'ь': '',
            'э': 'e', 'ю': 'yu', 'я': 'ya',
        }
```

**Why**: Russian names appear in both Cyrillic and Latin transliterations:
- "Газпром" = "Gazprom"
- Different sources use different transliteration standards
- OFAC may list one, a corporate registry has the other

This is **critical** for Russia sanctions matching.

**How**: Character-by-character mapping. Multi-character outputs (ж→zh, щ→shch) are correct per standard transliteration.

---

```python
        result = []
        for char in text.lower():
            if char in cyrillic_to_latin_map:
                result.append(cyrillic_to_latin_map[char])
            else:
                result.append(char)
        
        return ''.join(result)
```

**Why**: We keep non-Cyrillic characters unchanged, so mixed text ("Газпром Energy") becomes "gazprom energy".

**How**: List-based string building is more efficient than string concatenation in Python.

---

##### Method: `match_by_identifier()`

```python
    def match_by_identifier(
        self,
        sanctions_df: pl.DataFrame,
        corporate_df: pl.DataFrame,
        id_column: str,
    ) -> pl.DataFrame:
        """
        Exact match on identifiers.
        
        High confidence matches:
        - Registration numbers
        - Tax IDs (INN for Russia)
        - LEI codes
        """
```

**Why**: Identifier matching is:
- 100% confidence (if IDs match, it's the same entity)
- Fast (simple join operation)
- Should always be tried first

**How**: We parameterize the ID column so this works for any identifier type.

---

```python
        sanctions_with_id = sanctions_df.filter(
            pl.col(id_column).is_not_null()
        )
        corporate_with_id = corporate_df.filter(
            pl.col(id_column).is_not_null()
        )
        
        matches = sanctions_with_id.join(
            corporate_with_id,
            on=id_column,
            suffix="_corp"
        )
```

**Why**: 
- Filter to non-null IDs (can't match on missing data)
- Inner join finds only matching records
- `suffix="_corp"` prevents column name collisions

**How**: Polars joins are optimized and parallel. This is much faster than iterating.

---

```python
        matches = matches.with_columns([
            pl.lit("identifier").alias("match_type"),
            pl.lit(100).alias("confidence_score"),
            pl.col(id_column).alias("match_key"),
        ])
        
        return matches
```

**Why**: We add metadata to every match:
- `match_type`: How we matched (for audit trail)
- `confidence_score`: 100 for exact ID matches
- `match_key`: What value matched (for investigation)

**How**: `pl.lit()` creates a literal column (same value for every row).

---

##### Method: `fuzzy_name_match()`

```python
    def fuzzy_name_match(
        self,
        sanctions_df: pl.DataFrame,
        corporate_df: pl.DataFrame,
        sanctions_name_col: str = "names",
        corporate_name_col: str = "name",
    ) -> pl.DataFrame:
        """
        Fuzzy match on entity names.
        
        Uses token set ratio which handles:
        - Word reordering ("John Smith" vs "Smith, John")
        - Partial matches ("Acme Corp" vs "Acme Corporation Ltd")
        """
```

**Why**: Fuzzy matching catches:
- Typos and OCR errors
- Name variations
- Partial matches
- Different word ordering

**How**: We use RapidFuzz, a fast fuzzy string matching library.

---

```python
        corporate_names = corporate_df.select([
            corporate_name_col,
            "company_number",
            "jurisdiction_code"
        ]).to_dicts()
        
        name_lookup = {
            self.normalize_name(r[corporate_name_col]): r 
            for r in corporate_names
            if r[corporate_name_col]
        }
        normalized_names = list(name_lookup.keys())
```

**Why**: We build a lookup table because:
- Fuzzy matching returns the matched string, not the record
- We need to retrieve full company details for matches
- Pre-normalizing names is faster than normalizing during each comparison

**How**: Dictionary comprehension creates {normalized_name: original_record} mapping.

---

```python
        for row in sanctions_df.iter_rows(named=True):
            all_names = []
            if row.get("names"):
                all_names.extend(row["names"].split("|"))
            if row.get("aliases"):
                all_names.extend(row["aliases"].split("|"))
            
            for name in all_names:
                normalized = self.normalize_name(name)
                if not normalized:
                    continue
                
                transliterated = self.normalize_name(
                    self.cyrillic_to_latin(name)
                )
```

**Why**: For each sanctioned entity, we try matching:
- All known names
- All aliases
- Both original and transliterated versions

This maximizes our chances of finding matches.

**How**: We split the pipe-delimited names back into lists.

---

```python
                for query_name in [normalized, transliterated]:
                    results = process.extract(
                        query_name,
                        normalized_names,
                        scorer=fuzz.token_set_ratio,
                        limit=5,
                        score_cutoff=self.name_threshold,
                    )
```

**Why**: 
- `token_set_ratio`: Best scorer for names because it ignores word order and handles subsets
- `limit=5`: Get top 5 matches (may be multiple similar companies)
- `score_cutoff`: Only return matches above threshold

**How**: `process.extract()` efficiently compares one string against many candidates.

---

```python
                    for match_name, score, _ in results:
                        corp_record = name_lookup[match_name]
                        matches.append({
                            "sanctions_entity_id": row["entity_id"],
                            "sanctions_name": name,
                            "corporate_name": corp_record[corporate_name_col],
                            "company_number": corp_record["company_number"],
                            "jurisdiction_code": corp_record["jurisdiction_code"],
                            "match_type": "fuzzy_name",
                            "confidence_score": score,
                            "match_key": f"{query_name} -> {match_name}",
                        })
        
        return pl.DataFrame(matches)
```

**Why**: For each match, we record:
- Both entity IDs for joining
- Original names for human review
- Match type and confidence for filtering
- Match key showing exactly what matched

**How**: Building match records enables downstream filtering and analysis.

---

##### Function: `identify_high_risk_matches()`

```python
def identify_high_risk_matches(
    matches_df: pl.DataFrame,
    threshold: int = 90
) -> pl.DataFrame:
    """
    Filter to high-confidence matches requiring investigation.
    
    Risk factors:
    - High fuzzy match score
    - Multiple matching signals (name + address)
    - Jurisdiction known for opacity (BVI, Seychelles, etc.)
    """
    high_risk_jurisdictions = [
        "vg",  # British Virgin Islands
        "ky",  # Cayman Islands
        "sc",  # Seychelles
        "pa",  # Panama
        "bz",  # Belize
        "ws",  # Samoa
        "mh",  # Marshall Islands
    ]
```

**Why**: These jurisdictions are:
- Known for corporate secrecy
- Commonly used for shell companies
- Higher prior probability of sanctions evasion

Even a lower-confidence match in BVI warrants investigation.

**How**: ISO 3166-1 alpha-2 country codes for consistent matching.

---

```python
    return matches_df.filter(
        (pl.col("confidence_score") >= threshold) |
        (pl.col("jurisdiction_code").is_in(high_risk_jurisdictions))
    ).sort("confidence_score", descending=True)
```

**Why**: We flag matches that are EITHER:
- High confidence (90%+ match score)
- In a secrecy jurisdiction (any confidence)

This balances precision and recall.

**How**: Polars filter with OR condition, sorted by confidence for priority review.

---

### Network Analysis

#### `src/transform/network_builder.py`

This module builds and analyzes the corporate ownership graph using NetworkX.

---

##### Class: `CorporateNetworkBuilder`

```python
class CorporateNetworkBuilder:
    """
    Build and analyze corporate ownership networks.
    
    Key patterns to detect:
    - Hub nodes (entities connected to many others)
    - Bridge nodes (connecting otherwise separate clusters)
    - Suspicious paths (sanctioned -> clean company chains)
    """
    
    def __init__(self):
        self.graph = nx.DiGraph()
```

**Why**: 
- `DiGraph`: Directed graph because ownership has direction (A owns B ≠ B owns A)
- Single graph instance maintains state across operations

**How**: NetworkX provides the graph data structure and algorithms.

---

##### Method: `add_entities()`

```python
    def add_entities(self, entities_df: pl.DataFrame) -> None:
        """Add entity nodes to the graph."""
        for row in entities_df.iter_rows(named=True):
            self.graph.add_node(
                row["entity_id"],
                name=row.get("caption") or row.get("name"),
                node_type=row.get("schema", "unknown"),
                jurisdiction=row.get("jurisdiction"),
                is_sanctioned=True,
                datasets=row.get("datasets", ""),
            )
```

**Why**: Each node stores attributes:
- `name`: For display and investigation
- `node_type`: Person vs Company (different analysis)
- `jurisdiction`: For geographic analysis
- `is_sanctioned`: Critical flag for risk analysis
- `datasets`: Which lists include them (OFAC, EU, etc.)

**How**: NetworkX nodes can have arbitrary attributes stored as kwargs.

---

##### Method: `add_matches()`

```python
    def add_matches(self, matches_df: pl.DataFrame) -> None:
        """
        Add edges from entity resolution matches.
        These connect sanctioned entities to potential corporate matches.
        """
        for row in matches_df.iter_rows(named=True):
            corporate_id = f"{row['jurisdiction_code']}_{row['company_number']}"
            
            self.graph.add_edge(
                row["sanctions_entity_id"],
                corporate_id,
                relationship_type="potential_match",
                confidence=row["confidence_score"],
                match_type=row["match_type"],
            )
```

**Why**: Match edges are different from ownership edges:
- They represent "may be the same entity"
- Confidence score indicates match quality
- Enables finding companies linked to sanctioned entities

**How**: Edge attributes store match metadata for filtering.

---

##### Method: `find_sanctioned_corporate_paths()`

```python
    def find_sanctioned_corporate_paths(
        self,
        max_depth: int = 4
    ) -> Iterator[list[str]]:
        """
        Find paths from sanctioned entities to non-sanctioned companies.
        
        These represent potential sanctions evasion routes:
        Sanctioned Person -> Shell Co 1 -> Shell Co 2 -> Clean Company
        """
        sanctioned_nodes = [
            n for n, d in self.graph.nodes(data=True)
            if d.get("is_sanctioned", False)
        ]
        
        clean_companies = [
            n for n, d in self.graph.nodes(data=True)
            if d.get("node_type") == "company" and not d.get("is_sanctioned", False)
        ]
```

**Why**: Path analysis reveals:
- How sanctioned entities might access the financial system
- The "degrees of separation" used to obscure ownership
- Intermediary entities that facilitate evasion

**How**: We identify start nodes (sanctioned) and end nodes (clean companies).

---

```python
        for sanctioned in sanctioned_nodes:
            for company in clean_companies:
                try:
                    paths = nx.all_simple_paths(
                        self.graph,
                        sanctioned,
                        company,
                        cutoff=max_depth
                    )
                    for path in paths:
                        yield path
                except nx.NetworkXNoPath:
                    continue
```

**Why**: 
- `max_depth=4`: Typical evasion structures use 2-4 shell company layers
- `all_simple_paths`: Finds all paths (not just shortest)
- Generator yields paths as found (memory efficient)

**How**: NetworkX path algorithms handle the graph traversal. We catch NoPath exceptions for unconnected pairs.

---

##### Method: `identify_hub_entities()`

```python
    def identify_hub_entities(self, top_n: int = 20) -> list[tuple[str, dict]]:
        """
        Find entities with unusually high connectivity.
        
        High degree nodes are often:
        - Formation agents (legitimate but worth noting)
        - Shell company factories (suspicious)
        - Key intermediaries in evasion networks
        """
        degree_centrality = nx.degree_centrality(self.graph)
        
        sorted_nodes = sorted(
            degree_centrality.items(),
            key=lambda x: x[1],
            reverse=True
        )[:top_n]
```

**Why**: Degree centrality measures how connected each node is. High-degree nodes are interesting because:
- They might be formation agents (create many companies)
- They might be nexus points in evasion networks
- Even legitimate, they warrant attention

**How**: `nx.degree_centrality()` normalizes by (n-1) so values are comparable across graphs.

---

```python
        return [
            (node_id, {
                **self.graph.nodes[node_id],
                "degree_centrality": score,
                "in_degree": self.graph.in_degree(node_id),
                "out_degree": self.graph.out_degree(node_id),
            })
            for node_id, score in sorted_nodes
        ]
```

**Why**: We return both the centrality score and directional degrees:
- `in_degree`: How many entities point to this one (e.g., companies owned by this person)
- `out_degree`: How many entities this one points to (e.g., companies this entity owns)

**How**: Dictionary unpacking (`**`) merges node attributes with computed metrics.

---

##### Method: `identify_bridge_entities()`

```python
    def identify_bridge_entities(self, top_n: int = 20) -> list[tuple[str, float]]:
        """
        Find entities that bridge otherwise disconnected clusters.
        
        High betweenness centrality indicates an entity that:
        - Connects separate networks
        - Is critical for information/value flow
        - May be deliberately positioned as an intermediary
        """
        betweenness = nx.betweenness_centrality(self.graph)
        
        return sorted(
            betweenness.items(),
            key=lambda x: x[1],
            reverse=True
        )[:top_n]
```

**Why**: Betweenness centrality measures how often a node appears on shortest paths between other nodes. High betweenness indicates:
- Bridge between otherwise separate groups
- Critical chokepoint for value/information flow
- Potentially deliberate intermediary role

This is often more interesting than degree centrality for finding hidden facilitators.

**How**: More computationally expensive than degree centrality, but worth it.

---

##### Method: `detect_communities()`

```python
    def detect_communities(self) -> dict[str, int]:
        """
        Detect communities/clusters in the network.
        
        Useful for identifying:
        - Related shell company groups
        - Networks controlled by the same beneficial owner
        """
        undirected = self.graph.to_undirected()
        
        communities = nx.community.louvain_communities(undirected)
        
        node_to_community = {}
        for community_id, community in enumerate(communities):
            for node in community:
                node_to_community[node] = community_id
        
        return node_to_community
```

**Why**: Community detection reveals:
- Clusters of related entities
- Possibly common ownership/control
- Organized structures designed to work together

**How**: Louvain algorithm is fast and produces high-quality communities. Requires undirected graph.

---

##### Method: `calculate_sanctions_exposure()`

```python
    def calculate_sanctions_exposure(self) -> dict[str, float]:
        """
        Calculate how "close" each non-sanctioned entity is to sanctioned ones.
        
        Higher score = more connections to sanctioned entities = higher risk
        """
        exposure_scores = {}
        
        sanctioned_nodes = {
            n for n, d in self.graph.nodes(data=True)
            if d.get("is_sanctioned", False)
        }
        
        for node in self.graph.nodes():
            if node in sanctioned_nodes:
                continue
            
            min_distance = float('inf')
            sanctioned_neighbors = 0
            
            for sanctioned in sanctioned_nodes:
                try:
                    dist1 = nx.shortest_path_length(self.graph, node, sanctioned)
                    dist2 = nx.shortest_path_length(self.graph, sanctioned, node)
                    distance = min(dist1, dist2)
                    min_distance = min(min_distance, distance)
                    
                    if distance == 1:
                        sanctioned_neighbors += 1
                except nx.NetworkXNoPath:
                    continue
```

**Why**: Sanctions exposure scoring:
- Measures risk for non-sanctioned entities
- Closer to sanctioned entities = higher risk
- Direct connections (distance 1) are especially concerning

**How**: We check both directions because the graph is directed but risk flows both ways.

---

```python
            if min_distance < float('inf'):
                exposure_scores[node] = {
                    "min_distance_to_sanctioned": min_distance,
                    "direct_sanctioned_connections": sanctioned_neighbors,
                    "risk_score": (1 / min_distance) * (1 + sanctioned_neighbors),
                }
        
        return exposure_scores
```

**Why**: Risk formula:
- `1 / min_distance`: Closer = higher (distance 1 → score 1, distance 2 → score 0.5)
- `(1 + sanctioned_neighbors)`: Multiplier for direct connections
- Combined: Entity 1 hop from 3 sanctioned entities scores higher than 2 hops from 1

**How**: Simple formula that captures key risk factors. Could be refined with ML.

---

##### Method: `export_to_neo4j()`

```python
    def export_to_neo4j(self, uri: str, user: str, password: str) -> None:
        """Export to Neo4j for advanced graph queries."""
        from neo4j import GraphDatabase
        
        driver = GraphDatabase.driver(uri, auth=(user, password))
        
        with driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
```

**Why**: Neo4j enables:
- Cypher query language for complex graph queries
- Visualization in Neo4j Browser
- Scale to millions of nodes
- Pattern matching for investigation

**How**: Official Python driver. We clear existing data before import.

---

```python
            for node_id, data in self.graph.nodes(data=True):
                labels = ["Entity"]
                if data.get("is_sanctioned"):
                    labels.append("Sanctioned")
                if data.get("node_type") == "company":
                    labels.append("Company")
                elif data.get("node_type") == "Person":
                    labels.append("Person")
                
                label_str = ":".join(labels)
                
                session.run(
                    f"CREATE (n:{label_str} $props)",
                    props={
                        "id": node_id,
                        "name": data.get("name", ""),
                        "jurisdiction": data.get("jurisdiction", ""),
                    }
                )
```

**Why**: Multiple labels enable flexible queries:
- `MATCH (n:Sanctioned)` - all sanctioned entities
- `MATCH (n:Company:Sanctioned)` - sanctioned companies only
- `MATCH (n:Person)-[:OWNS]->(c:Company)` - people who own companies

**How**: Dynamic label construction based on node attributes.

---

### Orchestration

#### `dagster/assets.py`

Dagster manages the data pipeline, ensuring reproducibility and observability.

---

```python
@asset(
    description="Raw sanctions data from OpenSanctions",
    group_name="ingestion",
)
def raw_sanctions_data(context: AssetExecutionContext) -> MaterializeResult:
    """Download and parse the latest sanctions data."""
```

**Why**: Dagster assets are:
- Declarative (describe what, not how)
- Observable (track lineage, metadata)
- Cacheable (don't recompute if inputs unchanged)
- Testable (isolated functions)

**How**: The `@asset` decorator registers this function as a materializable data asset.

---

```python
    client = OpenSanctionsClient()
    
    filepath = client.download_dataset("sanctions")
    entities_df = client.parse_entities(filepath)
    relationships_df = client.extract_relationships(filepath)
    
    output_dir = Path("./data/processed")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    entities_df.write_parquet(output_dir / "sanctions_entities.parquet")
    relationships_df.write_parquet(output_dir / "sanctions_relationships.parquet")
```

**Why**: 
- Parquet format: Columnar, compressed, fast to read
- Explicit output paths for reproducibility
- Intermediate storage enables debugging and reuse

**How**: Standard file I/O, using Polars' native Parquet support.

---

```python
    context.log.info(f"Ingested {len(entities_df)} entities")
    context.log.info(f"Ingested {len(relationships_df)} relationships")
    
    return MaterializeResult(
        metadata={
            "entity_count": len(entities_df),
            "relationship_count": len(relationships_df),
        }
    )
```

**Why**: 
- Logging for operational visibility
- Metadata attached to materialization for tracking over time
- Enables alerts if counts change dramatically

**How**: `MaterializeResult` is Dagster's way of returning structured output.

---

```python
@asset(
    deps=["raw_sanctions_data"],
    description="Entity resolution matches",
    group_name="transform",
)
def entity_matches(context: AssetExecutionContext) -> MaterializeResult:
```

**Why**: 
- `deps=["raw_sanctions_data"]`: Declares dependency, Dagster ensures correct ordering
- Groups organize assets in the UI
- Explicit dependencies enable incremental updates

**How**: Dagster builds a DAG from declared dependencies.

---

### Reporting

#### `src/export/reports.py`

Generate human-readable reports for analysts and decision-makers.

---

```python
def generate_risk_report(
    high_risk_matches: pl.DataFrame,
    network_stats: dict,
    output_path: Path,
) -> None:
    """Generate a markdown risk assessment report."""
    
    report = f"""# Sanctions Evasion Network Analysis Report
Generated: {datetime.now():%Y-%m-%d %H:%M}

## Executive Summary

This report identifies potential sanctions evasion risks based on 
cross-referencing global sanctions lists with corporate registry data.
```

**Why**: Markdown reports are:
- Human-readable
- Convertible to PDF/HTML
- Versionable in git
- Easy to generate programmatically

**How**: F-string templating with datetime formatting.

---

```python
## Network Statistics

- **Total Entities Analyzed**: {network_stats['node_count']:,}
- **Relationships Mapped**: {network_stats['edge_count']:,}
- **High-Risk Matches Identified**: {len(high_risk_matches):,}
```

**Why**: Executive summary stats give quick overview:
- Scale of analysis
- Key findings
- Basis for resource allocation

**How**: `:,` format specifier adds thousand separators.

---

```python
## High-Risk Matches

The following entities show potential connections to sanctioned parties:

| Sanctioned Entity | Matched Company | Jurisdiction | Confidence |
|-------------------|-----------------|--------------|------------|
"""
    
    for row in high_risk_matches.head(20).iter_rows(named=True):
        report += f"| {row['sanctions_name'][:30]} | {row['corporate_name'][:30]} | {row['jurisdiction_code']} | {row['confidence_score']}% |\n"
```

**Why**: Table format for:
- Easy scanning
- Sortable in rendered markdown
- Truncated names (30 chars) prevent table breakage

**How**: Markdown table syntax, iterating over top matches.

---

```python
## Recommendations

1. Review high-confidence matches (>90%) for immediate action
2. Investigate hub entities for potential intermediary roles
3. Monitor jurisdictions with high sanctions exposure
4. Implement ongoing screening for new entity registrations

---
*This report is for analytical purposes only and does not constitute legal advice.*
```

**Why**: Actionable recommendations:
- Prioritize analyst time
- Suggest next steps
- Appropriate disclaimers

**How**: Standard report structure familiar to analysts.

---

## Usage

### Basic Pipeline Run

```bash
# Download and process sanctions data
python -m src.ingest.opensanctions

# Run entity resolution
python -m src.transform.entity_resolution

# Build and analyze network
python -m src.transform.network_builder

# Generate report
python -m src.export.reports
```

### With Dagster

```bash
# Start Dagster UI
dagster dev

# Or run via CLI
dagster asset materialize --select raw_sanctions_data
dagster asset materialize --select entity_matches
dagster asset materialize --select network_graph
```

### Neo4j Queries

```cypher
-- Find all companies within 2 hops of sanctioned entities
MATCH path = (s:Sanctioned)-[*1..2]-(c:Company)
WHERE NOT c:Sanctioned
RETURN path

-- Find common intermediaries
MATCH (s1:Sanctioned)-[:OWNS]->(i:Company)<-[:OWNS]-(s2:Sanctioned)
WHERE s1 <> s2
RETURN i.name, count(*) as shared_by
ORDER BY shared_by DESC

-- High-risk jurisdiction companies connected to sanctions
MATCH (s:Sanctioned)-[*1..3]-(c:Company)
WHERE c.jurisdiction IN ['vg', 'ky', 'pa', 'sc']
RETURN c.name, c.jurisdiction, s.name
```

---

## Intelligence Tradecraft Notes

### Why This Approach Works

1. **Multi-source correlation**: Single-source intelligence is weak. Cross-referencing sanctions with corporate data provides corroboration.

2. **Entity resolution is key**: Adversaries use name variations deliberately. Robust matching catches what exact-string matching misses.

3. **Network analysis reveals structure**: Individual entities may look clean. The network reveals hidden connections.

4. **Automation enables scale**: Manual analysis can't keep up with thousands of entities. Automated pipelines provide continuous monitoring.

### Limitations

1. **False positives**: Fuzzy matching produces false matches. Human review is essential.

2. **Data lag**: Corporate registries update slowly. Newly created shell companies may not appear.

3. **Sophisticated evasion**: This catches common patterns. Highly sophisticated actors use more complex structures.

4. **Jurisdiction coverage**: Data availability varies. Some jurisdictions (Russia, China) have limited corporate registry access.

### Extending This Project

- Add vessel/aircraft registries for maritime/aviation sanctions
- Integrate with financial transaction data (if available)
- Add ML-based entity resolution for higher accuracy
- Build real-time alerting on new sanctions designations
- Expand to PEP (Politically Exposed Persons) screening

---

## License

MIT License - See LICENSE file for details.

## Contributing

Contributions welcome. Please read CONTRIBUTING.md first.

## Disclaimer

This tool is for educational and analytical purposes. Users are responsible for ensuring compliance with applicable laws and regulations. This is not legal advice.
