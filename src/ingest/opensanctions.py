"""
OpenSanctions Data Ingestion Module

Downloads and parses sanctions data from OpenSanctions, which aggregates
100+ global sanctions lists including OFAC, EU, UN, and many others.

OpenSanctions uses the Follow the Money (FtM) data model:
https://followthemoney.tech/

Data is provided as newline-delimited JSON (NDJSON) where each line
is a complete entity record.

Usage:
    from src.ingest.opensanctions import OpenSanctionsClient, ingest_opensanctions
    
    # Quick usage
    entities_df, relationships_df = ingest_opensanctions()
    
    # Or with more control
    client = OpenSanctionsClient()
    filepath = client.download_dataset("sanctions")
    entities_df = client.parse_entities(filepath)
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Generator

import httpx
import polars as pl
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import settings

logger = logging.getLogger(__name__)


class OpenSanctionsClient:
    """
    Client for downloading and parsing OpenSanctions data.
    
    OpenSanctions aggregates sanctions and PEP data from 100+ sources:
    - US OFAC (SDN List, Consolidated Sanctions)
    - EU Sanctions
    - UN Security Council Sanctions
    - UK HM Treasury
    - And many more...
    
    Data is freely available under CC BY 4.0 license for non-commercial use.
    
    Attributes:
        cache_dir: Directory for caching downloaded files
        client: HTTP client for making requests
    
    Example:
        >>> client = OpenSanctionsClient()
        >>> filepath = client.download_dataset("sanctions")
        >>> entities_df = client.parse_entities(filepath)
        >>> print(f"Loaded {len(entities_df)} sanctioned entities")
    """
    
    # Available datasets from OpenSanctions
    DATASETS = {
        "default": "default/entities.ftm.json",      # Everything combined
        "sanctions": "sanctions/entities.ftm.json",  # Just sanctions
        "peps": "peps/entities.ftm.json",            # Politically Exposed Persons
        "crime": "crime/entities.ftm.json",          # Wanted/criminal lists
    }
    
    def __init__(
        self,
        cache_dir: Path | None = None,
        base_url: str | None = None,
    ):
        """
        Initialize the OpenSanctions client.
        
        Args:
            cache_dir: Directory to cache downloaded files. Defaults to
                       data/raw/opensanctions from settings.
            base_url: Base URL for OpenSanctions data. Defaults to
                      settings.opensanctions_base_url.
        """
        self.cache_dir = cache_dir or (settings.raw_data_dir / "opensanctions")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.base_url = base_url or settings.opensanctions_base_url
        
        # Configure HTTP client with timeout and connection pooling
        self.client = httpx.Client(
            timeout=httpx.Timeout(settings.http_timeout),
            follow_redirects=True,
            limits=httpx.Limits(max_keepalive_connections=5),
        )
        
        logger.info(f"Initialized OpenSanctionsClient with cache_dir={self.cache_dir}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close HTTP client."""
        self.client.close()
    
    def close(self):
        """Close the HTTP client."""
        self.client.close()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=60),
    )
    def download_dataset(
        self,
        dataset: str = "sanctions",
        force: bool = False,
    ) -> Path:
        """
        Download a dataset from OpenSanctions.
        
        Downloads are streamed to disk to handle large files (500MB+)
        without loading everything into memory.
        
        Args:
            dataset: Which dataset to download. Options:
                     - "default": All data combined
                     - "sanctions": Just sanctioned entities (recommended)
                     - "peps": Politically Exposed Persons
                     - "crime": Wanted/criminal lists
            force: If True, download even if a recent file exists.
        
        Returns:
            Path to the downloaded file.
        
        Raises:
            ValueError: If dataset name is not recognized.
            httpx.HTTPError: If download fails after retries.
        
        Example:
            >>> client = OpenSanctionsClient()
            >>> filepath = client.download_dataset("sanctions")
            >>> print(f"Downloaded to {filepath}")
        """
        if dataset not in self.DATASETS:
            raise ValueError(
                f"Unknown dataset: {dataset}. "
                f"Available: {list(self.DATASETS.keys())}"
            )
        
        # Check for existing recent download (within 24 hours)
        date_str = datetime.now().strftime("%Y%m%d")
        local_path = self.cache_dir / f"{dataset}_{date_str}.json"
        
        if local_path.exists() and not force:
            logger.info(f"Using cached file: {local_path}")
            return local_path
        
        # Build download URL
        url = f"{self.base_url}/{self.DATASETS[dataset]}"
        logger.info(f"Downloading {dataset} dataset from {url}")
        
        # Stream download to handle large files
        with self.client.stream("GET", url) as response:
            response.raise_for_status()
            
            # Get total size for progress logging
            total_size = int(response.headers.get("content-length", 0))
            downloaded = 0
            
            with open(local_path, "wb") as f:
                for chunk in response.iter_bytes(chunk_size=8192):
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    # Log progress every 50MB
                    if total_size and downloaded % (50 * 1024 * 1024) < 8192:
                        pct = (downloaded / total_size) * 100
                        logger.info(f"Download progress: {pct:.1f}%")
        
        file_size_mb = local_path.stat().st_size / (1024 * 1024)
        logger.info(f"Downloaded {file_size_mb:.1f}MB to {local_path}")
        
        return local_path
    
    def parse_entities(self, filepath: Path) -> pl.DataFrame:
        """
        Parse FtM JSON format into a structured DataFrame.
        
        The Follow the Money (FtM) format stores entities with:
        - Core fields: id, schema (type), caption (name), datasets
        - Properties: name, alias, country, address, and type-specific fields
        
        Entity types (schemas) include:
        - Person: Individual people
        - Company: Corporations and businesses
        - Organization: Non-commercial organizations
        - LegalEntity: Generic legal entities
        - Vessel: Ships and boats
        - Aircraft: Planes and helicopters
        
        Args:
            filepath: Path to the downloaded NDJSON file.
        
        Returns:
            DataFrame with columns:
            - entity_id: Unique identifier
            - schema: Entity type
            - caption: Display name
            - datasets: Comma-separated source lists
            - names: Pipe-separated known names
            - aliases: Pipe-separated aliases
            - countries: Pipe-separated associated countries
            - addresses: Pipe-separated known addresses
            - birth_date: For persons
            - nationality: For persons
            - incorporation_date: For companies
            - jurisdiction: For companies
            - registration_number: Official registration
            - inn_code: Russian tax ID
            - ogrn_code: Russian registration number
            - lei_code: Legal Entity Identifier
        
        Example:
            >>> entities_df = client.parse_entities(filepath)
            >>> print(entities_df.schema)
        """
        logger.info(f"Parsing entities from {filepath}")
        
        entities = []
        line_count = 0
        error_count = 0
        
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line_count += 1
                
                try:
                    entity = json.loads(line)
                except json.JSONDecodeError as e:
                    error_count += 1
                    if error_count <= 5:
                        logger.warning(f"JSON parse error on line {line_count}: {e}")
                    continue
                
                # Extract core fields present on all entities
                record = {
                    "entity_id": entity.get("id"),
                    "schema": entity.get("schema"),
                    "caption": entity.get("caption"),
                    "datasets": ",".join(entity.get("datasets", [])),
                    "first_seen": entity.get("first_seen"),
                    "last_seen": entity.get("last_seen"),
                    "last_change": entity.get("last_change"),
                }
                
                # Extract properties (multi-valued fields stored as arrays)
                props = entity.get("properties", {})
                
                # Common fields across entity types
                # Using pipe delimiter because it rarely appears in names
                record["names"] = "|".join(props.get("name", []))
                record["aliases"] = "|".join(props.get("alias", []))
                record["countries"] = "|".join(props.get("country", []))
                record["addresses"] = "|".join(props.get("address", []))
                
                # Topics/categories (sanctions, pep, crime, etc.)
                record["topics"] = "|".join(props.get("topics", []))
                
                # Person-specific fields
                record["birth_date"] = self._get_first(props, "birthDate")
                record["death_date"] = self._get_first(props, "deathDate")
                record["nationality"] = "|".join(props.get("nationality", []))
                record["gender"] = self._get_first(props, "gender")
                record["position"] = "|".join(props.get("position", []))
                
                # Company-specific fields
                record["incorporation_date"] = self._get_first(props, "incorporationDate")
                record["dissolution_date"] = self._get_first(props, "dissolutionDate")
                record["jurisdiction"] = self._get_first(props, "jurisdiction")
                record["registration_number"] = self._get_first(props, "registrationNumber")
                record["status"] = self._get_first(props, "status")
                
                # Important identifiers for exact matching
                record["inn_code"] = self._get_first(props, "innCode")      # Russian tax ID
                record["ogrn_code"] = self._get_first(props, "ogrnCode")    # Russian registration
                record["lei_code"] = self._get_first(props, "leiCode")      # Legal Entity Identifier
                record["swift_bic"] = self._get_first(props, "swiftBic")    # Bank identifier
                record["imo_number"] = self._get_first(props, "imoNumber")  # Ship identifier
                
                # Sanctions-specific metadata
                record["program"] = "|".join(props.get("program", []))      # Which sanctions program
                record["summary"] = self._get_first(props, "summary")       # Reason for listing
                
                entities.append(record)
                
                # Progress logging
                if line_count % 100000 == 0:
                    logger.info(f"Parsed {line_count:,} entities...")
        
        if error_count > 0:
            logger.warning(f"Encountered {error_count} JSON parse errors out of {line_count} lines")
        
        df = pl.DataFrame(entities)
        
        # Log schema breakdown
        schema_counts = df.group_by("schema").len().sort("len", descending=True)
        logger.info(f"Parsed {len(df):,} entities from {line_count:,} lines")
        logger.info(f"Schema distribution:\n{schema_counts}")
        
        return df
    
    def extract_relationships(self, filepath: Path) -> pl.DataFrame:
        """
        Extract relationships between entities.
        
        FtM encodes relationships as properties on entities pointing
        to other entity IDs. This extracts them as explicit edges.
        
        Relationship types include:
        - ownership: Company owned by person/company
        - directorship: Person directs company
        - family: Person related to person
        - associate: Person associated with person
        - membership: Person member of organization
        
        Args:
            filepath: Path to the downloaded NDJSON file.
        
        Returns:
            DataFrame with columns:
            - source_id: Entity ID that has the relationship
            - target_id: Entity ID being referenced
            - relationship_type: Type of relationship
            - properties: Additional relationship metadata (JSON string)
        
        Example:
            >>> relationships_df = client.extract_relationships(filepath)
            >>> print(f"Found {len(relationships_df)} relationships")
        """
        logger.info(f"Extracting relationships from {filepath}")
        
        relationships = []
        
        # Map of property names to relationship types
        # Format: property_name -> (relationship_type, reverse_name)
        relationship_props = {
            "ownershipOwner": ("owned_by", "owns"),
            "ownershipAsset": ("owns", "owned_by"),
            "directorshipDirector": ("directed_by", "directs"),
            "directorshipOrganization": ("directs", "directed_by"),
            "familyPerson": ("family_of", "family_of"),
            "familyRelative": ("related_to", "related_to"),
            "associateOf": ("associate_of", "associate_of"),
            "memberOf": ("member_of", "has_member"),
            "employerOf": ("employer_of", "employed_by"),
            "employees": ("employed_by", "employer_of"),
        }
        
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    entity = json.loads(line)
                except json.JSONDecodeError:
                    continue
                
                source_id = entity.get("id")
                props = entity.get("properties", {})
                
                for prop_name, (rel_type, _) in relationship_props.items():
                    targets = props.get(prop_name, [])
                    for target_id in targets:
                        if target_id and target_id != source_id:
                            relationships.append({
                                "source_id": source_id,
                                "target_id": target_id,
                                "relationship_type": rel_type,
                            })
        
        df = pl.DataFrame(relationships)
        
        # Remove duplicates (same relationship from both directions)
        df = df.unique()
        
        # Log relationship type breakdown
        if len(df) > 0:
            rel_counts = df.group_by("relationship_type").len().sort("len", descending=True)
            logger.info(f"Extracted {len(df):,} relationships")
            logger.info(f"Relationship types:\n{rel_counts}")
        else:
            logger.info("No relationships found in dataset")
        
        return df
    
    def _get_first(self, props: dict, key: str) -> str | None:
        """
        Get the first value from a property array.
        
        FtM stores most properties as arrays even for single values.
        This helper extracts just the first value or None.
        """
        values = props.get(key, [])
        return values[0] if values else None
    
    def get_dataset_stats(self, filepath: Path) -> dict:
        """
        Get statistics about a downloaded dataset without full parsing.
        
        Useful for quick inspection of data files.
        
        Args:
            filepath: Path to the downloaded NDJSON file.
        
        Returns:
            Dictionary with stats:
            - total_entities: Total number of entities
            - schemas: Count by entity type
            - datasets: Count by source dataset
            - file_size_mb: File size in megabytes
        """
        schemas: dict[str, int] = {}
        datasets: dict[str, int] = {}
        total = 0
        
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    entity = json.loads(line)
                    total += 1
                    
                    schema = entity.get("schema", "unknown")
                    schemas[schema] = schemas.get(schema, 0) + 1
                    
                    for ds in entity.get("datasets", []):
                        datasets[ds] = datasets.get(ds, 0) + 1
                        
                except json.JSONDecodeError:
                    continue
        
        return {
            "total_entities": total,
            "schemas": dict(sorted(schemas.items(), key=lambda x: -x[1])),
            "datasets": dict(sorted(datasets.items(), key=lambda x: -x[1])),
            "file_size_mb": filepath.stat().st_size / (1024 * 1024),
        }


def ingest_opensanctions(
    dataset: str = "sanctions",
    output_dir: Path | None = None,
    force_download: bool = False,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Complete ingestion pipeline for OpenSanctions data.
    
    Downloads the specified dataset (if not cached), parses entities
    and relationships, and saves to Parquet files.
    
    Args:
        dataset: Which dataset to download ("sanctions", "default", "peps", "crime")
        output_dir: Where to save processed Parquet files. Defaults to
                    settings.processed_data_dir.
        force_download: If True, download fresh data even if cached.
    
    Returns:
        Tuple of (entities_df, relationships_df)
    
    Example:
        >>> entities_df, relationships_df = ingest_opensanctions()
        >>> print(f"Loaded {len(entities_df)} entities")
        >>> print(f"Loaded {len(relationships_df)} relationships")
    """
    output_dir = output_dir or settings.processed_data_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with OpenSanctionsClient() as client:
        # Download dataset
        filepath = client.download_dataset(dataset, force=force_download)
        
        # Parse entities
        entities_df = client.parse_entities(filepath)
        entities_path = output_dir / "sanctions_entities.parquet"
        entities_df.write_parquet(entities_path)
        logger.info(f"Saved entities to {entities_path}")
        
        # Extract relationships
        relationships_df = client.extract_relationships(filepath)
        relationships_path = output_dir / "sanctions_relationships.parquet"
        relationships_df.write_parquet(relationships_path)
        logger.info(f"Saved relationships to {relationships_path}")
    
    return entities_df, relationships_df


# Allow running as script
if __name__ == "__main__":
    import sys
    
    # Configure logging for CLI usage
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    
    # Run ingestion
    entities_df, relationships_df = ingest_opensanctions()
    
    print("\n" + "=" * 60)
    print("INGESTION COMPLETE")
    print("=" * 60)
    print(f"Entities: {len(entities_df):,}")
    print(f"Relationships: {len(relationships_df):,}")
    print(f"\nSample entities:")
    print(entities_df.head(5))
