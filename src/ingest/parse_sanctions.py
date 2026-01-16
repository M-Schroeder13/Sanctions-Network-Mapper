"""
Robust sanctions data parser that handles schema inconsistencies.
Forces all columns to string type to avoid Polars schema inference issues.
"""
import json
import polars as pl
from pathlib import Path


def parse_sanctions_data(
    input_path: Path | None = None,
    output_dir: Path | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Parse OpenSanctions data with robust type handling.
    """
    # Auto-detect input file
    if input_path is None:
        raw_dir = Path("data/raw/opensanctions")
        json_files = list(raw_dir.glob("sanctions_*.json"))
        if not json_files:
            raise FileNotFoundError(f"No sanctions JSON files found in {raw_dir}")
        input_path = sorted(json_files)[-1]
    
    output_dir = output_dir or Path("data/processed")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Parsing: {input_path}")
    print(f"Output:  {output_dir}")
    print()
    
    # Define all columns upfront - ALL as strings
    columns = [
        "entity_id", "schema", "caption", "datasets", "first_seen", "last_seen",
        "last_change", "names", "aliases", "countries", "addresses", "topics",
        "nationality", "program", "position", "birth_date", "death_date", "gender",
        "incorporation_date", "dissolution_date", "jurisdiction", "registration_number",
        "status", "summary", "inn_code", "ogrn_code", "lei_code", "swift_bic", "imo_number",
    ]
    
    # Initialize lists for each column
    data = {col: [] for col in columns}
    relationships = []
    count = 0
    errors = 0
    
    def to_str(val) -> str:
        """Convert any value to string, empty string for None."""
        if val is None:
            return ""
        return str(val)
    
    def get_first(props: dict, key: str) -> str:
        """Get first value as string."""
        vals = props.get(key, [])
        if vals and len(vals) > 0:
            return to_str(vals[0])
        return ""
    
    def join_list(props: dict, key: str, sep: str = "|") -> str:
        """Join list values as string."""
        vals = props.get(key, [])
        if vals:
            return sep.join(to_str(v) for v in vals if v is not None)
        return ""
    
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                entity = json.loads(line)
                count += 1
                
                if count % 100000 == 0:
                    print(f"  Processed {count:,} entities...")
                
                props = entity.get("properties", {})
                
                # Append to each column list
                data["entity_id"].append(to_str(entity.get("id")))
                data["schema"].append(to_str(entity.get("schema")))
                data["caption"].append(to_str(entity.get("caption")))
                data["datasets"].append(",".join(str(d) for d in entity.get("datasets", [])))
                data["first_seen"].append(to_str(entity.get("first_seen")))
                data["last_seen"].append(to_str(entity.get("last_seen")))
                data["last_change"].append(to_str(entity.get("last_change")))
                
                data["names"].append(join_list(props, "name"))
                data["aliases"].append(join_list(props, "alias"))
                data["countries"].append(join_list(props, "country"))
                data["addresses"].append(join_list(props, "address"))
                data["topics"].append(join_list(props, "topics"))
                data["nationality"].append(join_list(props, "nationality"))
                data["program"].append(join_list(props, "program"))
                data["position"].append(join_list(props, "position"))
                
                data["birth_date"].append(get_first(props, "birthDate"))
                data["death_date"].append(get_first(props, "deathDate"))
                data["gender"].append(get_first(props, "gender"))
                data["incorporation_date"].append(get_first(props, "incorporationDate"))
                data["dissolution_date"].append(get_first(props, "dissolutionDate"))
                data["jurisdiction"].append(get_first(props, "jurisdiction"))
                data["registration_number"].append(get_first(props, "registrationNumber"))
                data["status"].append(get_first(props, "status"))
                data["summary"].append(get_first(props, "summary"))
                
                data["inn_code"].append(get_first(props, "innCode"))
                data["ogrn_code"].append(get_first(props, "ogrnCode"))
                data["lei_code"].append(get_first(props, "leiCode"))
                data["swift_bic"].append(get_first(props, "swiftBic"))
                data["imo_number"].append(get_first(props, "imoNumber"))
                
                # Extract relationships
                rel_props = {
                    "ownershipOwner": "owned_by",
                    "ownershipAsset": "owns",
                    "directorshipDirector": "directed_by",
                    "directorshipOrganization": "directs",
                    "familyPerson": "family_of",
                    "familyRelative": "related_to",
                    "associateOf": "associate_of",
                    "memberOf": "member_of",
                }
                
                for prop_name, rel_type in rel_props.items():
                    for target in props.get(prop_name, []):
                        if target:
                            relationships.append({
                                "source_id": to_str(entity.get("id")),
                                "target_id": to_str(target),
                                "relationship_type": rel_type,
                            })
                
            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"  Warning: Error on line {count}: {e}")
                continue
    
    print()
    print(f"Total lines processed: {count:,}")
    print(f"Errors: {errors:,}")
    print()
    
    print("Creating DataFrames with explicit string schema...")
    
    # Create DataFrame with explicit schema - all strings
    schema = {col: pl.Utf8 for col in columns}
    entities_df = pl.DataFrame(data, schema=schema)
    
    if relationships:
        relationships_df = pl.DataFrame(relationships, schema={
            "source_id": pl.Utf8,
            "target_id": pl.Utf8,
            "relationship_type": pl.Utf8,
        }).unique()
    else:
        relationships_df = pl.DataFrame(schema={
            "source_id": pl.Utf8,
            "target_id": pl.Utf8,
            "relationship_type": pl.Utf8,
        })
    
    print(f"  Entities: {len(entities_df):,} rows")
    print(f"  Relationships: {len(relationships_df):,} rows")
    print()
    
    # Save to parquet
    entities_path = output_dir / "sanctions_entities.parquet"
    relationships_path = output_dir / "sanctions_relationships.parquet"
    
    print("Saving to Parquet...")
    entities_df.write_parquet(entities_path)
    relationships_df.write_parquet(relationships_path)
    print(f"  Saved: {entities_path}")
    print(f"  Saved: {relationships_path}")
    print()
    
    # Show schema breakdown
    print("Schema breakdown:")
    schema_counts = entities_df.group_by("schema").len().sort("len", descending=True)
    for row in schema_counts.iter_rows():
        print(f"  {row[0]}: {row[1]:,}")
    
    return entities_df, relationships_df


if __name__ == "__main__":
    parse_sanctions_data()
