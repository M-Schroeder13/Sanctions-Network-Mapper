import json
import polars as pl
from pathlib import Path

filepath = Path("data/raw/opensanctions/sanctions_20260114.json")
output_dir = Path("data/processed")
output_dir.mkdir(parents=True, exist_ok=True)

print("Parsing entities with explicit schema handling...")

entities = []
relationships = []
count = 0

with open(filepath, "r", encoding="utf-8") as f:
    for line in f:
        try:
            entity = json.loads(line)
            count += 1
            
            if count % 100000 == 0:
                print(f"  Processed {count:,} entities...")
            
            props = entity.get("properties", {})
            
            def get_first(d, key):
                vals = d.get(key, [])
                return str(vals[0]) if vals else None
            
            def join_list(d, key, sep="|"):
                vals = d.get(key, [])
                return sep.join(str(v) for v in vals) if vals else ""
            
            record = {
                "entity_id": str(entity.get("id", "")),
                "schema": str(entity.get("schema", "")),
                "caption": str(entity.get("caption", "")),
                "datasets": ",".join(entity.get("datasets", [])),
                "first_seen": get_first(entity, "first_seen") or entity.get("first_seen"),
                "last_seen": get_first(entity, "last_seen") or entity.get("last_seen"),
                "names": join_list(props, "name"),
                "aliases": join_list(props, "alias"),
                "countries": join_list(props, "country"),
                "addresses": join_list(props, "address"),
                "birth_date": get_first(props, "birthDate"),
                "nationality": join_list(props, "nationality"),
                "jurisdiction": get_first(props, "jurisdiction"),
                "registration_number": get_first(props, "registrationNumber"),
                "inn_code": get_first(props, "innCode"),
                "ogrn_code": get_first(props, "ogrnCode"),
            }
            entities.append(record)
            
            for owner in props.get("ownershipOwner", []):
                relationships.append({
                    "source_id": str(entity.get("id")),
                    "target_id": str(owner),
                    "relationship_type": "owned_by",
                })
                
        except Exception as e:
            continue

print(f"\nTotal entities: {count:,}")
print("Creating DataFrames...")

entities_df = pl.DataFrame(entities)
relationships_df = pl.DataFrame(relationships) if relationships else pl.DataFrame({"source_id": [], "target_id": [], "relationship_type": []})

print(f"Entities DataFrame: {len(entities_df):,} rows")
print(f"Relationships DataFrame: {len(relationships_df):,} rows")

print("\nSaving to Parquet...")
entities_df.write_parquet(output_dir / "sanctions_entities.parquet")
relationships_df.write_parquet(output_dir / "sanctions_relationships.parquet")

print("\nDone! Schema breakdown:")
print(entities_df.group_by("schema").len().sort("len", descending=True))
