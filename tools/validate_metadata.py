from pathlib import Path
import yaml
import re


def validate(root="metadata"):
    sources = yaml.safe_load((Path(root) / "sources.yml").read_text())
    names = [source["name"] for source in sources["sources"]]
    if len(names) != len(set(names)):
        raise ValueError("Duplicate source names")
    for source in sources["sources"]:
        for required in ["endpoint", "natural_key", "status"]:
            if not source.get(required):
                raise ValueError("Missing source metadata: " + required)
        if not source["endpoint"].startswith("https://"):
            raise ValueError("Source must use HTTPS")
    reporting = yaml.safe_load((Path(root) / "reporting.yml").read_text())
    if not reporting.get("rules"):
        raise ValueError("Missing metric rules")
    for layer in ("silver", "gold"):
        warehouse_path = Path(root) / (layer + "_tables.yml")
        if not warehouse_path.exists():
            continue
        warehouse = yaml.safe_load(warehouse_path.read_text())
        tables = warehouse.get("tables", [])
        identifiers = [table["name"] for table in tables]
        if not tables or len(identifiers) != len(set(identifiers)):
            raise ValueError("Missing or duplicate warehouse table metadata")
        by_short_name = {}
        for table in tables:
            if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*\.[A-Za-z_][A-Za-z_0-9]*\.[A-Za-z_][A-Za-z_0-9]*",table["name"]):
                raise ValueError("Invalid warehouse identifier")
            for required in ["key", "grain", "schema_sql", "format"]:
                if not table.get(required):
                    raise ValueError("Missing warehouse contract: " + required)
            keys = [table["key"]] if isinstance(table["key"], str) else table["key"]
            if not isinstance(keys, list) or not all(isinstance(key,str) for key in keys):
                raise ValueError("Invalid warehouse key")
            for key in keys:
                if not re.search(r"(?:^|,)\s*"+re.escape(key)+r"\s+",table["schema_sql"]):
                    raise ValueError("Warehouse key absent from schema")
            by_short_name[table["name"].split(".")[-1]] = table
        for reference in warehouse.get("foreign_keys", []):
            for end in ["source", "target"]:
                table, column = reference[end].split(".")
                contract = by_short_name.get(table)
                if not contract or not re.search(r"(?:^|,)\s*"+re.escape(column)+r"\s+",contract["schema_sql"]):
                    raise ValueError("Invalid warehouse foreign key")
    return len(names)

if __name__ == "__main__":
    print(f"Validated {validate()} source definitions and reporting rules")
