from pathlib import Path
import yaml


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
    return len(names)

if __name__ == "__main__":
    print(f"Validated {validate()} source definitions and reporting rules")
