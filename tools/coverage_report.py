"""Combine subprocess or CI-suite data and write readable coverage reports."""

import argparse
from pathlib import Path
import coverage


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("test-results"))
    parser.add_argument("--output-dir", type=Path, default=Path("test-results"))
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    # Include existing main-process data and all subprocess files, or downloaded suites.
    files = [str(p.resolve()) for p in args.input_dir.rglob("coverage-data*") if p.is_file()]
    if not files:
        raise RuntimeError(f"No coverage data found in {args.input_dir}")
    # Combining into a new path avoids combining the destination file into itself.
    cov = coverage.Coverage(data_file=str(args.output_dir / "combined-coverage"))
    cov.combine(data_paths=files, strict=True, keep=True)
    cov.save()
    cov.report()
    cov.xml_report(outfile=str(args.output_dir / "coverage.xml"))
    cov.json_report(outfile=str(args.output_dir / "coverage.json"))
    cov.html_report(directory=str(args.output_dir / "coverage-html"))


if __name__ == "__main__":
    main()
