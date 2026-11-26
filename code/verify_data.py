"""Verify packaged data and optionally extract the benchmark's Turtle inputs."""
import argparse
import gzip
import hashlib
import json
import shutil
from pathlib import Path

from prepare_data import canonical_records, record_digest

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--extract", action="store_true",
                        help="Also extract the seven verified .ttl.gz files into data/.")
    args = parser.parse_args()
    for line in (DATA / "CHECKSUMS.sha256").read_text().splitlines():
        expected, name = line.split("  ", 1)
        require(digest(DATA / name) == expected, f"Packaged file differs: {name}")
    manifest = json.loads((DATA / "manifest.json").read_text())
    require(digest(DATA / "Mol_Sluis_Dessel_data.csv") == manifest["source_sha256"],
            "Source CSV differs from original benchmark")
    records, excluded = canonical_records()
    require(len(records) == manifest["valid_readings"], "Valid reading count differs")
    require(excluded == manifest["excluded_per_sensor"], "Excluded reading counts differ")
    require(record_digest(records) == manifest["canonical_digest"], "Canonical records differ")
    for rep in manifest["representations"].values():
        target = DATA / rep["file"]
        archive = target.with_suffix(".ttl.gz")
        with gzip.open(archive, "rb") as stream:
            actual = hashlib.file_digest(stream, "sha256").hexdigest()
        require(actual == rep["sha256"], f"Uncompressed RDF differs: {archive.name}")
        if args.extract and (not target.exists() or digest(target) != actual):
            temporary = target.with_suffix(".ttl.tmp")
            try:
                with gzip.open(archive, "rb") as source, temporary.open("wb") as output:
                    shutil.copyfileobj(source, output)
                require(digest(temporary) == actual, f"Extraction differs: {target.name}")
                temporary.replace(target)
            finally:
                temporary.unlink(missing_ok=True)
    print(f"Verified all packaged files, {len(records):,} readings and seven RDF representations.")
    if args.extract:
        print("Turtle files are available in data/ for the benchmark and cross-parameter audit.")


if __name__ == "__main__":
    main()
