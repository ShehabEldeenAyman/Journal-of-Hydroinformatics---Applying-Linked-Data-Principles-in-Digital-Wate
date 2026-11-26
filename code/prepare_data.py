"""Create comparable five-field Dessel benchmark representations from the CSV."""
import csv
import gzip
import hashlib
import json
import math
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data"
SOURCE = ROOT / "data/Mol_Sluis_Dessel_data.csv"
PREFIXES = '''@prefix sosa: <http://www.w3.org/ns/sosa/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix tss: <https://w3id.org/tss#> .
@prefix ldes: <https://w3id.org/ldes#> .
@prefix tree: <https://w3id.org/tree#> .
'''
PROPERTIES = {"73617010": "river-stage", "73626010": "river-discharge"}


def canonical_records():
    records = []
    excluded = defaultdict(int)
    with SOURCE.open(newline="", encoding="utf-8") as stream:
        for row in csv.DictReader(stream):
            sid = row["ts_id"]
            try:
                value = float(row["Value"])
            except (ValueError, TypeError):
                excluded[sid] += 1
                continue
            if not math.isfinite(value):
                excluded[sid] += 1
                continue
            timestamp = row["Timestamp"]
            records.append((f"http://example.com/observations/{sid}/{quote(timestamp, safe='')}",
                            f"http://example.com/waterinfo/{sid}",
                            f"http://example.com/property/{PROPERTIES[sid]}", timestamp, value))
    records.sort(key=lambda row: (row[1], row[3], row[0]))
    if len({(r[1], r[3]) for r in records}) != len(records):
        raise ValueError("Duplicate source sensor/timestamp pairs")
    return records, dict(excluded)


def record_digest(records):
    h = hashlib.sha256()
    for oid, sensor, prop, timestamp, value in sorted(records, key=lambda r: (r[1], r[3], r[0])):
        h.update(json.dumps([oid, sensor, prop, timestamp, float(value).hex()], separators=(",", ":")).encode())
        h.update(b"\n")
    return h.hexdigest()


def hypermedia(stream, members, days, time_path):
    root = "https://example.org/dessel-benchmark/view"
    es = "https://example.org/dessel-benchmark/stream"
    stream.write(f'<{es}> a ldes:EventStream ; ldes:timestampPath {time_path} ; tree:view <{root}> .\n')
    count = 3
    for member in members:
        stream.write(f'<{es}> tree:member <{member}> .\n')
        count += 1
    for index, day in enumerate(sorted(days)):
        start = datetime.fromisoformat(day)
        end = start + timedelta(days=1)
        page = f"https://example.org/dessel-benchmark/page/{day}"
        for suffix, relation, bound in [("ge", "GreaterThanOrEqualToRelation", start), ("lt", "LessThanRelation", end)]:
            stream.write(f'<{root}> tree:relation _:r{index}{suffix} .\n')
            stream.write(f'_:r{index}{suffix} a tree:{relation} ; tree:node <{page}> ; tree:path {time_path} ; tree:value "{bound.isoformat()}"^^xsd:dateTime .\n')
            count += 5
    return count


def write_sosa(records, ldes=False):
    path = OUT / ("ldes_sosa.ttl" if ldes else "plain_sosa.ttl")
    started = time.perf_counter()
    with path.open("w", encoding="utf-8") as f:
        f.write(PREFIXES)
        for oid, sensor, prop, timestamp, value in records:
            f.write(f'<{oid}> a sosa:Observation ; sosa:madeBySensor <{sensor}> ; sosa:observedProperty <{prop}> ; sosa:resultTime "{timestamp}"^^xsd:dateTime ; sosa:hasSimpleResult "{repr(value)}"^^xsd:double .\n')
        count = 5 * len(records)
        if ldes:
            count += hypermedia(f, (r[0] for r in records), {r[3][:10] for r in records}, "sosa:resultTime")
    return path, count, len(records), time.perf_counter() - started


def write_tss(records, minutes):
    started = time.perf_counter()
    groups = defaultdict(list)
    for r in records:
        dt = datetime.fromisoformat(r[3])
        midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        start = midnight + timedelta(minutes=(dt.hour * 60 + dt.minute) // minutes * minutes)
        groups[(r[1], r[2], start)].append(r)
    path = OUT / f"tss_ldes_{minutes}m.ttl"
    members = []
    with path.open("w", encoding="utf-8") as f:
        f.write(PREFIXES)
        for index, ((sensor, prop, start), rows) in enumerate(sorted(groups.items())):
            snippet = f"https://example.org/dessel-benchmark/snippet/{sensor.rsplit('/', 1)[-1]}/{minutes}/{start.strftime('%Y%m%d%H%M')}"
            members.append(snippet)
            points = [{"id": r[0], "time": r[3], "value": r[4]} for r in rows]
            literal = json.dumps(json.dumps(points, separators=(",", ":")), ensure_ascii=False)
            context = json.dumps(json.dumps({"id": "@id", "time": {"@id": "http://www.w3.org/ns/sosa/resultTime", "@type": "http://www.w3.org/2001/XMLSchema#dateTime"}, "value": {"@id": "http://www.w3.org/ns/sosa/hasSimpleResult", "@type": "http://www.w3.org/2001/XMLSchema#double"}}, separators=(",", ":")))
            f.write(f'<{snippet}> a tss:Snippet ; tss:from "{start.isoformat()}"^^xsd:dateTime ; tss:until "{(start + timedelta(minutes=minutes)).isoformat()}"^^xsd:dateTime ; tss:about _:t{index} ; tss:pointType sosa:Observation ; tss:points {literal}^^rdf:JSON ; tss:context {context}^^rdf:JSON .\n')
            f.write(f'_:t{index} a tss:PointTemplate ; sosa:madeBySensor <{sensor}> ; sosa:observedProperty <{prop}> .\n')
        count = len(groups) * 10
        count += hypermedia(f, members, {r[3][:10] for r in records}, "tss:from")
    return path, count, len(groups), time.perf_counter() - started


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    records, excluded = canonical_records()
    with (OUT / "canonical.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["observation", "sensor", "property", "time", "value"])
        writer.writerows(records)
    representations = {}
    generated = [write_sosa(records), write_sosa(records, True)]
    generated += [write_tss(records, minutes) for minutes in [15, 60, 360, 720, 1440]]
    for path, triples, members, seconds in generated:
        gz = path.with_suffix(path.suffix + ".gz")
        with path.open("rb") as src, gz.open("wb") as target:
            with gzip.GzipFile(filename="", fileobj=target, mode="wb", mtime=0) as compressor:
                while chunk := src.read(1048576):
                    compressor.write(chunk)
        representations[path.stem] = {"file": path.name, "triples": triples,
            "members": members, "snippets": members if path.stem.startswith("tss_") else 0,
            "bytes": path.stat().st_size, "gzip_bytes": gz.stat().st_size,
            "generation_seconds": seconds, "sha256": hashlib.file_digest(path.open("rb"), "sha256").hexdigest()}
        print(path.name, representations[path.stem], flush=True)
    manifest = {"source_sha256": hashlib.file_digest(SOURCE.open("rb"), "sha256").hexdigest(),
        "valid_readings": len(records), "excluded_per_sensor": excluded,
        "canonical_digest": record_digest(records), "start": min(r[3] for r in records),
        "end": max(r[3] for r in records), "representations": representations,
        "scope": "Five core observation fields only: observation identifier, sensor identifier, property identifier, source timestamp, double value. Source timestamps are retained without adding an unverified timezone. Runoff, quality flags, units, and station metadata are outside this benchmark profile. LDES navigation triples are materialized in one named graph; this experiment measures stored representations and SPARQL retrieval, not HTTP feed traversal."}
    (OUT / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
