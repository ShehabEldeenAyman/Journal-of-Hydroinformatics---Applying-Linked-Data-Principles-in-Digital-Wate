"""Historical Dessel transformation pipeline, separate from timed benchmarks.

This standalone adapter reuses the benchmark canonicalisation/snippet encoding
and the paper's worked-example SHACL resources. RML mapping uses RMLMapper.
"""
import argparse
import csv
import hashlib
import json
import subprocess
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import prepare_data

ROOT = Path(__file__).resolve().parents[1]
RESOURCES = ROOT / "data/pipeline_resources"
FIELDS = ["observation", "sensor", "property", "time", "value"]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def expected_records(output):
    with (output / "canonical.csv").open(newline="", encoding="utf-8") as stream:
        return [(r["observation"], r["sensor"], r["property"], r["time"], float(r["value"]))
                for r in csv.DictReader(stream)]


def preprocess(source, output, limit=None):
    previous = prepare_data.SOURCE
    try:
        prepare_data.SOURCE = source
        records, excluded = prepare_data.canonical_records()
    finally:
        prepare_data.SOURCE = previous
    if limit:
        counts, selected = Counter(), []
        for row in records:
            if counts[row[1]] < limit:
                selected.append(row)
                counts[row[1]] += 1
        records = selected
    with (output / "canonical.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(FIELDS)
        writer.writerows(records)
    report = {"source": str(source), "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
              "readings": len(records), "excluded_per_sensor_in_full_source": excluded,
              "limit_per_sensor": limit, "canonical_digest": prepare_data.record_digest(records),
              "timestamp_policy": "Source timestamps retained without assigning a time zone",
              "units": {"73617010": "m", "73626010": "m3/s"}}
    (output / "preprocessing.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def map_rml(output, jar):
    require(jar is not None and jar.is_file(), "Mapping needs --rmlmapper /path/to/rmlmapper.jar")
    # JSON string escaping also provides a valid quoted Turtle source path.
    mapping = (RESOURCES / "dessel.rml.ttl").read_text().replace(
        '"canonical.csv"', json.dumps(str(output / "canonical.csv")))
    path = output / "mapping.rml.ttl"
    path.write_text(mapping)
    command = ["java", "-jar", str(jar), "-m", str(path),
               "-o", str(output / "observations.ttl"), "-s", "turtle"]
    result = subprocess.run(command, capture_output=True, text=True)
    (output / "rmlmapper.log").write_text(result.stdout + result.stderr)
    require(result.returncode == 0, "RMLMapper failed; see rmlmapper.log")
    return {"command": command, "exit_code": result.returncode}


def mapped_records(output):
    from pyoxigraph import RdfFormat, Store
    store = Store()
    store.bulk_load(path=str(output / "observations.ttl"), format=RdfFormat.TURTLE)
    query = '''PREFIX sosa: <http://www.w3.org/ns/sosa/>
    SELECT ?id ?sensor ?property ?time ?value WHERE {
      ?id a sosa:Observation ; sosa:madeBySensor ?sensor ;
          sosa:observedProperty ?property ; sosa:resultTime ?time ;
          sosa:hasSimpleResult ?value . }'''
    rows = [(r["id"].value, r["sensor"].value, r["property"].value,
             r["time"].value, float(r["value"].value)) for r in store.query(query)]
    expected = expected_records(output)
    require(len(rows) == len(expected) and
            prepare_data.record_digest(rows) == prepare_data.record_digest(expected),
            "Mapped RDF does not reconstruct the preprocessed source records")
    return sorted(rows, key=lambda r: (r[1], r[3], r[0]))


def validate_rdf(output):
    from rdflib import Graph
    from pyshacl import validate
    records = mapped_records(output)
    graph = Graph().parse(output / "observations.ttl", format="turtle")
    graph.parse(RESOURCES / "sensor_context.ttl", format="turtle")
    shapes = Graph().parse(RESOURCES / "shapes.ttl", format="turtle")
    conforms, results_graph, report = validate(graph, shacl_graph=shapes, inference="none")
    results_graph.serialize(output / "shacl_report.ttl", format="turtle")
    (output / "shacl_report.txt").write_text(report)
    require(conforms, "SHACL violations found; see shacl_report.txt")
    return {"conforms": bool(conforms), "readings": len(records),
            "canonical_digest": prepare_data.record_digest(records),
            "context": "Explicit sensor classification supplied by the worked example"}


def compact(output, minutes):
    records = mapped_records(output)
    previous = prepare_data.OUT
    try:
        prepare_data.OUT = output
        path, triples, snippets, _ = prepare_data.write_tss(records, minutes)
    finally:
        prepare_data.OUT = previous
    return {"file": path.name, "triples": triples, "snippets": snippets,
            "readings": len(records), "window_minutes": minutes}


def publish(output, minutes, base_url):
    """Write daily fragments and a TREE index, verifying snippet reconstruction."""
    from rdflib import Graph, URIRef, BNode, Literal, Namespace, RDF, XSD
    from pyoxigraph import RdfFormat, Store
    tss, sosa = Namespace("https://w3id.org/tss#"), Namespace("http://www.w3.org/ns/sosa/")
    tree, ldes = Namespace("https://w3id.org/tree#"), Namespace("https://w3id.org/ldes#")
    store = Store()
    store.bulk_load(path=str(output / f"tss_ldes_{minutes}m.ttl"), format=RdfFormat.TURTLE)
    query = '''PREFIX tss: <https://w3id.org/tss#>
    PREFIX sosa: <http://www.w3.org/ns/sosa/>
    SELECT ?snippet ?start ?end ?sensor ?property ?points ?context WHERE {
      ?snippet a tss:Snippet ; tss:from ?start ; tss:until ?end ;
        tss:about ?template ; tss:points ?points ; tss:context ?context .
      ?template sosa:madeBySensor ?sensor ; sosa:observedProperty ?property . }'''
    days, reconstructed = defaultdict(list), []
    for row in store.query(query):
        days[row["start"].value[:10]].append(row)
        for point in json.loads(row["points"].value):
            require(row["start"].value <= point["time"] < row["end"].value,
                    "Snippet contains a point outside its half-open bounds")
            reconstructed.append((point["id"], row["sensor"].value,
                                  row["property"].value, point["time"], point["value"]))
    expected = expected_records(output)
    require(len(reconstructed) == len(expected) and
            prepare_data.record_digest(reconstructed) == prepare_data.record_digest(expected),
            "Snippet reconstruction differs from source")
    folder = output / "ldes"
    (folder / "days").mkdir(parents=True, exist_ok=True)
    base = base_url.rstrip("/") + "/"
    feed, root = URIRef(base + "#stream"), URIRef(base + "index.ttl")
    index = Graph()
    index.add((feed, RDF.type, ldes.EventStream))
    index.add((feed, ldes.timestampPath, tss["from"]))
    index.add((feed, tree.view, root))
    index.add((root, RDF.type, tree.Node))
    for day, rows in sorted(days.items()):
        node = URIRef(base + f"days/{day}.ttl")
        graph = Graph()
        for prefix, namespace in [("tss", tss), ("sosa", sosa), ("tree", tree), ("ldes", ldes)]:
            graph.bind(prefix, namespace)
        graph.add((feed, RDF.type, ldes.EventStream))
        graph.add((feed, ldes.timestampPath, tss["from"]))
        graph.add((feed, tree.view, root))
        graph.add((node, RDF.type, tree.Node))
        for row in rows:
            snippet, template = URIRef(row["snippet"].value), BNode()
            graph.add((feed, tree.member, snippet))
            graph.add((snippet, RDF.type, tss.Snippet))
            graph.add((snippet, tss.about, template))
            graph.add((snippet, tss.pointType, sosa.Observation))
            for predicate, key, datatype in [(tss["from"], "start", XSD.dateTime),
                                              (tss.until, "end", XSD.dateTime),
                                              (tss.points, "points", RDF.JSON),
                                              (tss.context, "context", RDF.JSON)]:
                graph.add((snippet, predicate, Literal(row[key].value, datatype=datatype)))
            graph.add((template, RDF.type, tss.PointTemplate))
            graph.add((template, sosa.madeBySensor, URIRef(row["sensor"].value)))
            graph.add((template, sosa.observedProperty, URIRef(row["property"].value)))
        graph.serialize(folder / f"days/{day}.ttl", format="turtle")
        start = datetime.fromisoformat(day)
        for kind, bound in [(tree.GreaterThanOrEqualToRelation, start),
                            (tree.LessThanRelation, start + timedelta(days=1))]:
            relation = BNode()
            for triple in [(root, tree.relation, relation), (relation, RDF.type, kind),
                           (relation, tree.node, node), (relation, tree.path, tss["from"]),
                           (relation, tree.value, Literal(bound.isoformat(), datatype=XSD.dateTime))]:
                index.add(triple)
    index.serialize(folder / "index.ttl", format="turtle")
    return {"daily_fragments": len(days), "reconstructed_readings": len(reconstructed),
            "base_url": base, "index": "ldes/index.ttl",
            "status": "Local publication files generated; HTTP hosting is separate"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT / "data/Mol_Sluis_Dessel_data.csv")
    parser.add_argument("--output", type=Path, default=ROOT / "results/transformation")
    parser.add_argument("--rmlmapper", type=Path, help="Path to separately installed RMLMapper JAR")
    parser.add_argument("--stage", choices=["all", "preprocess", "map", "validate", "compact", "publish"], default="all")
    parser.add_argument("--minutes", type=int, choices=[15, 60, 360, 720, 1440], default=1440)
    parser.add_argument("--limit-per-sensor", type=int, help="Optional small demonstration; omit for full source")
    parser.add_argument("--base-url", default="http://localhost:8000/", help="URL where ldes/ will be served")
    args = parser.parse_args()
    if args.limit_per_sensor is not None and args.limit_per_sensor < 1:
        parser.error("--limit-per-sensor must be positive")
    if args.stage in ("all", "map") and (args.rmlmapper is None or not args.rmlmapper.is_file()):
        parser.error("--rmlmapper must point to an installed RMLMapper JAR for the mapping stage")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    stages = {"preprocess": lambda: preprocess(args.source.resolve(), output, args.limit_per_sensor),
              "map": lambda: map_rml(output, args.rmlmapper.resolve()),
              "validate": lambda: validate_rdf(output),
              "compact": lambda: compact(output, args.minutes),
              "publish": lambda: publish(output, args.minutes, args.base_url)}
    for name, action in stages.items():
        if args.stage in ("all", name):
            print(f"Starting {name}", flush=True)
            result = action()
            (output / f"{name}_stage.json").write_text(json.dumps(result, indent=2) + "\n")
            print(json.dumps(result, indent=2), flush=True)


if __name__ == "__main__":
    main()
