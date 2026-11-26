"""Additional normalized relational control; run after the primary suite."""
import argparse
import csv
import hashlib
import json
import logging
import random
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from run_benchmarks import BASE, INPUTS, RUNTIME, Engine, folder_bytes, workloads
from prepare_data import canonical_records, record_digest


QUERY = '''SELECT
 'http://example.com/observations/' || o.sensor_id::text || '/' ||
 replace(to_char(o.time, 'YYYY-MM-DD"T"HH24:MI:SS'), ':', '%%3A') AS observation,
 s.sensor, s.property, o.time, o.value
 FROM observations_normalized o JOIN series s USING (sensor_id)
 WHERE o.sensor_id=%s'''


class Normalized(Engine):
    def ingest(self, representation):
        started = time.perf_counter()
        self.connection.execute("CREATE TABLE series (sensor_id integer PRIMARY KEY, sensor text NOT NULL UNIQUE, property text NOT NULL)")
        with (INPUTS / "normalized_series.csv").open(newline="") as f:
            for row in csv.DictReader(f):
                self.connection.execute("INSERT INTO series VALUES (%s,%s,%s)",
                    [int(row["sensor_id"]), row["sensor"], row["property"]])
        self.connection.execute("CREATE TABLE observations_normalized (sensor_id integer REFERENCES series(sensor_id), time timestamp NOT NULL, value double precision NOT NULL, PRIMARY KEY(sensor_id,time))")
        with self.connection.cursor() as cursor:
            with cursor.copy("COPY observations_normalized FROM STDIN WITH (FORMAT CSV, HEADER TRUE)") as cp:
                with (INPUTS / "normalized.csv").open("rb") as f:
                    while data := f.read(1048576):
                        cp.write(data)
        self.connection.execute("ANALYZE series")
        self.connection.execute("ANALYZE observations_normalized")
        return time.perf_counter() - started

    def count(self):
        return self.connection.execute("SELECT count(*) FROM observations_normalized").fetchone()[0]


def query_for(w):
    return QUERY + (" AND o.time >= %s AND o.time < %s" if w["start"] else "") + " ORDER BY o.time"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("directory", type=Path)
    args = p.parse_args()
    output = args.directory.resolve()
    environment_file = output / "environment.json"
    environment = json.loads(environment_file.read_text())
    settings = environment["arguments"]
    name = "normalized_relational"
    environment["additional_postgresql_representations"] = [name]
    environment["normalized_relational_protocol"] = "Two-row sensor/property lookup and (sensor_id integer, timestamp, double value) observations with composite primary key. Known deterministic observation IRI mapping is reconstructed in SQL. All five returned logical fields are checked against the same canonical records. Load includes foreign-key/primary-key maintenance and ANALYZE."
    environment["normalized_runner_sha256"] = hashlib.file_digest(Path(__file__).open("rb"), "sha256").hexdigest()
    environment_file.write_text(json.dumps(environment, indent=2) + "\n")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", handlers=[logging.FileHandler(output / "execution.log", mode="a"), logging.StreamHandler()])
    source, _ = canonical_records()
    with (INPUTS / "normalized.csv").open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["sensor_id", "time", "value"])
        writer.writerows((int(r[1].rsplit('/', 1)[-1]), r[3], r[4]) for r in source)
    with (INPUTS / "normalized_series.csv").open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["sensor_id", "sensor", "property"])
        writer.writerows((int(s.rsplit('/', 1)[-1]), s, prop) for s, prop in sorted({(r[1], r[2]) for r in source}))
    expected = environment["expected_results"]
    results_file = output / "measurements.jsonl"
    existing = [json.loads(line) for line in results_file.read_text().splitlines()]
    done = {r["repetition"] for r in existing if r["engine"] == "postgresql" and r["representation"] == name and r["event"] == "complete"}
    def save(r):
        r["recorded_at_utc"] = datetime.now(timezone.utc).isoformat()
        with results_file.open("a") as f:
            f.write(json.dumps(r) + "\n")
    for w in workloads():
        (output / "queries" / f"{w['id']}_normalized.sql").write_text(query_for(w) + ";\n-- Parameters: " + json.dumps([int(w["sensor"].rsplit('/', 1)[-1])] + ([w["start"], w["end"]] if w["start"] else [])) + "\n")
    for repetition in range(1, settings["repetitions"] + 1):
        if repetition in done:
            continue
        case = f"postgresql_{name}_rep{repetition}"
        directory = RUNTIME / "runs" / settings["run_name"] / case
        if directory.exists():
            shutil.rmtree(directory)
        engine = Normalized("postgresql", directory, output / "services" / case)
        common = {"engine": "postgresql", "representation": name, "repetition": repetition}
        logging.info("START %s", case)
        try:
            engine.setup()
            engine.stop()
            before = folder_bytes(directory)
            engine.start()
            elapsed = engine.ingest(name)
            count = engine.count()
            assert count == len(source)
            table_bytes = engine.connection.execute("SELECT pg_total_relation_size('observations_normalized') + pg_total_relation_size('series')").fetchone()[0]
            plans = {}
            for w in workloads():
                params = [int(w["sensor"].rsplit('/', 1)[-1])] + ([w["start"], w["end"]] if w["start"] else [])
                plans[w["id"]] = engine.connection.execute("EXPLAIN (FORMAT JSON) " + query_for(w), params).fetchone()[0]
            checkpoint_started = time.perf_counter()
            engine.checkpoint()
            checkpoint_seconds = time.perf_counter() - checkpoint_started
            engine.stop()
            after = folder_bytes(directory)
            save({**common, "event": "ingest", "ingest_seconds": elapsed, "stored_count": count,
                  "storage_before": before, "storage_after": after, "checkpoint_seconds": checkpoint_seconds,
                  "incremental_logical_bytes": after["logical_bytes"] - before["logical_bytes"],
                  "table_and_indexes_bytes": table_bytes, "query_plans": plans})
            logging.info("INGEST %s %.3fs count=%s", case, elapsed, count)
            engine.start()
            for pass_no in range(settings["warm_repeats"] + 1):
                jobs = workloads()
                random.Random(143 + repetition - 1 + pass_no).shuffle(jobs)
                for w in jobs:
                    params = [int(w["sensor"].rsplit('/', 1)[-1])] + ([w["start"], w["end"]] if w["start"] else [])
                    started = time.perf_counter()
                    values = engine.connection.execute(query_for(w), params).fetchall()
                    received = time.perf_counter()
                    records = [(o, s, p, t.isoformat(), float(v)) for o, s, p, t, v in values]
                    finished = time.perf_counter()
                    digest = record_digest(records)
                    assert len(records) == expected[w["id"]]["count"] and digest == expected[w["id"]]["digest"], w["id"]
                    save({**common, "event": "fetch", "workload": w["id"], "pass": pass_no,
                          "cache_phase": "first_pass_after_restart" if pass_no == 0 else "warm",
                          "query_and_transfer_seconds": received - started, "decode_and_expand_seconds": finished - received,
                          "fetch_seconds": finished - started, "response_bytes": None,
                          "returned_rows_or_snippets": len(records), "reading_count": len(records), "digest": digest, "verified": True})
                    logging.info("FETCH %s %s pass=%s %.3fs verified=%s", case, w["id"], pass_no, finished - started, len(records))
            engine.stop()
            save({**common, "event": "complete"})
            if repetition < settings["repetitions"]:
                shutil.rmtree(directory)
            logging.info("COMPLETE %s", case)
        except Exception as error:
            save({**common, "event": "error", "error": repr(error)})
            raise
        finally:
            engine.stop()


if __name__ == "__main__":
    main()
