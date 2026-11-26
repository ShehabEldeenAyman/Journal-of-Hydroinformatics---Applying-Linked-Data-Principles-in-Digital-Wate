"""Sequential isolated PostgreSQL/Oxigraph/Virtuoso benchmark with result checks."""
import argparse
import csv
import hashlib
import importlib.metadata
import json
import logging
import os
import platform
import random
import shutil
import signal
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
import psycopg
import pyodbc
import requests

from prepare_data import canonical_records, record_digest

BASE = ROOT
INPUTS = ROOT / "data"
# Install database engines separately; paths can be overridden without edits.
RUNTIME = Path(os.environ.get("HYDRO_RUNTIME", ROOT / ".runtime")).expanduser().resolve()
PG = Path(os.environ.get("HYDRO_PG_BIN", ROOT / "tools/postgresql-19.0/bin")).expanduser().resolve()
VIRT = Path(os.environ.get("HYDRO_VIRTUOSO_HOME", ROOT / "tools/virtuoso-opensource")).expanduser().resolve()
OXI = Path(os.environ.get("HYDRO_OXIGRAPH", shutil.which("oxigraph") or ROOT / "tools/oxigraph")).expanduser().resolve()
VIRT_ODBC = Path(os.environ.get("HYDRO_VIRTUOSO_ODBC", VIRT / "lib/virtodbcu.so")).expanduser().resolve()
GRAPH = "https://example.org/dessel-benchmark/graph"
SOSA = "http://www.w3.org/ns/sosa/"
TSS = "https://w3id.org/tss#"
CPU_SET = set(sorted(os.sched_getaffinity(0))[:4])


def restrict_cpu():
    os.sched_setaffinity(0, CPU_SET)


def run(command, **kwargs):
    return subprocess.run([str(x) for x in command], check=True, text=True,
                          capture_output=True, preexec_fn=restrict_cpu, **kwargs)


def wait_port(port, process, timeout=90):
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        if process.poll() is not None:
            raise RuntimeError(f"Database exited with status {process.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.2)
    raise TimeoutError(f"Database did not listen on {port}")


def folder_bytes(folder):
    logical = allocated = 0
    for p in folder.rglob("*"):
        if p.is_file():
            stat = p.stat()
            logical += stat.st_size
            allocated += stat.st_blocks * 512
    return {"logical_bytes": logical, "allocated_bytes": allocated}


def virtuoso_ini():
    return f'''[Database]
DatabaseFile = virtuoso.db
ErrorLogFile = virtuoso.log
LockFile = virtuoso.lck
TransactionFile = virtuoso.trx
xa_persistent_file = virtuoso.pxa
ErrorLogLevel = 7
FileExtend = 200
MaxCheckpointRemap = 2000
Striping = 0
TempStorage = TempDatabase
[TempDatabase]
DatabaseFile = virtuoso-temp.db
TransactionFile = virtuoso-temp.trx
MaxCheckpointRemap = 2000
Striping = 0
[Parameters]
ServerPort = 127.0.0.1:11129
DisableUnixSocket = 1
MaxClientConnections = 10
CheckpointInterval = 0
SchedulerInterval = 0
O_DIRECT = 0
CaseMode = 2
DirsAllowed = .
NumberOfBuffers = 32768
MaxDirtyBuffers = 20000
MaxQueryMem = 1G
ThreadsPerQuery = 4
AsyncQueueMaxThreads = 4
[HTTPServer]
ServerPort = 127.0.0.1:18890
ServerRoot = {VIRT / 'vsp'}
MaxClientConnections = 10
DavRoot = DAV
EnabledDavVSP = 0
MaxKeepAlives = 10000
KeepAliveTimeout = 30
HTTPThreadSize = 280000
Charset = UTF-8
EnabledGzipContent = 0
MaxPostSize = 2000000000
[SPARQL]
ResultSetMaxRows = 1000000
MaxQueryCostEstimationTime = 0
MaxQueryExecutionTime = 1800
'''


class Engine:
    def __init__(self, name, directory, logs):
        self.name, self.directory, self.logs = name, directory, logs
        directory.mkdir(parents=True, exist_ok=True)
        logs.mkdir(parents=True, exist_ok=True)
        self.process = None
        self.connection = None
        self.session = requests.Session()
        self.session.headers.update({"Accept-Encoding": "identity"})
        self.handle = None

    def setup(self):
        if self.name == "postgresql":
            run([PG / "initdb", "-D", self.directory / "data", "-U", "hydrobench", "--auth=trust", "--encoding=UTF8", "--locale=C"])
            config = self.directory / "data/postgresql.conf"
            with config.open("a") as f:
                f.write("\nshared_buffers='256MB'\nwork_mem='32MB'\nmax_parallel_workers_per_gather=2\nmax_parallel_workers=4\nmax_worker_processes=8\ncheckpoint_timeout='30min'\njit=off\n")
        elif self.name == "virtuoso":
            (self.directory / "virtuoso.ini").write_text(virtuoso_ini())
        self.start()
        if self.name == "virtuoso":
            self.connection.cursor().execute('GRANT SPARQL_UPDATE TO "SPARQL"')
        self.checkpoint()

    def start(self):
        self.handle = (self.logs / "server-output.log").open("a")
        if self.name == "postgresql":
            sockdir = Path("/tmp/hydro-pg19-socket")
            sockdir.mkdir(exist_ok=True)
            command = [PG / "postgres", "-D", self.directory / "data", "-h", "127.0.0.1", "-p", "15439", "-k", sockdir]
            port = 15439
        elif self.name == "oxigraph":
            command = [OXI, "serve", "--location", self.directory / "data", "--bind", "127.0.0.1:17878"]
            port = 17878
        else:
            command = [VIRT / "bin/virtuoso-t", "+foreground", "+configfile", self.directory / "virtuoso.ini"]
            port = 18890
        env = os.environ.copy()
        env["RAYON_NUM_THREADS"] = "4"
        self.process = subprocess.Popen([str(x) for x in command], cwd=self.directory,
            stdout=self.handle, stderr=subprocess.STDOUT, env=env, preexec_fn=restrict_cpu)
        wait_port(port, self.process)
        if self.name == "postgresql":
            for attempt in range(100):
                try:
                    self.connection = psycopg.connect(host="127.0.0.1", port=15439, dbname="postgres", user="hydrobench", autocommit=True)
                    break
                except psycopg.OperationalError:
                    time.sleep(0.1)
            if self.connection is None:
                raise RuntimeError("PostgreSQL connection not ready")
        elif self.name == "virtuoso":
            driver = VIRT_ODBC
            last_error = None
            for attempt in range(100):
                try:
                    self.connection = pyodbc.connect(f"DRIVER={{{driver}}};HOST=127.0.0.1:11129;UID=dba;PWD=dba;wideAsUTF16=Y", autocommit=True, timeout=5)
                    break
                except pyodbc.Error as error:
                    last_error = error
                    time.sleep(0.2)
            if self.connection is None:
                raise RuntimeError(f"Virtuoso ODBC connection not ready: {last_error}")
        if self.name != "postgresql":
            self.select("ASK {}")

    def stop(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None
        if self.process and self.process.poll() is None:
            self.process.send_signal(signal.SIGINT if self.name != "virtuoso" else signal.SIGTERM)
            try:
                self.process.wait(timeout=90)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
                raise RuntimeError("Database failed to stop gracefully")
        if self.handle:
            self.handle.close()
            self.handle = None
        self.session.close()
        self.session = requests.Session()
        self.session.headers.update({"Accept-Encoding": "identity"})

    def checkpoint(self):
        if self.name == "postgresql":
            self.connection.execute("CHECKPOINT")
        elif self.name == "virtuoso":
            self.connection.cursor().execute("checkpoint")

    def select(self, query):
        if self.name == "oxigraph":
            url = "http://127.0.0.1:17878/query"
        else:
            url = "http://127.0.0.1:18890/sparql"
        response = self.session.post(url, data={"query": query},
                                    headers={"Accept": "application/sparql-results+json"}, timeout=1800)
        if not response.ok:
            raise RuntimeError(f"{self.name} HTTP {response.status_code}: {response.text[:2000]}")
        if response.headers.get("X-SQL-State") not in (None, "00000"):
            raise RuntimeError(f"Incomplete Virtuoso result: {dict(response.headers)}")
        return response

    def ingest(self, representation):
        started = time.perf_counter()
        if self.name == "postgresql":
            self.connection.execute("CREATE TABLE observations (observation text PRIMARY KEY, sensor text NOT NULL, property text NOT NULL, time timestamp NOT NULL, value double precision NOT NULL)")
            with self.connection.cursor() as cursor:
                with cursor.copy("COPY observations FROM STDIN WITH (FORMAT CSV, HEADER TRUE)") as cp:
                    with (INPUTS / "canonical.csv").open("rb") as f:
                        while data := f.read(1048576):
                            cp.write(data)
            self.connection.execute("CREATE INDEX sensor_time_idx ON observations (sensor, time)")
            self.connection.execute("ANALYZE observations")
        else:
            path = INPUTS / f"{representation}.ttl"
            url = "http://127.0.0.1:17878/store" if self.name == "oxigraph" else "http://127.0.0.1:18890/sparql-graph-crud"
            with path.open("rb") as stream:
                response = self.session.post(url, params={"graph": GRAPH}, data=stream,
                    headers={"Content-Type": "text/turtle", "Accept": "text/plain"}, timeout=1800)
            if not response.ok:
                raise RuntimeError(f"Ingestion HTTP {response.status_code}: {response.text[:2000]}")
        return time.perf_counter() - started

    def count(self):
        if self.name == "postgresql":
            return self.connection.execute("SELECT count(*) FROM observations").fetchone()[0]
        result = self.select(f"SELECT (COUNT(*) AS ?n) WHERE {{ GRAPH <{GRAPH}> {{ ?s ?p ?o }} }}").json()
        return int(result["results"]["bindings"][0]["n"]["value"])


def sparql(workload, snippets):
    sensor, start, end = workload["sensor"], workload["start"], workload["end"]
    prefix = f"PREFIX sosa: <{SOSA}> PREFIX tss: <{TSS}> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"
    if snippets:
        filters = f'FILTER(?from < "{end}"^^xsd:dateTime && ?until > "{start}"^^xsd:dateTime)' if start else ""
        return prefix + f'''SELECT ?snippet ?property ?from ?until ?points WHERE {{ GRAPH <{GRAPH}> {{
          ?snippet a tss:Snippet ; tss:about ?template ; tss:from ?from ; tss:until ?until ; tss:points ?points .
          ?template sosa:madeBySensor <{sensor}> ; sosa:observedProperty ?property .
          {filters}
        }} }} ORDER BY ?from'''
    filters = f'FILTER(?time >= "{start}"^^xsd:dateTime && ?time < "{end}"^^xsd:dateTime)' if start else ""
    return prefix + f'''SELECT ?observation ?property ?time ?value WHERE {{ GRAPH <{GRAPH}> {{
      ?observation a sosa:Observation ; sosa:madeBySensor <{sensor}> ; sosa:observedProperty ?property ; sosa:resultTime ?time ; sosa:hasSimpleResult ?value .
      {filters}
    }} }} ORDER BY ?time ?observation'''


def fetch(engine, workload, representation):
    started = time.perf_counter()
    if engine.name == "postgresql":
        query = "SELECT observation, sensor, property, time, value FROM observations WHERE sensor=%s"
        params = [workload["sensor"]]
        if workload["start"]:
            query += " AND time >= %s AND time < %s"
            params += [workload["start"], workload["end"]]
        query += " ORDER BY time, observation"
        values = engine.connection.execute(query, params).fetchall()
        received = time.perf_counter()
        records = [(o, s, p, t.isoformat(), float(v)) for o, s, p, t, v in values]
        network_bytes = None
        returned_units = len(values)
    else:
        snippets = representation.startswith("tss_")
        response = engine.select(sparql(workload, snippets))
        received = time.perf_counter()
        network_bytes = len(response.content)
        bindings = response.json()["results"]["bindings"]
        returned_units = len(bindings)
        sensor = workload["sensor"]
        records = []
        if snippets:
            for row in bindings:
                prop = row["property"]["value"]
                for point in json.loads(row["points"]["value"]):
                    timestamp = point["time"]
                    if workload["start"] and not (workload["start"] <= timestamp < workload["end"]):
                        continue
                    records.append((point["id"], sensor, prop, timestamp, float(point["value"])))
        else:
            records = [(r["observation"]["value"], sensor, r["property"]["value"],
                        datetime.fromisoformat(r["time"]["value"]).isoformat(), float(r["value"]["value"])) for r in bindings]
    finished = time.perf_counter()
    return records, {"query_and_transfer_seconds": received - started,
        "decode_and_expand_seconds": finished - received, "fetch_seconds": finished - started,
        "response_bytes": network_bytes, "returned_rows_or_snippets": returned_units}


def workloads():
    entries = []
    for sid, kind in [("73617010", "stage"), ("73626010", "discharge")]:
        for label, start, end in [("full", None, None), ("2025", "2025-01-01T00:00:00", "2026-01-01T00:00:00")]:
            entries.append({"id": f"{label}_{kind}", "sensor": f"http://example.com/waterinfo/{sid}", "start": start, "end": end})
    entries.append({"id": "one_day_stage", "sensor": "http://example.com/waterinfo/73617010", "start": "2025-11-06T00:00:00", "end": "2025-11-07T00:00:00"})
    return entries


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--engines", nargs="+", default=["postgresql", "oxigraph", "virtuoso"])
    p.add_argument("--representations", nargs="+", default=["plain_sosa", "ldes_sosa", "tss_ldes_15m", "tss_ldes_60m", "tss_ldes_360m", "tss_ldes_720m", "tss_ldes_1440m"])
    p.add_argument("--repetitions", type=int, default=3)
    p.add_argument("--warm-repeats", type=int, default=3)
    p.add_argument("--run-name", default=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    p.add_argument("--keep-all-databases", action="store_true")
    args = p.parse_args()
    output = BASE / "results" / args.run_name
    output.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", handlers=[logging.FileHandler(output / "execution.log", mode="a"), logging.StreamHandler()])
    source, _ = canonical_records()
    manifest = json.loads((INPUTS / "manifest.json").read_text())
    expected = {}
    for w in workloads():
        rows = [r for r in source if r[1] == w["sensor"] and (not w["start"] or w["start"] <= r[3] < w["end"])]
        expected[w["id"]] = {"count": len(rows), "digest": record_digest(rows)}
    virt_version = subprocess.run([str(VIRT / "bin/virtuoso-t"), "+version"], text=True, capture_output=True)
    virt_text = (virt_version.stdout + virt_version.stderr).split("Usage:")[0].strip()
    if "7.2.17" not in virt_text:
        raise RuntimeError(f"Unexpected Virtuoso version: {virt_text}")
    versions = {"oxigraph": run([OXI, "--version"]).stdout.strip(),
                "virtuoso": virt_text,
                "postgresql": run([PG / "postgres", "--version"]).stdout.strip() if PG.exists() else None}
    environment = {"arguments": vars(args), "versions": versions, "python": sys.version,
        "platform": platform.platform(), "cpu_affinity_for_databases": sorted(CPU_SET),
        "cpuinfo": Path("/proc/cpuinfo").read_text(), "memory": Path("/proc/meminfo").read_text(),
        "packages": {name: importlib.metadata.version(name) for name in ["psycopg", "requests", "pyodbc"]},
        "expected_results": expected, "input_manifest": manifest,
        "protocol": {"load": "Fresh database per repetition; SQL COPY plus primary/index creation and ANALYZE versus same complete local Turtle HTTP POST for both RDF engines. Preparation, database initialization, checkpoint, restart, and validation hashes are outside load/fetch timings.",
                     "fetch": "Native SQL/TCP versus SPARQL/HTTP JSON; includes complete response and client decoding. TSS includes JSON expansion and point filtering. Five returned core fields are hashed against source; hashes computed outside timings.",
                     "cache": "Server restarted after checkpoint and before first pass. OS page cache is not dropped. First-pass timings are not claimed to be OS-cold; subsequent randomized passes are warm.",
                     "storage": "Logical and allocated fresh database directory sizes, before and after load; includes engine files and transaction logs. Serialized input size reported separately. Database directories retained only for final repetition by default.",
                     "scope": manifest["scope"]}}
    environment["installation"] = json.loads((ROOT / "database_versions.json").read_text())
    environment["runner_sha256"] = {p.name: hashlib.file_digest(p.open("rb"), "sha256").hexdigest() for p in [Path(__file__), Path(__file__).with_name("prepare_data.py")]}
    environment["source_repositories"] = json.loads((ROOT / "provenance.json").read_text())["source_repositories"]
    environment["database_paths"] = {"postgresql_bin": str(PG), "virtuoso_home": str(VIRT), "virtuoso_odbc": str(VIRT_ODBC), "oxigraph": str(OXI)}
    (output / "environment.json").write_text(json.dumps(environment, indent=2))
    (output / "queries").mkdir(exist_ok=True)
    for w in workloads():
        for style in ["sosa", "tss"]:
            (output / "queries" / f"{w['id']}_{style}.rq").write_text(sparql(w, style == "tss"))
    results_file = output / "measurements.jsonl"
    existing = []
    if results_file.exists():
        existing = [json.loads(line) for line in results_file.read_text().splitlines()]
    completed = {(r["engine"], r["representation"], r["repetition"]) for r in existing if r["event"] == "complete"}
    def save(record):
        record["recorded_at_utc"] = datetime.now(timezone.utc).isoformat()
        with results_file.open("a") as f:
            f.write(json.dumps(record) + "\n")
    for repetition in range(args.repetitions):
        cases = [(e, r) for e in args.engines for r in (["relational"] if e == "postgresql" else args.representations)]
        random.Random(20261125 + repetition).shuffle(cases)
        for name, representation in cases:
            key = (name, representation, repetition + 1)
            if key in completed:
                continue
            case = f"{name}_{representation}_rep{repetition + 1}"
            directory = RUNTIME / "runs" / args.run_name / case
            if directory.exists():
                shutil.rmtree(directory)
            engine = Engine(name, directory, output / "services" / case)
            common = {"engine": name, "representation": representation, "repetition": repetition + 1}
            logging.info("START %s", case)
            try:
                engine.setup()
                engine.stop()
                before = folder_bytes(directory)
                engine.start()
                elapsed = engine.ingest(representation)
                count = engine.count()
                expected_count = len(source) if name == "postgresql" else manifest["representations"][representation]["triples"]
                if count != expected_count:
                    raise AssertionError(f"Stored count {count} != expected {expected_count}")
                extra = {}
                if name == "postgresql":
                    extra["table_and_indexes_bytes"] = engine.connection.execute("SELECT pg_total_relation_size('observations')").fetchone()[0]
                    extra["index_definitions"] = engine.connection.execute("SELECT indexdef FROM pg_indexes WHERE tablename='observations'").fetchall()
                    extra["query_plans"] = {}
                    for w in workloads():
                        extra["query_plans"][w["id"]] = engine.connection.execute("EXPLAIN (FORMAT JSON) SELECT * FROM observations WHERE sensor=%s" + (" AND time >= %s AND time < %s" if w["start"] else "") + " ORDER BY time, observation", [w["sensor"]] + ([w["start"], w["end"]] if w["start"] else [])).fetchone()[0]
                checkpoint_started = time.perf_counter()
                engine.checkpoint()
                checkpoint_seconds = time.perf_counter() - checkpoint_started
                engine.stop()
                after = folder_bytes(directory)
                save({**common, "event": "ingest", "ingest_seconds": elapsed, "stored_count": count,
                      "checkpoint_seconds": checkpoint_seconds, "storage_before": before, "storage_after": after,
                      "incremental_logical_bytes": after["logical_bytes"] - before["logical_bytes"], **extra})
                logging.info("INGEST %s %.3fs count=%s directory=%.2f MB", case, elapsed, count, after["logical_bytes"] / 1e6)
                engine.start()
                for warm_pass in range(args.warm_repeats + 1):
                    jobs = workloads()
                    random.Random(143 + repetition + warm_pass).shuffle(jobs)
                    for w in jobs:
                        rows, timing = fetch(engine, w, representation)
                        digest = record_digest(rows)
                        reference = expected[w["id"]]
                        if len(rows) != reference["count"] or digest != reference["digest"]:
                            sample = {"workload": w, "returned_count": len(rows), "digest": digest, "expected": reference, "sample": rows[:3]}
                            (output / f"{case}_mismatch.json").write_text(json.dumps(sample, indent=2))
                            raise AssertionError(f"Result mismatch: {case} {w['id']} count={len(rows)} expected={reference['count']}")
                        save({**common, "event": "fetch", "workload": w["id"],
                            "cache_phase": "first_pass_after_restart" if warm_pass == 0 else "warm", "pass": warm_pass,
                            "reading_count": len(rows), "digest": digest, "verified": True, **timing})
                        logging.info("FETCH %s %s pass=%s %.3fs verified=%s", case, w["id"], warm_pass, timing["fetch_seconds"], len(rows))
                engine.stop()
                save({**common, "event": "complete"})
                completed.add(key)
                if not args.keep_all_databases and repetition < args.repetitions - 1:
                    shutil.rmtree(directory)
                logging.info("COMPLETE %s", case)
            except Exception as error:
                save({**common, "event": "error", "error": repr(error)})
                logging.exception("FAILED %s", case)
                raise
            finally:
                engine.stop()
    logging.info("ALL BENCHMARKS COMPLETE: %s", output)


if __name__ == "__main__":
    main()
