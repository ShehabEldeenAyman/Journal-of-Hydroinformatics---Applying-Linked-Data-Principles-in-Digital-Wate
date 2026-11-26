# Applying Linked Data Principles in Digital Water

Data and Python code accompanying the Journal of Hydroinformatics paper,
*Applying Linked Data Principles in Digital Water: A Modular Framework for
Semantic Integration and FAIR-oriented Data Sharing*.

## What to install

There are two workflows:

| Workflow | Required software | Result |
|---|---|---|
| Transform the historical source data | Python 3.12+, Python requirements, Java 21+, RMLMapper 8.1.0 | SOSA RDF, SHACL reports, snippets and LDES/TREE files |
| Reproduce the database benchmark | Python requirements, PostgreSQL 19.0, Oxigraph CLI 0.5.13, Virtuoso Open Source 7.2.17, unixODBC | Load/storage/retrieval measurements and reports |

The transformation workflow writes local files and needs no running database.
The benchmark runner creates fresh databases, starts their processes, loads the
prepared inputs, executes the queries, and stops the processes. Install the
executables; do not start separate database services for the benchmark.
The Python `pyoxigraph` package is used for local correctness checks; the
separate Oxigraph **CLI executable** is also required for the database benchmark.
Fuseki, Neo4j, MySQL and an EYE reasoner are not dependencies of these scripts.

The commands below target **Linux x86-64** with Bash and Debian/Ubuntu package
names. The benchmark uses Linux CPU-affinity and `/proc` interfaces; on Windows
use an x86-64 Linux environment such as WSL2. Other architectures need matching
binaries. The original benchmark used Python 3.12.14; the pipeline adapter was
checked with RMLMapper 8.1.0 and Java 25. RMLMapper 8.1.0 targets Java 21.
See its [versioned build configuration](https://github.com/RMLio/rmlmapper-java/blob/v8.1.0/pom.xml).

## 1. Prepare the environment

Open a terminal **in this repository's root directory**. Keep this directory
in `HYDRO_REPO` so later installation commands work from any subdirectory:

```bash
export HYDRO_REPO="$(pwd)"
mkdir -p tools/downloads tools/src
```

Install system dependencies. Only package installation needs `sudo`; run the
Python programs and database processes as your ordinary user:

```bash
sudo apt-get update
sudo apt-get install -y python3 python3-venv python3-dev openjdk-21-jre-headless \
  curl ca-certificates tar bzip2 build-essential pkg-config bison flex \
  zlib1g-dev unixodbc unixodbc-dev
```

Create the Python environment and install the recorded packages:

```bash
python3 --version
java -version
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip check
```

Check that Python is at least 3.12 and Java at least 21. If using a different
installed Python, substitute its executable in the `venv` command. In each new
terminal, return to the repository, set `HYDRO_REPO` again and activate `.venv`.
`tools/`, `.venv/`, `.runtime/` and `results/` are ignored by Git.

## 2. Install RMLMapper

Download the executable **all-dependencies JAR**, rather than a source archive,
from the [RMLMapper 8.1.0 release](https://github.com/RMLio/rmlmapper-java/releases/tag/v8.1.0):

```bash
curl --fail --location \
  https://github.com/RMLio/rmlmapper-java/releases/download/v8.1.0/rmlmapper-8.1.0-r380-all.jar \
  --output tools/rmlmapper.jar
sha256sum tools/rmlmapper.jar
java -jar tools/rmlmapper.jar --help
```

The expected SHA256 is:

```text
819371d49ca47d8ffddae0f34e95f38e8eaaf588ee023e3c2c7527a14d302f58
```

The JAR stays under `tools/`; it does not belong in `code/`. Its download URL
and checksum are also recorded in `provenance.json`. It runs as a command-line
program and does not listen on a port.

## 3. Run the transformation pipeline

```text
source CSV → preprocessing → RML/SOSA mapping → SHACL validation
           → RDFTSS compaction → daily LDES/TREE publication files
```

Start with a small demonstration containing 100 readings per sensor:

```bash
python code/run_pipeline.py --rmlmapper "$HYDRO_REPO/tools/rmlmapper.jar" \
  --limit-per-sensor 100 --output results/pipeline-demo
```

This demonstration has been checked through all five stages: 200 observations
become four daily snippets in two daily fragments, with exact reconstruction
of the source identifiers, timestamps and values. A malformed string-typed
result was also checked and rejected by SHACL.

To process all 350,120 valid observations, omit the limit:

```bash
python code/run_pipeline.py --rmlmapper "$HYDRO_REPO/tools/rmlmapper.jar" \
  --output results/transformation
```

Full-source RDF mapping and SHACL validation need substantially more time and
memory than the demonstration; full-dataset execution of this adapter has not
been verified. Stage outputs are:

| Output under the selected directory | Purpose |
|---|---|
| `canonical.csv`, `preprocessing.json` | Cleaned five-field records, exclusions and source hash |
| `mapping.rml.ttl`, `rmlmapper.log`, `observations.ttl` | Executed RML mapping, mapper output and SOSA RDF |
| `shacl_report.txt`, `shacl_report.ttl` | Text and RDF validation reports |
| `tss_ldes_1440m.ttl` | Default daily snippet representation |
| `ldes/index.ttl`, `ldes/days/*.ttl` | TREE index and daily publication fragments |
| `*_stage.json` | Per-stage completion details |

Use `--minutes 15`, `60`, `360`, `720` or `1440` to select a snippet window.
The default is 1440 minutes. To rerun one stage, use `--stage preprocess`,
`map`, `validate`, `compact` or `publish` with the **same output directory**;
its preceding stage files must already exist. For example:

```bash
python code/run_pipeline.py --stage validate --output results/pipeline-demo
```

Serve the demonstration's publication files in a separate terminal:

```bash
python -m http.server 8000 --bind 127.0.0.1 --directory results/pipeline-demo/ldes
```

Open [the local TREE index](http://localhost:8000/index.ttl); its links point to
files under `days/`. Stop this server with Ctrl+C. If using another hosting URL,
regenerate with `--base-url https://your-host.example/path/` before serving.
Publishing creates local files; hosting and continuous updates are separate.

### Mapping and validation resources

`data/pipeline_resources/dessel.rml.ttl` maps the cleaned observation, sensor,
property, time and value fields. The runner replaces its CSV source path with
the actual output path. `shapes.ttl` and `sensor_context.ttl` are copied from
the paper's worked example and supply observation constraints and explicit
stage/discharge sensor classifications. The original Dessel mapping is retained
as `original_dessel_mapping.rml.ttl` for provenance; it emits literal sensor and
property terms and is not used for the revised five-field profile.

This entry point is a **standalone adapter added when assembling the repository**.
It reuses the benchmark canonicalisation and snippet encoding. The reported
performance experiment instead generated controlled representations directly
with `prepare_data.py`; its timings exclude RML mapping, SHACL and publication.
The adapter preserves source timestamps without assigning a time zone and keeps
stage in metres and discharge in m³/s. Conductivity conversion, live acquisition
and client synchronisation are outside this historical-export workflow.

## 4. Install the database engines for the benchmark

Install all three engines below for the complete comparison. Their release
versions, archive hashes and original configuration details are in
`database_versions.json`. These versions reproduce the study environment;
they are not an instruction to substitute whatever release is newest.
The commands install inside this repository's ignored `tools/` directory.

### PostgreSQL 19.0 — relational database

Build the pinned release with the same ICU/readline options as the experiment.
The [official installation procedure](https://www.postgresql.org/docs/19/install-make.html)
explains the configure, build and installation steps.

```bash
curl --fail --location \
  https://ftp.postgresql.org/pub/source/v19.0/postgresql-19.0.tar.bz2 \
  --output tools/downloads/postgresql-19.0.tar.bz2
sha256sum tools/downloads/postgresql-19.0.tar.bz2
tar -xjf tools/downloads/postgresql-19.0.tar.bz2 -C tools/src
cd tools/src/postgresql-19.0
./configure --prefix="$HYDRO_REPO/tools/postgresql-19.0" --without-icu --without-readline
make -j4
make install
cd "$HYDRO_REPO"
```

Expected archive SHA256:

```text
a89fd70efbad6a08c7dbed8d4872e3af30170d9e5ca9dc7906523be95b4e692d
```

`HYDRO_PG_BIN` must point to a directory containing both `postgres` and `initdb`.
An existing PostgreSQL 19.0 installation can be used by setting that variable
instead of building. Do not create a cluster manually: the runner creates its
own clusters and the flat and normalised relational schemas.

### Oxigraph CLI 0.5.13 — RDF graph database

Download the Linux x86-64 executable from the
[Oxigraph 0.5.13 release](https://github.com/oxigraph/oxigraph/releases/tag/v0.5.13):

```bash
curl --fail --location \
  https://github.com/oxigraph/oxigraph/releases/download/v0.5.13/oxigraph_v0.5.13_x86_64_linux_gnu \
  --output tools/oxigraph
sha256sum tools/oxigraph
chmod +x tools/oxigraph
```

Expected executable SHA256:

```text
05f54af6d7ebd73888a600bcf25017ef304c51726428f6dae10d32e9185618be
```

### Virtuoso Open Source 7.2.17 — RDF graph database

Extract the binary distribution from the
[Virtuoso 7.2.17 release](https://github.com/openlink/virtuoso-opensource/releases/tag/v7.2.17):

```bash
curl --fail --location \
  https://github.com/openlink/virtuoso-opensource/releases/download/v7.2.17/virtuoso-opensource.x86_64-generic_glibc25-linux-gnu.tar.gz \
  --output tools/downloads/virtuoso-7.2.17-linux.tar.gz
sha256sum tools/downloads/virtuoso-7.2.17-linux.tar.gz
tar -xzf tools/downloads/virtuoso-7.2.17-linux.tar.gz -C tools
```

Expected archive SHA256:

```text
45385ea8c1940e41d88d628f9318bbc992081c4cd5c4e18b57277b6df516b15b
```

The extracted `tools/virtuoso-opensource/` must contain `bin/virtuoso-t`, `vsp/`
and `lib/virtodbcu.so`. The Python client uses the unixODBC driver manager and
the explicit driver path; registering a system ODBC DSN is not needed. The
runner generates `virtuoso.ini` for each trial, including its HTTP and SQL ports.

### Set database paths and check versions

Run these exports in the same terminal as the benchmark:

```bash
export HYDRO_PG_BIN="$HYDRO_REPO/tools/postgresql-19.0/bin"
export HYDRO_OXIGRAPH="$HYDRO_REPO/tools/oxigraph"
export HYDRO_VIRTUOSO_HOME="$HYDRO_REPO/tools/virtuoso-opensource"
export HYDRO_VIRTUOSO_ODBC="$HYDRO_VIRTUOSO_HOME/lib/virtodbcu.so"
export HYDRO_RUNTIME="$HYDRO_REPO/.runtime"

"$HYDRO_PG_BIN/postgres" --version
"$HYDRO_PG_BIN/initdb" --version
"$HYDRO_OXIGRAPH" --version
"$HYDRO_VIRTUOSO_HOME/bin/virtuoso-t" +version
python -c 'import psycopg, pyodbc, requests, pyoxigraph, pyshacl, rdflib; print("Python clients available")'
```

Check for PostgreSQL 19.0, Oxigraph 0.5.13 and Virtuoso 7.2.17. To use engines
installed elsewhere, change these paths. Database paths are configurable through
environment variables; the benchmark's port numbers are currently set in
`code/run_benchmarks.py`.

### Ports and endpoints

All benchmark connections use `127.0.0.1`. These are the ports in the code,
which differ from common database defaults:

| Service | Port | Endpoint or connection | Started by |
|---|---|---|---|
| PostgreSQL SQL | `15439` | Host `127.0.0.1`, database `postgres`, user `hydrobench` | Benchmark runner |
| Oxigraph HTTP/SPARQL | `17878` | Query `/query`; graph loading `/store` | Benchmark runner |
| Virtuoso HTTP/SPARQL | `18890` | Query `/sparql`; graph loading `/sparql-graph-crud` | Benchmark runner |
| Virtuoso SQL/ODBC | `11129` | Host `127.0.0.1:11129`, local trial credentials `dba` / `dba` | Benchmark runner |
| Optional LDES file server | `8000` | `http://localhost:8000/index.ttl` | Your `http.server` command |
| RMLMapper | None | `java -jar tools/rmlmapper.jar ...` | Pipeline runner |

The PostgreSQL trial cluster uses local trust authentication. The credentials
above apply to the isolated local experiment. The RDF benchmark named graph is
`https://example.org/dessel-benchmark/graph`.

Before running, check whether a required port is already listening:

```bash
ss -ltnp | awk 'NR == 1 || /:(15439|17878|18890|11129|8000)[[:space:]]/'
```

Only one benchmark process can use these ports at a time. Stop a conflicting
service you started before beginning. If changing benchmark ports, update all
matching listen addresses and client URLs in `code/run_benchmarks.py`; no
`--port` or port environment-variable override is implemented. Benchmark
services are stopped after trials, so their endpoints are not permanent servers.

## 5. Prepare inputs and run the database benchmark

Verify the packaged data and extract the seven RDF inputs:

```bash
python code/verify_data.py --extract
```

The `.ttl.gz` files expand to the exact original RDF payloads; extracted `.ttl`
files are ignored by Git. Expanded inputs need approximately 1 GB of disk space;
database trials need additional space. The runner retains each configuration's
last database by default and removes earlier completed repetitions' databases.

Run the complete experiment from the repository root, with the environment
activated and the database path exports set:

```bash
python code/run_benchmarks.py --repetitions 3 --warm-repeats 3 --run-name reproduction
python code/run_normalized_postgresql.py results/reproduction
python code/summarize.py results/reproduction
```

The normalised PostgreSQL control runs after the primary suite and regenerates
its two CSV inputs. The complete setup produces 48 independent database load
trials and verifies 960 retrievals against source-derived counts and hashes.
The runner restricts database processes to the first four available logical
CPUs. Each database is checkpointed and restarted before one first pass and
three warm query passes. The OS page cache is not cleared. Exact timings depend
on hardware and system load.

For a shorter installation check, use a **different run name**, one repetition,
one warm repeat and just the daily RDF representation:

```bash
python code/run_benchmarks.py --representations tss_ldes_1440m \
  --repetitions 1 --warm-repeats 1 --run-name installation-check
python code/run_normalized_postgresql.py results/installation-check
python code/summarize.py results/installation-check
```

This shorter run still loads the full valid dataset into the three engines;
it reduces configurations and repetitions, and does not reproduce the paper's
complete measurement set. Even with `--engines` selecting a subset, the current
runner probes all three engine versions at startup, so install all three for
these commands.

Inspect `results/reproduction/report.html`, `summary.json`,
`configuration_summary.csv`, `query_summary.csv` and the generated figures.
`measurements.jsonl`, `execution.log`, `queries/` and `services/` hold detailed
evidence. Runtime databases live under `.runtime/runs/<run-name>/`.
Use a new run name for an independent experiment; reusing a name resumes completed
trials. Do not mix settings under the same run name.

To regenerate benchmark representations directly from the source instead of
extracting the supplied inputs:

```bash
python code/prepare_data.py
```

Regeneration overwrites prepared files and `data/manifest.json`; generation
durations change. `data/CHECKSUMS.sha256` describes the supplied files. The
benchmark generator is separate from the RML/SHACL transformation adapter.

### Optional: inspect pipeline RDF in Oxigraph

After installing Oxigraph, start an inspection server in one terminal:

```bash
mkdir -p "$HYDRO_RUNTIME"
"$HYDRO_OXIGRAPH" serve --location "$HYDRO_RUNTIME/inspection" --bind 127.0.0.1:17878
```

In a second terminal at the repository root, load the pipeline demonstration
and count its observations:

```bash
curl --fail --request POST \
  'http://127.0.0.1:17878/store?graph=https%3A%2F%2Fexample.org%2Fdessel-pipeline%2Fgraph' \
  --header 'Content-Type: text/turtle' \
  --data-binary @results/pipeline-demo/observations.ttl
curl --fail 'http://127.0.0.1:17878/query' \
  --header 'Accept: application/sparql-results+json' \
  --data-urlencode 'query=SELECT (COUNT(*) AS ?n) WHERE { GRAPH <https://example.org/dessel-pipeline/graph> { ?s a <http://www.w3.org/ns/sosa/Observation> } }'
```

For the 100-per-sensor demonstration the count is 200. Stop the inspection
server with Ctrl+C before running the benchmark, since it uses the same port.

## 6. Reproduce the stage/discharge illustration

With the Python requirements installed and benchmark RDF extracted:

```bash
python code/audit_cross_parameter.py
```

This local check needs no database server. It verifies 101 paired timestamps
from 6 November 2025 at 10:45 through 7 November 2025 at 11:45, inclusive, against
an independent alignment of the source CSV. The query, aligned CSV, audit and
Figure 3 PDF are written to `results/cross_parameter/`. It is separate from the
timed benchmark and does not validate hydraulic causes or event detection.

## Troubleshooting

| Symptom | Action |
|---|---|
| `UnsupportedClassVersionError` | Select Java 21 or later; check `java -version`. |
| Mapper cannot find the JAR | Check `tools/rmlmapper.jar`; pass its absolute path with `--rmlmapper`. |
| Python import error | Activate `.venv`, reinstall `requirements.txt`, then run `python -m pip check`. |
| `libodbc.so.2` missing | Install the system unixODBC driver manager, then retry `import pyodbc`. |
| Virtuoso ODBC library cannot be opened | Check `HYDRO_VIRTUOSO_ODBC` and run `ldd "$HYDRO_VIRTUOSO_ODBC"` for missing libraries. |
| Virtuoso driver reports an executable-stack error | Apply the compatibility step below to a copy of the driver. |
| `initdb` cannot run as root | Run the benchmark as your ordinary user. |
| Database exits or a port is in use | Check `ss -ltnp` and the trial's `results/<run-name>/services/` logs. |
| `.ttl` file missing | Run `python code/verify_data.py --extract`. |
| SHACL stage fails | Read `shacl_report.txt`; a failed validation stops the pipeline. |
| Large pipeline run exceeds available memory | Check the 100-per-sensor demonstration first; full-source RDF/SHACL needs more memory. |
| Checksum fails after regeneration | The checksum file records the packaged originals; compare regenerated payload hashes with the original manifest. |

On modern glibc, the distributed Virtuoso ODBC driver may contain an obsolete
executable-stack flag. The recorded experiment cleared that flag in a **copy**
of the client library; the database server binary stayed unchanged. With a
`patchelf` build that supports `--clear-execstack`:

```bash
cp "$HYDRO_VIRTUOSO_HOME/lib/virtodbcu.so" "$HYDRO_VIRTUOSO_HOME/lib/compat-virtodbcu.so"
patchelf --clear-execstack "$HYDRO_VIRTUOSO_HOME/lib/compat-virtodbcu.so"
export HYDRO_VIRTUOSO_ODBC="$HYDRO_VIRTUOSO_HOME/lib/compat-virtodbcu.so"
```

If that option is unavailable, install a supporting release from the
[patchelf project](https://github.com/NixOS/patchelf). The flag and its operation
are described in the [upstream manual](https://github.com/NixOS/patchelf/blob/master/patchelf.1).
The original experiment's compatible-driver hash is in `database_versions.json`.

## Repository contents and provenance

- `data/`: original Waterinfo CSV, flat and normalised relational CSVs, seven
  compressed RDF inputs, mapping/SHACL resources, input manifest and checksums.
- `code/`: Python scripts for the pipeline, benchmark and correctness checks.
- `requirements.txt`: recorded Python package versions.
- `database_versions.json`: database downloads, versions, hashes and build details.
- `provenance.json`: source commits, original query expectations, adaptation
  details and the pipeline demonstration's verification record.

The source contains 350,592 rows from 7 November 2020 through 7 November 2025:
350,120 finite readings and 472 missing values. Series `73617010` is stage in
metres; `73626010` is discharge in m³/s. The source's `Runoff Value` is discharge
expressed in L/s, not an independent runoff series. Timestamps have no declared
time zone. The evaluated profile contains observation IRI, sensor IRI, property
IRI, timestamp and double value; wider contextual metadata is outside it.

The unchanged source CSV comes from the pinned
[Dessel source repository](https://github.com/ShehabEldeenAyman/Mol_sluis_Dessel_Usecase/tree/838e016d9b3a45df73d78a496de7330d59380d40).
Observations originate from VMM/HIC through Waterinfo. Source and derived-file
hashes identify the experimental inputs; this repository assigns no new licence
to the source data. The snippet encoding follows the study's prototype convention.
