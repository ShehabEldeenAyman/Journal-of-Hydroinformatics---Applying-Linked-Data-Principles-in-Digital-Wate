"""Reproduce the illustrative stage/discharge join and Figure 3.

This checks data retrieval and alignment, not hydraulic causes or event detection.
Run with requirements.txt installed in a Python
environment. The audit records its own versions separately from benchmark runs.
"""
import csv
import hashlib
import importlib.metadata
import json
import math
import platform
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter
from pyoxigraph import RdfFormat, Store


PROJECT = Path(__file__).resolve().parents[1]
ROOT = PROJECT
SOURCE = PROJECT / "data/Mol_Sluis_Dessel_data.csv"
INPUTS = PROJECT / "data"
OUT = PROJECT / "results/cross_parameter"
START, END = "2025-11-06T10:45:00", "2025-11-07T11:45:00"
SENSORS = {"73617010": "stage_m", "73626010": "discharge_m3_s"}
QUERY = f"""PREFIX sosa: <http://www.w3.org/ns/sosa/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT ?time ?stage ?discharge WHERE {{
  ?stageObservation a sosa:Observation ;
    sosa:madeBySensor <http://example.com/waterinfo/73617010> ;
    sosa:observedProperty <http://example.com/property/river-stage> ;
    sosa:resultTime ?time ; sosa:hasSimpleResult ?stage .
  ?dischargeObservation a sosa:Observation ;
    sosa:madeBySensor <http://example.com/waterinfo/73626010> ;
    sosa:observedProperty <http://example.com/property/river-discharge> ;
    sosa:resultTime ?time ; sosa:hasSimpleResult ?discharge .
  FILTER(?time >= "{START}"^^xsd:dateTime &&
         ?time <= "{END}"^^xsd:dateTime)
}} ORDER BY ?time
"""


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    manifest = json.loads((INPUTS / "manifest.json").read_text())
    rdf = INPUTS / "plain_sosa.ttl"
    assert sha(SOURCE) == manifest["source_sha256"], "Source differs from benchmark input"
    assert sha(rdf) == manifest["representations"]["plain_sosa"]["sha256"]
    source = {sid: {} for sid in SENSORS}
    missing = {sid: 0 for sid in SENSORS}
    with SOURCE.open(newline="", encoding="utf-8") as stream:
        for row in csv.DictReader(stream):
            sid, timestamp = row["ts_id"], row["Timestamp"]
            if sid not in source or not START <= timestamp <= END:
                continue
            try:
                value = float(row["Value"])
            except (ValueError, TypeError):
                value = math.nan
            if not math.isfinite(value):
                missing[sid] += 1
                continue
            assert timestamp not in source[sid], "Duplicate sensor/timestamp"
            source[sid][timestamp] = value
    times = sorted(set.intersection(*(set(s) for s in source.values())))
    expected = [(t, source["73617010"][t], source["73626010"][t]) for t in times]
    grid = []
    current, end = datetime.fromisoformat(START), datetime.fromisoformat(END)
    while current <= end:
        grid.append(current.isoformat())
        current += timedelta(minutes=15)
    assert times == grid and not any(missing.values()), "Window no longer complete"

    # Run on the actual, hash-checked five-field RDF input used by the benchmark.
    # This extra correctness check is not a new timing measurement.
    store = Store()
    store.bulk_load(path=str(rdf), format=RdfFormat.TURTLE)
    actual = [(r["time"].value, float(r["stage"].value), float(r["discharge"].value))
              for r in store.query(QUERY)]
    assert actual == expected, "SPARQL join differs from source CSV alignment"
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "join.rq").write_text(QUERY)
    with (OUT / "aligned_observations.csv").open("w", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["source_timestamp", "stage_m", "discharge_m3_s"])
        writer.writerows(actual)

    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10,
                         "pdf.fonttype": 42, "ps.fonttype": 42})
    fig, axes = plt.subplots(2, 1, figsize=(7.0, 4.8), sharex=True,
                             layout="constrained")
    dates = [datetime.fromisoformat(row[0]) for row in actual]
    discharge = [row[2] for row in actual]
    stage = [row[1] for row in actual]
    for axis, values, label, panel in zip(
        axes, [discharge, stage], [r"Discharge (m$^3$/s)", "Stage (m)"], ["(a)", "(b)"]
    ):
        axis.plot(dates, values, color="#174b70", lw=1.15, marker="o", ms=2.2)
        axis.set_ylabel(label)
        axis.grid(axis="y", alpha=0.25, lw=0.6)
        axis.spines[["top", "right"]].set_visible(False)
        axis.set_title(panel, loc="left", fontsize=10, pad=4)
        axis.set_xlim(dates[0], dates[-1])
        formatter = ScalarFormatter(useOffset=False)
        formatter.set_scientific(False)
        axis.yaxis.set_major_formatter(formatter)
    axes[0].axhline(0, color="0.35", lw=0.7, ls="--")
    axes[0].set_ylim(-40, 40)
    axes[1].set_ylim(29.62, 29.84)
    axes[1].xaxis.set_major_locator(mdates.HourLocator(byhour=[0, 6, 12, 18]))
    axes[1].xaxis.set_major_formatter(mdates.DateFormatter("%d Nov\n%H:%M"))
    axes[1].set_xlabel("2025 source timestamp (time zone unspecified)")
    figure = OUT / "fig3_cross_parameter.pdf"
    fig.savefig(figure, metadata={"CreationDate": None, "ModDate": None})
    plt.close(fig)

    report = {
        "purpose": "Illustrative exact-timestamp join; no causal or anomaly-detection validation",
        "source": str(SOURCE.relative_to(PROJECT)), "source_sha256": sha(SOURCE),
        "rdf": str(rdf.relative_to(PROJECT)), "rdf_sha256": sha(rdf),
        "window_start_inclusive": START, "window_end_inclusive": END,
        "selection": "Retrospective reproduction of the manuscript's Figure 3 window; not an independently labelled event",
        "timestamp_policy": "Source timestamps retained without assigning a time zone; exact equality join; no resampling or interpolation",
        "filter": "Finite Value entries only; source quality flags not independently validated or used as event labels",
        "sensor_fields": SENSORS, "paired_timestamps": len(actual),
        "nonfinite_values_in_window": missing,
        "unpaired_valid_readings": {sid: len(rows) - len(times) for sid, rows in source.items()},
        "interval_minutes": 15, "all_query_rows_match_csv": actual == expected,
        "stage_min_m": min(stage), "stage_max_m": max(stage),
        "discharge_min_m3_s": min(discharge), "discharge_max_m3_s": max(discharge),
        "discharge_mean_m3_s": sum(discharge) / len(discharge),
        "negative_discharge_count": sum(v < 0 for v in discharge),
        "negative_discharge_percent": 100 * sum(v < 0 for v in discharge) / len(discharge),
        "independent_operation_records": False, "rainfall_raw_series_evaluated": False,
        "versions": {"python": platform.python_version(),
                     **{p: importlib.metadata.version(p) for p in ["pyoxigraph", "matplotlib"]}},
        "generated_sha256": {str(p.relative_to(ROOT)): sha(p) for p in
                             [OUT / "join.rq", OUT / "aligned_observations.csv", figure,
                              Path(__file__).resolve()]},
    }
    (OUT / "audit.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
