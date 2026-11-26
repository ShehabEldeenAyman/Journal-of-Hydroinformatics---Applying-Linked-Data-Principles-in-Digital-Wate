"""Aggregate completed benchmark trials into CSV tables and an HTML report."""
import argparse
import base64
import csv
import html
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

INPUTS = Path(__file__).resolve().parents[1] / "data"

LABELS = {"relational": "Flat relational", "normalized_relational": "Normalized relational", "plain_sosa": "SOSA", "ldes_sosa": "LDES + SOSA",
          "tss_ldes_15m": "TSS + LDES 15 min", "tss_ldes_60m": "TSS + LDES 1 h",
          "tss_ldes_360m": "TSS + LDES 6 h", "tss_ldes_720m": "TSS + LDES 12 h",
          "tss_ldes_1440m": "TSS + LDES 24 h"}


def stats(values):
    return {"n": len(values), "median": statistics.median(values), "mean": statistics.mean(values),
            "sd": statistics.stdev(values) if len(values) > 1 else 0.0,
            "min": min(values), "max": max(values)}


def write_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def table(headers, rows):
    return '<table><thead><tr>' + ''.join(f'<th>{html.escape(str(x))}</th>' for x in headers) + '</tr></thead><tbody>' + ''.join('<tr>' + ''.join(f'<td>{html.escape(str(x))}</td>' for x in row) + '</tr>' for row in rows) + '</tbody></table>'


def main():
    p = argparse.ArgumentParser()
    p.add_argument("directory", type=Path)
    args = p.parse_args()
    out = args.directory
    environment = json.loads((out / "environment.json").read_text())
    raw = [json.loads(line) for line in (out / "measurements.jsonl").read_text().splitlines()]
    completed = {(r["engine"], r["representation"], r["repetition"]) for r in raw if r["event"] == "complete"}
    settings = environment["arguments"]
    configurations = {(e, rep) for e in settings["engines"]
                      for rep in (["relational"] if e == "postgresql" else settings["representations"])}
    configurations.update(("postgresql", rep) for rep in environment.get("additional_postgresql_representations", []))
    expected_keys = {(e, rep, n) for e, rep in configurations for n in range(1, settings["repetitions"] + 1)}
    expected_trials = len(expected_keys)
    if completed != expected_keys:
        raise RuntimeError(f"Benchmark incomplete: {len(completed)} of {expected_trials} independent trials finished")
    valid = [r for r in raw if (r["engine"], r["representation"], r["repetition"]) in completed]
    expected_coordinates = {(w, p) for w in environment["expected_results"] for p in range(settings["warm_repeats"] + 1)}
    for key in sorted(expected_keys):
        rows = [r for r in valid if (r["engine"], r["representation"], r["repetition"]) == key]
        fetches = [r for r in rows if r["event"] == "fetch"]
        if (sum(r["event"] == "ingest" for r in rows) != 1
                or sum(r["event"] == "complete" for r in rows) != 1
                or any(r["event"] == "error" for r in rows)
                or len(fetches) != len(expected_coordinates)
                or {(r["workload"], r["pass"]) for r in fetches} != expected_coordinates):
            raise RuntimeError(f"Incomplete or duplicate measurements for {key}")
        for r in fetches:
            expected = environment["expected_results"][r["workload"]]
            if (not r["verified"] or r["reading_count"] != expected["count"]
                    or r["digest"] != expected["digest"]):
                raise RuntimeError(f"Output verification failed for {key}, {r['workload']}, pass {r['pass']}")
    grouped = defaultdict(list)
    query_groups = defaultdict(list)
    pairs = defaultdict(dict)
    for r in valid:
        if r["event"] == "ingest":
            grouped[(r["engine"], r["representation"])].append(r)
        elif r["event"] == "fetch":
            query_groups[(r["engine"], r["representation"], r["workload"], r["cache_phase"])].append(r)
            if r["workload"] in ("full_stage", "full_discharge"):
                pairs[(r["engine"], r["representation"], r["repetition"], r["cache_phase"], r["pass"])][r["workload"]] = r
    for (engine, representation, rep, phase, pass_no), pair in pairs.items():
        if len(pair) != 2:
            continue
        a, b = pair["full_stage"], pair["full_discharge"]
        r = {"reading_count": a["reading_count"] + b["reading_count"], "verified": a["verified"] and b["verified"]}
        for key in ["fetch_seconds", "query_and_transfer_seconds", "decode_and_expand_seconds"]:
            r[key] = a[key] + b[key]
        query_groups[(engine, representation, "full_both_sensors_sum", phase)].append(r)
    query_summary = []
    for (engine, representation, workload, phase), rows in sorted(query_groups.items()):
        row = {"engine": engine, "representation": representation, "workload": workload, "cache_phase": phase,
               "reading_count": rows[0]["reading_count"], "all_verified": all(r["verified"] for r in rows)}
        row.update({f"fetch_seconds_{k}": v for k, v in stats([r["fetch_seconds"] for r in rows]).items()})
        row["query_transfer_seconds_median"] = statistics.median(r["query_and_transfer_seconds"] for r in rows)
        row["decode_expand_seconds_median"] = statistics.median(r["decode_and_expand_seconds"] for r in rows)
        query_summary.append(row)
    lookups = {(r["engine"], r["representation"], r["workload"], r["cache_phase"]): r for r in query_summary}
    summary = []
    representations = environment["input_manifest"]["representations"]
    for (engine, representation), rows in sorted(grouped.items()):
        row = {"engine": engine, "representation": representation, "reading_count": environment["input_manifest"]["valid_readings"]}
        row.update({f"ingest_seconds_{k}": v for k, v in stats([r["ingest_seconds"] for r in rows]).items()})
        row["stored_count"] = rows[0]["stored_count"]
        row["stored_count_kind"] = "relational_rows" if engine == "postgresql" else "RDF_triples"
        row["snippet_count"] = 0 if engine == "postgresql" else representations[representation]["snippets"]
        row["database_directory_MB_median"] = statistics.median(r["storage_after"]["logical_bytes"] for r in rows) / 1e6
        row["baseline_directory_MB_median"] = statistics.median(r["storage_before"]["logical_bytes"] for r in rows) / 1e6
        row["incremental_directory_MB_median"] = statistics.median(r["incremental_logical_bytes"] for r in rows) / 1e6
        row["allocated_directory_MB_median"] = statistics.median(r["storage_after"]["allocated_bytes"] for r in rows) / 1e6
        row["table_and_indexes_MB_median"] = statistics.median(r["table_and_indexes_bytes"] for r in rows) / 1e6 if engine == "postgresql" else None
        pg_input = "normalized.csv" if representation == "normalized_relational" else "canonical.csv"
        row["serialized_MB"] = representations[representation]["bytes"] / 1e6 if engine != "postgresql" else (INPUTS / pg_input).stat().st_size / 1e6
        if representation == "normalized_relational":
            row["serialized_MB"] += (INPUTS / "normalized_series.csv").stat().st_size / 1e6
        row["gzip_MB"] = representations[representation]["gzip_bytes"] / 1e6 if engine != "postgresql" else None
        for workload in ["full_stage", "full_discharge", "full_both_sensors_sum", "2025_stage", "2025_discharge", "one_day_stage"]:
            q = lookups[(engine, representation, workload, "warm")]
            row[f"{workload}_warm_seconds_median"] = q["fetch_seconds_median"]
        summary.append(row)
    write_csv(out / "configuration_summary.csv", summary)
    write_csv(out / "query_summary.csv", query_summary)
    result = {"completed_trials": len(completed), "verified_query_executions": sum(r["event"] == "fetch" for r in valid),
              "all_results_verified": all(r.get("verified", True) for r in valid), "configuration_summary": summary,
              "query_summary": query_summary}
    (out / "summary.json").write_text(json.dumps(result, indent=2) + "\n")

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    core_keys = [("postgresql", "normalized_relational"), ("postgresql", "relational"), ("oxigraph", "plain_sosa"), ("oxigraph", "ldes_sosa"),
                 ("oxigraph", "tss_ldes_1440m"), ("virtuoso", "plain_sosa"), ("virtuoso", "ldes_sosa"), ("virtuoso", "tss_ldes_1440m")]
    by_key = {(r["engine"], r["representation"]): r for r in summary}
    core = [by_key[key] for key in core_keys if key in by_key]
    colors = {"postgresql": "#46566b", "oxigraph": "#217a80", "virtuoso": "#af6535"}
    names = {"postgresql": "PostgreSQL", "oxigraph": "Oxigraph", "virtuoso": "Virtuoso"}
    labels = [f"{names[r['engine']]} — {LABELS[r['representation']]}" for r in core]
    fig, axes = plt.subplots(1, 3, figsize=(15, 6), sharey=True)
    for ax, metric, title in zip(axes, ["ingest_seconds_median", "full_both_sensors_sum_warm_seconds_median", "database_directory_MB_median"],
                                ["Ingestion (seconds)", "Full-history retrieval (seconds)\nSum of both sensor queries", "Database directory (MB)\nIncludes engine and log files"]):
        bars = ax.barh(range(len(core)), [r[metric] for r in core], color=[colors[r["engine"]] for r in core])
        ax.set_title(title, fontsize=11)
        ax.set_yticks(range(len(core)), labels)
        ax.grid(axis="x", alpha=.2)
        ax.set_axisbelow(True)
        for bar, row in zip(bars, core):
            ax.text(bar.get_width(), bar.get_y()+bar.get_height()/2, f" {row[metric]:.3g}", va="center", fontsize=9)
        ax.margins(x=.22)
    axes[0].invert_yaxis()
    fig.suptitle("Dessel benchmark with latest stable database releases\nMedians of independent loads and verified warm retrievals", fontsize=13)
    fig.tight_layout()
    fig.savefig(out / "benchmark_overview.png", dpi=180, bbox_inches="tight")
    fig.savefig(out / "benchmark_overview.svg", bbox_inches="tight")
    plt.close(fig)
    fig, axes = plt.subplots(1, 3, figsize=(14, 4))
    minutes = [15, 60, 360, 720, 1440]
    for engine in ["oxigraph", "virtuoso"]:
        rows = [by_key[(engine, f"tss_ldes_{m}m")] for m in minutes]
        for ax, key, title in zip(axes, ["serialized_MB", "ingest_seconds_median", "full_both_sensors_sum_warm_seconds_median"],
                                  ["Serialized Turtle (MB)", "Ingestion (seconds)", "Both sensor retrievals (seconds)"]):
            if key == "serialized_MB":
                if engine == "oxigraph":
                    ax.plot(range(5), [r[key] for r in rows], marker="o", color="#46566b", label="Same input for both engines")
                    ax.legend(fontsize=9)
            else:
                ax.plot(range(5), [r[key] for r in rows], marker="o", color=colors[engine], label=names[engine])
            ax.set_xticks(range(5), ["15 min", "1 h", "6 h", "12 h", "24 h"])
            ax.set_xlabel("Snippet window")
            ax.set_title(title)
            ax.grid(alpha=.2)
    axes[-1].legend()
    fig.suptitle("Snippet-size sensitivity — equal source observations and verified outputs")
    fig.tight_layout()
    fig.savefig(out / "snippet_sensitivity.png", dpi=180, bbox_inches="tight")
    fig.savefig(out / "snippet_sensitivity.svg", bbox_inches="tight")
    plt.close(fig)
    images = ''.join(f'<img alt="{name.replace("_", " ")}" src="data:image/png;base64,{base64.b64encode((out / (name + ".png")).read_bytes()).decode()}">' for name in ["benchmark_overview", "snippet_sensitivity"])
    core_table = table(["Engine", "Representation", "Load median s", "Load SD s", "Both sensor queries s", "Stored directory MB", "Serialized MB", "Gzip MB"],
        [[r["engine"], LABELS[r["representation"]], f"{r['ingest_seconds_median']:.3f}", f"{r['ingest_seconds_sd']:.3f}",
          f"{r['full_both_sensors_sum_warm_seconds_median']:.3f}", f"{r['database_directory_MB_median']:.2f}", f"{r['serialized_MB']:.2f}",
          f"{r['gzip_MB']:.2f}" if r['gzip_MB'] is not None else "—"] for r in core])
    size_table = table(["Engine", "Snippet window", "Load median s", "Both sensor queries s", "Stored directory MB", "Serialized MB", "Gzip MB"],
        [[r["engine"], LABELS[r["representation"]], f"{r['ingest_seconds_median']:.3f}", f"{r['full_both_sensors_sum_warm_seconds_median']:.3f}",
          f"{r['database_directory_MB_median']:.2f}", f"{r['serialized_MB']:.2f}", f"{r['gzip_MB']:.2f}"] for r in summary if r["representation"].startswith("tss_")])
    findings = []
    for engine in ["oxigraph", "virtuoso"]:
        plain = by_key[(engine, "plain_sosa")]
        daily = by_key[(engine, "tss_ldes_1440m")]
        load_ratio = plain["ingest_seconds_median"] / daily["ingest_seconds_median"]
        fetch_ratio = plain["full_both_sensors_sum_warm_seconds_median"] / daily["full_both_sensors_sum_warm_seconds_median"]
        findings.append(f"{names[engine]}: daily TSS + LDES reduces median load time by {100 * (1 - 1/load_ratio):.1f}% and median summed full-history retrieval time by {100 * (1 - 1/fetch_ratio):.1f}% relative to plain SOSA in this setup. Loaded directory sizes are {daily['database_directory_MB_median']:.2f} MB versus {plain['database_directory_MB_median']:.2f} MB; reduced serialized size does not guarantee a smaller database directory.")
    plain = by_key[("oxigraph", "plain_sosa")]
    daily = by_key[("oxigraph", "tss_ldes_1440m")]
    findings.append(f"Daily snippets reduce uncompressed Turtle from {plain['serialized_MB']:.2f} to {daily['serialized_MB']:.2f} MB. Gzip narrows that difference to {plain['gzip_MB']:.3f} versus {daily['gzip_MB']:.3f} MB. The 15-minute representation has more triples and a larger exchange file than plain SOSA, so savings depend on the chosen window.")
    if ("postgresql", "normalized_relational") in by_key:
        normalized = by_key[("postgresql", "normalized_relational")]
        flat = by_key[("postgresql", "relational")]
        findings.append(f"PostgreSQL normalization reduces table-plus-index size from {flat['table_and_indexes_MB_median']:.2f} to {normalized['table_and_indexes_MB_median']:.2f} MB. Median summed full-history times are {flat['full_both_sensors_sum_warm_seconds_median']:.3f} s for flat rows and {normalized['full_both_sensors_sum_warm_seconds_median']:.3f} s for normalized rows, including deterministic IRI reconstruction. This is a strong relational baseline for interpreting semantic-model performance claims.")
    protocol = dict(environment["protocol"])
    if "normalized_relational_protocol" in environment:
        protocol["normalized relational control"] = environment["normalized_relational_protocol"]
    report = f'''<!doctype html><html lang="en"><meta charset="utf-8"><title>Dessel database benchmark results</title>
    <style>body{{font:16px/1.55 system-ui,sans-serif;color:#17262a;max-width:1250px;margin:40px auto;padding:0 24px}}h1,h2{{line-height:1.2}}table{{border-collapse:collapse;width:100%;margin:20px 0;font-size:14px}}th,td{{padding:10px;border:1px solid #d9dfe2;text-align:right}}th:first-child,td:first-child,th:nth-child(2),td:nth-child(2){{text-align:left}}th{{background:#eaf0f2}}img{{width:100%;height:auto;margin:20px 0}}code{{font-size:13px}}p{{max-width:1100px}}</style>
    <h1>Dessel database benchmark results</h1>
    <p>Measured on 25 November 2026 using PostgreSQL 19.0, Oxigraph 0.5.13 and Virtuoso 7.2.17. The input contains 350,120 finite readings from 7 November 2020 to 7 November 2025. These are new Linux measurements with a documented common core-data profile; they do not reproduce the original Windows setup.</p>
    <p><strong>{len(completed)} independent load trials; {result['verified_query_executions']} query executions verified against source-derived counts and SHA256 hashes.</strong> Every request returns the same five core fields. Invalid/missing source values are excluded consistently.</p>
    <h2>Measured findings</h2><ul>{''.join('<li>' + html.escape(x) + '</li>' for x in findings)}</ul>
    <h2>Core comparison</h2>{core_table}
    <p>Both-sensor retrieval is the sum of stage and discharge retrieval times within each pass, not a separately timed combined request. Individual workload timings and first-pass measurements are in query_summary.csv. Warm-query medians pool three passes across three independent databases.</p>
    {images}<h2>Snippet-size comparison</h2>{size_table}
    <h2>Measurement protocol and limits</h2>
    {''.join(f'<p><strong>{html.escape(k.title())}.</strong> {html.escape(v)}</p>' for k,v in protocol.items())}
    <p>Three independent loads per configuration estimate run variability. Nine warm measurements per workload reuse those three databases and are not nine independent database builds. Performance is specific to this machine, dataset, schema, configuration, and protocol; it does not establish general engine superiority or continuous-feed reliability.</p>
    <p>LDES fragments are materialized into one named graph for these storage/query experiments. TREE navigation is not used by the SPARQL query planner. Snippet construction times are recorded separately in the input manifest; publication delay is not measured by this database benchmark.</p>
    <p>Actual database-directory size includes baseline engine files and transaction logs. PostgreSQL table-plus-index size and incremental directory size are also reported in configuration_summary.csv. These must not be confused with serialized exchange-file size.</p>
    <p>The normalized PostgreSQL control stores series metadata once and reconstructs the deterministic observation IRI in SQL; the flat control stores every IRI in each row. Both return and verify the same five fields. No equal per-engine memory cap is asserted: four logical CPUs are shared as an affinity limit, and the actual engine-specific buffer settings are recorded in configurations/.</p>
    <h2>Files</h2><ul><li><a href="configuration_summary.csv">Configuration summary CSV</a></li><li><a href="query_summary.csv">Query summary CSV</a></li><li><a href="measurements.jsonl">Raw measurements JSONL</a></li><li><a href="environment.json">Versions configuration and protocol</a></li><li><a href="execution.log">Execution log</a></li></ul></html>'''
    (out / "report.html").write_text(report, encoding="utf-8")
    print(json.dumps({"completed_trials": len(completed), "verified_queries": result["verified_query_executions"], "report": str(out / "report.html")}, indent=2))


if __name__ == "__main__":
    main()
