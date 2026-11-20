"""Markdown tables from results/*.json, for the README."""
import json
import statistics
import sys
from pathlib import Path

ORDER = ["job", "skew", "joins", "small_files", "udf", "cache"]


def mb(n) -> str:
    return f"{n / 2**20:,.1f}"


def row(v: dict, exp: str) -> list[str]:
    m = v["metrics"]
    w = m["worst_stage"] or {}
    cells = [v["name"], f"{v['median_s']:.1f} ({v['min_s']:.1f} to {v['max_s']:.1f})", str(m["stages"]),
             mb(m["shuffle_write_bytes"]), mb(m["spill_disk_bytes"])]
    if exp == "skew":
        cells += [f"{w.get('task_ms_max', 0) / 1000:.2f} / {w.get('task_ms_median', 0) / 1000:.2f}",
                  v["observed"][-1]["join"] + (" (AQE split)" if v["observed"][-1]["skew_split_by_aqe"] else "")]
    elif exp == "joins":
        cells += [v["observed"][-1]["join"]]
    elif exp == "small_files":
        reads = [o["read_query_s"] for o in v["observed"]]
        cells += [f"{v['answer']['files']:,}", f"{v['answer']['avg_file_kb']:,.0f}", f"{statistics.median(reads):.1f}"]
    elif exp == "cache":
        o = (v.get("observed") or [{}])[-1] or {}
        cells += [mb(o.get("mem_bytes", 0)), mb(o.get("disk_bytes", 0))]
    return cells


HEADERS = {
    "skew": ["slowest task / median task (s)", "join"],
    "joins": ["join"],
    "small_files": ["files", "avg file KB", "read query s"],
    "cache": ["cached in memory MB", "cached on disk MB"],
}


def table(result: dict) -> str:
    exp = result["experiment"]
    head = ["variant", "median s (min to max)", "stages", "shuffle write MB", "spill MB"] + HEADERS.get(exp, [])
    lines = ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    lines += ["| " + " | ".join(row(v, exp)) + " |" for v in result["variants"]]
    return "\n".join(lines)


def main(folder: str = "results") -> None:
    files = {json.loads(p.read_text())["experiment"]: json.loads(p.read_text())
             for p in Path(folder).glob("*.json")}
    for name in ORDER:
        if name in files:
            r = files[name]
            print(f"### {name} ({r['year']}, {r['runs']} timed runs, {r['finished_at']})\n")
            print(table(r) + "\n")


if __name__ == "__main__":
    main(*sys.argv[1:])
