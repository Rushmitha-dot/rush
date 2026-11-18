"""Static HTML report from the marts, the page GitHub Pages would serve.

Charts are matplotlib SVG inlined into one file, so the page needs no
JavaScript and no server. Every chart has its numbers in a table nearby.
"""

import html
import io
from datetime import datetime
from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402

WEEKNIGHT = "#2a78d6"
WEEKEND = "#eb6834"
THIRD = "#1baf7a"
INK = "#0b0b0b"
INK_2 = "#52514e"
GRID = "#e4e3df"
HOUR_LABELS = ["10p", "11p", "12a", "1a", "2a"]
CLUSTER_COLOR = {"northeastern": WEEKNIGHT, "fenway": WEEKEND, "longwood": THIRD}

plt.rcParams.update({
    "font.family": "sans-serif", "font.size": 9, "axes.edgecolor": GRID, "axes.labelcolor": INK_2,
    "xtick.color": INK_2, "ytick.color": INK_2, "axes.spines.top": False, "axes.spines.right": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6, "axes.axisbelow": True,
    "svg.fonttype": "none",
})


def _svg(fig) -> str:
    buf = io.StringIO()
    fig.savefig(buf, format="svg", bbox_inches="tight", transparent=True)
    plt.close(fig)
    text = buf.getvalue()
    return text[text.index("<svg"):]


def _table(cols: list[str], rows: list[tuple]) -> str:
    head = "".join(f"<th>{html.escape(c)}</th>" for c in cols)
    body = "".join("<tr>" + "".join(f"<td>{html.escape(_fmt(v))}</td>" for v in r) + "</tr>" for r in rows)
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def _fmt(v) -> str:
    if isinstance(v, float):
        return f"{v:,.2f}" if abs(v) < 10 else f"{v:,.0f}"
    if isinstance(v, int):
        return f"{v:,}"
    return "" if v is None else str(v)


def nightly_chart(con) -> str:
    rows = con.execute("""
        select night, trips, trips_rolling_7_nights, period
        from marts.mart_nightly_demand where term = 'fall_2025' order by night""").fetchall()
    fig, ax = plt.subplots(figsize=(8.4, 2.8))
    ax.bar([r[0] for r in rows], [r[1] for r in rows], width=0.75,
           color=["#8a8984" if r[3] == "break" else "#b9d3f2" for r in rows], label="trips per night")
    ax.plot([r[0] for r in rows], [r[2] for r in rows], color=WEEKNIGHT, linewidth=2, label="rolling 7 nights")
    ax.set_ylabel("trips")
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax.legend(frameon=False, loc="upper right", ncols=2)
    ax.grid(axis="x", visible=False)
    return _svg(fig)


def hourly_chart(con) -> str:
    rows = con.execute("""
        select night_type, hour_order, sum(trips) / count(distinct night)
        from marts.mart_hourly_demand group by 1, 2 order by 1, 2""").fetchall()
    fig, ax = plt.subplots(figsize=(5.6, 2.6))
    width = 0.38
    for i, (nt, color) in enumerate((("weeknight", WEEKNIGHT), ("weekend", WEEKEND))):
        pts = [r for r in rows if r[0] == nt]
        ax.bar([r[1] + (i - 0.5) * width for r in pts], [r[2] for r in pts], width=width - 0.03, color=color,
               label=nt)
    ax.set_xticks(range(5), HOUR_LABELS)
    ax.set_ylabel("trips started, average night")
    ax.legend(frameon=False)
    ax.grid(axis="x", visible=False)
    return _svg(fig)


def duration_chart(con) -> str:
    rows = con.execute("""
        select hour_order, duration_bucket, sum(trips) from marts.mart_duration_buckets
        group by 1, 2 order by 1, 2""").fetchall()
    buckets = sorted({r[1] for r in rows})
    # one hue light to dark, longer trips darker
    shades = ["#cfe0f6", "#9fc2ee", "#6aa2e4", "#2a78d6", "#1d5aa6", "#123c70"]
    totals = {h: sum(r[2] for r in rows if r[0] == h) for h in range(5)}
    fig, ax = plt.subplots(figsize=(6.4, 2.6))
    bottom = [0.0] * 5
    for b, color in zip(buckets, shades, strict=False):
        vals = [100 * next((r[2] for r in rows if r[0] == h and r[1] == b), 0) / totals[h] for h in range(5)]
        ax.bar(range(5), vals, bottom=bottom, color=color, edgecolor="white", linewidth=1, label=f"{b} min",
               width=0.6)
        bottom = [x + y for x, y in zip(bottom, vals, strict=True)]
    ax.set_xticks(range(5), HOUR_LABELS)
    ax.set_ylabel("% of trips")
    ax.legend(frameon=False, ncols=6, loc="upper center", bbox_to_anchor=(0.5, 1.2), fontsize=7)
    ax.grid(axis="x", visible=False)
    return _svg(fig)


def speed_chart(con) -> str:
    rows = con.execute("""
        select hour_order, classic_bike_kmh, ebike_kmh, bus_straight_line_kmh
        from marts.mart_bike_vs_bus_speed where night_type = 'weeknight' order by hour_order""").fetchall()
    fig, ax = plt.subplots(figsize=(5.6, 2.6))
    lines = ((1, "classic bike", WEEKNIGHT), (2, "e-bike", WEEKEND), (3, "MBTA bus, scheduled", THIRD))
    for idx, label, color in lines:
        ax.plot([r[0] for r in rows], [r[idx] for r in rows], color=color, linewidth=2, marker="o", markersize=4,
                label=label)
    ax.set_xticks(range(5), HOUR_LABELS)
    ax.set_ylabel("km/h, straight line")
    ax.set_ylim(bottom=0)
    ax.legend(frameon=False, fontsize=8)
    return _svg(fig)


def station_map(con) -> str:
    rows = con.execute("""
        select station_name, lat, lon, sum(departures) as departures, any_value(cluster)
        from marts.mart_station_hotspots group by station_name, lat, lon order by departures desc""").fetchall()
    fig, ax = plt.subplots(figsize=(5.4, 4.4))
    top = rows[0][3]
    for cluster, color in CLUSTER_COLOR.items():
        pts = [r for r in rows if r[4] == cluster]
        ax.scatter([r[2] for r in pts], [r[1] for r in pts], s=[700 * r[3] / top + 8 for r in pts], color=color,
                   alpha=0.6, edgecolor="white", linewidth=1.5, label=cluster)
    for r in rows[:5]:
        ax.annotate(r[0], (r[2], r[1]), fontsize=7, color=INK_2, xytext=(6, 4), textcoords="offset points")
    ax.set_aspect(1 / 0.74)  # roughly equal distances at Boston's latitude
    ax.set_xticks([])
    ax.set_yticks([])
    ax.grid(False)
    for sp in ax.spines.values():
        sp.set_visible(False)
    ax.legend(frameon=False, loc="lower left", fontsize=8, markerscale=0.4)
    return _svg(fig)


def build(warehouse: Path, out_dir: Path) -> Path:
    con = duckdb.connect(str(warehouse), read_only=True)
    first, last, nights = con.execute(
        "select min(night), max(night), count(*) from marts.mart_nightly_demand").fetchone()
    t = con.execute("""
        select count(*),
               round(100.0 * count(*) filter (where member_casual = 'member') / count(*), 1),
               round(100.0 * count(*) filter (where rideable_type = 'electric_bike') / count(*), 1),
               round(median(duration_min), 1)
        from marts.fct_late_night_trips""").fetchone()
    per_night = dict(con.execute("""
        select night_type, round(avg(trips)) from marts.mart_nightly_demand
        where term = 'fall_2025' and period <> 'break' group by 1""").fetchall())
    peaks = con.execute("""
        select night_type, window_start, avg_trips_60 from marts.mart_peak_windows
        where is_peak order by night_type, demand_rank""").fetchall()
    hotspots = con.execute("""
        select station_name, cluster, sum(departures), sum(arrivals),
               round(sum(net_flow_per_night), 1), max(capacity)
        from marts.mart_station_hotspots group by 1, 2 order by 3 desc limit 10""").fetchall()
    gap = con.execute("""
        select station_name, cluster, round(weeknight_departures_per_night, 1), meters_to_nearest_night_stop,
               mbta_weeknight_departures_400m
        from marts.mart_transit_gap order by weeknight_departures_per_night desc nulls last limit 10""").fetchall()
    dq = con.execute("""
        select sum(trips), sum(quarantined), sum(trips_without_end_station), sum(trips_in_repeated_hour),
               count(*) filter (where status = 'loaded_with_warnings')
        from marts.mart_data_quality""").fetchone()
    charts = {"nightly": nightly_chart(con), "hourly": hourly_chart(con), "duration": duration_chart(con),
              "speed": speed_chart(con), "map": station_map(con)}
    con.close()

    page = TEMPLATE.format(
        first=first, last=last, nights=nights,
        generated=datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z"),
        trips=f"{t[0]:,}", member=t[1], ebike=t[2], duration=t[3],
        weeknight=f"{per_night.get('weeknight', 0):,.0f}", weekend=f"{per_night.get('weekend', 0):,.0f}",
        nightly=charts["nightly"], hourly=charts["hourly"], duration_chart=charts["duration"],
        speed=charts["speed"], map=charts["map"],
        peaks=_table(["night type", "60 min window from", "trips in window, average night"], peaks),
        hotspots=_table(["station", "cluster", "departures", "arrivals", "net flow per night", "docks"], hotspots),
        gap=_table(["station", "cluster", "weeknight departures per night", "m to nearest night stop",
                    "MBTA departures within 400 m per weeknight"], gap),
        dq=_table(["trips loaded", "quarantined", "no end station", "in repeated DST hour", "nights with warnings"],
                  [dq]),
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "index.html"
    out.write_text(page)
    return out


TEMPLATE = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Late Night Campus Mobility</title>
<style>
:root {{ --bg: #fcfcfb; --ink: #0b0b0b; --ink-2: #52514e; --rule: #e4e3df; }}
body {{ background: var(--bg); color: var(--ink); font: 15px/1.5 -apple-system, system-ui, sans-serif;
       max-width: 900px; margin: 0 auto; padding: 24px 16px 64px; }}
h1 {{ font-size: 24px; margin: 0 0 4px; }} h2 {{ font-size: 17px; margin: 36px 0 6px; }}
p {{ color: var(--ink-2); margin: 4px 0 12px; }}
.tiles {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 12px; margin: 16px 0; }}
.tile {{ border: 1px solid var(--rule); border-radius: 8px; padding: 10px 12px; }}
.tile b {{ display: block; font-size: 22px; }} .tile span {{ color: var(--ink-2); font-size: 13px; }}
svg {{ max-width: 100%; height: auto; }}
.scroll {{ overflow-x: auto; }}
table {{ border-collapse: collapse; font-size: 13px; margin: 8px 0; }}
th, td {{ text-align: left; padding: 4px 10px 4px 0; border-bottom: 1px solid var(--rule); white-space: nowrap; }}
th {{ color: var(--ink-2); font-weight: 600; }}
small {{ color: var(--ink-2); }}
</style></head><body>
<h1>Late-night trips around campus</h1>
<p>Bluebikes trips that start between 10 p.m. and 3 a.m. and start or end within 1 km of Northeastern,
Longwood or the Fenway. Nights {first} to {last} ({nights} nights). Generated {generated}.</p>
<div class="tiles">
<div class="tile"><b>{trips}</b><span>late-night trips</span></div>
<div class="tile"><b>{weeknight}</b><span>per weeknight</span></div>
<div class="tile"><b>{weekend}</b><span>per Friday or Saturday night</span></div>
<div class="tile"><b>{member}%</b><span>by members</span></div>
<div class="tile"><b>{ebike}%</b><span>on e-bikes</span></div>
<div class="tile"><b>{duration} min</b><span>median trip</span></div>
</div>
<h2>Trips per night, fall 2025</h2>
<p>Gray bars are Thanksgiving week. The line is the rolling 7 night average.</p>
{nightly}
<h2>When people ride</h2>
{hourly}
<h2>Peak 60 minute windows</h2>
<div class="scroll">{peaks}</div>
<h2>Trip length by start hour</h2>
{duration_chart}
<h2>Station hotspots</h2>
<p>Circle size is late-night departures. Net flow is arrivals minus departures per night.</p>
{map}
<div class="scroll">{hotspots}</div>
<h2>Where night transit is thin</h2>
<p>MBTA departures after 10 p.m. from stops within 400 m, from the scheduled GTFS feed.</p>
<div class="scroll">{gap}</div>
<h2>Bike speed next to the night bus</h2>
<p>Weeknights, trips inside the campus area. Bus speed is scheduled speed along the route divided by the
route's detour factor, so all three are straight-line speeds.</p>
{speed}
<h2>What the pipeline set aside</h2>
<div class="scroll">{dq}</div>
<p><small>Trip data from the Bluebikes public system data under the Bluebikes Data License Agreement. Transit data
from MBTA GTFS. Not affiliated with or endorsed by Bluebikes, Lyft or the MBTA.</small></p>
</body></html>
"""
