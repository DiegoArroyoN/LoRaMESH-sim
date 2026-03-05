#!/usr/bin/env python3
import csv
import datetime as dt
import math
import re
import statistics
from collections import Counter, defaultdict
from pathlib import Path


THIS_DIR = Path(__file__).resolve().parent
DATE_STR = dt.datetime.now().strftime("%Y-%m-%d")

PAT_TX_DETAIL = re.compile(
    r"DATA_TX detail: node=(\d+) src=(\d+) dst=(\d+) seq=(\d+) time=([0-9eE+\-.]+)s nextHop=(\d+) sf=(\d+)"
)
PAT_DELIVER = re.compile(
    r"FWDTRACE deliver time=([0-9eE+\-.]+) node=(\d+) src=(\d+) dst=(\d+) seq=(\d+)"
)

RX_WINDOW_SEC = 15.0
BUSY_EXACT_SEC = 0.05
BUSY_OVERLAP_SEC = 1.50
BEACON_RECENCY_SEC = 60.0


def _safe_float(v):
    try:
        return float(v)
    except Exception:
        return math.nan


def _safe_int(v):
    try:
        return int(v)
    except Exception:
        return 0


def find_latest_campaign():
    root = THIS_DIR / "validation_results"
    candidates = sorted(root.glob("pdr_post_queue_*"))
    if not candidates:
        raise FileNotFoundError("No existe carpeta validation_results/pdr_post_queue_*")
    return candidates[-1]


def parse_tx_detail(log_text):
    # key=(src,dst,seq) -> {'next_hop', 'sf', 't_log'}
    out = {}
    for m in PAT_TX_DETAIL.finditer(log_text):
        tx_node = _safe_int(m.group(1))
        src = _safe_int(m.group(2))
        if tx_node != src:
            continue
        key = (src, _safe_int(m.group(3)), _safe_int(m.group(4)))
        rec = {
            "next_hop": _safe_int(m.group(6)),
            "sf": _safe_int(m.group(7)),
            "t_log": _safe_float(m.group(5)),
        }
        prev = out.get(key)
        if prev is None or rec["t_log"] < prev["t_log"]:
            out[key] = rec
    return out


def parse_delivered(log_text):
    delivered = set()
    for m in PAT_DELIVER.finditer(log_text):
        delivered.add((_safe_int(m.group(3)), _safe_int(m.group(4)), _safe_int(m.group(5))))
    return delivered


def parse_tx_csv(path):
    src_tx = {}  # key=(src,dst,seq) -> {'t','sf'}
    tx_by_node = defaultdict(list)  # node -> list[(t,sf,dst,seq)]
    with path.open() as f:
        r = csv.DictReader(f)
        for row in r:
            ok = _safe_int(row.get("ok", "0")) == 1
            if not ok:
                continue
            t = _safe_float(row.get("timestamp(s)", "nan"))
            node = _safe_int(row.get("nodeId", "0"))
            dst = _safe_int(row.get("dst", "0"))
            sf = _safe_int(row.get("sf", "0"))
            seq = _safe_int(row.get("seq", "0"))
            tx_by_node[node].append((t, sf, dst, seq))
            if dst == 65535:
                continue
            key = (node, dst, seq)
            prev = src_tx.get(key)
            if prev is None or t < prev["t"]:
                src_tx[key] = {"t": t, "sf": sf}
    for node, vals in tx_by_node.items():
        vals.sort(key=lambda x: x[0])
    return src_tx, tx_by_node


def parse_rx_csv(path):
    rx_by_key = defaultdict(list)  # key=(src,dst,seq) -> list[(node,t,sf)]
    beacon_rx_by_pair = defaultdict(list)  # (receiver,beacon_src) -> list[(t,sf)]
    with path.open() as f:
        r = csv.DictReader(f)
        for row in r:
            t = _safe_float(row.get("timestamp(s)", "nan"))
            node = _safe_int(row.get("nodeId", "0"))
            src = _safe_int(row.get("src", "0"))
            dst = _safe_int(row.get("dst", "0"))
            seq = _safe_int(row.get("seq", "0"))
            sf = _safe_int(row.get("sf", "0"))
            key = (src, dst, seq)
            rx_by_key[key].append((node, t, sf))
            if dst == 65535:
                beacon_rx_by_pair[(node, src)].append((t, sf))
    for k, vals in rx_by_key.items():
        vals.sort(key=lambda x: x[1])
    for k, vals in beacon_rx_by_pair.items():
        vals.sort(key=lambda x: x[0])
    return rx_by_key, beacon_rx_by_pair


def latest_beacon_before(beacon_series, t):
    # beacon_series sorted by time asc
    best = None
    for bt, bsf in beacon_series:
        if bt <= t:
            best = (bt, bsf)
        else:
            break
    return best


def count_tx_in_window(tx_list, t0, left, right):
    a = t0 - left
    b = t0 + right
    c = 0
    for tt, sf, dst, seq in tx_list:
        if a <= tt <= b:
            c += 1
    return c


def classify_cause(ev):
    if ev["nh_busy_overlap"] and ev["others_heard_count"] > 0:
        return "next_hop_busy_likely"
    if ev["no_recent_beacon_60"]:
        return "no_recent_beacon_link_stale"
    if ev["beacon_known"] and ev["sf_used"] < ev["last_beacon_sf"]:
        return "sf_more_aggressive_than_recent_beacon"
    if ev["others_heard_count"] == 0:
        return "channel_or_interference_before_first_hop"
    return "mixed_or_other"


def process_run(run_dir, scenario, profile, seed):
    run_log = run_dir / "run.log"
    tx_csv = run_dir / "mesh_dv_metrics_tx.csv"
    rx_csv = run_dir / "mesh_dv_metrics_rx.csv"
    if not (run_log.exists() and tx_csv.exists() and rx_csv.exists()):
        return [], None

    text = run_log.read_text(errors="ignore")
    tx_detail = parse_tx_detail(text)
    delivered = parse_delivered(text)
    src_tx, tx_by_node = parse_tx_csv(tx_csv)
    rx_by_key, beacon_rx_by_pair = parse_rx_csv(rx_csv)

    events = []
    by_sf_total = Counter()
    by_sf_fail = Counter()
    src_tx_total = 0

    for key, detail in tx_detail.items():
        if key not in src_tx:
            continue
        src, dst, seq = key
        src_tx_total += 1
        tx_t = src_tx[key]["t"]
        sf_used = detail["sf"]
        by_sf_total[sf_used] += 1

        if key in delivered:
            continue

        next_hop = detail["next_hop"]
        rx_hits = [
            (n, t, sf)
            for (n, t, sf) in rx_by_key.get(key, [])
            if tx_t <= t <= (tx_t + RX_WINDOW_SEC) and n == next_hop
        ]
        if rx_hits:
            continue

        # first_hop_no_rx
        by_sf_fail[sf_used] += 1
        others = [
            (n, t, sf)
            for (n, t, sf) in rx_by_key.get(key, [])
            if tx_t <= t <= (tx_t + RX_WINDOW_SEC) and n != next_hop
        ]

        bseries = beacon_rx_by_pair.get((src, next_hop), [])
        last_b = latest_beacon_before(bseries, tx_t)
        beacon_known = last_b is not None
        if beacon_known:
            last_bt, last_bsf = last_b
            beacon_age = tx_t - last_bt
        else:
            last_bt, last_bsf, beacon_age = math.nan, math.nan, math.nan

        nh_txs = tx_by_node.get(next_hop, [])
        nh_busy_exact_count = count_tx_in_window(nh_txs, tx_t, BUSY_EXACT_SEC, BUSY_EXACT_SEC)
        nh_busy_overlap_count = count_tx_in_window(nh_txs, tx_t, BUSY_OVERLAP_SEC, BUSY_EXACT_SEC)

        ev = {
            "scenario": scenario,
            "profile": profile,
            "seed": seed,
            "src": src,
            "dst": dst,
            "seq": seq,
            "tx_time_s": tx_t,
            "sf_used": sf_used,
            "next_hop": next_hop,
            "others_heard_count": len(others),
            "beacon_known": int(beacon_known),
            "last_beacon_time_s": last_bt if beacon_known else "",
            "last_beacon_sf": int(last_bsf) if beacon_known else "",
            "beacon_age_s": round(beacon_age, 6) if beacon_known else "",
            "recent_beacon_60": int(beacon_known and beacon_age <= BEACON_RECENCY_SEC),
            "no_recent_beacon_60": int((not beacon_known) or (beacon_age > BEACON_RECENCY_SEC)),
            "sf_minus_last_beacon_sf": (sf_used - int(last_bsf)) if beacon_known else "",
            "nh_busy_exact": int(nh_busy_exact_count > 0),
            "nh_busy_exact_count": nh_busy_exact_count,
            "nh_busy_overlap": int(nh_busy_overlap_count > 0),
            "nh_busy_overlap_count": nh_busy_overlap_count,
        }
        ev["cause_label"] = classify_cause(
            {
                "nh_busy_overlap": bool(ev["nh_busy_overlap"]),
                "others_heard_count": ev["others_heard_count"],
                "no_recent_beacon_60": bool(ev["no_recent_beacon_60"]),
                "beacon_known": bool(ev["beacon_known"]),
                "sf_used": ev["sf_used"],
                "last_beacon_sf": (int(last_bsf) if beacon_known else 0),
            }
        )
        events.append(ev)

    run_summary = {
        "scenario": scenario,
        "profile": profile,
        "seed": seed,
        "source_tx_total": src_tx_total,
        "first_hop_no_rx_total": len(events),
        "first_hop_no_rx_ratio": (len(events) / src_tx_total) if src_tx_total > 0 else 0.0,
    }
    for sf in range(7, 13):
        total = by_sf_total.get(sf, 0)
        fail = by_sf_fail.get(sf, 0)
        run_summary[f"sf{sf}_tx_total"] = total
        run_summary[f"sf{sf}_first_hop_no_rx"] = fail
        run_summary[f"sf{sf}_first_hop_no_rx_ratio"] = (fail / total) if total > 0 else 0.0
    return events, run_summary


def main():
    campaign = find_latest_campaign()
    outdir = THIS_DIR / "validation_results" / f"first_hop_no_rx_forensics_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    outdir.mkdir(parents=True, exist_ok=True)

    all_events = []
    run_rows = []
    for scenario_dir in sorted([d for d in campaign.iterdir() if d.is_dir()]):
        scenario = scenario_dir.name
        for profile_dir in sorted([d for d in scenario_dir.iterdir() if d.is_dir()]):
            profile = profile_dir.name
            for seed_dir in sorted([d for d in profile_dir.iterdir() if d.is_dir() and d.name.startswith("seed_")]):
                seed = int(seed_dir.name.split("_")[1])
                events, run_summary = process_run(seed_dir, scenario, profile, seed)
                if run_summary is None:
                    continue
                all_events.extend(events)
                run_rows.append(run_summary)

    events_csv = outdir / "first_hop_no_rx_events.csv"
    if all_events:
        with events_csv.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(all_events[0].keys()))
            w.writeheader()
            w.writerows(all_events)

    runs_csv = outdir / "first_hop_no_rx_runs.csv"
    if run_rows:
        with runs_csv.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(run_rows[0].keys()))
            w.writeheader()
            w.writerows(run_rows)

    # Aggregate by scenario/profile
    grp_events = defaultdict(list)
    grp_runs = defaultdict(list)
    for e in all_events:
        grp_events[(e["scenario"], e["profile"])].append(e)
    for r in run_rows:
        grp_runs[(r["scenario"], r["profile"])].append(r)

    summary_rows = []
    for key in sorted(grp_runs.keys()):
        scenario, profile = key
        rs = grp_runs[key]
        es = grp_events.get(key, [])
        row = {
            "scenario": scenario,
            "profile": profile,
            "runs": len(rs),
            "source_tx_total_mean": statistics.mean([x["source_tx_total"] for x in rs]),
            "first_hop_no_rx_total_mean": statistics.mean([x["first_hop_no_rx_total"] for x in rs]),
            "first_hop_no_rx_ratio_mean": statistics.mean([x["first_hop_no_rx_ratio"] for x in rs]),
            "recent_beacon_60_pct": 0.0,
            "no_recent_beacon_60_pct": 0.0,
            "sf_aggressive_vs_last_beacon_pct": 0.0,
            "nh_busy_exact_pct": 0.0,
            "nh_busy_overlap_pct": 0.0,
            "others_heard_pct": 0.0,
            "cause_next_hop_busy_likely_pct": 0.0,
            "cause_no_recent_beacon_link_stale_pct": 0.0,
            "cause_sf_more_aggressive_than_recent_beacon_pct": 0.0,
            "cause_channel_or_interference_before_first_hop_pct": 0.0,
            "cause_mixed_or_other_pct": 0.0,
        }
        n = len(es)
        if n > 0:
            row["recent_beacon_60_pct"] = sum(int(e["recent_beacon_60"]) for e in es) / n
            row["no_recent_beacon_60_pct"] = sum(int(e["no_recent_beacon_60"]) for e in es) / n
            row["nh_busy_exact_pct"] = sum(int(e["nh_busy_exact"]) for e in es) / n
            row["nh_busy_overlap_pct"] = sum(int(e["nh_busy_overlap"]) for e in es) / n
            row["others_heard_pct"] = sum(1 for e in es if e["others_heard_count"] > 0) / n
            known = [e for e in es if int(e["beacon_known"]) == 1 and e["last_beacon_sf"] != ""]
            if known:
                row["sf_aggressive_vs_last_beacon_pct"] = (
                    sum(1 for e in known if int(e["sf_used"]) < int(e["last_beacon_sf"])) / len(known)
                )
            labels = Counter(e["cause_label"] for e in es)
            row["cause_next_hop_busy_likely_pct"] = labels.get("next_hop_busy_likely", 0) / n
            row["cause_no_recent_beacon_link_stale_pct"] = labels.get("no_recent_beacon_link_stale", 0) / n
            row["cause_sf_more_aggressive_than_recent_beacon_pct"] = (
                labels.get("sf_more_aggressive_than_recent_beacon", 0) / n
            )
            row["cause_channel_or_interference_before_first_hop_pct"] = (
                labels.get("channel_or_interference_before_first_hop", 0) / n
            )
            row["cause_mixed_or_other_pct"] = labels.get("mixed_or_other", 0) / n
        summary_rows.append(row)

    summary_csv = outdir / "first_hop_no_rx_summary.csv"
    if summary_rows:
        with summary_csv.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(summary_rows[0].keys()))
            w.writeheader()
            w.writerows(summary_rows)

    # SF breakdown
    sf_rows = []
    for key in sorted(grp_runs.keys()):
        scenario, profile = key
        rs = grp_runs[key]
        row = {"scenario": scenario, "profile": profile}
        for sf in range(7, 13):
            tx_vals = [x[f"sf{sf}_tx_total"] for x in rs]
            fail_vals = [x[f"sf{sf}_first_hop_no_rx"] for x in rs]
            tx_m = statistics.mean(tx_vals) if tx_vals else 0.0
            fail_m = statistics.mean(fail_vals) if fail_vals else 0.0
            row[f"sf{sf}_tx_mean"] = tx_m
            row[f"sf{sf}_first_hop_no_rx_mean"] = fail_m
            row[f"sf{sf}_first_hop_no_rx_ratio_mean"] = (fail_m / tx_m) if tx_m > 0 else 0.0
        sf_rows.append(row)

    sf_csv = outdir / "first_hop_no_rx_by_sf.csv"
    if sf_rows:
        with sf_csv.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(sf_rows[0].keys()))
            w.writeheader()
            w.writerows(sf_rows)

    report = THIS_DIR / f"ANALISIS_FORENSE_FIRST_HOP_NO_RX_{DATE_STR}.md"
    with report.open("w") as f:
        f.write("# Forense Específico `first_hop_no_rx`\n\n")
        f.write(f"- Fecha: {dt.datetime.now().isoformat()}\n")
        f.write(f"- Campaña base analizada: `{campaign}`\n")
        f.write("- Objetivo: separar causas por SF usado, next-hop esperado y ventana temporal.\n")
        f.write(f"- Ventanas: RX={RX_WINDOW_SEC}s, busy_exact=±{BUSY_EXACT_SEC}s, busy_overlap=[t-{BUSY_OVERLAP_SEC}, t+{BUSY_EXACT_SEC}]s.\n\n")

        f.write("## Resumen por Perfil\n")
        f.write(
            "| Escenario | Perfil | first_hop_no_rx ratio | beacon reciente <=60s | sf agresivo vs beacon | nh busy overlap | packet escuchado por otros |\n"
        )
        f.write("|---|---|---:|---:|---:|---:|---:|\n")
        for r in summary_rows:
            f.write(
                f"| {r['scenario']} | {r['profile']} | {r['first_hop_no_rx_ratio_mean']:.4f} | "
                f"{r['recent_beacon_60_pct']:.3f} | {r['sf_aggressive_vs_last_beacon_pct']:.3f} | "
                f"{r['nh_busy_overlap_pct']:.3f} | {r['others_heard_pct']:.3f} |\n"
            )

        f.write("\n## Etiquetas de Causa (heurística)\n")
        f.write(
            "| Escenario | Perfil | next_hop_busy_likely | no_recent_beacon_link_stale | sf_more_aggressive_than_recent_beacon | channel_or_interference_before_first_hop | mixed_or_other |\n"
        )
        f.write("|---|---|---:|---:|---:|---:|---:|\n")
        for r in summary_rows:
            f.write(
                f"| {r['scenario']} | {r['profile']} | {r['cause_next_hop_busy_likely_pct']:.3f} | "
                f"{r['cause_no_recent_beacon_link_stale_pct']:.3f} | "
                f"{r['cause_sf_more_aggressive_than_recent_beacon_pct']:.3f} | "
                f"{r['cause_channel_or_interference_before_first_hop_pct']:.3f} | "
                f"{r['cause_mixed_or_other_pct']:.3f} |\n"
            )

        f.write("\n## Artefactos\n")
        f.write(f"- Eventos: `{events_csv}`\n")
        f.write(f"- Resumen corridas: `{runs_csv}`\n")
        f.write(f"- Resumen agregado: `{summary_csv}`\n")
        f.write(f"- Desglose por SF: `{sf_csv}`\n")
        f.write(f"- Carpeta forense: `{outdir}`\n")

    print(f"DONE outdir={outdir}")
    print(f"REPORT {report}")


if __name__ == "__main__":
    main()
