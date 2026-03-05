#!/usr/bin/env python3
import csv
import datetime as dt
import re
import statistics
from collections import Counter, defaultdict
from pathlib import Path


THIS_DIR = Path(__file__).resolve().parent
DATE_STR = dt.datetime.now().strftime("%Y-%m-%d")

PAT_APP = re.compile(r"APP_SEND_DATA src=(\d+) dst=(\d+) seq=(\d+) time=([0-9eE+\-.]+)")
PAT_NOROUTE = re.compile(
    r"FWDTRACE DATA_NOROUTE time=([0-9eE+\-.]+) node=(\d+) src=(\d+) dst=(\d+) seq=(\d+).*reason=([a-zA-Z0-9_]+)"
)
PAT_QUEUE = re.compile(
    r"FWDTRACE QUEUE_STATE time=([0-9eE+\-.]+) node=(\d+) src=(\d+) dst=(\d+) seq=(\d+) sf=(\d+) reason=([a-zA-Z0-9_]+) deferCount=(\d+) queueSize=(\d+)"
)
PAT_PENDING = re.compile(
    r"FWDTRACE ORIGIN_PENDING_END time=([0-9eE+\-.]+) node=(\d+) src=(\d+) dst=(\d+) seq=(\d+) sf=(\d+) reason=([a-zA-Z0-9_]+) deferCount=(\d+) queueSize=(\d+)"
)
PAT_TX_DETAIL = re.compile(
    r"DATA_TX detail: node=(\d+) src=(\d+) dst=(\d+) seq=(\d+) time=([0-9eE+\-.]+)s nextHop=(\d+) sf=(\d+)"
)


def _safe_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default


def _safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default


def latest_campaign():
    root = THIS_DIR / "validation_results"
    cands = sorted(root.glob("pdr_post_queue_*"))
    if not cands:
        raise FileNotFoundError("No hay campañas pdr_post_queue_*")
    return cands[-1]


def parse_log(log_path):
    text = log_path.read_text(errors="ignore")
    app = {}
    noroute = defaultdict(list)
    qstate = defaultdict(list)
    pend = {}
    tx_attempt = set()

    for m in PAT_APP.finditer(text):
        key = (_safe_int(m.group(1)), _safe_int(m.group(2)), _safe_int(m.group(3)))
        app[key] = {"t": _safe_float(m.group(4))}

    for m in PAT_NOROUTE.finditer(text):
        key = (_safe_int(m.group(3)), _safe_int(m.group(4)), _safe_int(m.group(5)))
        noroute[key].append(
            {
                "t": _safe_float(m.group(1)),
                "node": _safe_int(m.group(2)),
                "reason": m.group(6),
            }
        )

    for m in PAT_QUEUE.finditer(text):
        key = (_safe_int(m.group(3)), _safe_int(m.group(4)), _safe_int(m.group(5)))
        qstate[key].append(
            {
                "t": _safe_float(m.group(1)),
                "node": _safe_int(m.group(2)),
                "sf": _safe_int(m.group(6)),
                "reason": m.group(7),
                "deferCount": _safe_int(m.group(8)),
                "queueSize": _safe_int(m.group(9)),
            }
        )

    for m in PAT_PENDING.finditer(text):
        key = (_safe_int(m.group(3)), _safe_int(m.group(4)), _safe_int(m.group(5)))
        pend[key] = {
            "t": _safe_float(m.group(1)),
            "node": _safe_int(m.group(2)),
            "sf": _safe_int(m.group(6)),
            "reason": m.group(7),
            "deferCount": _safe_int(m.group(8)),
            "queueSize": _safe_int(m.group(9)),
        }

    for m in PAT_TX_DETAIL.finditer(text):
        node = _safe_int(m.group(1))
        src = _safe_int(m.group(2))
        if node != src:
            continue
        key = (src, _safe_int(m.group(3)), _safe_int(m.group(4)))
        tx_attempt.add(key)

    for vals in qstate.values():
        vals.sort(key=lambda x: x["t"])
    return app, noroute, qstate, pend, tx_attempt


def parse_source_tx_ok(tx_csv):
    ok_keys = set()
    with tx_csv.open() as f:
        r = csv.DictReader(f)
        for row in r:
            ok = _safe_int(row.get("ok", "0")) == 1
            if not ok:
                continue
            node = _safe_int(row.get("nodeId", "0"))
            dst = _safe_int(row.get("dst", "0"))
            seq = _safe_int(row.get("seq", "0"))
            if dst == 65535:
                continue
            key = (node, dst, seq)
            ok_keys.add(key)
    return ok_keys


def root_cause_for_key(key, noroute, qstate, pend, tx_attempt):
    # Prioridad: estado final de cola al cierre > razones explícitas no_route/no_mac > estado de cola
    if key in pend:
        pr = pend[key]["reason"]
        return f"pending_{pr}"
    reasons = [x["reason"] for x in noroute.get(key, [])]
    if "no_link_addr_for_unicast" in reasons or "no_mac_for_unicast" in reasons:
        return "no_link_addr_for_unicast"
    if "no_route" in reasons:
        return "no_route"
    if key in tx_attempt:
        return "tx_attempt_no_ok"
    qs = qstate.get(key, [])
    if qs:
        return f"queue_{qs[-1]['reason']}"
    return "unknown"


def process_run(run_dir, scenario, profile, seed):
    log = run_dir / "run.log"
    tx_csv = run_dir / "mesh_dv_metrics_tx.csv"
    if not (log.exists() and tx_csv.exists()):
        return None, [], []

    app, noroute, qstate, pend, tx_attempt = parse_log(log)
    src_ok = parse_source_tx_ok(tx_csv)
    app_keys = set(app.keys())
    no_tx_keys = sorted(app_keys - src_ok)

    rows = []
    per_node = Counter()
    cause_ctr = Counter()
    for key in no_tx_keys:
        src, dst, seq = key
        qs = qstate.get(key, [])
        last_q = qs[-1] if qs else {}
        first_q = qs[0] if qs else {}
        p = pend.get(key, {})
        rc = root_cause_for_key(key, noroute, qstate, pend, tx_attempt)
        cause_ctr[rc] += 1
        per_node[src] += 1
        rows.append(
            {
                "scenario": scenario,
                "profile": profile,
                "seed": seed,
                "src": src,
                "dst": dst,
                "seq": seq,
                "root_cause": rc,
                "has_pending_end": int(bool(p)),
                "pending_reason": p.get("reason", ""),
                "pending_deferCount": p.get("deferCount", ""),
                "pending_queueSize": p.get("queueSize", ""),
                "has_queue_state": int(bool(qs)),
                "q_first_reason": first_q.get("reason", ""),
                "q_last_reason": last_q.get("reason", ""),
                "q_last_deferCount": last_q.get("deferCount", ""),
                "q_state_samples": len(qs),
            }
        )

    summary = {
        "scenario": scenario,
        "profile": profile,
        "seed": seed,
        "generated": len(app_keys),
        "source_tx_ok": len(src_ok),
        "no_tx_origin": len(no_tx_keys),
        "no_tx_origin_ratio": (len(no_tx_keys) / len(app_keys)) if app_keys else 0.0,
    }
    for k, v in cause_ctr.items():
        summary[f"cause_{k}"] = v

    node_rows = []
    for node, cnt in sorted(per_node.items()):
        node_rows.append(
            {
                "scenario": scenario,
                "profile": profile,
                "seed": seed,
                "node": node,
                "no_tx_origin_count": cnt,
            }
        )

    return summary, rows, node_rows


def mean(v):
    return statistics.mean(v) if v else 0.0


def main():
    campaign = latest_campaign()
    outdir = THIS_DIR / "validation_results" / f"no_tx_origin_forensics_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    outdir.mkdir(parents=True, exist_ok=True)

    summary_rows = []
    packet_rows = []
    node_rows = []

    for sc_dir in sorted([d for d in campaign.iterdir() if d.is_dir()]):
        scenario = sc_dir.name
        for pf_dir in sorted([d for d in sc_dir.iterdir() if d.is_dir()]):
            profile = pf_dir.name
            for sd in sorted([d for d in pf_dir.iterdir() if d.is_dir() and d.name.startswith("seed_")]):
                seed = _safe_int(sd.name.split("_")[1], 0)
                s, p, n = process_run(sd, scenario, profile, seed)
                if s is None:
                    continue
                summary_rows.append(s)
                packet_rows.extend(p)
                node_rows.extend(n)

    # normalizar columnas de summary
    all_summary_keys = set()
    for r in summary_rows:
        all_summary_keys.update(r.keys())
    summary_fields = sorted(all_summary_keys)

    summary_csv = outdir / "no_tx_origin_summary_per_run.csv"
    with summary_csv.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=summary_fields)
        w.writeheader()
        for r in summary_rows:
            w.writerow(r)

    packet_csv = outdir / "no_tx_origin_packets.csv"
    if packet_rows:
        with packet_csv.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(packet_rows[0].keys()))
            w.writeheader()
            w.writerows(packet_rows)

    node_csv = outdir / "no_tx_origin_per_node.csv"
    if node_rows:
        with node_csv.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(node_rows[0].keys()))
            w.writeheader()
            w.writerows(node_rows)

    # agregado por escenario/perfil
    grp = defaultdict(list)
    for r in summary_rows:
        grp[(r["scenario"], r["profile"])].append(r)

    agg_rows = []
    for (scenario, profile), items in sorted(grp.items()):
        row = {
            "scenario": scenario,
            "profile": profile,
            "runs": len(items),
            "generated_mean": mean([x["generated"] for x in items]),
            "source_tx_ok_mean": mean([x["source_tx_ok"] for x in items]),
            "no_tx_origin_mean": mean([x["no_tx_origin"] for x in items]),
            "no_tx_origin_ratio_mean": mean([x["no_tx_origin_ratio"] for x in items]),
        }
        # causas dinámicas
        dyn_causes = set()
        for x in items:
            for k in x.keys():
                if k.startswith("cause_"):
                    dyn_causes.add(k)
        for ck in sorted(dyn_causes):
            row[f"{ck}_mean"] = mean([x.get(ck, 0) for x in items])
        agg_rows.append(row)

    agg_csv = outdir / "no_tx_origin_summary_agg.csv"
    if agg_rows:
        agg_fields = sorted({k for r in agg_rows for k in r.keys()})
        with agg_csv.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=agg_fields)
            w.writeheader()
            w.writerows(agg_rows)

    # top nodos (agregado por escenario/perfil/node)
    node_grp = defaultdict(list)
    for r in node_rows:
        node_grp[(r["scenario"], r["profile"], r["node"])].append(r["no_tx_origin_count"])
    node_agg = []
    for (scenario, profile, node), vals in sorted(node_grp.items()):
        node_agg.append(
            {
                "scenario": scenario,
                "profile": profile,
                "node": node,
                "no_tx_origin_mean": mean(vals),
            }
        )
    node_agg_csv = outdir / "no_tx_origin_per_node_agg.csv"
    if node_agg:
        with node_agg_csv.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(node_agg[0].keys()))
            w.writeheader()
            w.writerows(node_agg)

    report = THIS_DIR / f"ANALISIS_FORENSE_NO_TX_ORIGIN_{DATE_STR}.md"
    with report.open("w") as f:
        f.write("# Forense Dedicado `no_tx_origin` (QUEUE_STATE + ORIGIN_PENDING_END)\n\n")
        f.write(f"- Fecha: {dt.datetime.now().isoformat()}\n")
        f.write(f"- Campaña analizada: `{campaign}`\n")
        f.write("- Definición: `no_tx_origin = APP_SEND_DATA - TX origen ok==1`.\n")
        f.write("- Trazas usadas: `DATA_NOROUTE`, `QUEUE_STATE`, `ORIGIN_PENDING_END`.\n\n")

        f.write("## Resumen por Perfil\n")
        f.write("| Escenario | Perfil | Generated | TX origen ok | no_tx_origin | ratio |\n")
        f.write("|---|---|---:|---:|---:|---:|\n")
        for r in agg_rows:
            f.write(
                f"| {r['scenario']} | {r['profile']} | {r['generated_mean']:.2f} | "
                f"{r['source_tx_ok_mean']:.2f} | {r['no_tx_origin_mean']:.2f} | "
                f"{r['no_tx_origin_ratio_mean']:.3f} |\n"
            )

        f.write("\n## Causas Agregadas (medias)\n")
        cause_cols = sorted(
            {
                k
                for r in agg_rows
                for k in r.keys()
                if k.startswith("cause_") and k.endswith("_mean")
            }
        )
        if cause_cols:
            f.write("| Escenario | Perfil | " + " | ".join(c.replace("_mean", "") for c in cause_cols) + " |\n")
            f.write("|---|---|" + "|".join(["---:"] * len(cause_cols)) + "|\n")
            for r in agg_rows:
                f.write(
                    f"| {r['scenario']} | {r['profile']} | "
                    + " | ".join(f"{r.get(c, 0.0):.2f}" for c in cause_cols)
                    + " |\n"
                )

        f.write("\n## Top Nodos con no_tx_origin (promedio)\n")
        f.write("| Escenario | Perfil | Nodo | no_tx_origin_mean |\n")
        f.write("|---|---|---:|---:|\n")
        for r in sorted(node_agg, key=lambda x: (x["scenario"], x["profile"], -x["no_tx_origin_mean"], x["node"]))[:80]:
            f.write(
                f"| {r['scenario']} | {r['profile']} | {r['node']} | {r['no_tx_origin_mean']:.2f} |\n"
            )

        f.write("\n## Artefactos\n")
        f.write(f"- Summary run-level: `{summary_csv}`\n")
        f.write(f"- Summary agregada: `{agg_csv}`\n")
        f.write(f"- Paquetes no_tx_origin: `{packet_csv}`\n")
        f.write(f"- Nodo agregado: `{node_agg_csv}`\n")
        f.write(f"- Carpeta: `{outdir}`\n")

    print(f"DONE outdir={outdir}")
    print(f"REPORT {report}")


if __name__ == "__main__":
    main()
