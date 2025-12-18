#!/usr/bin/env python3
"""
Herramienta de depuración para correlacionar trazas FWDTRACE del routing
con la tabla DV exportada (routes_raw.csv). Útil para ver ruta planeada vs ruta seguida.

Uso:
  python3 analysis/correlate_fwd.py --log fwd.log --routes analysis/results/<label>/routes_raw.csv \
    --out analysis/results/<label>/fwdtrace_correlated.csv

Entradas:
  - Log con líneas que comienzan por "FWDTRACE ..." (insertadas en mesh_dv_app.cc).
    Cada línea usa pares clave=valor (src, dst, seq, nextHop, etc.).
  - routes_raw.csv exportado por el simulador (columnas típicas: time,nodeId,destId,nextHop,hops,cost).

Salida:
  - CSV con las trazas parseadas y, si hay rutas, columnas plannedNext/plannedHops y match_planned.
"""
import argparse
import os
import re
import sys
from typing import Dict, List, Tuple

import pandas as pd


def parse_log_line(line: str) -> Tuple[str, Dict[str, str]]:
  if "FWDTRACE" not in line:
    return "", {}
  parts = line.strip().split()
  if len(parts) < 2 or parts[0] != "FWDTRACE":
    return "", {}
  kind = parts[1]
  data: Dict[str, str] = {"kind": kind}
  kv_pattern = re.compile(r"(?P<k>[^=]+)=(?P<v>.+)")
  for token in parts[2:]:
    match = kv_pattern.match(token)
    if not match:
      continue
    key = match.group("k")
    val = match.group("v").rstrip(",")
    data[key] = val
  return kind, data


def parse_log(path: str) -> pd.DataFrame:
  rows: List[Dict[str, str]] = []
  with open(path, "r") as f:
    for line in f:
      kind, data = parse_log_line(line)
      if not kind:
        continue
      rows.append(data)
  df = pd.DataFrame(rows)
  # Normalizar columnas numéricas
  for col in ["time", "node", "src", "dst", "seq", "nextHop", "hopsPlanned", "hops", "ttl", "ttlAfter"]:
    if col in df:
      df[col] = pd.to_numeric(df[col], errors="coerce")
  return df


def load_routes(path: str) -> pd.DataFrame:
  routes = pd.read_csv(path)
  rename_map = {
      "nodeId": "node",
      "destId": "dst",
      "destination": "dst",
      "nextHop": "plannedNext",
      "hops": "plannedHops",
      "seq": "seq",
  }
  for old, new in rename_map.items():
    if old in routes.columns:
      routes = routes.rename(columns={old: new})
  # Asegurar columnas básicas
  if "node" not in routes.columns or "dst" not in routes.columns:
    raise ValueError("routes_raw.csv debe contener columnas nodeId/node y destId/dst")
  return routes


def correlate(logs: pd.DataFrame, routes: pd.DataFrame) -> pd.DataFrame:
  routes = routes.copy()
  # Preferimos correlacionar por (src,dst,seq,node) para no mezclar paquetes.
  merge_keys = [c for c in ["src", "dst", "seq", "node"] if c in logs.columns and c in routes.columns]
  if merge_keys:
    merged = logs.merge(routes, how="left", on=merge_keys, suffixes=("", "_route"))
  elif "time" in routes.columns:
    # merge_asof exige orden
    logs_sorted = logs.sort_values("time") if "time" in logs.columns else logs.copy()
    routes_sorted = routes.sort_values("time")
    merged = pd.merge_asof(
        logs_sorted,
        routes_sorted,
        on="time",
        by=[c for c in ["node", "dst"] if c in logs.columns and c in routes.columns],
        direction="backward",
    )
  else:
    merge_keys = [c for c in ["node", "dst"] if c in logs.columns and c in routes.columns]
    merged = logs.merge(routes, how="left", on=merge_keys)

  if "plannedNext" in merged.columns and "nextHop" in merged.columns:
    merged["match_planned"] = merged["nextHop"] == merged["plannedNext"]
  return merged


def main():
  parser = argparse.ArgumentParser(description="Correlaciona trazas FWDTRACE con rutas DV.")
  parser.add_argument("--log", required=True, help="Archivo de log con líneas FWDTRACE.")
  parser.add_argument("--routes", required=False, default=None, help="routes_raw.csv a usar.")
  parser.add_argument("--out", required=False, default="analysis/results/fwdtrace_correlated.csv",
                      help="Ruta de salida CSV.")
  args = parser.parse_args()

  logs = parse_log(args.log)
  if logs.empty:
    print("No se encontraron trazas FWDTRACE en el log", file=sys.stderr)
    sys.exit(1)

  correlated = logs
  if args.routes:
    routes = load_routes(args.routes)
    correlated = correlate(logs, routes)

  os.makedirs(os.path.dirname(args.out), exist_ok=True)
  correlated.to_csv(args.out, index=False)

  print(f"Trazas procesadas: {len(logs)}")
  if "kind" in logs.columns:
    print("Eventos por tipo:\n", logs["kind"].value_counts())
  if "match_planned" in correlated.columns:
    match_rate = correlated["match_planned"].mean()
    print(f"Coincidencia con tabla DV: {match_rate * 100:.2f}%")
  print(f"Resultado guardado en {args.out}")


if __name__ == "__main__":
  main()
