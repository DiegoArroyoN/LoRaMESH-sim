#!/usr/bin/env python3
import argparse
import os

import pandas as pd


PROFILE_LABELS = {
    "pueyo2024": "Referencia (ToA-only, sin CSMA, sin duty-cycle)",
    "proposal_pueyo_like": "Propuesta (CSMA/CAD + duty-cycle 1\\% + costo compuesto)",
    "proposal_pueyo_like_observed": "Propuesta (variante SF observado)",
}

TOPO_LABELS = {
    "pueyo_grid": "Grid",
    "pueyo_random_equiv": "Aleatoria (area equivalente)",
}


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", required=True, help="Path a results_runs.csv")
    ap.add_argument("--out", required=True, help="Archivo .tex de salida")
    return ap.parse_args()


def ordered_unique_str(series: pd.Series) -> str:
    vals = []
    for item in series.tolist():
        s = str(item)
        if s not in vals:
            vals.append(s)
    return ", ".join(vals)


def ordered_unique_int_set(series: pd.Series) -> str:
    vals = sorted({int(v) for v in series.tolist()})
    return "{" + ",".join(str(v) for v in vals) + "}"


def main() -> None:
    args = parse_args()
    df = pd.read_csv(args.runs)

    required = {
        "profile",
        "topology",
        "n_nodes",
        "load",
        "seed",
        "tx_power_dbm",
        "preamble_symbols",
        "shadowing_sigma_db",
        "coding_rate",
    }
    missing = required.difference(df.columns)
    if missing:
        raise RuntimeError(f"Faltan columnas en results_runs.csv: {sorted(missing)}")

    group_cols = ["profile", "topology"]
    rows = []
    for (profile, topology), g in df.groupby(group_cols, sort=True):
        row = {
            "Perfil": PROFILE_LABELS.get(profile, profile),
            "Topologia": TOPO_LABELS.get(topology, topology),
            "N": ordered_unique_int_set(g["n_nodes"]),
            "Carga": ordered_unique_str(g["load"]),
            "Seeds": ordered_unique_int_set(g["seed"]),
            "Potencia TX (dBm)": str(int(g["tx_power_dbm"].iloc[0])),
            "Preambulo": str(int(g["preamble_symbols"].iloc[0])),
            "CR": str(g["coding_rate"].iloc[0]),
            "Sigma shadowing (dB)": str(g["shadowing_sigma_db"].iloc[0]),
        }
        rows.append(row)

    out_df = pd.DataFrame(rows).sort_values(["Perfil", "Topologia"])

    lines = []
    lines.append("% Auto-generated from results_runs.csv\n")
    lines.append("\\begin{table}[t]\n\\centering\n")
    lines.append("\\caption{Configuracion resumida de la mini-bateria de validacion funcional para Fig.~11--12.}\n")
    lines.append("\\label{tab:prelim_config}\n")
    lines.append("\\footnotesize\n")
    lines.append(out_df.to_latex(index=False, escape=False))
    lines.append("\\end{table}\n")

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w", encoding="utf-8") as f:
        f.write("".join(lines))


if __name__ == "__main__":
    main()
