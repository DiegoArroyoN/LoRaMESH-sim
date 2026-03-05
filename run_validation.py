
import subprocess
import os
import re
import sys
from datetime import datetime

# Configuración
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
NS3_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, "..", ".."))
LOG_ROOT = os.path.join(SCRIPT_DIR, "validation_logs")
RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_DIR = os.path.join(LOG_ROOT, RUN_ID)
SIM_TIME = 800 # Aumentado a 800s para mitigar cold-start y obtener estadística robusta
TRIALS = 3 # Número de repeticiones (seeds)

scenarios = {
    "1_scalability": [
        {"nEd": 8, "period": 30},
        {"nEd": 16, "period": 30},
        {"nEd": 32, "period": 30},
        # {"nEd": 64, "period": 30} # Omitido para rapidez inicial
    ],
    "2_load": [
        {"nEd": 16, "period": 100},
        {"nEd": 16, "period": 30},
        {"nEd": 16, "period": 10},
        {"nEd": 16, "period": 5},
        {"nEd": 16, "period": 1}
    ]
}

def period_to_load(period_s):
    if period_s >= 60:
        return "low"
    if period_s >= 5:
        return "medium"
    if period_s >= 1:
        return "high"
    return "saturation"

def run_simulation(name, params, trial):
    load = period_to_load(params["period"])
    cmd = [
        "./ns3", "run",
        f"mesh_dv_baseline --nEd={params['nEd']} --stopSec={SIM_TIME} --trafficLoad={load} --rngRun={trial}"
    ]
    
    # Agregar parámetros específicos si existen
    if "extra_args" in params:
        cmd[2] += " " + params["extra_args"]

    log_file = os.path.join(LOG_DIR, f"{name}_n{params['nEd']}_p{params['period']}_t{trial}.log")
    
    print(f"Running {name} [N={params['nEd']}, P={params['period']}, T={trial}]...")
    
    with open(log_file, "w") as f:
        result = subprocess.run(cmd, cwd=NS3_DIR, stdout=f, stderr=subprocess.STDOUT)
        if result.returncode != 0:
            raise RuntimeError(f"Simulation failed (rc={result.returncode}): {' '.join(cmd)}")
        
    return log_file

def analyze_log(log_file):
    metrics = {
        "tx": 0, "rx": 0, "pdr": 0.0, "beacons": 0, 
        "energy": 0.0, "convergence": 0.0
    }
    
    with open(log_file, "r") as f:
        content = f.read()
        
        # PDR
        metrics["tx"] = len(re.findall(r"APP_SEND_DATA", content))
        metrics["rx"] = len(re.findall(r"DELIVERED", content))
        if metrics["tx"] > 0:
            metrics["pdr"] = (metrics["rx"] / metrics["tx"]) * 100
            
        # Beacons
        metrics["beacons"] = len(re.findall(r"BEACON_TX", content))
        
        # Energy directly from log summary if available
        # === ROUTING TABLE Node X ... Energy=0.123J ...
        energy_matches = re.findall(r"Energy=([\d\.]+)J", content)
        if energy_matches:
            total_energy = sum([float(e) for e in energy_matches])
            # Promedio aproximado ya que log imprime varias veces por nodo
            # Mejor estrategia: parsear la última aparición por nodo
            # Simplificación: tomar el promedio de todos los matches para tener una idea
            metrics["energy"] = total_energy / len(energy_matches) if len(energy_matches) > 0 else 0

    return metrics

def main():
    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(LOG_ROOT, exist_ok=True)

    # Keep only the newest 10 runs to bound disk usage.
    run_dirs = sorted(
        [d for d in os.listdir(LOG_ROOT) if os.path.isdir(os.path.join(LOG_ROOT, d))]
    )
    if len(run_dirs) > 10:
        for old in run_dirs[:-10]:
            old_path = os.path.join(LOG_ROOT, old)
            for root, dirs, files in os.walk(old_path, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
            os.rmdir(old_path)
        
    results = {}
    
    print(f"Starting validation run at {datetime.now()}")
    print(f"Log directory: {LOG_DIR}")
    print(f"Sim Time: {SIM_TIME}s, Trials: {TRIALS}")
    
    # 1. Ejecutar Escenarios
    for scen_name, variants in scenarios.items():
        results[scen_name] = []
        for v in variants:
            for t in range(1, TRIALS + 1):
                log = run_simulation(scen_name, v, t)
                m = analyze_log(log)
                m.update(v) # Add params to metrics
                results[scen_name].append(m)
                print(f"  -> PDR: {m['pdr']:.2f}%, RX: {m['rx']}/{m['tx']}")

    # 2. Generar Reporte
    report_file = os.path.join(SCRIPT_DIR, "validation_report.md")
    with open(report_file, "w") as f:
        f.write("# Validación Cuantitativa LoRaMESH\n\n")
        f.write(f"**Fecha**: {datetime.now()}\n")
        f.write(f"**Sim Time**: {SIM_TIME}s\n\n")
        
        for name, data in results.items():
            f.write(f"## Escenario: {name}\n\n")
            f.write("| Nodos | Periodo | PDR (%) | RX/TX |\n")
            f.write("|-------|---------|---------|-------|\n")
            for r in data:
                f.write(f"| {r['nEd']} | {r['period']}s | {r['pdr']:.2f}% | {r['rx']}/{r['tx']} |\n")
            f.write("\n")

    print(f"\nValidation finished. Report saved to {report_file}")

if __name__ == "__main__":
    main()
