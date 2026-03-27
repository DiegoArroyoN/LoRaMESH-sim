#!/usr/bin/env python3
from __future__ import annotations
import argparse, csv, datetime as dt, json, statistics, subprocess, time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
NS3_DIR = BASE_DIR.parents[1]
NS3_BIN = NS3_DIR / 'ns3'
DEFAULT_STEM = 'pueyo_best_of_sf_range'
SPACING = 177
DATA_START = 300.0
DATA_STOP = 3900.0
STOP = 4500.0
PDR_END = 600.0
SFMAXES = [(7,12),(7,9),(8,9),(7,8)]
SIDES = [3,5]
TOPOLOGIES = ['pueyo_grid','pueyo_random_equiv']
SEEDS = [1,2,3]


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def run(cmd, cwd: Path, log: Path):
    ensure_dir(log.parent)
    with log.open('w', encoding='utf-8') as f:
        return subprocess.run(cmd, cwd=str(cwd), stdout=f, stderr=subprocess.STDOUT, text=True, check=False)


def git_commit() -> str:
    p = subprocess.run(['git','rev-parse','HEAD'], cwd=str(NS3_DIR), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return p.stdout.strip() if p.returncode == 0 else 'unknown'


def cli(args: dict[str, object]) -> str:
    return ' '.join(f'--{k}={v}' for k,v in args.items())


def mean_std_ci95(vals):
    if not vals:
        return 0.0, 0.0, 0.0, 0.0
    if len(vals) == 1:
        x=float(vals[0]); return x,0.0,x,x
    m=float(statistics.mean(vals))
    s=float(statistics.stdev(vals))
    h=1.96*(s/(len(vals)**0.5))
    return m,s,m-h,m+h


def load_json(path: Path):
    return json.loads(path.read_text(encoding='utf-8'))


common = {
    'profile': 'extended',
    'enableCsma': 'false',
    'enableDuty': 'false',
    'dutyLimit': 1.0,
    'routeMetricMode': 'toa_only',
    'sfLinkMode': 'deterministic_sensitivity',
    'sfLinkMarginDb': 0.0,
    'wireFormat': 'pueyo7b',
    'txPowerDbm': 20,
    'preambleSymbols': 16,
    'initTtl': 63,
    'useProbabilisticSfForBeacons': 'true',
    'interferenceModel': 'pueyo_fixed_capture',
    'shadowingSigmaDb': 3.57,
    'trafficMode': 'pueyo_all_to_all',
    'pueyoPacketsPerPair': 100,
    'beaconIntervalWarmSec': 60,
    'beaconIntervalStableSec': 60,
    'routeTimeoutFactor': 5,
    'routeSwitchMinDeltaX100': 0,
    'dvPayloadMaxBytes': 251,
    'routeAdvertPolicy': 'cost_weighted',
    'maxRoutesPerDestination': 2,
    'maxTotalRoutes': 1024,
    'costEncoding': 'cost255',
    'beaconLatestOnly': 'false',
    'prioritizeBeacons': 'false',
    'pueyoStrictQueueScheduler': 'true',
    'controlBackoffFactor': 1.0,
    'dataBackoffFactor': 10.0,
    'enablePcap': 'false',
    'verboseLogs': 'false',
    'dataPeriodJitterMaxSec': 0.0,
    'sfScanResetOnNewSignal': 'true',
    'sfScanEdThresholdDbm': -120.0,
    'dataPayloadSizeBytes': 20,
    'dataStartSec': DATA_START,
    'dataStopSec': DATA_STOP,
    'stopSec': STOP,
    'pdrEndWindowSec': PDR_END,
    'trafficLoad': 'low',
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--load', choices=['low', 'medium', 'high', 'saturation'], default='low')
    ap.add_argument('--out-stem', default=DEFAULT_STEM)
    args = ap.parse_args()

    outdir = BASE_DIR / 'validation_results' / f"{args.out_stem}_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}"
    ensure_dir(outdir)
    gith = git_commit()
    rows=[]
    total = len(SIDES)*len(TOPOLOGIES)*len(SFMAXES)*len(SEEDS)
    idx=0
    for side in SIDES:
        n = side*side
        area = (side-1)*SPACING
        for topo in TOPOLOGIES:
            for sfmin,sfmax in SFMAXES:
                for seed in SEEDS:
                    idx += 1
                    cfg = dict(common)
                    cfg.update({
                        'nodePlacementMode': topo,
                        'pueyoGridSide': side,
                        'pueyoGridSpacingM': SPACING,
                        'nEd': n,
                        'areaWidth': area,
                        'areaHeight': area,
                        'rngRun': seed,
                        'sfMin': sfmin,
                        'sfMax': sfmax,
                        'trafficLoad': args.load,
                    })
                    run_dir = outdir / topo / f'n{n}' / f'sf_{sfmin}_{sfmax}' / f'seed_{seed}'
                    sim_args = 'mesh_dv_baseline ' + cli(cfg)
                    cmd = [str(NS3_BIN), 'run', '--no-build', sim_args]
                    print(f'[{idx:03d}/{total}] {topo} n={n} sf={sfmin}-{sfmax} seed={seed}', flush=True)
                    t0=time.time()
                    proc = run(cmd, NS3_DIR, run_dir/'run.log')
                    if proc.returncode != 0:
                        raise SystemExit(f'run failed rc={proc.returncode} at {run_dir}')
                    summary_src = NS3_DIR/'mesh_dv_summary.json'
                    (run_dir/'mesh_dv_summary.json').write_bytes(summary_src.read_bytes())
                    (run_dir/'meta.json').write_text(json.dumps({
                        'git_commit': gith,
                        'sim_args': sim_args,
                        'elapsed_s': time.time()-t0,
                    }, indent=2), encoding='utf-8')
                    j = load_json(run_dir/'mesh_dv_summary.json')
                    row = {
                        'topology': topo,
                        'spacing_m': SPACING,
                        'load': args.load,
                        'n_nodes': n,
                        'profile_semantics': 'pueyo2024',
                        'runtime_profile': j['simulation']['profile'],
                        'sf_range': f'{sfmin}-{sfmax}',
                        'sf_min': sfmin,
                        'sf_max': sfmax,
                        'seed': seed,
                        'delivery_ratio': j['pdr']['delivery_ratio'],
                        'delay_avg_s': j['delay']['avg_s'],
                        'delay_p95_s': j['delay']['p95_s'],
                        'beacon_tx_sent': j['control_plane']['beacon_tx_sent'],
                        'beacon_rx_ok': j['control_plane']['beacon_rx_ok'],
                        'data_tx_sent': j['control_plane']['data_tx_sent'],
                        'delivered_count': j['pdr']['delivered'],
                        'routes_total': j['routes']['routes_total'],
                        'forward_tx_sent_total': j['forwarding']['forward_tx_sent_total'],
                        'forwarded_unique_count': j['forwarding']['forwarded_unique_count'],
                        'wire_format': j['simulation']['wire_format'],
                        'sf_scan_ed_threshold_dbm': j['simulation']['sf_scan_ed_threshold_dbm'],
                        'route_metric_mode': j['simulation']['route_metric_mode'],
                        'sf_link_mode': j['simulation']['sf_link_mode'],
                        'run_dir': str(run_dir),
                    }
                    rows.append(row)
                    if idx % 6 == 0 or idx == total:
                        with (outdir/'pueyo_best_of_sf_range_results_raw.csv').open('w', newline='', encoding='utf-8') as f:
                            w=csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
    # aggregate
    grouped = {}
    for r in rows:
        key=(r['topology'], r['spacing_m'], r['load'], r['n_nodes'], r['sf_range'], r['sf_min'], r['sf_max'])
        grouped.setdefault(key, []).append(r)
    agg=[]
    best=[]
    for key, rr in sorted(grouped.items()):
        topo,spacing,load,n,sfr,sfmin,sfmax = key
        pdrs=[float(x['delivery_ratio']) for x in rr]
        goodputs=[float(x['delivered_count'])*20*8/(DATA_STOP-DATA_START) for x in rr]
        davg=[float(x['delay_avg_s']) for x in rr]
        dp95=[float(x['delay_p95_s']) for x in rr]
        btx=[float(x['beacon_tx_sent']) for x in rr]
        brx=[float(x['beacon_rx_ok']) for x in rr]
        dtx=[float(x['data_tx_sent']) for x in rr]
        ddel=[float(x['delivered_count']) for x in rr]
        routes=[float(x['routes_total']) for x in rr]
        fwd=[float(x['forwarded_unique_count']) for x in rr]
        mu,sd,lo,hi=mean_std_ci95(pdrs)
        agg.append({
            'topology': topo,
            'spacing_m': spacing,
            'load': load,
            'n_nodes': n,
            'profile_semantics': 'pueyo2024',
            'sf_range': sfr,
            'sf_min': sfmin,
            'sf_max': sfmax,
            'seed_set': '1,2,3',
            'n': len(rr),
            'pdr_mean': mu,
            'pdr_std': sd,
            'pdr_ci95_lo': lo,
            'pdr_ci95_hi': hi,
            'delay_avg_s_mean': statistics.mean(davg),
            'delay_p95_s_mean': statistics.mean(dp95),
            'beacon_tx_sent_mean': statistics.mean(btx),
            'beacon_rx_ok_mean': statistics.mean(brx),
            'data_tx_sent_mean': statistics.mean(dtx),
            'delivered_count_mean': statistics.mean(ddel),
            'goodput_bps_mean': statistics.mean(goodputs),
            'routes_total_mean': statistics.mean(routes),
            'forwarded_unique_count_mean': statistics.mean(fwd),
            'wire_format': 'pueyo7b',
            'sf_scan_ed_threshold_dbm': -120.0,
        })
    bypoint={}
    for r in agg:
        key=(r['topology'], r['spacing_m'], r['load'], r['n_nodes'])
        bypoint.setdefault(key, []).append(r)
    for key, rr in sorted(bypoint.items()):
        chosen=max(rr, key=lambda x:(x['pdr_mean'], x['goodput_bps_mean'], -x['sf_min'], -x['sf_max']))
        best.append(chosen)
    with (outdir/'pueyo_best_of_sf_range_results.csv').open('w', newline='', encoding='utf-8') as f:
        w=csv.DictWriter(f, fieldnames=list(agg[0].keys())); w.writeheader(); w.writerows(agg)
    summary={
        'outdir': str(outdir),
        'git_commit': gith,
        'spacing_m_used': [SPACING],
        'profile_semantics': 'pueyo2024',
        'runtime_profile': 'extended_clone_for_sf_sweep',
        'why_clone_is_needed': 'strict pueyo2024 hard-locks sfMin/sfMax=7..12; this harness clones pueyo2024 semantics and varies only sfMin/sfMax',
        'load': args.load,
        'sf_ranges': [f'{a}-{b}' for a,b in SFMAXES],
        'n_runs': len(rows),
        'best_by_point': best,
    }
    with (outdir/'pueyo_best_of_sf_range_summary.json').open('w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2)
    print(outdir)

if __name__=='__main__':
    main()
