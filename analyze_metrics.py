#!/usr/bin/env python3
"""
LoRaMESH Metrics Analyzer
Genera gráficas automáticas desde CSVs de simulación
"""

import sys
import os
import json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

def load_csv(prefix, suffix):
    """Load a CSV file with the given prefix and suffix"""
    filename = f"{prefix}_{suffix}.csv"
    if not os.path.exists(filename):
        print(f"Warning: {filename} not found")
        return None
    return pd.read_csv(filename)

def load_json(prefix):
    """Load JSON summary"""
    filename = f"{prefix}_summary.json"
    if not os.path.exists(filename):
        print(f"Warning: {filename} not found")
        return None
    with open(filename) as f:
        return json.load(f)

def plot_delay_histogram(df, output_dir, prefix):
    """Plot delay distribution"""
    if df is None or 'delay(s)' not in df.columns:
        return
    
    plt.figure(figsize=(10, 6))
    delays = df[df['delivered'] == 1]['delay(s)'] * 1000  # Convert to ms
    plt.hist(delays, bins=50, edgecolor='black', alpha=0.7)
    plt.xlabel('End-to-End Delay (ms)')
    plt.ylabel('Frequency')
    plt.title(f'Delay Distribution - {prefix}')
    plt.grid(True, alpha=0.3)
    
    # Add statistics
    stats_text = f'Mean: {delays.mean():.2f} ms\nStd: {delays.std():.2f} ms\nMax: {delays.max():.2f} ms'
    plt.text(0.95, 0.95, stats_text, transform=plt.gca().transAxes, 
             verticalalignment='top', horizontalalignment='right',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    plt.savefig(f"{output_dir}/{prefix}_delay_hist.png", dpi=150, bbox_inches='tight')
    plt.close()
    print(f"✓ Saved: {output_dir}/{prefix}_delay_hist.png")

def plot_energy_over_time(df_tx, output_dir, prefix):
    """Plot energy consumption over time per node"""
    if df_tx is None or 'energyFrac' not in df_tx.columns:
        return
    
    plt.figure(figsize=(12, 6))
    
    for node_id in df_tx['nodeId'].unique():
        node_data = df_tx[df_tx['nodeId'] == node_id].sort_values('timestamp(s)')
        if len(node_data) > 0:
            plt.plot(node_data['timestamp(s)'], node_data['energyFrac'], 
                    label=f'Node {node_id}', alpha=0.7)
    
    plt.xlabel('Time (s)')
    plt.ylabel('Energy Fraction Remaining')
    plt.title(f'Energy Consumption Over Time - {prefix}')
    plt.grid(True, alpha=0.3)
    plt.legend(loc='upper right', fontsize='small', ncol=2)
    plt.ylim(0, 1.05)
    
    plt.savefig(f"{output_dir}/{prefix}_energy_time.png", dpi=150, bbox_inches='tight')
    plt.close()
    print(f"✓ Saved: {output_dir}/{prefix}_energy_time.png")

def plot_route_events(df_routes, output_dir, prefix):
    """Plot route events over time"""
    if df_routes is None:
        return
    
    plt.figure(figsize=(12, 6))
    
    # Count events per time bucket
    df_routes['time_bucket'] = (df_routes['timestamp(s)'] // 10) * 10
    events_per_bucket = df_routes.groupby(['time_bucket', 'action']).size().unstack(fill_value=0)
    
    events_per_bucket.plot(kind='bar', stacked=True, ax=plt.gca(), width=0.8)
    
    plt.xlabel('Time (s)')
    plt.ylabel('Number of Route Events')
    plt.title(f'Route Events Over Time - {prefix}')
    plt.grid(True, alpha=0.3, axis='y')
    plt.legend(title='Action')
    
    plt.savefig(f"{output_dir}/{prefix}_route_events.png", dpi=150, bbox_inches='tight')
    plt.close()
    print(f"✓ Saved: {output_dir}/{prefix}_route_events.png")

def plot_sf_distribution(df_tx, output_dir, prefix):
    """Plot SF distribution"""
    if df_tx is None or 'sf' not in df_tx.columns:
        return
    
    plt.figure(figsize=(8, 6))
    
    sf_counts = df_tx['sf'].value_counts().sort_index()
    colors = plt.cm.viridis(np.linspace(0, 1, len(sf_counts)))
    
    plt.bar(sf_counts.index, sf_counts.values, color=colors)
    plt.xlabel('Spreading Factor (SF)')
    plt.ylabel('Number of Transmissions')
    plt.title(f'SF Distribution - {prefix}')
    plt.grid(True, alpha=0.3, axis='y')
    
    # Add percentage labels
    total = sf_counts.sum()
    for i, (sf, count) in enumerate(sf_counts.items()):
        plt.text(sf, count + total*0.01, f'{100*count/total:.1f}%', 
                ha='center', fontsize=9)
    
    plt.savefig(f"{output_dir}/{prefix}_sf_dist.png", dpi=150, bbox_inches='tight')
    plt.close()
    print(f"✓ Saved: {output_dir}/{prefix}_sf_dist.png")

def plot_pdr_timeline(df_delay, output_dir, prefix):
    """Plot PDR over time (moving average)"""
    if df_delay is None:
        return
    
    plt.figure(figsize=(12, 6))
    
    # Sort by timestamp and calculate moving PDR
    df = df_delay.sort_values('timestamp(s)').copy()
    window_size = max(10, len(df) // 50)
    df['delivered_ma'] = df['delivered'].rolling(window=window_size, min_periods=1).mean()
    
    plt.plot(df['timestamp(s)'], df['delivered_ma'], 'b-', linewidth=2)
    plt.axhline(y=0.9, color='g', linestyle='--', label='90% threshold', alpha=0.7)
    plt.axhline(y=df['delivered'].mean(), color='r', linestyle=':', 
                label=f'Overall PDR: {df["delivered"].mean():.2%}', alpha=0.7)
    
    plt.xlabel('Time (s)')
    plt.ylabel('PDR (Moving Average)')
    plt.title(f'PDR Over Time - {prefix}')
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.ylim(0, 1.05)
    
    plt.savefig(f"{output_dir}/{prefix}_pdr_time.png", dpi=150, bbox_inches='tight')
    plt.close()
    print(f"✓ Saved: {output_dir}/{prefix}_pdr_time.png")

def print_summary(json_data, prefix):
    """Print summary statistics"""
    if json_data is None:
        return
    
    print("\n" + "="*60)
    print(f"SUMMARY: {prefix}")
    print("="*60)
    
    pdr = json_data.get('pdr', {})
    delay = json_data.get('delay', {})
    energy = json_data.get('energy', {})
    overhead = json_data.get('overhead', {})
    thesis = json_data.get('thesis_metrics', {})
    
    print(f"\n📊 PDR:")
    print(f"   Total TX: {pdr.get('total_tx', 0)}")
    print(f"   Delivered: {pdr.get('delivered', 0)}")
    print(f"   PDR: {pdr.get('pdr', 0):.2%}")
    
    print(f"\n⏱️  Delay:")
    print(f"   Average: {delay.get('avg_s', 0)*1000:.2f} ms")
    print(f"   Min: {delay.get('min_s', 0)*1000:.2f} ms")
    print(f"   Max: {delay.get('max_s', 0)*1000:.2f} ms")
    
    print(f"\n🔋 Energy:")
    print(f"   Total Used: {energy.get('total_used_j', 0):.4f} J")
    print(f"   Min Remaining: {energy.get('min_remaining_frac', 0):.2%}")
    print(f"   Max Remaining: {energy.get('max_remaining_frac', 0):.2%}")
    
    print(f"\n📡 Overhead:")
    print(f"   Beacon Bytes: {overhead.get('beacon_bytes', 0)}")
    print(f"   Data Bytes: {overhead.get('data_bytes', 0)}")
    print(f"   Ratio: {overhead.get('ratio', 0):.2f}")
    
    if thesis.get('t50_s', -1) > 0 or thesis.get('fnd_s', -1) > 0:
        print(f"\n📈 Thesis Metrics:")
        print(f"   T50: {thesis.get('t50_s', -1):.2f} s")
        print(f"   FND: {thesis.get('fnd_s', -1):.2f} s")
    
    print("\n" + "="*60)

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 analyze_metrics.py <prefix> [output_dir]")
        print("Example: python3 analyze_metrics.py simulation_test ./plots")
        sys.exit(1)
    
    prefix = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "plots"
    
    # Create output directory
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    print(f"Loading metrics for: {prefix}")
    print(f"Output directory: {output_dir}")
    
    # Load data
    df_delay = load_csv(prefix, "delay")
    df_tx = load_csv(prefix, "tx")
    df_routes = load_csv(prefix, "routes")
    json_data = load_json(prefix)
    
    # Generate plots
    print("\nGenerating plots...")
    plot_delay_histogram(df_delay, output_dir, prefix)
    plot_energy_over_time(df_tx, output_dir, prefix)
    plot_route_events(df_routes, output_dir, prefix)
    plot_sf_distribution(df_tx, output_dir, prefix)
    plot_pdr_timeline(df_delay, output_dir, prefix)
    
    # Print summary
    print_summary(json_data, prefix)
    
    print(f"\n✅ Analysis complete! Plots saved to: {output_dir}/")

if __name__ == "__main__":
    main()
